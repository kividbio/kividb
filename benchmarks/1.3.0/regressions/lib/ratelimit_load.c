// Load for suite 24 (#894): the Redis traffic of envoyproxy/ratelimit replicas.
//
//   ratelimit_load <port> <fg|bg> <conns> <threads> <seconds> <rate> <group> <depth> <keys>
//
//  fg: closed loop on each connection; a "check" is INCRBY k1 1, EXPIRE k1 60,
//      INCRBY k2 1, EXPIRE k2 60 on two random keys. group=4 writes the four at
//      once (as the limiter's client pipelines them), group=1 one at a time.
//      rate: the limiter's own time per check, in microseconds, slept after
//      each reply (0 = none).
//  bg: each send is depth INCRBY + EXPIRE pairs in one write, paced so that
//      all connections together send `rate` commands a second (0 = no pacing).
// Warms up 3 s, measures <seconds>, and prints one line:
//   <fg|bg> ops/s <n> cmds/s <n> p50 <us> p99 <us> p999 <us> max <us>
// (an op is a check for fg, a send for bg; latencies are of an op).
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define HMAX 2000000 /* us */
static int port, conns, nthreads, seconds, group, depth, keys, fg;
static double rate;
static volatile int stop_all, measuring;

static uint64_t now_us(void) {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return (uint64_t)t.tv_sec * 1000000 + t.tv_nsec / 1000;
}

typedef struct {
  int fd;
  int pending;  /* replies outstanding */
  int step;     /* fg group=1: which command of 4 */
  uint64_t t0;  /* start of check/send */
  uint64_t next;/* bg: next send time */
  unsigned seed;
  char rbuf[65536];
  int rlen;
  long k1, k2;
} conn_t;

typedef struct {
  int id, n;
  conn_t *c;
  uint32_t *hist;
  uint64_t done, cmds;
} thr_t;

static int mkconn(void) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons(port)};
  inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
  if (connect(fd, (void *)&a, sizeof a)) { perror("connect"); exit(1); }
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
  return fd;
}

static int put_cmd(char *b, const char *cmd, long key, const char *arg) {
  char k[48];
  int kl = snprintf(k, sizeof k, "rl:%ld", key);
  int cl = strlen(cmd), al = strlen(arg);
  return sprintf(b, "*3\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", cl, cmd, kl, k, al, arg);
}

static void wr(int fd, const char *b, int n) {
  while (n > 0) {
    int w = write(fd, b, n);
    if (w < 0) { if (errno == EAGAIN || errno == EINTR) continue; perror("write"); exit(1); }
    b += w; n -= w;
  }
}

static void send_fg(conn_t *c) {
  char b[512]; int n = 0;
  if (c->step == 0) {
    c->k1 = rand_r(&c->seed) % keys; c->k2 = rand_r(&c->seed) % keys;
    c->t0 = now_us();
  }
  if (group == 4) {
    n += put_cmd(b + n, "INCRBY", c->k1, "1");
    n += put_cmd(b + n, "EXPIRE", c->k1, "60");
    n += put_cmd(b + n, "INCRBY", c->k2, "1");
    n += put_cmd(b + n, "EXPIRE", c->k2, "60");
    c->pending = 4;
  } else {
    long k = c->step < 2 ? c->k1 : c->k2;
    n += put_cmd(b, (c->step & 1) ? "EXPIRE" : "INCRBY", k, (c->step & 1) ? "60" : "1");
    c->pending = 1;
  }
  wr(c->fd, b, n);
}

static void send_bg(conn_t *c) {
  static __thread char b[65536];
  int n = 0;
  for (int i = 0; i < depth; i++) {
    long k = rand_r(&c->seed) % keys;
    n += put_cmd(b + n, "INCRBY", k, "1");
    n += put_cmd(b + n, "EXPIRE", k, "60");
  }
  c->t0 = now_us();
  c->pending = 2 * depth;
  wr(c->fd, b, n);
}

/* count complete RESP integer/simple/error replies in buffer */
static int eat_replies(conn_t *c) {
  int got = 0, i = 0;
  for (;;) {
    char *e = memchr(c->rbuf + i, '\n', c->rlen - i);
    if (!e) break;
    if (c->rbuf[i] == '-') { fprintf(stderr, "error reply: %.*s\n", (int)(e - c->rbuf - i), c->rbuf + i); }
    i = e - c->rbuf + 1; got++;
  }
  memmove(c->rbuf, c->rbuf + i, c->rlen - i);
  c->rlen -= i;
  return got;
}

static void *run(void *arg) {
  thr_t *t = arg;
  int ep = epoll_create1(0);
  double per_conn_interval = 0; /* us between sends per conn */
  if (!fg && rate > 0) per_conn_interval = 1e6 * conns * (2.0 * depth) / rate;
  uint64_t start = now_us();
  for (int i = 0; i < t->n; i++) {
    conn_t *c = &t->c[i];
    struct epoll_event ev = {.events = EPOLLIN, .data.ptr = c};
    epoll_ctl(ep, EPOLL_CTL_ADD, c->fd, &ev);
    if (fg) send_fg(c);
    else c->next = start + (uint64_t)(per_conn_interval * (i * nthreads + t->id) / conns);
  }
  struct epoll_event evs[256];
  while (!stop_all) {
    int k = epoll_wait(ep, evs, 256, (fg && rate == 0) ? 100 : 0);
    for (int j = 0; j < k; j++) {
      conn_t *c = evs[j].data.ptr;
      int r = read(c->fd, c->rbuf + c->rlen, sizeof c->rbuf - c->rlen);
      if (r <= 0) { if (r < 0 && errno == EAGAIN) continue; fprintf(stderr, "read %d\n", r); exit(1); }
      c->rlen += r;
      c->pending -= eat_replies(c);
      if (c->pending > 0) continue;
      if (fg) {
        if (group == 1 && c->step < 3) { c->step++; send_fg(c); continue; }
        c->step = 0;
        uint64_t nw = now_us();
        if (measuring) {
          uint64_t l = nw - c->t0;
          t->hist[l < HMAX ? l : HMAX - 1]++;
          t->done++; t->cmds += 4;
        }
        if (rate > 0) { c->next = nw + (uint64_t)rate; c->pending = -1; } /* think time */
        else send_fg(c);
      } else {
        if (measuring) {
          uint64_t l = now_us() - c->t0;
          t->hist[l < HMAX ? l : HMAX - 1]++;
          t->done++; t->cmds += 2 * depth;
        }
        c->pending = -1; /* idle */
      }
    }
    if (fg && rate > 0) {
      uint64_t now = now_us();
      for (int i = 0; i < t->n; i++) {
        conn_t *c = &t->c[i];
        if (c->pending < 0 && c->next <= now) send_fg(c);
      }
    }
    if (!fg) {
      uint64_t now = now_us();
      for (int i = 0; i < t->n; i++) {
        conn_t *c = &t->c[i];
        if (c->pending > 0) continue;
        if (rate > 0) {
          if (c->next > now) continue;
          c->next += (uint64_t)per_conn_interval;
          if (c->next + 100000 < now) c->next = now; /* do not burst after a long stall */
        }
        send_bg(c);
      }
    }
  }
  return NULL;
}

static uint64_t pct(uint32_t *h, uint64_t total, double p) {
  uint64_t want = (uint64_t)(total * p), s = 0;
  for (uint64_t i = 0; i < HMAX; i++) { s += h[i]; if (s > want) return i; }
  return HMAX;
}

int main(int argc, char **argv) {
  if (argc < 10) { fprintf(stderr, "usage\n"); return 2; }
  port = atoi(argv[1]); fg = !strcmp(argv[2], "fg"); conns = atoi(argv[3]); nthreads = atoi(argv[4]);
  seconds = atoi(argv[5]); rate = atof(argv[6]); group = atoi(argv[7]); depth = atoi(argv[8]); keys = atoi(argv[9]);
  int warm = 3;
  thr_t *ts = calloc(nthreads, sizeof *ts);
  conn_t *all = calloc(conns, sizeof *all);
  for (int i = 0; i < conns; i++) { all[i].fd = mkconn(); all[i].seed = i * 7919 + 1; all[i].pending = -1; }
  pthread_t th[64];
  int per = conns / nthreads, off = 0;
  for (int i = 0; i < nthreads; i++) {
    ts[i].id = i; ts[i].n = per + (i < conns % nthreads); ts[i].c = all + off; off += ts[i].n;
    ts[i].hist = calloc(HMAX, sizeof(uint32_t));
    pthread_create(&th[i], NULL, run, &ts[i]);
  }
  sleep(warm);
  measuring = 1;
  uint64_t m0 = now_us();
  sleep(seconds);
  measuring = 0;
  double el = (now_us() - m0) / 1e6;
  stop_all = 1;
  for (int i = 0; i < nthreads; i++) pthread_join(th[i], NULL);
  uint32_t *h = calloc(HMAX, sizeof(uint32_t));
  uint64_t done = 0, cmds = 0;
  for (int i = 0; i < nthreads; i++) {
    for (int j = 0; j < HMAX; j++) h[j] += ts[i].hist[j];
    done += ts[i].done; cmds += ts[i].cmds;
  }
  printf("%s ops/s %.0f cmds/s %.0f p50 %lu p99 %lu p999 %lu max %lu\n", fg ? "fg" : "bg", done / el, cmds / el,
         pct(h, done, .5), pct(h, done, .99), pct(h, done, .999), pct(h, done, 1.0 - 1e-12));
  return 0;
}
