/* yii2-redis Mutex cycles for suite 54 (#1049): each connection, on a lock
 * of its own, sends `SET <lock> <token> NX PX 30000` and then the release,
 * `EVAL <compare-and-delete> 1 <lock> <token>` with the script's text, one
 * command at a time, as yii\redis\Mutex does from a PHP worker.
 *
 *   mutex_load <port> <connections> <warmup s> <seconds>
 *
 * One thread a connection, blocking sockets. Prints one line of JSON: the
 * cycles measured, commands a second (two a cycle), the p50 and p99 of each
 * command in microseconds, and how many replies were not +OK / :1.
 */
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define BUCKETS 200001 /* 1 us each, the last one 200 ms and over */

static const char *SCRIPT =
    "if redis.call(\"GET\",KEYS[1])==ARGV[1] then return redis.call(\"DEL\",KEYS[1]) else return 0 end";

static int port;
static atomic_int measuring, stopping;

struct worker {
    pthread_t thread;
    int id;
    uint32_t *set_hist, *eval_hist;
    uint64_t cycles, bad;
};

static uint64_t now_ns(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000000u + (uint64_t)t.tv_nsec;
}

static void add(uint32_t *hist, uint64_t ns) {
    uint64_t us = ns / 1000;
    hist[us < BUCKETS ? us : BUCKETS - 1]++;
}

static void send_all(int fd, const char *buf, size_t len) {
    while (len > 0) {
        ssize_t n = send(fd, buf, len, 0);
        if (n <= 0) {
            perror("send");
            exit(1);
        }
        buf += n;
        len -= (size_t)n;
    }
}

/* One reply line (every reply here is one), into line; its length. */
struct reader {
    char buf[4096];
    size_t start, end;
};

static size_t read_line(int fd, struct reader *r, char *line, size_t cap) {
    for (;;) {
        for (size_t i = r->start; i + 1 < r->end; i++) {
            if (r->buf[i] == '\r' && r->buf[i + 1] == '\n') {
                size_t len = i - r->start;
                if (len >= cap) len = cap - 1;
                memcpy(line, r->buf + r->start, len);
                line[len] = 0;
                r->start = i + 2;
                return len;
            }
        }
        if (r->start > 0) {
            memmove(r->buf, r->buf + r->start, r->end - r->start);
            r->end -= r->start;
            r->start = 0;
        }
        ssize_t n = recv(fd, r->buf + r->end, sizeof r->buf - r->end, 0);
        if (n <= 0) {
            fprintf(stderr, "the server closed the connection\n");
            exit(1);
        }
        r->end += (size_t)n;
    }
}

static size_t bulk(char *out, size_t cap, int argc, const char **argv) {
    size_t at = (size_t)snprintf(out, cap, "*%d\r\n", argc);
    for (int i = 0; i < argc; i++)
        at += (size_t)snprintf(out + at, cap - at, "$%zu\r\n%s\r\n", strlen(argv[i]), argv[i]);
    return at;
}

static void *run(void *arg) {
    struct worker *w = arg;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0) {
        perror("connect");
        exit(1);
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    /* yii2: keyPrefix . md5(json_encode([class, name])): 32 hex digits, no hash tag. */
    char key[64], token[32], out[1024], line[256];
    snprintf(key, sizeof key, "a1b2c%016llx%016llx", (unsigned long long)w->id * 0x9E3779B97F4A7C15ull,
             (unsigned long long)w->id);
    struct reader r = {.start = 0, .end = 0};
    for (uint64_t n = 1; !atomic_load(&stopping); n++) {
        /* 20 characters, as Security::generateRandomString(20) makes. */
        snprintf(token, sizeof token, "t%02d%017llu", w->id % 100, (unsigned long long)n);
        const char *set[] = {"SET", key, token, "NX", "PX", "30000"};
        const char *eval[] = {"EVAL", SCRIPT, "1", key, token};
        uint64_t t0 = now_ns();
        send_all(fd, out, bulk(out, sizeof out, 6, set));
        read_line(fd, &r, line, sizeof line);
        uint64_t t1 = now_ns();
        int ok = strcmp(line, "+OK") == 0;
        send_all(fd, out, bulk(out, sizeof out, 5, eval));
        read_line(fd, &r, line, sizeof line);
        uint64_t t2 = now_ns();
        ok &= strcmp(line, ":1") == 0;
        if (atomic_load(&measuring)) {
            add(w->set_hist, t1 - t0);
            add(w->eval_hist, t2 - t1);
            w->cycles++;
            w->bad += !ok;
        }
    }
    close(fd);
    return NULL;
}

static long pct(struct worker *ws, int n, int eval, double p) {
    uint64_t total = 0;
    for (int i = 0; i < n; i++) total += ws[i].cycles;
    if (total == 0) return -1;
    uint64_t want = (uint64_t)(total * p), acc = 0;
    for (long b = 0; b < BUCKETS; b++) {
        for (int i = 0; i < n; i++) acc += (eval ? ws[i].eval_hist : ws[i].set_hist)[b];
        if (acc > want) return b;
    }
    return BUCKETS - 1;
}

int main(int argc, char **argv) {
    if (argc != 5) {
        fprintf(stderr, "usage: mutex_load <port> <connections> <warmup s> <seconds>\n");
        return 2;
    }
    port = atoi(argv[1]);
    int conns = atoi(argv[2]);
    double warmup = atof(argv[3]), seconds = atof(argv[4]);
    struct worker *ws = calloc((size_t)conns, sizeof *ws);
    for (int i = 0; i < conns; i++) {
        ws[i].id = i;
        ws[i].set_hist = calloc(BUCKETS, sizeof(uint32_t));
        ws[i].eval_hist = calloc(BUCKETS, sizeof(uint32_t));
        pthread_create(&ws[i].thread, NULL, run, &ws[i]);
    }
    usleep((useconds_t)(warmup * 1e6));
    atomic_store(&measuring, 1);
    uint64_t t0 = now_ns();
    usleep((useconds_t)(seconds * 1e6));
    atomic_store(&measuring, 0);
    double took = (double)(now_ns() - t0) / 1e9;
    atomic_store(&stopping, 1);
    uint64_t cycles = 0, bad = 0;
    for (int i = 0; i < conns; i++) {
        pthread_join(ws[i].thread, NULL);
        cycles += ws[i].cycles;
        bad += ws[i].bad;
    }
    printf("{\"cycles\": %llu, \"ops\": %.1f, \"set_p50\": %ld, \"set_p99\": %ld, \"eval_p50\": %ld, "
           "\"eval_p99\": %ld, \"bad\": %llu, \"seconds\": %.3f}\n",
           (unsigned long long)cycles, 2.0 * (double)cycles / took, pct(ws, conns, 0, 0.5), pct(ws, conns, 0, 0.99),
           pct(ws, conns, 1, 0.5), pct(ws, conns, 1, 0.99), (unsigned long long)bad, took);
    return 0;
}
