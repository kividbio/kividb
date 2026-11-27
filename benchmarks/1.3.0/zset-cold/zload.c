// Cold-cache sorted-set load: P processes, one connection each, pipelines of D.
// usage: zload <port> <case> <keys> <members> <count> <procs> <depth> <seed>
// cases: load zscore_hit zscore_miss zadd_zrem feed zrank zrange zrevrange zincrby
// Keys feed:home:<10000+k>; member j of key n is (1000+j)*10^11+n, score = id.
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <stdint.h>

static uint64_t rs;
static uint64_t rnd(void) { rs ^= rs << 13; rs ^= rs >> 7; rs ^= rs << 17; return rs; }

static char out[1 << 22];
static size_t outn;

static void arg(const char *s, size_t n) { outn += sprintf(out + outn, "$%zu\r\n", n); memcpy(out + outn, s, n); outn += n; out[outn++] = '\r'; out[outn++] = '\n'; }
static void args(int n) { outn += sprintf(out + outn, "*%d\r\n", n); }
static void sarg(const char *s) { arg(s, strlen(s)); }
static void uarg(unsigned long long v) { char b[32]; int n = sprintf(b, "%llu", v); arg(b, n); }
static void iarg(long long v) { char b[32]; int n = sprintf(b, "%lld", v); arg(b, n); }

static long keys, members;
static const char *cas;
static unsigned long long feedctr;

static unsigned long long id(long j, long n) { return (unsigned long long)(1000 + j) * 100000000000ULL + n; }

// One command into out; returns commands added.
static int gen(void) {
    char key[32];
    long k = rnd() % keys, n = 10000 + k;
    sprintf(key, "feed:home:%ld", n);
    if (!strcmp(cas, "zscore_hit")) { args(3); sarg("ZSCORE"); sarg(key); uarg(id(rnd() % members, n)); return 1; }
    if (!strcmp(cas, "zscore_miss")) { args(3); sarg("ZSCORE"); sarg(key); uarg(id(members + rnd() % members, n)); return 1; }
    if (!strcmp(cas, "zrank")) { args(3); sarg("ZRANK"); sarg(key); uarg(id(rnd() % members, n)); return 1; }
    if (!strcmp(cas, "zrange")) { args(4); sarg("ZRANGE"); sarg(key); sarg("0"); sarg("19"); return 1; }
    if (!strcmp(cas, "zrevrange")) { args(4); sarg("ZREVRANGE"); sarg(key); sarg("0"); sarg("19"); return 1; }
    if (!strcmp(cas, "zincrby")) {
        long long d = (long long)(rnd() % 2000001) - 1000000; d *= 100000LL;
        args(4); sarg("ZINCRBY"); sarg(key); iarg(d); uarg(id(rnd() % members, n)); return 1;
    }
    if (!strcmp(cas, "zadd_zrem")) {
        unsigned long long m = id(2 * members + rnd() % members, n);
        unsigned long long sc = id(rnd() % members, n) + 7;
        args(4); sarg("ZADD"); sarg(key); uarg(sc); uarg(m);
        args(3); sarg("ZREM"); sarg(key); uarg(m); return 2;
    }
    if (!strcmp(cas, "feed")) {
        unsigned long long v = 8000000000000000ULL + (feedctr++) * 100000ULL + n;
        args(4); sarg("ZADD"); sarg(key); uarg(v); uarg(v);
        args(4); sarg("ZREMRANGEBYRANK"); sarg(key); sarg("0"); sarg("0"); return 2;
    }
    fprintf(stderr, "bad case %s\n", cas); exit(2);
}

// Length of one complete reply at p (n bytes), or -1.
static long reply(const char *p, long n) {
    const char *e = memchr(p, '\n', n);
    if (!e) return -1;
    long head = e - p + 1;
    if (p[0] == '$') { long len = atol(p + 1); if (len < 0) return head; return n >= head + len + 2 ? head + len + 2 : -1; }
    if (p[0] == '*') {
        long cnt = atol(p + 1), at = head;
        for (long i = 0; i < cnt; i++) { long r = reply(p + at, n - at); if (r < 0) return -1; at += r; }
        return at;
    }
    return head;
}

static void worker(int port, long count, int depth) {
    int fd = socket(AF_INET, SOCK_STREAM, 0), one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    struct sockaddr_in a = {0}; a.sin_family = AF_INET; a.sin_port = htons(port); a.sin_addr.s_addr = htonl(0x7f000001);
    if (connect(fd, (void *)&a, sizeof a)) { perror("connect"); exit(1); }
    static char in[1 << 23]; long inn = 0, errors = 0;
    long sent = 0;
    while (sent < count) {
        outn = 0; long batch = 0;
        while (batch < depth && sent + batch < count) {
            if (!strcmp(cas, "load")) {
                // ZADD of 200 members in order.
                static long lk = -1, lj = 0;
                if (lk < 0 || lj >= members) { lk++; lj = 0; }
                long n = 10000 + lk, m = members - lj < 200 ? members - lj : 200;
                char key[32]; sprintf(key, "feed:home:%ld", n);
                args(2 + 2 * m); sarg("ZADD"); sarg(key);
                for (long j = lj; j < lj + m; j++) { uarg(id(j, n)); uarg(id(j, n)); }
                lj += m; batch++;
            } else batch += gen();
        }
        for (size_t w = 0; w < outn;) { ssize_t r = write(fd, out + w, outn - w); if (r <= 0) { perror("write"); exit(1); } w += r; }
        long got = 0;
        while (got < batch) {
            ssize_t r = read(fd, in + inn, sizeof in - inn);
            if (r <= 0) { perror("read"); exit(1); }
            inn += r;
            long at = 0, l;
            while (got < batch && at < inn && (l = reply(in + at, inn - at)) > 0) { if (in[at] == '-') { if (errors++ < 3) fprintf(stderr, "error: %.*s", (int)l, in + at); } at += l; got++; }
            memmove(in, in + at, inn - at); inn -= at;
        }
        sent += batch;
    }
    if (errors) { fprintf(stderr, "%ld error replies\n", errors); exit(3); }
    exit(0);
}

int main(int c, char **v) {
    if (c < 9) { fprintf(stderr, "usage\n"); return 2; }
    int port = atoi(v[1]); cas = v[2]; keys = atol(v[3]); members = atol(v[4]);
    long count = atol(v[5]); int procs = atoi(v[6]), depth = atoi(v[7]); long seed = atol(v[8]);
    if (!strcmp(cas, "load")) { procs = 1; count = keys * ((members + 199) / 200); }
    for (int p = 0; p < procs; p++) {
        if (fork() == 0) { rs = 0x9E3779B97F4A7C15ULL ^ (seed * 1000003ULL + p * 7919ULL + 1); feedctr = (unsigned long long)p * 1000000000ULL; worker(port, count / procs, depth); }
    }
    int st, bad = 0;
    while (wait(&st) > 0) if (!WIFEXITED(st) || WEXITSTATUS(st)) bad = 1;
    return bad;
}
