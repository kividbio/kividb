/*
 * memcmp and bcmp whose cost depends only on the length compared, for
 * measure.sh (CMP=plain): preloaded into the server under callgrind.
 *
 * glibc's AVX2 memcmp takes a slower path when the two addresses, OR-ed,
 * are within 32 bytes of the end of a page. A command's name is compared
 * from a buffer on the stack (`execute_command`), and a three-letter one
 * (GET, SET, DEL) against a literal through a call to bcmp, several times a
 * command. So any change to the size of a frame above it, in a connection
 * loop for instance, can add or take away about 9 instructions a call, some
 * 40 a command, with nothing on the command's path changed (#867, #877).
 * With this, two builds of the same code count the same.
 */
#include <stddef.h>

__attribute__((noinline)) int memcmp(const void *a, const void *b, size_t n) {
    const unsigned char *p = a, *q = b;
    for (size_t i = 0; i < n; i++)
        if (p[i] != q[i])
            return p[i] < q[i] ? -1 : 1;
    return 0;
}

int bcmp(const void *a, const void *b, size_t n) {
    return memcmp(a, b, n);
}
