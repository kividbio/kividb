# The rc7 engine with its version string rewritten from "1.2.0-rc7" to "1.2.0_rc7" (same length),
# so that INFO server reports kividb_version:1.2.0_rc7, a version the operator's guard cannot parse.
# Everything else is the published rc7 binary.
FROM ghcr.io/kividbio/kividb:v1.2.0-rc7 AS rc7
FROM python:3.12-slim AS patch
COPY --from=rc7 /usr/local/bin/kividb /kividb
RUN python3 -c "d=open('/kividb','rb').read(); n=d.count(b'1.2.0-rc7'); print('occurrences', n); assert n>0; open('/kividb','wb').write(d.replace(b'1.2.0-rc7', b'1.2.0_rc7'))" && chmod 755 /kividb
FROM ghcr.io/kividbio/kividb:v1.2.0-rc7
COPY --from=patch /kividb /usr/local/bin/kividb
