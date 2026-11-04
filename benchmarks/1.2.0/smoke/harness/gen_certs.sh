#!/usr/bin/env bash
# A throwaway CA, a server certificate for localhost/127.0.0.1 and a client certificate.
set -eu
D=${1:-$HOME/certs}
mkdir -p "$D"; cd "$D"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=smoke-ca" -keyout ca.key -out ca.crt 2>/dev/null
for who in server client; do
  openssl req -newkey rsa:2048 -nodes -subj "/CN=$who" -keyout $who.key -out $who.csr 2>/dev/null
  printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=%s\n' \
    "$([ $who = server ] && echo serverAuth || echo clientAuth)" > $who.ext
  openssl x509 -req -in $who.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 2 -extfile $who.ext -out $who.crt 2>/dev/null
done
chmod 644 ./*.key ./*.crt   # the image runs as UID 1000 and reads them from a bind mount
echo "certs in $D"
