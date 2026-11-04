#!/usr/bin/env bash
# attest_all.sh <tag> <dir-with-oras-pulled-files>
# Checks SHA256SUMS.txt, then the signed build provenance (GitHub attestations, Sigstore) of every file in
# the ORAS artifact and of the four multi-arch images: who signed it (tag, commit, workflow run).
# Needs gh (signed in; the API call is authenticated) and sha256sum. No Docker.
set -u
TAG=${1:?tag} D=${2:?dir}
cd "$D" || exit 2
echo "## sha256sum -c SHA256SUMS.txt"
sha256sum -c SHA256SUMS.txt
echo
echo "## gh attestation verify (subject, exit, ref, commit, run)"
show() { python -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: print('no-json'); sys.exit()
s=d[0]['verificationResult']['signature']['certificate']
print(s.get('sourceRepositoryRef'), s.get('sourceRepositoryDigest','')[:12], s.get('runInvocationURI','').split('/actions/')[-1])
"; }
for f in kividb*; do
  r=$(timeout 120 gh attestation verify "$f" --repo kividbio/kividb --format json 2>/dev/null); e=$?
  printf '%s\texit=%s\t%s\n' "$f" "$e" "$(echo "$r" | show)"
done
for v in "" -lua -tls -full; do
  ref=oci://ghcr.io/kividbio/kividb:$TAG$v
  r=$(timeout 120 gh attestation verify "$ref" --repo kividbio/kividb --format json 2>/dev/null); e=$?
  printf '%s\texit=%s\t%s\n' "$ref" "$e" "$(echo "$r" | show)"
done
