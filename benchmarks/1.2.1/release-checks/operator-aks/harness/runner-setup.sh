# runner-setup.sh (in the runner pod): kubectl + helm, harness in /h, work dir /work
set -e
python3 - <<'P'
import urllib.request,tarfile,io,os
v=urllib.request.urlopen("https://dl.k8s.io/release/stable.txt").read().decode().strip()
urllib.request.urlretrieve(f"https://dl.k8s.io/release/{v}/bin/linux/amd64/kubectl","/usr/local/bin/kubectl")
t=tarfile.open(fileobj=io.BytesIO(urllib.request.urlopen("https://get.helm.sh/helm-v3.19.0-linux-amd64.tar.gz").read()))
open("/usr/local/bin/helm","wb").write(t.extractfile("linux-amd64/helm").read())
for f in ("kubectl","helm"): os.chmod("/usr/local/bin/"+f,0o755)
P
apt-get update -qq >/dev/null && apt-get install -y -qq procps gawk >/dev/null 2>&1 || true
mkdir -p /work/res /work/evidence; cp /h/show.py /work/
kubectl version 2>&1 | head -2; helm version --short
