# build.sh <operator checkout>: operator images from a kividb-operator checkout, plus the loader and the S3 stand-in, all with az acr build
H="$(cd "$(dirname "$0")" && pwd)"; OP="$1"; ACR=$(cat "$D/acrname"); C=$(git -C "$OP" rev-parse --short=7 HEAD)
cd "$OP"
az acr build -r $ACR -t kividb-operator:0.7.0-$C -f Dockerfile . -o none &
az acr build -r $ACR -t kividb-operator-agent:0.7.0-$C -f Dockerfile.agent . -o none &
az acr build -r $ACR -t kividb-operator-gui:0.7.0-$C -f Dockerfile.gui . -o none &
cd "$H/loader" && az acr build -r $ACR -t e2e-loader:6 . -o none &
cd "$H/fakes3" && az acr build -r $ACR -t fakes3:2 -f Dockerfile . -o none &
wait
