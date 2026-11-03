# build.sh <operator checkout>: operator images (manager, agent, GUI) from a kividb-operator checkout,
# the loader, the S3 stand-in and the unparsable-version engine image, all with az acr build (nothing built locally)
H="$(cd "$(dirname "$0")" && pwd)"; OP="$1"; ACR=$(cat "$D/acrname"); C=$(git -C "$OP" rev-parse --short=7 HEAD)
cd "$OP"
az acr build -r $ACR -t kividb-operator:0.7.0-$C -f Dockerfile . -o none > "$D/b-manager.log" 2>&1 &
az acr build -r $ACR -t kividb-operator-agent:0.7.0-$C -f Dockerfile.agent . -o none > "$D/b-agent.log" 2>&1 &
az acr build -r $ACR -t kividb-operator-gui:0.7.0-$C -f Dockerfile.gui . -o none > "$D/b-gui.log" 2>&1 &
cd "$H/loader" && az acr build -r $ACR -t e2e-loader:6 . -o none > "$D/b-loader.log" 2>&1 &
cd "$H/fakes3" && az acr build -r $ACR -t fakes3:2 -f Dockerfile . -o none > "$D/b-fakes3.log" 2>&1 &
cd "$H/badver" && az acr build -r $ACR -t kividb:v1.2.0-rc7-badver . -o none > "$D/b-badver.log" 2>&1 &
wait
az acr repository list -n $ACR -o tsv
echo BUILD_DONE
