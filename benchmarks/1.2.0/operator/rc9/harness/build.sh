# build.sh <operator tree> <operator commit> <rc9 engine context> <rc8 engine context>: everything built with az acr build
# (nothing locally): the operator (manager, agent, GUI), both engines from `git archive` trees with the repo's Dockerfile
# (Cargo.toml, Cargo.lock, src/, LICENSE, NOTICE, Dockerfile), and the loader from this harness.
H="$(cd "$(dirname "$0")" && pwd)"; OP="$1"; C="$2"; E9="$3"; E8="$4"; ACR=$(cat "$D/acrname")
( cd "$E9" && az acr build -r $ACR -t kividb:1.2.0-rc9 --timeout 7200 --platform linux/amd64 . > "$D/b-engine-rc9.log" 2>&1 ) &
( cd "$E8" && az acr build -r $ACR -t kividb:1.2.0-rc8 --timeout 7200 --platform linux/amd64 . > "$D/b-engine-rc8.log" 2>&1 ) &
cd "$OP"
az acr build -r $ACR -t kividb-operator:0.7.0-$C -f Dockerfile . > "$D/b-manager.log" 2>&1 &
az acr build -r $ACR -t kividb-operator-agent:0.7.0-$C -f Dockerfile.agent . > "$D/b-agent.log" 2>&1 &
az acr build -r $ACR -t kividb-operator-gui:0.7.0-$C -f Dockerfile.gui . > "$D/b-gui.log" 2>&1 &
cd "$H/loader" && az acr build -r $ACR -t e2e-loader:6 . > "$D/b-loader.log" 2>&1 &
wait
az acr repository list -n $ACR -o tsv
for f in "$D"/b-*.log; do echo "$(basename $f): $(grep -E 'Run ID: .* (was successful|failed)' $f | tail -1) $(grep -m1 -oE 'digest: sha256:[0-9a-f]+' $f)"; done
echo BUILD_DONE
