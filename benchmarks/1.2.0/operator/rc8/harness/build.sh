# build.sh <operator tree> <operator commit> <engine context>: everything built with az acr build (nothing locally):
#  - the operator (manager, agent, GUI) from a kividb-operator tree (git archive of the commit);
#  - the engine from a kividb tree at the rc8 commit, with the repo's Dockerfile (the context holds only what it copies:
#    Cargo.toml, Cargo.lock, src/, LICENSE, NOTICE, Dockerfile);
#  - the loader and the in-cluster S3 stand-in from this harness.
H="$(cd "$(dirname "$0")" && pwd)"; OP="$1"; C="$2"; ENGCTX="$3"; ACR=$(cat "$D/acrname")
[ -n "$ENGCTX" ] && ( cd "$ENGCTX" && az acr build -r $ACR -t kividb:1.2.0-rc8 --timeout 7200 --platform linux/amd64 . -o none > "$D/b-engine.log" 2>&1 ) &
cd "$OP"
az acr build -r $ACR -t kividb-operator:0.7.0-$C -f Dockerfile . -o none > "$D/b-manager.log" 2>&1 &
az acr build -r $ACR -t kividb-operator-agent:0.7.0-$C -f Dockerfile.agent . -o none > "$D/b-agent.log" 2>&1 &
az acr build -r $ACR -t kividb-operator-gui:0.7.0-$C -f Dockerfile.gui . -o none > "$D/b-gui.log" 2>&1 &
cd "$H/loader" && az acr build -r $ACR -t e2e-loader:6 . -o none > "$D/b-loader.log" 2>&1 &
cd "$H/fakes3" && az acr build -r $ACR -t fakes3:2 -f Dockerfile . -o none > "$D/b-fakes3.log" 2>&1 &
wait
az acr repository list -n $ACR -o tsv
echo BUILD_DONE
