#!/usr/bin/env bash
# srv-build.sh: rc9 and rc8 built on the server one after the other with fetch-kividb.sh (as release.yml builds arm64).
cd ~/h
bash fetch-kividb.sh 4a47644e53b8cf261df10309d919f2c19ebadef0 rc9 > ~/fetch-rc9.log 2>&1
bash fetch-kividb.sh 082dab3d8929c180bccf993835e28992ea054fb5 rc8 > ~/fetch-rc8.log 2>&1
for b in rc9 rc8; do cat ~/bin/kividb-$b.source; ~/bin/kividb-$b --version; sha256sum ~/bin/kividb-$b | sed "s#$HOME/##"; done > ~/builds.txt
echo SRV-BUILD-DONE >> ~/builds.txt
