export D=/work A=kividbc3aks111209.azurecr.io
cd /h; source ./env.sh
cpcli
nohup bash /h/capture.sh > /work/capture.out 2>&1 < /dev/null &
nohup sh -c 'kubectl -n e2e exec cli -- sh /tmp/pingall.sh shd' > /work/pingall-shd.log 2>&1 < /dev/null &
sleep 5; q loader-rep /stats | head -c 300; echo; q loader-shd /stats | head -c 300; echo
