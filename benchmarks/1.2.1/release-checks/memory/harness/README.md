# Memory suite harness, 1.2.1 rerun

The harness of [`benchmarks/1.2.0/memory/harness/`](../../../../1.2.0/memory/harness/README.md), with the changes
listed in [../README.md](../README.md#reproduce): `setup-server.sh` installs release binaries instead of building
git refs; `eng.sh`, `warefeats.sh` and `analyze.py` use the labels `kividb-1.2.1` and `kividb-1.2.0`;
`create-vms.sh` and `create-vms-b.sh` work with an existing resource group in any region. Every other driver is
unchanged. `queue-a.txt` and `queue-b.txt` are the exact job lists that ran on pair A and pair B, in order;
`make-report.sh` and `compact.py` are the 1.2.0 suite's.
