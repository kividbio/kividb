# Notes (AKS operator e2e, 1.2.1 35a413c0, operator master 473ae58)

- RG kividb-c3-aks (centralindia), ACR kividbc3aks111209 (Basic, private), AKS e2e 4 x Standard_D4s_v5. AKS created with `--attach-acr` (AcrPull for the kubelet identity on the ACR in the same RG, as in the earlier recipe; it goes away with the RG). API server authorized range: the laptop /32 only. Scenarios ran from an in-cluster runner pod (cluster-admin via in-cluster RBAC).
- Wall time 03:55-04:52 UTC for the scenarios (cluster up ~04:00); AKS nodes ~1 h. RG delete started ~04:53 UTC.
- Order: rep and shd tracks in parallel (04:09-04:28), then drain, bulk load, 2 reshard cycles, B7, GUI, backups (04:28-04:51).
- Harness glitch, not a product finding: `scen-reshard.sh` counts pods by the `app.kubernetes.io/instance=shd` label, which the completed backup Job's pod also carries; the second cycle's 3->4 waited until I deleted the completed job `shd-bk-044243` (its archive was already in S3 and verified). The cycle's verify is unaffected.
- `scen-roll.sh` runs a command and waits for every pod UID of the cluster to change; used for B3, B7, A4a/b/c, B-pw.
- Old-password checks right after A4c/B-pw still saw PONG on 2 of 3 / 2 of 6 pods (the kubelet's Secret volume refresh, as in 1.2.0); at the end of the run every pod refused the old password.
- Not run this time (not in the asked plan): A6/A7 SIGSTOP in replication mode, T1 TLS, the AOF-off sharded cluster (B2-off), operator 0.6.1 -> master upgrade.
- Nothing pushed, published or posted. Images lived only in the private ACR (deleted with the RG). Kubeconfig deleted locally.
