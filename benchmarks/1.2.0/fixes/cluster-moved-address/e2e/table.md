| scenario | binary | run | addresses right everywhere (s after restart) | all 6 views healthy + cluster_state ok, for good (s) | FAIL-by-quorum events | failed / attempted SETs after stop | of which CLUSTERDOWN | last failed SET (s after stop) |
|---|---|---|---|---|---|---|---|---|
| two-masters | unpatched | 1 | never (> 180) | never (> 180) | 1 | 183 / 33355 | 96 | 190 |
| two-masters | unpatched | 2 | never (> 180) | never (> 180) | 1 | 204 / 33547 | 116 | 184 |
| two-masters | unpatched | 3 | never (> 180) | never (> 180) | 1 | 189 / 33437 | 102 | 190 |
| two-masters | 69c2bef2 | 1 | 10.5 | 11.5 | 1 | 166 / 4896 | 112 | 21 |
| two-masters | 69c2bef2 | 2 | 10.4 | 12.5 | 1 | 218 / 5116 | 165 | 22 |
| two-masters | 69c2bef2 | 3 | 10.5 | 11.5 | 1 | 172 / 5031 | 122 | 22 |
| two-masters | 69c2bef2 + follow-up | 1 | 2.0 | 2.0 | 0 | 57 / 3347 | 9 | 11 |
| two-masters | 69c2bef2 + follow-up | 2 | 2.0 | 2.0 | 0 | 55 / 3353 | 8 | 11 |
| two-masters | 69c2bef2 + follow-up | 3 | 2.0 | 2.0 | 0 | 91 / 3411 | 18 | 11 |
| two-masters-down3s | unpatched | 1 | never (> 180) | never (> 180) | 0 | 107 / 31146 | 0 | 183 |
| two-masters-down3s | 69c2bef2 | 1 | 10.5 | 11.5 | 0 | 22 / 4966 | 0 | 13 |
| two-masters-down3s | 69c2bef2 + follow-up | 1 | 2.0 | 2.0 | 0 | 17 / 3435 | 0 | 4 |
| master-plus-other-replica | unpatched | 1 | never (> 180) | never (> 180) | 3 | 32 / 36282 | 4 | 11 |
| master-plus-other-replica | 69c2bef2 | 1 | 10.5 | 11.5 | 3 | 34 / 6093 | 6 | 11 |
| master-plus-other-replica | 69c2bef2 + follow-up | 1 | 2.0 | 2.0 | 2 | 35 / 4133 | 6 | 12 |
| one-master | unpatched | 1 | 1.5 | 1.5 | 1 | 31 / 4181 | 4 | 11 |
| one-master | 69c2bef2 | 1 | 1.5 | 1.5 | 1 | 30 / 4184 | 4 | 7 |
| one-master | 69c2bef2 + follow-up | 1 | 1.5 | 1.5 | 1 | 32 / 4133 | 6 | 7 |
| rapid-move | unpatched | 1 | 1.4 | 1.4 | 1 | 34 / 5128 | 7 | 7 |
| rapid-move | 69c2bef2 | 1 | 1.4 | 1.4 | 1 | 34 / 5110 | 6 | 11 |
| rapid-move | 69c2bef2 + follow-up | 1 | 1.5 | 1.5 | 1 | 32 / 5122 | 6 | 7 |
| rapid-move-two-masters | unpatched | 1 | never (> 180) | never (> 180) | 0 | 153 / 31570 | 8 | 189 |
| rapid-move-two-masters | unpatched | 2 | never (> 180) | never (> 180) | 0 | 161 / 31527 | 8 | 194 |
| rapid-move-two-masters | 69c2bef2 | 1 | 10.5 | 11.5 | 0 | 85 / 5286 | 11 | 25 |
| rapid-move-two-masters | 69c2bef2 | 2 | 10.5 | 11.5 | 0 | 70 / 5579 | 8 | 20 |
| rapid-move-two-masters | 69c2bef2 + follow-up | 1 | 1.9 | 1.9 | 0 | 71 / 3759 | 8 | 16 |
| rapid-move-two-masters | 69c2bef2 + follow-up | 2 | 1.9 | 1.9 | 0 | 73 / 3647 | 7 | 17 |
