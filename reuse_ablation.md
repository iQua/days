## Reuse ablation (own-calib vs reuse allreduce-calib on validation set)

Validation set is defined as `is_anchor=false` under the holdout anchor split.

| workload | own MAPE | reuse MAPE | own big mean | reuse big mean | own big max | reuse big max |
|---|---:|---:|---:|---:|---:|---:|
| bcast_tp2 | 0.156 | 0.323 | 0.139 | 0.267 | 0.194 | 0.368 |
| bcast_tp3 | 0.096 | 0.175 | 0.004 | 0.017 | 0.006 | 0.026 |
| pp_01 | 0.133 | 0.560 | 0.170 | 0.675 | 0.343 | 0.769 |
| pp_12 | 0.107 | 0.480 | 0.003 | 0.469 | 0.004 | 0.477 |
