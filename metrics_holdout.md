## Hold-out calibration (anchor-fit, validate on remaining sizes)

- **Thresholds**: small (< 262144B), mid [262144B, 16777216B), big (>= 16777216B)
- **L anchors (targets)**: 4096, 16384, 65536
- **scale anchors (targets)**: 16777216, 67108864, 268435456

## Broadcast TP2

- **Fitted (holdout) scale**: `2.951930`
- **Fitted (holdout) L**: `83.0 us`
- **L anchors (target->chosen)**: 4096->4096, 16384->16384, 65536->65536
- **scale anchors (target->chosen)**: 16777216->16777216, 67108864->67108864, 268435456->268435456

- **scale fit**:
  - big-point ratios used: all sizes >=16MB
  - ratio min/max/iqr: `2.529` / `3.672` / `0.755`
  - scale-anchor ratios (t_real/t_sim):
    - `16777216:3.230`, `67108864:2.547`, `268435456:2.529`
  - scale-anchor relative std (time_us_std/time_us_mean):
    - `16777216:0.44`, `67108864:0.30`, `268435456:0.30`

- **Anchor sanity (|rel_err|)**:
  - mean: `0.073`, max: `0.169`
- **Validation set (is_anchor=false)**:
  - MAPE (all sizes): `0.156`
  - all mean/max: `0.156` / `0.289`
  - big (>=16MB) mean/max: `0.139` / `0.194`
  - mid ([256KB,16MB)) mean/max: `0.169` / `0.289`
  - small (<256KB) mean/max: `N/A` / `N/A`

## Broadcast TP3

- **Fitted (holdout) scale**: `0.990774`
- **Fitted (holdout) L**: `55.6 us`
- **L anchors (target->chosen)**: 4096->4096, 16384->16384, 65536->65536
- **scale anchors (target->chosen)**: 16777216->16777216, 67108864->67108864, 268435456->268435456

- **scale fit**:
  - big-point ratios used: all sizes >=16MB
  - ratio min/max/iqr: `0.987` / `1.012` / `0.008`
  - scale-anchor ratios (t_real/t_sim):
    - `16777216:1.012`, `67108864:0.988`, `268435456:0.987`
  - scale-anchor relative std (time_us_std/time_us_mean):
    - `16777216:0.30`, `67108864:0.30`, `268435456:0.30`

- **Anchor sanity (|rel_err|)**:
  - mean: `0.007`, max: `0.016`
- **Validation set (is_anchor=false)**:
  - MAPE (all sizes): `0.096`
  - all mean/max: `0.096` / `0.231`
  - big (>=16MB) mean/max: `0.004` / `0.006`
  - mid ([256KB,16MB)) mean/max: `0.165` / `0.231`
  - small (<256KB) mean/max: `N/A` / `N/A`

## P2P 0→1 (TP3)

- **Fitted (holdout) scale**: `2.843277`
- **Fitted (holdout) L**: `54.2 us`
- **L anchors (target->chosen)**: 4096->4096, 16384->16384, 65536->65536
- **scale anchors (target->chosen)**: 16777216->16777216, 67108864->67108864, 268435456->268435456

- **scale fit**:
  - big-point ratios used: all sizes >=16MB
  - ratio min/max/iqr: `2.542` / `4.332` / `0.567`
  - scale-anchor ratios (t_real/t_sim):
    - `16777216:2.620`, `67108864:2.542`, `268435456:3.192`
  - scale-anchor relative std (time_us_std/time_us_mean):
    - `16777216:0.38`, `67108864:0.36`, `268435456:0.23`

- **Anchor sanity (|rel_err|)**:
  - mean: `0.069`, max: `0.123`
- **Validation set (is_anchor=false)**:
  - MAPE (all sizes): `0.133`
  - all mean/max: `0.133` / `0.343`
  - big (>=16MB) mean/max: `0.170` / `0.343`
  - mid ([256KB,16MB)) mean/max: `0.105` / `0.171`
  - small (<256KB) mean/max: `N/A` / `N/A`

## P2P 1→2 (TP3)

- **Fitted (holdout) scale**: `1.919536`
- **Fitted (holdout) L**: `17.7 us`
- **L anchors (target->chosen)**: 4096->4096, 16384->16384, 65536->65536
- **scale anchors (target->chosen)**: 16777216->16777216, 67108864->67108864, 268435456->268435456

- **scale fit**:
  - big-point ratios used: all sizes >=16MB
  - ratio min/max/iqr: `1.912` / `1.963` / `0.016`
  - scale-anchor ratios (t_real/t_sim):
    - `16777216:1.963`, `67108864:1.918`, `268435456:1.912`
  - scale-anchor relative std (time_us_std/time_us_mean):
    - `16777216:0.02`, `67108864:0.01`, `268435456:0.00`

- **Anchor sanity (|rel_err|)**:
  - mean: `0.066`, max: `0.188`
- **Validation set (is_anchor=false)**:
  - MAPE (all sizes): `0.107`
  - all mean/max: `0.107` / `0.240`
  - big (>=16MB) mean/max: `0.003` / `0.004`
  - mid ([256KB,16MB)) mean/max: `0.185` / `0.240`
  - small (<256KB) mean/max: `N/A` / `N/A`

## Completion definition evidence (spot-check)

Below, `collective_events.end_time_s` equals `max(flow_events.end_time_s)` (and start times match), consistent with the strict completion definition.

- evidence from `logs/bcast_tp3/size_16777216`:
  - `flow_events.csv` (head):
    - `flow_id,collective_id,collective_type,size_bytes,start_time_s,end_time_s`
    - `0,0,Broadcast,16777216,0.0,0.0026843545599985574`
    - `1,0,Broadcast,16777216,0.0,0.0026843955199985572`
  - `collective_events.csv` (head):
    - `collective_id,collective_type,size_bytes,start_time_s,end_time_s`
    - `0,Broadcast,16777216,0.0,0.0026843955199985572`
- evidence from `logs/bcast_tp2/size_16777216`:
  - `flow_events.csv` (head):
    - `flow_id,collective_id,collective_type,size_bytes,start_time_s,end_time_s`
    - `0,0,Broadcast,16777216,0.0,0.0013422182400009066`
  - `collective_events.csv` (head):
    - `collective_id,collective_type,size_bytes,start_time_s,end_time_s`
    - `0,Broadcast,16777216,0.0,0.0013422182400009066`

For pipeline P2P, we report message completion as the flow's end_time (last byte/packet at sink); we intentionally do not rely on collective_events typing.
- evidence from `logs/pp_01/size_16777216` (P2P message completion via flow end_time):
  - `flow_id=0, size_bytes=16777216, start_time_s=0.0, end_time_s=0.0013422182400009066`
- evidence from `logs/pp_12/size_16777216` (P2P message completion via flow end_time):
  - `flow_id=0, size_bytes=16777216, start_time_s=0.0, end_time_s=0.0013422182400009066`

## TP2 larger error: preliminary attribution

### Fitting contamination check (old vs holdout)
- **bcast_tp2** old(scale=2.9476, L=83.7us) vs holdout(scale=2.9519, L=83.0us): big-validation mean(|rel_err|) `0.139` -> `0.139`
- **pp_01** old(scale=2.8096, L=54.1us) vs holdout(scale=2.8433, L=54.2us): big-validation mean(|rel_err|) `0.172` -> `0.170`

### Bandwidth-region ratio consistency check
If big-point ratios vary significantly across sizes, a fixed-parameter bandwidth model will underfit some held-out big points.
- **bcast_tp2 big-point ratios** (t_real/t_sim): `16777216:3.230`, `33554432:3.366`, `67108864:2.547`, `134217728:3.672`, `268435456:2.529`, `536870912:2.665`
- **pp_01 big-point ratios** (t_real/t_sim): `16777216:2.620`, `33554432:2.562`, `67108864:2.542`, `134217728:2.999`, `268435456:3.192`, `536870912:4.332`

### TP2 vs TP3 noise evidence (Broadcast scale anchors)
- TP2 rel_std range on anchors: `0.30`..`0.44`
- TP3 rel_std range on anchors: `0.30`..`0.30`
- Interpretation: larger TP2 relative std and less stable big-point ratios make anchor-based calibration less stable, raising held-out big-point error under a single (scale, L) model.

- **Likely drivers**: (A) anchor-point noise in `time_us_mean` (large std) and/or (C) primitive-/path-specific effects; completion definition mismatch (B) is unlikely given the spot-check above.
- **Evidence**: TP2 `time_us_std` is large relative to `time_us_mean` on several big points (see `real_bcast_tp2_time.csv`), so a 3-point anchor median can shift noticeably; TP3 curves are much more self-consistent.

