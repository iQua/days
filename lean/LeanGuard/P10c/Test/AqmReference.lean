import LeanGuard.P10c.AqmEventLog

/-! Test-only reference copy of the P10c AQM checker's list-scan continuity check, as shipped at
`26dc1d1` (P16 lane L1). The shipped checker keys each queue's most recent row in a hash map;
`p10c_aqm_diff` runs both and requires identical results. Nothing in a shipped checker imports
this module. Bodies are verbatim apart from the `Reference` suffix on the names. -/

namespace LeanGuard.P10c.AqmEventLog

open LeanGuard.Shared
open LeanGuard.P10c.Semantics

def checkContinuityReference (rows : List Row) : Except String Unit := do
  let rec go (previous : List Row) : List Row → Except String Unit
    | [] => pure ()
    | row :: rest => do
        match previous.find? (sameQueue · row) with
        | none => pure ()
        | some prior =>
            require row.srcLine (sameConfig prior row)
              s!"AQM config does not continue the prior config for queue (node_id={row.nodeId}, queue_id={row.queueId})"
            if row.policy = "red" then
              require row.srcLine
                (row.beforeAverageScaled = prior.afterAverageScaled &&
                  row.beforeCounter = prior.afterCounter)
                s!"RED before-state does not continue the prior state for queue (node_id={row.nodeId}, queue_id={row.queueId})"
        go (row :: previous.filter (fun prior => !sameQueue prior row)) rest
  go [] rows

def checkRowsReference (rows : List Row) : Except String Unit := do
  let rows ← canonicalize rows
  for row in rows do
    checkRow row
  checkContinuityReference rows

end LeanGuard.P10c.AqmEventLog
