import LeanGuard.P10c.AqmEventLog

/-! Test-only reference for the P10c AQM checker. `p10c_aqm_diff` runs both and requires identical
results. Nothing in a shipped checker imports this module.
- The list-scan continuity check, as shipped at `26dc1d1` (P16 lane L1); the shipped checker keys
  each queue's most recent row in a hash map. Bodies are verbatim apart from the `Reference` suffix.
- P16 aqmkind: an independent data-only marking rule (its own kind table and exemption), and an
  independent per-packet kind check (rows grouped by sorting on payload, not a hash map). -/

namespace LeanGuard.P10c.AqmEventLog

open LeanGuard.Shared
open LeanGuard.P10c.Semantics

def sameQueue (first second : Row) : Bool :=
  first.nodeId = second.nodeId && first.queueId = second.queueId

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

/-- The executor's data kinds (`PacketKind::is_data`), written out independently of `dataKind`. -/
def dataKindsReference : List String := ["data", "tcp_data", "roce_data"]

/-- A mark on a packet whose kind is not a data kind admits it unmarked. -/
def exemptionReference : Exemption := fun row action =>
  if action = .mark && !dataKindsReference.contains row.packetKind then .enqueue else action

/-- Groups the rows by payload with a sort (stable by canonical position) and reports, among the
rows whose kind differs from their payload's first row, the one earliest in canonical order. -/
def checkPacketKindsReference (rows : List Row) : Except String Unit := do
  let indexed := rows.toArray.mapIdx fun index row => (index, row)
  let sorted := indexed.qsort fun (i, a) (j, b) =>
    a.payloadId < b.payloadId || (a.payloadId = b.payloadId && i < j)
  let mut violation : Option (Nat × Row × Row) := none
  let mut groupFirst : Option Row := none
  for (index, row) in sorted do
    match groupFirst with
    | some first =>
        if first.payloadId = row.payloadId then
          if first.packetKind ≠ row.packetKind then
            match violation with
            | some (earliest, _, _) =>
                if index < earliest then violation := some (index, row, first)
            | none => violation := some (index, row, first)
        else
          groupFirst := some row
    | none => groupFirst := some row
  match violation with
  | none => pure ()
  | some (_, row, first) =>
      throw s!"line {row.srcLine}: {kindChangeMessage row first.packetKind first.srcLine}"

def checkRowsReference (rows : List Row) : Except String Unit := do
  let rows ← canonicalize rows
  for row in rows do
    checkRow exemptionReference row
  checkContinuityReference rows
  checkPacketKindsReference rows

end LeanGuard.P10c.AqmEventLog
