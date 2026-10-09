import LeanGuard.P10c.AqmEventLog

/-! Test-only reference for the P10c AQM checker. `p10c_aqm_diff` runs both and requires identical
results. Nothing in a shipped checker imports this module.
- The list-scan continuity check, as shipped at `26dc1d1` (P16 lane L1); the shipped checker keys
  each queue's most recent row in a hash map. Bodies are verbatim apart from the `Reference` suffix.
- P16 aqmkind: an independent data-only marking rule (its own kind table), and an independent
  per-packet kind check (rows grouped by sorting on payload, not a hash map).
- P16 ecnramp: an independent ECN ramp decision. Its draw is SplitMix64 written out here (not
  `SplitMix.mix`), and its ramp test is the rational form `u · den · (kmax − kmin) <
  num · (d − kmin) · 2^64`, with no high multiply, so a broken shipped decision or draw shows as a
  differential mismatch. -/

namespace LeanGuard.P10c.AqmEventLog

open LeanGuard.Shared
open LeanGuard.P10c.Semantics

def sameQueue (first second : Row) : Bool :=
  first.nodeId = second.nodeId && first.queueId = second.queueId

def checkContinuityReference (rows : List Row) : Except String Unit := do
  let rec go (seed : Option Nat) (previous : List Row) : List Row → Except String Unit
    | [] => pure ()
    | row :: rest => do
        if let some first := seed then
          require row.srcLine (row.seed = first) "the seed differs from the run's first row"
        match previous.find? (sameQueue · row) with
        | none => pure ()
        | some prior =>
            require row.srcLine (sameConfig prior row)
              s!"AQM config does not continue the prior config for queue (node_id={row.nodeId}, queue_id={row.queueId})"
        go (some (seed.getD row.seed)) (row :: previous.filter (fun prior => !sameQueue prior row))
          rest
  go none [] rows

/-- The executor's data kinds (`PacketKind::is_data`), written out independently of `dataKind`. -/
def dataKindsReference : List String := ["data", "tcp_data", "roce_data"]

/-- SplitMix64's output function, written out with an explicit `2^64` modulus. -/
def splitMixReference (value : Nat) : Nat :=
  let m := 18446744073709551616
  let z := (value + 11400714819323198485) % m
  let z := ((z ^^^ (z / 1073741824)) * 13787848793156543929) % m
  let z := ((z ^^^ (z / 134217728)) * 10723151780598845931) % m
  z ^^^ (z / 2147483648)

/-- The ECN ramp decision, stated independently: the draw as the executor derives it, and the ramp
interval in its rational form. -/
def decisionReference : Decision := fun row =>
  let key := splitMixReference (splitMixReference (splitMixReference
    (row.seed ^^^ 0x45434e5f52414d50) ^^^ row.nodeId) ^^^ row.queueId)
  let u := splitMixReference (key ^^^ row.payloadId)
  let depth := row.queuedBytesBefore + row.packetSizeBytes
  if depth > 18446744073709551615 || depth > row.capacityBytes then
    .drop
  else if !dataKindsReference.contains row.packetKind || depth < row.kminBytes then
    .enqueue
  else if depth ≥ row.kmaxBytes then
    .mark
  else if u * row.pmaxDenominator * (row.kmaxBytes - row.kminBytes) <
      row.pmaxNumerator * (depth - row.kminBytes) * 18446744073709551616 then
    .mark
  else
    .enqueue

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
    checkRow decisionReference row
  checkContinuityReference rows
  checkPacketKindsReference rows

end LeanGuard.P10c.AqmEventLog
