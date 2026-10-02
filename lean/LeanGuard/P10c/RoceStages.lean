import LeanGuard.P10c.CollectiveEventLog
import LeanGuard.P10c.RoceEventLog

namespace LeanGuard.P10c.RoceStages

/-!
Collective stages over RoCE queue pairs (schema Amendment 4): the joins between a run's collective
progress log and its queue-pair logs, checked in trace mode with `--collective`.

A stage queue pair's sender, receiver and DCQCN rows are a plain queue pair's rows whose
`first_pacing_time_ns` is the release instant (ruling C2), so `RoceEventLog` checks them as it
checks any pair. What only the joint logs show is that each pair starts when its stage is
released and finishes where its successor sees it finish:

* **Release.** A released RoCE stage's queue pair runs its first pacing tick at the release
  instant, on a grid anchored there (the sender log's first row of the pair: a tick, possibly
  class-paused (C6), with `time_ns` and `first_pacing_time_ns` equal to the release row's
  `time_ns`), and its controller's first control tick is one control interval later (the pair's
  first DCQCN row, of any kind, holds `before_next_control_time_ns = time_ns +
  control_interval_ns`; a pair with no DCQCN row before the horizon is not checked).
* **No early start.** A logged RoCE stage that is never released has no queue-pair rows.
* **Completion.** A local completion caused by a RoCE queue pair (the successor's row names it as
  `cause_flow_id`, and it is in the sender log) shares its event key with that pair's sender row
  at the successor's node, which is an ACK (never a NACK, timeout or tick) that takes `snd_una`
  from below the pair's total to it, and whose acknowledgment is the row's `ack_number`.

Each log is read once into hash maps, so the check is linear in the logs' lengths.
-/

open LeanGuard.P10c
open LeanGuard.P10c.RoceEventLog (SenderRow requireAt)

/-- A queue pair: `(node_id, flow_id)`. -/
abbrev Pair := Nat × Nat

/-- A row of one queue pair at one event key: `(time, phase, origin node, origin sequence,
node_id, flow_id)`. -/
abbrev KeyedPair := Nat × Nat × Nat × Nat × Nat × Nat

def keyedPair (key : DaysExecutor.EventKey) (node flow : Nat) : KeyedPair :=
  (key.timeNs, key.phase, key.originNode, key.originSeq, node, flow)

/-- A local completion caused by a transport stage's completing ACK (not a compute timer). -/
def ackCompletion (row : CollectiveEventLog.Row) : Bool :=
  row.cause = .localCompletion && !CollectiveEventLog.causeIsTimer row

def checkStages (horizonNs : Option Nat) (collective : List CollectiveEventLog.Row)
    (sender : List SenderRow) (dcqcn : List DcqcnEventLog.Row) : Except String Unit := do
  -- The completions the collective log names, so only their sender rows are kept.
  let mut wanted : Std.HashSet KeyedPair := ∅
  let mut released : Std.HashSet Pair := ∅
  for row in collective do
    if let some horizon := horizonNs then
      requireAt "collective" row.srcLine (row.key.timeNs < horizon)
        s!"event at or after horizon_ns={horizon}"
    if ackCompletion row then
      wanted := wanted.insert (keyedPair row.key row.nodeId row.causeFlowId)
    if row.stageKind = .roce && row.activated then
      released := released.insert (row.nodeId, row.flowId)
  let mut firstSender : Std.HashMap Pair SenderRow := ∅
  let mut senderFlows : Std.HashSet Nat := ∅
  let mut atKey : Std.HashMap KeyedPair SenderRow := ∅
  for row in sender do
    if !firstSender.contains (row.nodeId, row.flowId) then
      firstSender := firstSender.insert (row.nodeId, row.flowId) row
    senderFlows := senderFlows.insert row.flowId
    let keyed := keyedPair row.key row.nodeId row.flowId
    if wanted.contains keyed then
      atKey := atKey.insert keyed row
  let mut firstDcqcn : Std.HashMap Pair DcqcnEventLog.Row := ∅
  for row in dcqcn do
    if !firstDcqcn.contains (row.nodeId, row.flowId) then
      firstDcqcn := firstDcqcn.insert (row.nodeId, row.flowId) row
  for row in collective do
    let at_ := requireAt "collective" row.srcLine
    let pair := (row.nodeId, row.flowId)
    if row.stageKind = .roce then
      if row.activated then
        match firstSender.get? pair with
        | none =>
            throw s!"collective: line {row.srcLine}: RoCE stage released without a pacing tick of its queue pair (node_id={row.nodeId}, flow_id={row.flowId})"
        | some first =>
            at_
              (first.kind = .tick && first.key.timeNs = row.key.timeNs &&
                first.config.firstPacingTimeNs = row.key.timeNs)
              s!"RoCE stage queue pair's first pacing tick is not at its release instant, on a grid anchored there (node_id={row.nodeId}, flow_id={row.flowId})"
        if let some first := firstDcqcn.get? pair then
          at_
            (first.before.nextControlTimeNs =
              row.key.timeNs + first.config.controlIntervalNs)
            s!"RoCE stage queue pair's first control tick is not one control interval after its release (node_id={row.nodeId}, flow_id={row.flowId})"
      else if !released.contains pair then
        at_ (!firstSender.contains pair)
          s!"unreleased RoCE stage has queue-pair rows (node_id={row.nodeId}, flow_id={row.flowId})"
    if ackCompletion row && senderFlows.contains row.causeFlowId then
      match atKey.get? (keyedPair row.key row.nodeId row.causeFlowId) with
      | none =>
          throw s!"collective: line {row.srcLine}: RoCE local completion without a sender row of its predecessor queue pair at its event key (node_id={row.nodeId}, flow_id={row.causeFlowId})"
      | some ack =>
          at_
            (ack.kind = .ack && ack.before.sndUna < ack.config.totalBytes &&
              ack.after.sndUna = ack.config.totalBytes &&
              ack.inputAcknowledgment = some row.ackNumber)
            s!"RoCE local completion is not the ACK that completes its predecessor queue pair (node_id={row.nodeId}, flow_id={row.causeFlowId}, sender line {ack.srcLine})"

end LeanGuard.P10c.RoceStages
