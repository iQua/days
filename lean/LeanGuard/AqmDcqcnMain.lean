import LeanGuard.AqmEventLog
import LeanGuard.DcqcnEventLog
import LeanGuard.Shared.Check

open LeanGuard.AqmEventLog
open LeanGuard.DcqcnEventLog

private def isAqmMark (r : LeanGuard.AqmEventLog.Row) : Bool :=
  r.action = LeanGuard.Aqm.Semantics.Action.markEcn && r.ecnAfter == "ce"

private def addMarkTime
    (m : Std.HashMap (Nat × Nat) Nat)
    (key : Nat × Nat)
    (t : Nat) : Std.HashMap (Nat × Nat) Nat :=
  match m.get? key with
  | none => m.insert key t
  | some existing => if t < existing then m.insert key t else m

private def buildMarkMap (rows : List LeanGuard.AqmEventLog.Row) :
    Std.HashMap (Nat × Nat) Nat :=
  rows.foldl (fun acc r =>
    if isAqmMark r then
      addMarkTime acc (r.flowId, r.packetId) r.timeNs
    else
      acc) ∅

private def checkCrossLayer
    (marks : Std.HashMap (Nat × Nat) Nat)
    (rows : List LeanGuard.DcqcnEventLog.Row) : Except String Unit := do
  for r in rows do
    match r.kind with
    | LeanGuard.DcqcnEventLog.Kind.cnpSent =>
        let trig ← LeanGuard.Shared.requireSome r.srcLine "trigger_ecn" r.triggerEcn
        if trig = LeanGuard.DcqcnEventLog.Ecn.Ce then
          let pktId ← LeanGuard.Shared.requireSome r.srcLine "pkt_id" r.pktId
          let pktFlowId ← LeanGuard.Shared.requireSome r.srcLine "pkt_flow_id" r.pktFlowId
          let key := (pktFlowId, pktId)
          match marks.get? key with
          | none =>
              throw s!"line {r.srcLine}: missing AQM mark for packet (flow_id={pktFlowId}, pkt_id={pktId})"
          | some t =>
              LeanGuard.Shared.require r.srcLine (t <= r.timeNs)
                s!"AQM mark at time {t} occurs after CNP (time {r.timeNs})"
        else
          pure ()
    | _ => pure ()

private def checkAll (aqmRows : List LeanGuard.AqmEventLog.Row)
    (dcqcnRows : List LeanGuard.DcqcnEventLog.Row) : Except String Unit := do
  LeanGuard.AqmEventLog.checkRows aqmRows
  LeanGuard.DcqcnEventLog.checkRows dcqcnRows
  let marks := buildMarkMap aqmRows
  checkCrossLayer marks dcqcnRows

private def usage : String :=
  "usage: aqm_dcqcn_check <path/to/aqm_events.csv> <path/to/dcqcn_events.csv>"

def main (args : List String) : IO UInt32 := do
  match args with
  | [aqmPath, dcqcnPath] =>
      let aqmContent ← IO.FS.readFile aqmPath
      let dcqcnContent ← IO.FS.readFile dcqcnPath
      match LeanGuard.AqmEventLog.parseCsv aqmContent,
            LeanGuard.DcqcnEventLog.parseCsv dcqcnContent with
      | .ok aqmRows, .ok dcqcnRows =>
          match checkAll aqmRows dcqcnRows with
          | .ok _ =>
              IO.println "ACCEPT"
              pure 0
          | .error e =>
              IO.eprintln s!"REJECT: {e}"
              pure 1
      | .error e, _ =>
          IO.eprintln s!"REJECT: {e}"
          pure 1
      | _, .error e =>
          IO.eprintln s!"REJECT: {e}"
          pure 1
  | _ =>
      IO.eprintln usage
      pure 2
