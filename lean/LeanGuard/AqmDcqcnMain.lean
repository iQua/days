import LeanGuard.AqmEventLog
import LeanGuard.DcqcnEventLog
import LeanGuard.Shared.Check
import LeanGuard.Shared.Cli
import LeanGuard.Shared.Coverage

open LeanGuard.AqmEventLog
open LeanGuard.DcqcnEventLog
open LeanGuard.Shared

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

private def checkAllWithCoverage (aqmRows : List LeanGuard.AqmEventLog.Row)
    (dcqcnRows : List LeanGuard.DcqcnEventLog.Row) : CheckOutcome := do
  let covAqm ←
    match LeanGuard.AqmEventLog.checkRowsWithCoverage aqmRows with
    | .ok cov => pure cov
    | .error (e, cov) => throw (e, cov)
  let covDcqcn ←
    match LeanGuard.DcqcnEventLog.checkRowsWithCoverage dcqcnRows with
    | .ok cov => pure cov
    | .error (e, cov) => throw (e, covMerge covAqm cov)
  let cov := covMerge covAqm covDcqcn
  let marks := buildMarkMap aqmRows
  match checkCrossLayer marks dcqcnRows with
  | .ok _ => pure cov
  | .error e => throw (e, cov)

private def checkAll (aqmRows : List LeanGuard.AqmEventLog.Row)
    (dcqcnRows : List LeanGuard.DcqcnEventLog.Row) : Except String Unit := do
  match checkAllWithCoverage aqmRows dcqcnRows with
  | .ok _ => pure ()
  | .error (e, _) => throw e

private def usage : String :=
  "usage:\n"
  ++ "  aqm_dcqcn_check [--coverage-out <path>] <path/to/aqm_events.csv> <path/to/dcqcn_events.csv>\n"
  ++ "  aqm_dcqcn_check --emit-coverpoint-catalog"

def main (args : List String) : IO UInt32 := do
  match parseCoverageOut args with
  | .error _ =>
      IO.eprintln usage
      pure 2
  | .ok parsed =>
      if parsed.emitCoverpointCatalog then
        let cat :=
          sortStrings
              (LeanGuard.AqmEventLog.coverpointCatalog
                ++ LeanGuard.DcqcnEventLog.coverpointCatalog)
            |>.eraseDups
        for cp in cat do
          IO.println cp
        pure 0
      else
        match parsed.inputs with
        | [aqmPath, dcqcnPath] =>
          let aqmContent ← IO.FS.readFile aqmPath
          let dcqcnContent ← IO.FS.readFile dcqcnPath
          match LeanGuard.AqmEventLog.parseCsv aqmContent,
                LeanGuard.DcqcnEventLog.parseCsv dcqcnContent with
          | .ok aqmRows, .ok dcqcnRows =>
              let rowCount := aqmRows.length + dcqcnRows.length
              match checkAllWithCoverage aqmRows dcqcnRows with
              | .ok cov =>
                  let report : CoverageReport :=
                    { checker := "aqm_dcqcn_check"
                      accept := true
                      cover := covList cov
                      rows := rowCount
                      processedRows := cov.processedRows }
                  match parsed.coverageOut with
                  | none =>
                      IO.println "ACCEPT"
                      pure 0
                  | some out =>
                      match (← writeCoverageFile out report) with
                      | .ok _ =>
                          IO.println "ACCEPT"
                          pure 0
                      | .error we =>
                          IO.eprintln we
                          pure 2
              | .error (e, cov) =>
                  let report : CoverageReport :=
                    { checker := "aqm_dcqcn_check"
                      accept := false
                      cover := covList cov
                      rows := rowCount
                      processedRows := cov.processedRows
                      error := some e }
                  match parsed.coverageOut with
                  | none =>
                      IO.eprintln s!"REJECT: {e}"
                      pure 1
                  | some out =>
                      match (← writeCoverageFile out report) with
                      | .ok _ =>
                          IO.eprintln s!"REJECT: {e}"
                          pure 1
                      | .error we =>
                          IO.eprintln we
                          pure 2
          | .error e, _ =>
              let report : CoverageReport :=
                { checker := "aqm_dcqcn_check"
                  accept := false
                  cover := []
                  rows := 0
                  processedRows := 0
                  error := some e }
              match parsed.coverageOut with
              | none =>
                  IO.eprintln e
                  pure 2
              | some out =>
                  match (← writeCoverageFile out report) with
                  | .ok _ =>
                      IO.eprintln e
                      pure 2
                  | .error we =>
                      IO.eprintln we
                      pure 2
          | _, .error e =>
              let report : CoverageReport :=
                { checker := "aqm_dcqcn_check"
                  accept := false
                  cover := []
                  rows := 0
                  processedRows := 0
                  error := some e }
              match parsed.coverageOut with
              | none =>
                  IO.eprintln e
                  pure 2
              | some out =>
                  match (← writeCoverageFile out report) with
                  | .ok _ =>
                      IO.eprintln e
                      pure 2
                  | .error we =>
                      IO.eprintln we
                      pure 2
        | _ =>
            IO.eprintln usage
            pure 2
