import Std

import LeanGuard.Shared.Check
import LeanGuard.Shared.Coverage
import LeanGuard.Shared.Csv
import LeanGuard.Shared.Key
import LeanGuard.Shared.TraceSpec

namespace LeanGuard.TcpEventLog

open LeanGuard.Shared

def scale : Nat := 1000000000
def maxCubicWindow : Nat := 2000000 * scale
def maxU64 : Nat := 18446744073709551615

inductive Kind | newAck | duplicateAck | timeout
deriving DecidableEq, Repr

inductive Algorithm | reno | cubic
deriving DecidableEq, Repr

inductive Phase | slowStart | congestionAvoidance | fastRecovery
deriving DecidableEq, Repr

structure Snapshot where
  phase : Phase
  cwndBytes : Nat
  ssthreshBytes : Nat
  duplicateAcks : Nat
  recoveryHigh : Nat
  caCredit : Nat
  cwndScaled : Nat
  ssthreshScaled : Nat
  wMaxScaled : Nat
  wLastMaxScaled : Nat
  epochStartNs : Option Nat
  srttNs : Nat
  kNs : Nat
deriving DecidableEq, Repr

structure Row where
  timeNs : Nat
  eventId : Nat
  kind : Kind
  nodeId : Nat
  flowId : Nat
  algorithm : Algorithm
  mssBytes : Nat
  ackedBytes : Option Nat
  rttNs : Option Nat
  flightBytes : Nat
  acknowledgment : Option Nat
  recoveryHighInput : Option Nat
  before : Snapshot
  after : Snapshot
  srcLine : Nat
deriving DecidableEq, Repr

def key (r : Row) : Nat × Nat := (r.timeNs, r.eventId)

def parseKind : String → Except String Kind
  | "new_ack" => pure .newAck
  | "duplicate_ack" => pure .duplicateAck
  | "timeout" => pure .timeout
  | other => throw s!"invalid TCP transition kind: {other}"

def parseAlgorithm : String → Except String Algorithm
  | "Reno" => pure .reno
  | "CUBIC" => pure .cubic
  | other => throw s!"invalid TCP algorithm: {other}"

def parsePhase : String → Except String Phase
  | "slow_start" => pure .slowStart
  | "congestion_avoidance" => pure .congestionAvoidance
  | "fast_recovery" => pure .fastRecovery
  | other => throw s!"invalid TCP phase: {other}"

def parseSnapshot (side : String) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Snapshot := do
  pure {
    phase := ← parsePhase (← getField idx fields s!"{side}_phase")
    cwndBytes := ← parseNat (← getField idx fields s!"{side}_cwnd_bytes")
    ssthreshBytes := ← parseNat (← getField idx fields s!"{side}_ssthresh_bytes")
    duplicateAcks := ← parseNat (← getField idx fields s!"{side}_dupacks")
    recoveryHigh := ← parseNat (← getField idx fields s!"{side}_recovery_high")
    caCredit := ← parseNat (← getField idx fields s!"{side}_ca_credit")
    cwndScaled := ← parseNat (← getField idx fields s!"{side}_cwnd_scaled")
    ssthreshScaled := ← parseNat (← getField idx fields s!"{side}_ssthresh_scaled")
    wMaxScaled := ← parseNat (← getField idx fields s!"{side}_w_max_scaled")
    wLastMaxScaled := ← parseNat (← getField idx fields s!"{side}_w_last_max_scaled")
    epochStartNs := ← parseOpt parseNat (← getField idx fields s!"{side}_epoch_ns")
    srttNs := ← parseNat (← getField idx fields s!"{side}_srtt_ns")
    kNs := ← parseNat (← getField idx fields s!"{side}_k_ns") }

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let result : Except String Row := do
    pure {
      timeNs := ← parseNat (← getField idx fields "time_ns")
      eventId := ← parseNat (← getField idx fields "event_id")
      kind := ← parseKind (← getField idx fields "kind")
      nodeId := ← parseNat (← getField idx fields "node_id")
      flowId := ← parseNat (← getField idx fields "flow_id")
      algorithm := ← parseAlgorithm (← getField idx fields "algorithm")
      mssBytes := ← parseNat (← getField idx fields "mss_bytes")
      ackedBytes := ← parseOpt parseNat (← getField idx fields "acked_bytes")
      rttNs := ← parseOpt parseNat (← getField idx fields "rtt_ns")
      flightBytes := ← parseNat (← getField idx fields "flight_bytes")
      acknowledgment := ← parseOpt parseNat (← getField idx fields "acknowledgment")
      recoveryHighInput := ← parseOpt parseNat (← getField idx fields "recovery_high_input")
      before := ← parseSnapshot "before" idx fields
      after := ← parseSnapshot "after" idx fields
      srcLine := lineNo }
  match result with
  | .ok row => pure row
  | .error error => throw s!"line {lineNo}: {error}"

def parseCsv (content : String) : Except String (List Row) := do
  let lines := content.splitOn "\n" |>.map stripCR |>.map (·.trim) |>.filter (· != "")
  match lines with
  | [] => throw "empty CSV"
  | header :: data =>
      let idx := mkIndex (splitCsvLine header)
      let rec go (lineNo : Nat) (rows : List String) (acc : List Row) : Except String (List Row) := do
        match rows with
        | [] => pure acc.reverse
        | line :: rest =>
            let row ← parseRow lineNo idx (splitCsvLine line |>.toArray)
            go (lineNo + 1) rest (row :: acc)
      go 2 data []

def bytesToScaled (bytes mss : Nat) : Nat := bytes * scale / mss
def scaledToBytes (window mss : Nat) : Nat := window * mss / scale

def checkSnapshotShape (lineNo : Nat) (algorithm : Algorithm) (mss : Nat) (s : Snapshot) :
    Except String Unit := do
  require lineNo (mss > 0) "mss_bytes must be positive"
  let values := [s.cwndBytes, s.ssthreshBytes, s.duplicateAcks, s.recoveryHigh,
    s.caCredit, s.cwndScaled, s.ssthreshScaled, s.wMaxScaled, s.wLastMaxScaled, s.srttNs, s.kNs]
  require lineNo (values.all (· <= maxU64)) "snapshot exceeds the fixed u64 state domain"
  match algorithm with
  | .reno =>
      require lineNo (s.cwndScaled = bytesToScaled s.cwndBytes mss)
        "Reno cwnd byte/scaled projections disagree"
      require lineNo (s.ssthreshScaled = bytesToScaled s.ssthreshBytes mss)
        "Reno threshold byte/scaled projections disagree"
      require lineNo (s.wMaxScaled = 0 && s.wLastMaxScaled = 0 && s.epochStartNs = none
        && s.srttNs = 0 && s.kNs = 0) "Reno row contains CUBIC-only state"
  | .cubic =>
      require lineNo (s.cwndBytes = scaledToBytes s.cwndScaled mss)
        "CUBIC cwnd byte/scaled projections disagree"
      require lineNo (s.ssthreshBytes = scaledToBytes s.ssthreshScaled mss)
        "CUBIC threshold byte/scaled projections disagree"
      require lineNo (s.caCredit = 0) "CUBIC row contains Reno-only CA credit"

def initialSnapshot (algorithm : Algorithm) (mss : Nat) : Snapshot :=
  match algorithm with
  | .reno =>
      { phase := .slowStart, cwndBytes := 2 * mss, ssthreshBytes := 65535,
        duplicateAcks := 0, recoveryHigh := 0, caCredit := 0,
        cwndScaled := bytesToScaled (2 * mss) mss,
        ssthreshScaled := bytesToScaled 65535 mss, wMaxScaled := 0,
        wLastMaxScaled := 0, epochStartNs := none, srttNs := 0, kNs := 0 }
  | .cubic =>
      let threshold := bytesToScaled 65535 mss
      { phase := .slowStart, cwndBytes := mss,
        ssthreshBytes := scaledToBytes threshold mss,
        duplicateAcks := 0, recoveryHigh := 0, caCredit := 0,
        cwndScaled := scale, ssthreshScaled := threshold,
        wMaxScaled := 0, wLastMaxScaled := 0,
        epochStartNs := none, srttNs := 0, kNs := 0 }

partial def consumeRenoCredit (cwnd credit mss : Nat) : Nat × Nat :=
  if credit < max cwnd 1 then (cwnd, credit)
  else consumeRenoCredit (cwnd + mss) (credit - max cwnd 1) mss

def renoExpected (r : Row) : Except String Snapshot := do
  let b := r.before
  match r.kind with
  | .newAck =>
      let acked ← requireSome r.srcLine "acked_bytes" r.ackedBytes
      let acknowledgment ← requireSome r.srcLine "acknowledgment" r.acknowledgment
      let result :=
        match b.phase with
        | .slowStart =>
            let next := b.cwndBytes + min r.mssBytes acked
            if next >= b.ssthreshBytes then
              { b with
                phase := Phase.congestionAvoidance
                cwndBytes := b.ssthreshBytes
                duplicateAcks := 0
                caCredit := 0
                cwndScaled := bytesToScaled b.ssthreshBytes r.mssBytes }
            else
              { b with
                cwndBytes := next
                duplicateAcks := 0
                cwndScaled := bytesToScaled next r.mssBytes }
        | .congestionAvoidance =>
            let (cwnd, credit) := consumeRenoCredit b.cwndBytes (b.caCredit + acked) r.mssBytes
            { b with
              cwndBytes := cwnd
              duplicateAcks := 0
              caCredit := credit
              cwndScaled := bytesToScaled cwnd r.mssBytes }
        | .fastRecovery =>
            if b.recoveryHigh != 0 && acknowledgment >= b.recoveryHigh then
              { b with
                phase := Phase.congestionAvoidance
                cwndBytes := b.ssthreshBytes
                duplicateAcks := 0
                recoveryHigh := 0
                caCredit := 0
                cwndScaled := bytesToScaled b.ssthreshBytes r.mssBytes }
            else
              let cwnd := b.ssthreshBytes + r.mssBytes
              { b with
                cwndBytes := cwnd
                duplicateAcks := 0
                cwndScaled := bytesToScaled cwnd r.mssBytes }
      pure result
  | .duplicateAck =>
      let recoveryHigh ← requireSome r.srcLine "recovery_high_input" r.recoveryHighInput
      let dup := b.duplicateAcks + 1
      if dup = 3 then
        let threshold := max (r.flightBytes / 2) (2 * r.mssBytes)
        let cwnd := threshold + 3 * r.mssBytes
        pure { b with
          phase := Phase.fastRecovery
          cwndBytes := cwnd
          ssthreshBytes := threshold
          duplicateAcks := dup
          recoveryHigh := recoveryHigh
          caCredit := 0
          cwndScaled := bytesToScaled cwnd r.mssBytes
          ssthreshScaled := bytesToScaled threshold r.mssBytes }
      else if dup > 3 && b.phase = .fastRecovery then
        let cwnd := b.cwndBytes + r.mssBytes
        pure { b with
          cwndBytes := cwnd
          duplicateAcks := dup
          cwndScaled := bytesToScaled cwnd r.mssBytes }
      else pure { b with duplicateAcks := dup }
  | .timeout =>
      let threshold := max (r.flightBytes / 2) (2 * r.mssBytes)
      pure { b with
        phase := Phase.slowStart
        cwndBytes := r.mssBytes
        ssthreshBytes := threshold
        duplicateAcks := 0
        recoveryHigh := 0
        caCredit := 0
        cwndScaled := scale
        ssthreshScaled := bytesToScaled threshold r.mssBytes }

def updatedSrtt (before sample : Nat) : Nat :=
  if before = 0 then max sample 1 else (before * 7 + max sample 1) / 8

def cubicRadicand (wMaxScaled : Nat) : Nat :=
  wMaxScaled * 3 * 1000000000000000000 / 4

def checkCubicK (lineNo wMaxScaled kNs : Nat) : Except String Unit := do
  let radicand := cubicRadicand wMaxScaled
  require lineNo (kNs ^ 3 <= radicand && (kNs + 1) ^ 3 > radicand)
    "CUBIC K is not the floor cube root of its exact fixed-point radicand"

def cubicWindow (wMaxScaled kNs elapsedNs : Nat) : Nat :=
  let distance := if elapsedNs < kNs then kNs - elapsedNs else elapsedNs - kNs
  let magnitude := distance ^ 3 * 2 / 5000000000000000000
  if elapsedNs < kNs then max scale (wMaxScaled - magnitude)
  else min maxCubicWindow (max scale (wMaxScaled + magnitude))

def tcpFriendlyWindow (wMaxScaled elapsedNs rttNs : Nat) : Nat :=
  let base := wMaxScaled * 7 / 10
  let growth := scale * 9 * elapsedNs / (17 * max rttNs 1)
  min maxCubicWindow (max scale (base + growth))

def cubicAckStep (cwndScaled targetScaled : Nat) : Nat :=
  let denominator := max cwndScaled scale
  let next := if targetScaled >= cwndScaled then
      cwndScaled + (targetScaled - cwndScaled) * scale / denominator
    else
      cwndScaled - (cwndScaled - targetScaled) * scale / denominator
  min maxCubicWindow (max scale next)

def setCubicWindow (s : Snapshot) (mss window threshold : Nat) : Snapshot :=
  { s with
    cwndScaled := window
    ssthreshScaled := threshold
    cwndBytes := scaledToBytes window mss
    ssthreshBytes := scaledToBytes threshold mss }

def checkCubic (r : Row) : Except String Unit := do
  let b := r.before
  let a := r.after
  match r.kind with
  | .newAck =>
      let acked ← requireSome r.srcLine "acked_bytes" r.ackedBytes
      let rtt ← requireSome r.srcLine "rtt_ns" r.rttNs
      let acknowledgment ← requireSome r.srcLine "acknowledgment" r.acknowledgment
      require r.srcLine (acked > 0 && rtt > 0) "new ACK requires positive byte and RTT inputs"
      let srtt := updatedSrtt b.srttNs rtt
      match b.phase with
      | .slowStart =>
          let segments := (acked + r.mssBytes - 1) / r.mssBytes
          let window := min (b.cwndScaled + segments * scale) maxCubicWindow
          let next := setCubicWindow { b with duplicateAcks := 0, srttNs := srtt }
            r.mssBytes window b.ssthreshScaled
          let expected := if window >= b.ssthreshScaled then
              { next with
                phase := .congestionAvoidance
                epochStartNs := some r.timeNs
                wMaxScaled := if b.wMaxScaled = 0 then window else b.wMaxScaled
                kNs := if b.wMaxScaled = 0 then 0 else b.kNs }
            else next
          require r.srcLine (a = expected) "CUBIC slow-start transition mismatch"
      | .congestionAvoidance =>
          let creatingEpoch := b.epochStartNs.isNone
          let epoch := b.epochStartNs.getD r.timeNs
          let wMax := if creatingEpoch && b.wMaxScaled = 0 then b.cwndScaled else b.wMaxScaled
          let k := if creatingEpoch && b.wMaxScaled != 0 then a.kNs
            else if creatingEpoch then 0 else b.kNs
          if creatingEpoch && b.wMaxScaled != 0 then
            checkCubicK r.srcLine wMax k
          let elapsed := r.timeNs - epoch
          let cubicNow := cubicWindow wMax k elapsed
          let friendly := tcpFriendlyWindow wMax elapsed srtt
          let window := if cubicNow < friendly then friendly else
            cubicAckStep b.cwndScaled (cubicWindow wMax k (elapsed + srtt))
          let expected := setCubicWindow
            { b with
              duplicateAcks := 0
              srttNs := srtt
              epochStartNs := some epoch
              wMaxScaled := wMax
              kNs := k }
            r.mssBytes window b.ssthreshScaled
          require r.srcLine (a = expected) "CUBIC congestion-avoidance transition mismatch"
      | .fastRecovery =>
          let updated := { b with duplicateAcks := 0, srttNs := srtt }
          let expected := if b.recoveryHigh != 0 && acknowledgment >= b.recoveryHigh then
              setCubicWindow { updated with phase := .congestionAvoidance, recoveryHigh := 0 }
                r.mssBytes b.ssthreshScaled b.ssthreshScaled
            else updated
          require r.srcLine (a = expected) "CUBIC recovery ACK transition mismatch"
  | .duplicateAck =>
      let recoveryHigh ← requireSome r.srcLine "recovery_high_input" r.recoveryHighInput
      let dup := b.duplicateAcks + 1
      if dup = 3 then
        let flightScaled := min (bytesToScaled r.flightBytes r.mssBytes) maxCubicWindow
        let reduced := max (flightScaled * 7 / 10) scale
        let threshold := max reduced (2 * scale)
        let wMax := if b.wLastMaxScaled > 0 && b.cwndScaled < b.wLastMaxScaled
          then b.cwndScaled * 17 / 20 else b.cwndScaled
        checkCubicK r.srcLine wMax a.kNs
        let expected := setCubicWindow
          { b with
            phase := .fastRecovery
            duplicateAcks := 3
            recoveryHigh := recoveryHigh
            wMaxScaled := wMax
            wLastMaxScaled := b.cwndScaled
            epochStartNs := some r.timeNs
            kNs := a.kNs }
          r.mssBytes reduced threshold
        require r.srcLine (a = expected) "CUBIC fast-retransmit transition mismatch"
      else if dup > 3 && b.phase = .fastRecovery then
        let window := min (b.cwndScaled + scale) maxCubicWindow
        let expected := setCubicWindow { b with duplicateAcks := dup }
          r.mssBytes window b.ssthreshScaled
        require r.srcLine (a = expected) "CUBIC recovery inflation mismatch"
      else
        require r.srcLine (a = { b with duplicateAcks := dup })
          "CUBIC duplicate ACK transition mismatch"
  | .timeout =>
      let flightScaled := min (bytesToScaled r.flightBytes r.mssBytes) maxCubicWindow
      let threshold := max (flightScaled * 7 / 10) (2 * scale)
      let expected := setCubicWindow
        { b with
          phase := .slowStart
          duplicateAcks := 0
          recoveryHigh := 0
          wMaxScaled := 0
          wLastMaxScaled := 0
          epochStartNs := none
          kNs := 0 }
        r.mssBytes scale threshold
      require r.srcLine (a = expected) "CUBIC timeout transition mismatch"

structure FlowState where
  algorithm : Algorithm
  mssBytes : Nat
  snapshot : Snapshot
deriving Repr

structure ReplayState where
  flows : Std.HashMap (Nat × Nat) FlowState := ∅
  lastKey : Option (Nat × Nat) := none
deriving Repr

def stepRow (s : ReplayState) (r : Row) : Except String ReplayState := do
  match s.lastKey with
  | none => pure ()
  | some previous => require r.srcLine (keyLt previous (key r)) "global key went backwards"
  checkSnapshotShape r.srcLine r.algorithm r.mssBytes r.before
  checkSnapshotShape r.srcLine r.algorithm r.mssBytes r.after
  let flowKey := (r.nodeId, r.flowId)
  match s.flows.get? flowKey with
  | none => do
      require r.srcLine (r.before = initialSnapshot r.algorithm r.mssBytes)
        "first row does not start at the algorithm's initial state"
  | some previous => do
      require r.srcLine (previous.algorithm = r.algorithm && previous.mssBytes = r.mssBytes)
        "algorithm or MSS changed within a flow"
      require r.srcLine (previous.snapshot = r.before) "TCP state continuity mismatch"
  match r.algorithm with
  | .reno =>
      let expected ← renoExpected r
      require r.srcLine (r.after = expected) "Reno transition mismatch"
  | .cubic => checkCubic r
  let next : FlowState := { algorithm := r.algorithm, mssBytes := r.mssBytes, snapshot := r.after }
  pure { flows := s.flows.insert flowKey next, lastKey := some (key r) }

def recordCover (cov : CoverageState) (r : Row) : CoverageState :=
  let name := match r.algorithm, r.kind with
    | .reno, .newAck => "reno_new_ack"
    | .reno, .duplicateAck => "reno_duplicate_ack"
    | .reno, .timeout => "reno_timeout"
    | .cubic, .newAck => if r.before.phase = .congestionAvoidance
        then "cubic_ca_exact" else "cubic_new_ack_exact"
    | .cubic, .duplicateAck => "cubic_duplicate_ack"
    | .cubic, .timeout => "cubic_timeout"
  covHit (covTick cov) name

def traceSpec : TraceSpec :=
  { Row := Row, State := ReplayState, init := {}, step := stepRow }

def observeCoverage (cov : CoverageState) (_ : ReplayState) (r : Row) : CoverageState :=
  recordCover cov r

def replayCanonicalRowsWithCoverage (rows : List Row) (cov : CoverageState) :=
  TraceSpec.replayWithObserverM traceSpec observeCoverage traceSpec.init rows cov

theorem replayCanonicalRowsWithCoverage_sound {rows : List Row} {cov : CoverageState}
    {s : ReplayState} {cov' : CoverageState} :
    replayCanonicalRowsWithCoverage rows cov = .ok (s, cov') →
      TraceSpec.Replay traceSpec traceSpec.init rows s := by
  intro h
  exact TraceSpec.replayWithObserverM_sound traceSpec observeCoverage h

def checkRowsWithCoverage (rows : List Row) : CheckOutcome :=
  match canonicalizeRows rows key (fun r => r.srcLine) with
  | .error error => .error (error, {})
  | .ok sorted =>
      match replayCanonicalRowsWithCoverage sorted {} with
      | .ok (_, coverage) => .ok coverage
      | .error error => .error error

def checkRows (rows : List Row) : Except String Unit := do
  match checkRowsWithCoverage rows with
  | .ok _ => pure ()
  | .error (error, _) => throw error

end LeanGuard.TcpEventLog
