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
def maxU16 : Nat := 65535

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
  eventPhase : Nat
  eventOriginNode : Nat
  eventOriginSequence : Nat
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

structure CanonicalEventKey where
  timeNs : Nat
  phase : Nat
  originNode : Nat
  originSequence : Nat
deriving DecidableEq, Repr

def key (r : Row) : CanonicalEventKey :=
  { timeNs := r.timeNs, phase := r.eventPhase, originNode := r.eventOriginNode,
    originSequence := r.eventOriginSequence }

def eventKeyLt (a b : CanonicalEventKey) : Bool :=
  decide (a.timeNs < b.timeNs ∨
    (a.timeNs = b.timeNs ∧ (a.phase < b.phase ∨
      (a.phase = b.phase ∧ (a.originNode < b.originNode ∨
        (a.originNode = b.originNode ∧ a.originSequence < b.originSequence))))))

def canonicalizeTcpRows (rows : List Row) : Except String (List Row) := do
  let rec checkKeys : List Row → Except String Unit
    | [] => pure ()
    | [_] => pure ()
    | a :: b :: rest => do
        if decide (key a = key b) then
          throw (s!"duplicate canonical EventKey at lines {a.srcLine} and {b.srcLine}: " ++
            s!"(time_ns={a.timeNs}, phase={a.eventPhase}, origin_node={a.eventOriginNode}, " ++
            s!"origin_sequence={a.eventOriginSequence})")
        require b.srcLine (eventKeyLt (key a) (key b)) "canonical EventKey order violated"
        checkKeys (b :: rest)
  checkKeys rows
  pure rows

def parseBoundedNat (name : String) (upper : Nat) (value : String) : Except String Nat := do
  let parsed ← parseNat value
  if parsed <= upper then pure parsed
  else throw s!"{name} exceeds its fixed-width domain"

def parseU64 (name : String) : String → Except String Nat := parseBoundedNat name maxU64
def parseU16 (name : String) : String → Except String Nat := parseBoundedNat name maxU16

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
    cwndBytes := ← parseU64 s!"{side}_cwnd_bytes" (← getField idx fields s!"{side}_cwnd_bytes")
    ssthreshBytes := ← parseU64 s!"{side}_ssthresh_bytes" (← getField idx fields s!"{side}_ssthresh_bytes")
    duplicateAcks := ← parseU64 s!"{side}_dupacks" (← getField idx fields s!"{side}_dupacks")
    recoveryHigh := ← parseU64 s!"{side}_recovery_high" (← getField idx fields s!"{side}_recovery_high")
    caCredit := ← parseU64 s!"{side}_ca_credit" (← getField idx fields s!"{side}_ca_credit")
    cwndScaled := ← parseU64 s!"{side}_cwnd_scaled" (← getField idx fields s!"{side}_cwnd_scaled")
    ssthreshScaled := ← parseU64 s!"{side}_ssthresh_scaled" (← getField idx fields s!"{side}_ssthresh_scaled")
    wMaxScaled := ← parseU64 s!"{side}_w_max_scaled" (← getField idx fields s!"{side}_w_max_scaled")
    wLastMaxScaled := ← parseU64 s!"{side}_w_last_max_scaled" (← getField idx fields s!"{side}_w_last_max_scaled")
    epochStartNs := ← parseOpt (parseU64 s!"{side}_epoch_ns") (← getField idx fields s!"{side}_epoch_ns")
    srttNs := ← parseU64 s!"{side}_srtt_ns" (← getField idx fields s!"{side}_srtt_ns")
    kNs := ← parseU64 s!"{side}_k_ns" (← getField idx fields s!"{side}_k_ns") }

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let result : Except String Row := do
    pure {
      timeNs := ← parseU64 "time_ns" (← getField idx fields "time_ns")
      eventPhase := ← parseU16 "event_phase" (← getField idx fields "event_phase")
      eventOriginNode := ← parseU64 "event_origin_node" (← getField idx fields "event_origin_node")
      eventOriginSequence := ← parseU64 "event_origin_sequence" (← getField idx fields "event_origin_sequence")
      kind := ← parseKind (← getField idx fields "kind")
      nodeId := ← parseU64 "node_id" (← getField idx fields "node_id")
      flowId := ← parseU64 "flow_id" (← getField idx fields "flow_id")
      algorithm := ← parseAlgorithm (← getField idx fields "algorithm")
      mssBytes := ← parseU64 "mss_bytes" (← getField idx fields "mss_bytes")
      ackedBytes := ← parseOpt (parseU64 "acked_bytes") (← getField idx fields "acked_bytes")
      rttNs := ← parseOpt (parseU64 "rtt_ns") (← getField idx fields "rtt_ns")
      flightBytes := ← parseU64 "flight_bytes" (← getField idx fields "flight_bytes")
      acknowledgment := ← parseOpt (parseU64 "acknowledgment") (← getField idx fields "acknowledgment")
      recoveryHighInput := ← parseOpt (parseU64 "recovery_high_input") (← getField idx fields "recovery_high_input")
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

def saturatingU64 (value : Nat) : Nat := min value maxU64
def saturatingAdd (left right : Nat) : Nat := saturatingU64 (left + right)
def saturatingMul (left right : Nat) : Nat := saturatingU64 (left * right)
def saturatingSucc (value : Nat) : Nat := saturatingAdd value 1
def encodedEpoch (value : Nat) : Option Nat := if value = maxU64 then none else some value

def bytesToScaled (bytes mss : Nat) : Nat := saturatingU64 (bytes * scale / mss)
def scaledToBytes (window mss : Nat) : Nat := saturatingU64 (window * mss / scale)

def checkSnapshotShape (lineNo : Nat) (algorithm : Algorithm) (mss : Nat) (s : Snapshot) :
    Except String Unit := do
  require lineNo (mss > 0) "mss_bytes must be positive"
  let values := [s.cwndBytes, s.ssthreshBytes, s.duplicateAcks, s.recoveryHigh,
    s.caCredit, s.cwndScaled, s.ssthreshScaled, s.wMaxScaled, s.wLastMaxScaled, s.srttNs, s.kNs]
  require lineNo (values.all (· <= maxU64)) "snapshot exceeds the fixed u64 state domain"
  require lineNo (s.epochStartNs.all (· < maxU64))
    "snapshot epoch uses or exceeds Rust's reserved u64::MAX sentinel"
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
      let window := saturatingMul 2 mss
      { phase := .slowStart, cwndBytes := window, ssthreshBytes := 65535,
        duplicateAcks := 0, recoveryHigh := 0, caCredit := 0,
        cwndScaled := bytesToScaled window mss,
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
  else consumeRenoCredit (saturatingAdd cwnd mss) (credit - max cwnd 1) mss

def renoExpected (r : Row) : Except String Snapshot := do
  let b := r.before
  match r.kind with
  | .newAck =>
      let acked ← requireSome r.srcLine "acked_bytes" r.ackedBytes
      let acknowledgment ← requireSome r.srcLine "acknowledgment" r.acknowledgment
      let result :=
        match b.phase with
        | .slowStart =>
            let next := saturatingAdd b.cwndBytes (min r.mssBytes acked)
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
            let (cwnd, credit) := consumeRenoCredit b.cwndBytes
              (saturatingAdd b.caCredit acked) r.mssBytes
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
              let cwnd := saturatingAdd b.ssthreshBytes r.mssBytes
              { b with
                cwndBytes := cwnd
                duplicateAcks := 0
                cwndScaled := bytesToScaled cwnd r.mssBytes }
      pure result
  | .duplicateAck =>
      let recoveryHigh ← requireSome r.srcLine "recovery_high_input" r.recoveryHighInput
      let dup := saturatingSucc b.duplicateAcks
      if dup = 3 then
        let threshold := max (r.flightBytes / 2) (saturatingMul 2 r.mssBytes)
        let cwnd := saturatingAdd threshold (saturatingMul 3 r.mssBytes)
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
        let cwnd := saturatingAdd b.cwndBytes r.mssBytes
        pure { b with
          cwndBytes := cwnd
          duplicateAcks := dup
          cwndScaled := bytesToScaled cwnd r.mssBytes }
      else pure { b with duplicateAcks := dup }
  | .timeout =>
      let threshold := max (r.flightBytes / 2) (saturatingMul 2 r.mssBytes)
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
  if before = 0 then max sample 1
  else saturatingU64 ((before * 7 + max sample 1) / 8)

def cubicRadicand (wMaxScaled : Nat) : Nat :=
  wMaxScaled * 3 * 1000000000000000000 / 4

def checkCubicK (lineNo wMaxScaled kNs : Nat) : Except String Unit := do
  let radicand := cubicRadicand wMaxScaled
  require lineNo (kNs ^ 3 <= radicand && (kNs + 1) ^ 3 > radicand)
    "CUBIC K is not the floor cube root of its exact fixed-point radicand"

def cubicWindow (wMaxScaled kNs elapsedNs : Nat) : Nat :=
  let distance := if elapsedNs < kNs then kNs - elapsedNs else elapsedNs - kNs
  let magnitude := saturatingU64 (distance ^ 3 * 2 / 5000000000000000000)
  if elapsedNs < kNs then max scale (wMaxScaled - magnitude)
  else min maxCubicWindow (max scale (saturatingAdd wMaxScaled magnitude))

def tcpFriendlyWindow (wMaxScaled elapsedNs rttNs : Nat) : Nat :=
  let base := wMaxScaled * 7 / 10
  let growth := saturatingU64 (scale * 9 * elapsedNs / (17 * max rttNs 1))
  min maxCubicWindow (max scale (saturatingAdd base growth))

def cubicAckStep (cwndScaled targetScaled : Nat) : Nat :=
  let denominator := max cwndScaled scale
  let next := if targetScaled >= cwndScaled then
      saturatingAdd cwndScaled
        (saturatingU64 ((targetScaled - cwndScaled) * scale / denominator))
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
          let window := min (saturatingAdd b.cwndScaled (saturatingMul segments scale))
            maxCubicWindow
          let next := setCubicWindow { b with duplicateAcks := 0, srttNs := srtt }
            r.mssBytes window b.ssthreshScaled
          let expected := if window >= b.ssthreshScaled then
              { next with
                phase := .congestionAvoidance
                epochStartNs := encodedEpoch r.timeNs
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
            cubicAckStep b.cwndScaled
              (cubicWindow wMax k (saturatingAdd elapsed (max srtt 1)))
          let expected := setCubicWindow
            { b with
              duplicateAcks := 0
              srttNs := srtt
              epochStartNs := encodedEpoch epoch
              wMaxScaled := wMax
              kNs := k }
            r.mssBytes window b.ssthreshScaled
          require r.srcLine (a = expected) "CUBIC congestion-avoidance transition mismatch"
      | .fastRecovery =>
          let updated := { b with duplicateAcks := 0, srttNs := srtt }
          let expected := if b.recoveryHigh != 0 && acknowledgment >= b.recoveryHigh then
              let window := min maxCubicWindow (max scale b.ssthreshScaled)
              setCubicWindow { updated with phase := .congestionAvoidance, recoveryHigh := 0 }
                r.mssBytes window b.ssthreshScaled
            else updated
          require r.srcLine (a = expected) "CUBIC recovery ACK transition mismatch"
  | .duplicateAck =>
      let recoveryHigh ← requireSome r.srcLine "recovery_high_input" r.recoveryHighInput
      let dup := saturatingSucc b.duplicateAcks
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
            epochStartNs := encodedEpoch r.timeNs
            kNs := a.kNs }
          r.mssBytes reduced threshold
        require r.srcLine (a = expected) "CUBIC fast-retransmit transition mismatch"
      else if dup > 3 && b.phase = .fastRecovery then
        let window := min (saturatingAdd b.cwndScaled scale) maxCubicWindow
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
  lastKey : Option CanonicalEventKey := none
deriving Repr

def checkRowDomain (r : Row) : Except String Unit := do
  let values := [r.timeNs, r.eventOriginNode, r.eventOriginSequence, r.nodeId, r.flowId,
    r.mssBytes, r.flightBytes]
  require r.srcLine (values.all (· <= maxU64)) "row exceeds the fixed u64 domain"
  require r.srcLine (r.eventPhase <= maxU16) "event phase exceeds the fixed u16 domain"
  require r.srcLine (r.ackedBytes.all (· <= maxU64)) "acked_bytes exceeds the fixed u64 domain"
  require r.srcLine (r.rttNs.all (· <= maxU64)) "rtt_ns exceeds the fixed u64 domain"
  require r.srcLine (r.acknowledgment.all (· <= maxU64))
    "acknowledgment exceeds the fixed u64 domain"
  require r.srcLine (r.recoveryHighInput.all (· <= maxU64))
    "recovery_high_input exceeds the fixed u64 domain"

def stepRow (s : ReplayState) (r : Row) : Except String ReplayState := do
  match s.lastKey with
  | none => pure ()
  | some previous =>
      require r.srcLine (eventKeyLt previous (key r)) "global canonical EventKey went backwards"
  checkRowDomain r
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
  let base := match r.algorithm, r.kind with
    | .reno, .newAck => "reno_new_ack"
    | .reno, .duplicateAck => "reno_duplicate_ack"
    | .reno, .timeout => "reno_timeout"
    | .cubic, .newAck => if r.before.phase = .congestionAvoidance
        then "cubic_ca_exact" else "cubic_new_ack_exact"
    | .cubic, .duplicateAck => "cubic_duplicate_ack"
    | .cubic, .timeout => "cubic_timeout"
  let cov := covHit (covTick cov) base
  let cov := match r.algorithm, r.kind with
    | .reno, .duplicateAck =>
        if r.before.duplicateAcks = 2 then covHit cov "reno_fast_recovery_enter"
        else if r.before.phase = .fastRecovery && r.before.duplicateAcks >= 3 then
          covHit cov "reno_recovery_inflation"
        else cov
    | .cubic, .duplicateAck =>
        if r.before.duplicateAcks = 2 then
          covHit (covHit cov "cubic_fast_recovery_enter") "cubic_k_checked"
        else if r.before.phase = .fastRecovery && r.before.duplicateAcks >= 3 then
          covHit cov "cubic_recovery_inflation"
        else cov
    | _, _ => cov
  match r.algorithm, r.kind, r.before.phase, r.acknowledgment with
  | .reno, .newAck, .fastRecovery, some acknowledgment =>
      if r.before.recoveryHigh != 0 && acknowledgment >= r.before.recoveryHigh then
        covHit cov "reno_recovery_full_ack"
      else covHit cov "reno_recovery_partial_ack"
  | .cubic, .newAck, .fastRecovery, some acknowledgment =>
      if r.before.recoveryHigh != 0 && acknowledgment >= r.before.recoveryHigh then
        covHit cov "cubic_recovery_full_ack"
      else covHit cov "cubic_recovery_partial_ack"
  | .cubic, .newAck, .congestionAvoidance, _ =>
      if r.before.epochStartNs.isNone && r.before.wMaxScaled != 0 then
        covHit cov "cubic_k_checked"
      else cov
  | _, _, _, _ => cov

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
  match canonicalizeTcpRows rows with
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
