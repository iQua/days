import DaysExecutor.Event
import LeanGuard.P10c.Collective.Semantics
import LeanGuard.Shared.Check
import LeanGuard.Shared.Csv

namespace LeanGuard.P10c.CollectiveEventLog

open LeanGuard.Shared
open LeanGuard.P10c

def parseAlgorithm : String → Except String Collective.Algorithm
  | "allgather" => pure .allGather
  | "ring_allreduce" => pure .ringAllReduce
  | other => throw s!"invalid collective algorithm: '{other}'"

def parsePhase : String → Except String Collective.Phase
  | "reduce_scatter" => pure .reduceScatter
  | "allgather" => pure .allGather
  | other => throw s!"invalid collective phase: '{other}'"

def parseCause : String → Except String Collective.Cause
  | "local_completion" => pure .localCompletion
  | "inbound_arrival" => pure .inboundArrival
  | other => throw s!"invalid collective activation cause: '{other}'"

def parseStatus : String → Except String Collective.Status
  | "blocked" => pure .blocked
  | "scheduled" => pure .scheduled
  | "finished" => pure .finished
  | "stopped" => pure .stopped
  | other => throw s!"invalid post-activation status: '{other}'"

def parseBit (value : String) : Except String Bool :=
  match value with
  | "0" => pure false
  | "1" => pure true
  | other => throw s!"invalid bit: '{other}'"

def parseBounded (kind : String) (maximum : Nat) (value : String) : Except String Nat := do
  let parsed ← parseNat value
  if parsed ≤ maximum then
    pure parsed
  else
    throw s!"value exceeds {kind}: '{value}'"

def parseU16 (value : String) : Except String Nat :=
  parseBounded "u16" Collective.maxU16 value

def parseU32 (value : String) : Except String Nat :=
  parseBounded "u32" Collective.maxU32 value

def parseU64 (value : String) : Except String Nat :=
  parseBounded "u64" Collective.maxU64 value

def parseOptU64 (value : String) : Except String (Option Nat) :=
  parseOpt parseU64 value

structure Row where
  key : DaysExecutor.EventKey
  ordinal : Nat
  nodeId : Nat
  flowId : Nat
  cause : Collective.Cause
  causeFlowId : Nat
  arrivalBytes : Nat
  collectiveId : Nat
  algorithm : Collective.Algorithm
  groupSize : Nat
  declaredTotalBytes : Nat
  rank : Nat
  collectivePhase : Collective.Phase
  step : Nat
  chunkOffsetBytes : Nat
  chunkBytes : Nat
  packetSizeBytes : Nat
  intervalNs : Nat
  stopTimeNs : Nat
  localPredecessorFlowId : Option Nat
  inboundPredecessorFlowId : Option Nat
  inboundPredecessorBytes : Nat
  beforeLocalComplete : Bool
  beforeInboundComplete : Bool
  beforeInboundBytes : Nat
  activated : Bool
  afterLocalComplete : Bool
  afterInboundComplete : Bool
  afterInboundBytes : Nat
  afterPacketsEmitted : Nat
  afterBytesEmitted : Nat
  afterStatus : Collective.Status
  afterNextTimeNs : Nat
  srcLine : Nat
  deriving DecidableEq, Repr

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let result : Except String Row := do
    pure
      { key :=
          { timeNs := ← parseU64 (← getField idx fields "time_ns")
            phase := ← parseU16 (← getField idx fields "event_phase")
            originNode := ← parseU64 (← getField idx fields "event_origin_node")
            originSeq := ← parseU64 (← getField idx fields "event_origin_sequence") }
        ordinal := ← parseU64 (← getField idx fields "ordinal")
        nodeId := ← parseU64 (← getField idx fields "node_id")
        flowId := ← parseU64 (← getField idx fields "flow_id")
        cause := ← parseCause (← getField idx fields "cause")
        causeFlowId := ← parseU64 (← getField idx fields "cause_flow_id")
        arrivalBytes := ← parseU64 (← getField idx fields "arrival_bytes")
        collectiveId := ← parseU64 (← getField idx fields "collective_id")
        algorithm := ← parseAlgorithm (← getField idx fields "algorithm")
        groupSize := ← parseU32 (← getField idx fields "group_size")
        declaredTotalBytes := ← parseU64 (← getField idx fields "declared_total_bytes")
        rank := ← parseU32 (← getField idx fields "rank")
        collectivePhase := ← parsePhase (← getField idx fields "collective_phase")
        step := ← parseU32 (← getField idx fields "step")
        chunkOffsetBytes := ← parseU64 (← getField idx fields "chunk_offset_bytes")
        chunkBytes := ← parseU64 (← getField idx fields "chunk_bytes")
        packetSizeBytes := ← parseU64 (← getField idx fields "packet_size_bytes")
        intervalNs := ← parseU64 (← getField idx fields "interval_ns")
        stopTimeNs := ← parseU64 (← getField idx fields "stop_time_ns")
        localPredecessorFlowId :=
          ← parseOptU64 (← getField idx fields "local_predecessor_flow_id")
        inboundPredecessorFlowId :=
          ← parseOptU64 (← getField idx fields "inbound_predecessor_flow_id")
        inboundPredecessorBytes :=
          ← parseU64 (← getField idx fields "inbound_predecessor_bytes")
        beforeLocalComplete := ← parseBit (← getField idx fields "before_local_complete")
        beforeInboundComplete := ← parseBit (← getField idx fields "before_inbound_complete")
        beforeInboundBytes := ← parseU64 (← getField idx fields "before_inbound_bytes")
        activated := ← parseBit (← getField idx fields "activated")
        afterLocalComplete := ← parseBit (← getField idx fields "after_local_complete")
        afterInboundComplete := ← parseBit (← getField idx fields "after_inbound_complete")
        afterInboundBytes := ← parseU64 (← getField idx fields "after_inbound_bytes")
        afterPacketsEmitted := ← parseU64 (← getField idx fields "after_packets_emitted")
        afterBytesEmitted := ← parseU64 (← getField idx fields "after_bytes_emitted")
        afterStatus := ← parseStatus (← getField idx fields "after_status")
        afterNextTimeNs := ← parseU64 (← getField idx fields "after_next_time_ns")
        srcLine := lineNo }
  match result with
  | .ok row => pure row
  | .error error => throw s!"line {lineNo}: {error}"

def parseCsv (content : String) : Except String (List Row) := do
  let lines :=
    content.splitOn "\n" |>.map stripCR |>.map String.trim |>.filter (· != "")
  match lines with
  | [] => throw "empty CSV"
  | header :: data =>
      let idx := mkIndex (splitCsvLine header)
      let rec go (lineNo : Nat) (remaining : List String) (rows : List Row) := do
        match remaining with
        | [] => pure rows.reverse
        | line :: rest =>
            let row ← parseRow lineNo idx (splitCsvLine line).toArray
            go (lineNo + 1) rest (row :: rows)
      go 2 data []

def compositeLT (first second : Row) : Bool :=
  decide
    (first.key < second.key ∨
      (first.key = second.key ∧ first.ordinal < second.ordinal))

/-- Order certificates by the scalar event key and activation-cascade ordinal. -/
def canonicalize (rows : List Row) : Except String (List Row) := do
  let sorted := rows.toArray.qsort compositeLT |>.toList
  let rec check : List Row → Except String Unit
    | [] | [_] => pure ()
    | first :: second :: rest => do
        require second.srcLine (compositeLT first second)
          "duplicate canonical event key and activation ordinal"
        check (second :: rest)
  check sorted
  pure sorted

def checkOrdinals : List Row → Except String Unit
  | [] => pure ()
  | first :: rest => do
      require first.srcLine (first.ordinal = 0)
        "first activation ordinal for an event key must be zero"
      let rec go (previous : Row) : List Row → Except String Unit
        | [] => pure ()
        | row :: tail => do
            if previous.key = row.key then
              require row.srcLine (row.ordinal = previous.ordinal + 1)
                "activation ordinals for an event key must be contiguous"
            else
              require row.srcLine (row.ordinal = 0)
                "first activation ordinal for an event key must be zero"
            go row tail
      go first rest

def checkProgress (row : Row) : Except String Unit := do
  require row.srcLine
    (row.beforeInboundComplete = decide (row.beforeInboundBytes = row.inboundPredecessorBytes))
    "before inbound completion flag disagrees with the exact arrival total"
  require row.srcLine
    (row.afterInboundComplete = decide (row.afterInboundBytes = row.inboundPredecessorBytes))
    "after inbound completion flag disagrees with the exact arrival total"
  match row.cause with
  | .localCompletion =>
      require row.srcLine
        (row.localPredecessorFlowId = some row.causeFlowId)
        "local completion cause does not match the local predecessor"
      require row.srcLine (row.arrivalBytes = 0)
        "local completion must not carry arrival bytes"
      require row.srcLine (!row.beforeLocalComplete && row.afterLocalComplete)
        "local completion must change the local prerequisite from incomplete to complete"
      require row.srcLine
        (row.afterInboundComplete = row.beforeInboundComplete &&
          row.afterInboundBytes = row.beforeInboundBytes)
        "local completion changed inbound prerequisite state"
  | .inboundArrival =>
      require row.srcLine
        (row.inboundPredecessorFlowId = some row.causeFlowId)
        "inbound arrival cause does not match the inbound predecessor"
      require row.srcLine (row.arrivalBytes > 0)
        "inbound arrival must carry positive bytes"
      require row.srcLine (row.afterLocalComplete = row.beforeLocalComplete)
        "inbound arrival changed the local prerequisite"
      require row.srcLine (!row.beforeInboundComplete)
        "inbound arrival followed an already complete predecessor chunk"
      require row.srcLine
        (row.beforeInboundBytes + row.arrivalBytes ≤ Collective.maxU64)
        "inbound arrival byte counter exceeds u64"
      let remaining := row.inboundPredecessorBytes - row.beforeInboundBytes
      require row.srcLine
        (row.beforeInboundBytes < row.inboundPredecessorBytes &&
          row.arrivalBytes = min row.packetSizeBytes remaining)
        "inbound arrival size does not match the next predecessor packet"
      require row.srcLine
        (row.afterInboundBytes = row.beforeInboundBytes + row.arrivalBytes)
        "inbound arrival byte total mismatch"
  let expectedActivated :=
    row.chunkBytes > 0 && row.afterLocalComplete && row.afterInboundComplete
  require row.srcLine (row.activated = expectedActivated)
    "collective activated bit disagrees with prerequisite state"
  if row.activated then
    let first := Collective.firstPacketBytes row.packetSizeBytes row.chunkBytes
    let remaining := row.chunkBytes - first
    if remaining > 0 then
      require row.srcLine (row.key.timeNs + row.intervalNs ≤ Collective.maxU64)
        "next collective emission deadline exceeds u64"
    let expected :=
      Collective.activationAfter
        row.key.timeNs row.packetSizeBytes row.chunkBytes row.intervalNs row.stopTimeNs
    require row.srcLine
      (row.afterPacketsEmitted = expected.packetsEmitted &&
        row.afterBytesEmitted = expected.bytesEmitted)
      "first collective packet counters mismatch"
    require row.srcLine
      (row.afterStatus = expected.status && row.afterNextTimeNs = expected.nextTimeNs)
      "post-activation status or deadline mismatch"
  else
    require row.srcLine
      (row.chunkBytes = 0 || !(row.afterLocalComplete && row.afterInboundComplete))
      "nonactivated nonempty stage has both prerequisites complete"
    require row.srcLine
      (row.afterPacketsEmitted = 0 && row.afterBytesEmitted = 0)
      "nonactivated collective stage has emitted counters"
    let expectedStatus :=
      if row.chunkBytes = 0 then Collective.Status.finished else Collective.Status.blocked
    require row.srcLine
      (row.afterStatus = expectedStatus && row.afterNextTimeNs = 0)
      "nonactivated collective status or deadline mismatch"

def checkRingDomain (row : Row) : Except String Unit := do
  if row.algorithm = .ringAllReduce then
    require row.srcLine (row.groupSize ≤ row.declaredTotalBytes)
      "RingAllReduce declared total must cover every rank"

def checkRow (row : Row) : Except String Unit := do
  require row.srcLine (row.key.phase = 0)
    "collective activation certificate must have event phase 0"
  require row.srcLine
    (Collective.legalPosition
      row.algorithm row.collectivePhase row.groupSize row.rank row.step)
    "collective algorithm, phase, rank, or step is illegal"
  require row.srcLine
    (Collective.loggedActivationPosition row.algorithm row.collectivePhase row.step)
    "collective root stage cannot appear in the activation log"
  require row.srcLine (row.declaredTotalBytes > 0)
    "declared collective total must be positive"
  require row.srcLine (row.key.timeNs ≤ row.stopTimeNs)
    "collective progress occurs after the simulation stop time"
  require row.srcLine (row.packetSizeBytes > 0 && row.intervalNs > 0)
    "collective packet size and interval must be positive"
  let owner :=
    Collective.stageOwner
      row.algorithm row.collectivePhase row.groupSize row.rank row.step
  let expectedBounds :=
    Collective.chunkBounds row.declaredTotalBytes row.groupSize owner
  require row.srcLine
    (row.chunkOffsetBytes = expectedBounds.1 && row.chunkBytes = expectedBounds.2)
    "collective chunk does not match EqualRemainderLast"
  require row.srcLine (row.inboundPredecessorBytes = row.chunkBytes)
    "inbound predecessor byte count does not match the stage chunk"
  require row.srcLine
    (row.localPredecessorFlowId.isSome && row.inboundPredecessorFlowId.isSome)
    "a collective progress stage is missing a predecessor"
  checkProgress row

def sameCollective (first second : Row) : Bool :=
  first.collectiveId = second.collectiveId

def sameCollectiveConfig (first second : Row) : Bool :=
  first.algorithm = second.algorithm &&
    first.groupSize = second.groupSize &&
    first.declaredTotalBytes = second.declaredTotalBytes &&
    first.packetSizeBytes = second.packetSizeBytes &&
    first.intervalNs = second.intervalNs &&
    first.stopTimeNs = second.stopTimeNs

def sameStage (first second : Row) : Bool :=
  sameCollective first second &&
    first.collectivePhase = second.collectivePhase &&
    first.rank = second.rank && first.step = second.step

def distinctStageCount : List Row → Nat
  | [] => 0
  | row :: rest =>
      if rest.any (sameStage row ·) then
        distinctStageCount rest
      else
        distinctStageCount rest + 1

structure Position where
  phase : Collective.Phase
  rank : Nat
  step : Nat
  deriving DecidableEq, Repr

def position (row : Row) : Position :=
  { phase := row.collectivePhase, rank := row.rank, step := row.step }

def predecessorPhaseStep (row : Row) : Collective.Phase × Nat :=
  if row.step > 1 then
    (row.collectivePhase, row.step - 1)
  else
    (.reduceScatter, row.groupSize - 1)

def localPredecessorPosition (row : Row) : Position :=
  let predecessor := predecessorPhaseStep row
  { phase := predecessor.1, rank := row.rank, step := predecessor.2 }

def inboundPredecessorPosition (row : Row) : Position :=
  let predecessor := predecessorPhaseStep row
  let previousRank := if row.rank = 0 then row.groupSize - 1 else row.rank - 1
  { phase := predecessor.1, rank := previousRank, step := predecessor.2 }

def chunkBytesAt (row : Row) (wanted : Position) : Nat :=
  let owner :=
    Collective.stageOwner row.algorithm wanted.phase row.groupSize wanted.rank wanted.step
  (Collective.chunkBounds row.declaredTotalBytes row.groupSize owner).2

def atPosition (collective : Row) (wanted : Position) (candidate : Row) : Bool :=
  sameCollective collective candidate && position candidate = wanted

def findStage (rows : List Row) (collective : Row) (wanted : Position) : Option Row :=
  rows.find? (atPosition collective wanted)

def findActivatedStage (rows : List Row) (collective : Row) (wanted : Position) : Option Row :=
  rows.find? fun candidate => atPosition collective wanted candidate && candidate.activated

/-- Resolve a stage's flow, including an unlogged root via its local successor edge. -/
def resolveStageFlow (rows : List Row) (collective : Row) (wanted : Position) : Option Nat :=
  match findStage rows collective wanted with
  | some stage => some stage.flowId
  | none =>
      (rows.find? fun successor =>
        sameCollective collective successor &&
          localPredecessorPosition successor = wanted).bind (·.localPredecessorFlowId)

def checkPredecessors (rows : List Row) (row : Row) : Except String Unit := do
  let localPosition := localPredecessorPosition row
  let inboundPosition := inboundPredecessorPosition row
  let expectedLocal := resolveStageFlow rows row localPosition
  let expectedInbound := resolveStageFlow rows row inboundPosition
  match expectedLocal with
  | none => do
      require row.srcLine row.localPredecessorFlowId.isSome
        "local predecessor identity is missing"
  | some expected => do
      require row.srcLine (row.localPredecessorFlowId = some expected)
        "local predecessor identity does not match the stage recurrence"
  match expectedInbound with
  | none => do
      require row.srcLine row.inboundPredecessorFlowId.isSome
        "inbound predecessor identity is missing"
  | some expected => do
      require row.srcLine (row.inboundPredecessorFlowId = some expected)
        "inbound predecessor identity does not match the stage recurrence"
  match row.cause with
  | .localCompletion =>
      match findActivatedStage rows row localPosition with
      | none => pure ()
      | some predecessor =>
          require row.srcLine (compositeLT predecessor row)
            "local predecessor stage did not activate earlier"
  | .inboundArrival =>
      match findActivatedStage rows row inboundPosition with
      | none => pure ()
      | some predecessor =>
          require row.srcLine (compositeLT predecessor row)
            "inbound predecessor stage did not activate earlier"

def sameStageConfig (first second : Row) : Bool :=
  first.nodeId = second.nodeId && first.flowId = second.flowId &&
    first.chunkOffsetBytes = second.chunkOffsetBytes &&
    first.chunkBytes = second.chunkBytes &&
    first.localPredecessorFlowId = second.localPredecessorFlowId &&
    first.inboundPredecessorFlowId = second.inboundPredecessorFlowId &&
    first.inboundPredecessorBytes = second.inboundPredecessorBytes

def checkInitialState (row : Row) : Except String Unit := do
  let localInitiallyComplete := chunkBytesAt row (localPredecessorPosition row) = 0
  require row.srcLine
    (row.beforeLocalComplete = decide localInitiallyComplete)
    "first local prerequisite state does not match the predecessor chunk"
  require row.srcLine
    (row.beforeInboundComplete = decide (row.chunkBytes = 0) && row.beforeInboundBytes = 0)
    "first inbound prerequisite state is not initial"

def checkContinuity (rows : List Row) : Except String Unit := do
  let rec go (previous : List Row) : List Row → Except String Unit
    | [] => pure ()
    | row :: rest => do
        match previous.find? (sameCollective · row) with
        | none => pure ()
        | some prior =>
            require row.srcLine (sameCollectiveConfig prior row)
              s!"collective configuration discontinuity for collective_id={row.collectiveId}"
        for prior in previous do
          if sameCollective prior row then
            require row.srcLine
              ((prior.rank = row.rank) = (prior.nodeId = row.nodeId))
              "collective rank-to-node mapping is inconsistent"
          if prior.flowId = row.flowId then
            require row.srcLine (sameStage prior row)
              "collective flow identity changed stage position"
        match previous.find? (sameStage · row) with
        | none => checkInitialState row
        | some prior =>
            require row.srcLine (sameStageConfig prior row)
              "collective stage configuration changed during progress"
            require row.srcLine
              (row.beforeLocalComplete = prior.afterLocalComplete &&
                row.beforeInboundComplete = prior.afterInboundComplete &&
                row.beforeInboundBytes = prior.afterInboundBytes)
              "collective stage before-state does not continue the prior after-state"
        require row.srcLine
          (!previous.any (fun prior => sameStage prior row && prior.activated && row.activated))
          "collective stage activated more than once"
        go (row :: previous) rest
  go [] rows

/-- A trace must activate every nonempty non-root stage exactly once. -/
def checkCoverage (rows : List Row) : Except String Unit := do
  for row in rows do
    require row.srcLine
      (row.chunkBytes > 0 || chunkBytesAt row (localPredecessorPosition row) > 0)
      "collective stage has no prerequisite progress to log"
    let sameProgress := rows.filter (sameCollective row ·)
    let expectedProgress :=
      Collective.expectedProgressStageCount
        row.algorithm row.groupSize row.declaredTotalBytes
    require row.srcLine (distinctStageCount sameProgress = expectedProgress)
      s!"incomplete collective progress coverage for collective_id={row.collectiveId}: expected {expectedProgress}, found {distinctStageCount sameProgress}"
    let final? := rows.reverse.find? (sameStage · row)
    let final ← requireSome row.srcLine "final collective stage progress" final?
    require row.srcLine
      (final.afterLocalComplete && final.afterInboundComplete &&
        final.afterInboundBytes = final.inboundPredecessorBytes)
      "collective stage final prerequisite state is incomplete"
    let activated := rows.filter (fun candidate => sameCollective row candidate && candidate.activated)
    let expected :=
      Collective.expectedActivationCount
        row.algorithm row.groupSize row.declaredTotalBytes
    require row.srcLine (activated.length = expected)
      s!"incomplete collective activation coverage for collective_id={row.collectiveId}: expected {expected}, found {activated.length}"

def checkRows (rows : List Row) : Except String Unit := do
  require 1 (!rows.isEmpty) "empty collective activation trace"
  let canonical ← canonicalize rows
  checkOrdinals canonical
  for row in canonical do checkRingDomain row
  checkContinuity canonical
  for row in canonical do checkRow row
  checkCoverage canonical
  for row in canonical do checkPredecessors canonical row

end LeanGuard.P10c.CollectiveEventLog
