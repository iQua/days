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

def parseStageKind : String → Except String Collective.StageKind
  | "tcp" => pure .tcp
  | "compute" => pure .compute
  | other => throw s!"invalid stage kind: '{other}'"

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
  /-- Collective identity for a TCP stage; compute-group identity for a compute stage. -/
  collectiveId : Nat
  algorithm : Option Collective.Algorithm
  groupSize : Nat
  declaredTotalBytes : Nat
  rank : Nat
  collectivePhase : Option Collective.Phase
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
  stageKind : Collective.StageKind
  durationNs : Nat
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
        algorithm := ← parseOpt parseAlgorithm (← getField idx fields "algorithm")
        groupSize := ← parseU32 (← getField idx fields "group_size")
        declaredTotalBytes := ← parseU64 (← getField idx fields "declared_total_bytes")
        rank := ← parseU32 (← getField idx fields "rank")
        collectivePhase := ← parseOpt parsePhase (← getField idx fields "collective_phase")
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
        stageKind := ← parseStageKind (← getField idx fields "stage_kind")
        durationNs := ← parseU64 (← getField idx fields "duration_ns")
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

/-- Whether a TCP row is a root stage, which is logged only when a compute stage gates it. -/
def isRoot (row : Row) : Bool :=
  match row.algorithm, row.collectivePhase with
  | some algorithm, some phase => Collective.rootPosition algorithm phase row.step
  | _, _ => false

/-- The inbound prerequisite is complete when there is none or its whole chunk was delivered. -/
def inboundDone (row : Row) (bytes : Nat) : Bool :=
  row.inboundPredecessorFlowId.isNone || bytes = row.inboundPredecessorBytes

/-- Prerequisite transitions shared by TCP and compute stages.

Local completion flips the local flag (TCP: last byte acknowledged; compute: timer fired). An
inbound row advances the receiver's in-order TCP frontier, never raw arrivals, so it may cover
several segments at once but never more than the bytes still undelivered. -/
def checkProgress (row : Row) : Except String Unit := do
  require row.srcLine (row.beforeInboundComplete = inboundDone row row.beforeInboundBytes)
    "before inbound completion flag disagrees with the delivered total"
  require row.srcLine (row.afterInboundComplete = inboundDone row row.afterInboundBytes)
    "after inbound completion flag disagrees with the delivered total"
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
      require row.srcLine
        (row.beforeInboundBytes < row.inboundPredecessorBytes &&
          row.arrivalBytes ≤ row.inboundPredecessorBytes - row.beforeInboundBytes)
        "inbound frontier advance exceeds the undelivered predecessor bytes"
      require row.srcLine
        (row.afterInboundBytes = row.beforeInboundBytes + row.arrivalBytes)
        "inbound arrival byte total mismatch"
  require row.srcLine
    (row.activated = (row.afterLocalComplete && row.afterInboundComplete))
    "collective activated bit disagrees with prerequisite state"

def checkNotReleased (row : Row) : Except String Unit := do
  require row.srcLine (row.afterPacketsEmitted = 0 && row.afterBytesEmitted = 0)
    "nonactivated collective stage has emitted counters"
  require row.srcLine (row.afterStatus = .blocked && row.afterNextTimeNs = 0)
    "nonactivated collective status or deadline mismatch"

def checkTcpRow (row : Row) (algorithm : Collective.Algorithm) (phase : Collective.Phase) :
    Except String Unit := do
  require row.srcLine
    (Collective.legalPosition algorithm phase row.groupSize row.rank row.step)
    "collective algorithm, phase, rank, or step is illegal"
  require row.srcLine (row.declaredTotalBytes > 0)
    "declared collective total must be positive"
  require row.srcLine (row.groupSize ≤ row.declaredTotalBytes)
    "TCP collective declared total must cover every rank"
  require row.srcLine
    (row.packetSizeBytes > 0 && row.intervalNs = 0 && row.durationNs = 0)
    "TCP collective stage requires a positive MSS and no pacing interval or duration"
  let owner := Collective.stageOwner algorithm phase row.groupSize row.rank row.step
  let expectedBounds := Collective.chunkBounds row.declaredTotalBytes row.groupSize owner
  require row.srcLine
    (row.chunkOffsetBytes = expectedBounds.1 && row.chunkBytes = expectedBounds.2)
    "collective chunk does not match EqualRemainderLast"
  require row.srcLine (row.inboundPredecessorBytes = row.chunkBytes)
    "inbound predecessor byte count does not match the stage chunk"
  let root := isRoot row
  if root then
    require row.srcLine
      (row.localPredecessorFlowId.isSome && row.inboundPredecessorFlowId.isNone)
      "a root stage is logged only when a compute stage gates it"
  else
    require row.srcLine
      (row.localPredecessorFlowId.isSome && row.inboundPredecessorFlowId.isSome)
      "a collective progress stage is missing a predecessor"
  -- Only a compute timer (a phase-1 pacing event) completes a root's gate; every other cause is a
  -- phase-0 ACK or data arrival.
  require row.srcLine
    (row.key.phase = if row.cause = .localCompletion && root then 1 else 0)
    "collective progress event phase disagrees with its cause"
  checkProgress row
  if row.activated then
    require row.srcLine
      (Collective.tcpFirstWindow
        row.packetSizeBytes row.chunkBytes row.afterPacketsEmitted row.afterBytesEmitted)
      "first TCP window counters mismatch"
    require row.srcLine (row.afterStatus = .blocked && row.afterNextTimeNs = 0)
      "post-activation status or deadline mismatch"
  else
    checkNotReleased row

def checkComputeRow (row : Row) : Except String Unit := do
  require row.srcLine
    (row.algorithm.isNone && row.collectivePhase.isNone && row.step = 0 &&
      row.declaredTotalBytes = 0 && row.chunkOffsetBytes = 0 && row.chunkBytes = 0 &&
      row.packetSizeBytes = 0 && row.intervalNs = 0)
    "compute stage row carries collective fields"
  require row.srcLine (row.durationNs > 0)
    "compute stage duration must be positive"
  require row.srcLine (row.rank < row.groupSize)
    "compute stage rank is outside its group"
  require row.srcLine row.localPredecessorFlowId.isSome
    "a compute progress stage is missing its local predecessor"
  require row.srcLine
    (row.inboundPredecessorFlowId.isSome = decide (row.inboundPredecessorBytes > 0))
    "compute inbound predecessor and byte count disagree"
  -- After a compute group the local cause is that group's timer (phase 1); after a collective it
  -- is an ACK (phase 0), and inbound delivery is always a phase-0 data arrival.
  require row.srcLine
    (row.key.phase =
      if row.cause = .localCompletion && row.inboundPredecessorFlowId.isNone then 1 else 0)
    "collective progress event phase disagrees with its cause"
  checkProgress row
  if row.activated then
    require row.srcLine (row.key.timeNs + row.durationNs ≤ Collective.maxU64)
      "compute timer deadline exceeds u64"
    require row.srcLine (row.afterPacketsEmitted = 0 && row.afterBytesEmitted = 0)
      "compute stage has emitted counters"
    let expected := Collective.computeTimerAfter row.key.timeNs row.durationNs row.stopTimeNs
    require row.srcLine
      (row.afterStatus = expected.1 && row.afterNextTimeNs = expected.2)
      "compute timer status or deadline mismatch"
  else
    checkNotReleased row

def checkRow (row : Row) : Except String Unit := do
  require row.srcLine (row.key.timeNs ≤ row.stopTimeNs)
    "collective progress occurs after the simulation stop time"
  match row.stageKind, row.algorithm, row.collectivePhase with
  | .tcp, some algorithm, some phase => checkTcpRow row algorithm phase
  | .tcp, _, _ => require row.srcLine false "TCP stage row requires an algorithm and a phase"
  | .compute, _, _ => checkComputeRow row

/-- Two rows belong to the same collective or the same compute group. -/
def sameGroup (first second : Row) : Bool :=
  first.stageKind = second.stageKind && first.collectiveId = second.collectiveId

def sameGroupConfig (first second : Row) : Bool :=
  first.algorithm = second.algorithm &&
    first.groupSize = second.groupSize &&
    first.declaredTotalBytes = second.declaredTotalBytes &&
    first.packetSizeBytes = second.packetSizeBytes &&
    first.intervalNs = second.intervalNs &&
    first.durationNs = second.durationNs &&
    first.stopTimeNs = second.stopTimeNs

def sameStage (first second : Row) : Bool :=
  sameGroup first second &&
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

def position? (row : Row) : Option Position :=
  row.collectivePhase.map fun phase => { phase := phase, rank := row.rank, step := row.step }

def predecessorPhaseStep (row : Row) (phase : Collective.Phase) : Collective.Phase × Nat :=
  if row.step > 1 then
    (phase, row.step - 1)
  else
    (.reduceScatter, row.groupSize - 1)

def localPredecessorPosition (row : Row) (phase : Collective.Phase) : Position :=
  let predecessor := predecessorPhaseStep row phase
  { phase := predecessor.1, rank := row.rank, step := predecessor.2 }

def inboundPredecessorPosition (row : Row) (phase : Collective.Phase) : Position :=
  let predecessor := predecessorPhaseStep row phase
  let previousRank := if row.rank = 0 then row.groupSize - 1 else row.rank - 1
  { phase := predecessor.1, rank := previousRank, step := predecessor.2 }

def atPosition (collective : Row) (wanted : Position) (candidate : Row) : Bool :=
  sameGroup collective candidate && position? candidate = some wanted

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
        sameGroup collective successor && !isRoot successor &&
          (successor.collectivePhase.map (localPredecessorPosition successor ·)) = some wanted
        ).bind (·.localPredecessorFlowId)

def requireEarlierRelease (row : Row) (predecessor : Option Row) (message : String) :
    Except String Unit :=
  match predecessor with
  | none => pure ()
  | some predecessor => require row.srcLine (compositeLT predecessor row) message

def checkTcpPredecessors (rows : List Row) (row : Row) (phase : Collective.Phase) :
    Except String Unit := do
  if isRoot row then
    -- A root's gate is a compute stage, never a stage of its own collective.
    require row.srcLine
      (!rows.any fun candidate =>
        sameGroup row candidate && some candidate.flowId = row.localPredecessorFlowId)
      "a root stage's gate must be a compute stage outside its collective"
    return
  let localPosition := localPredecessorPosition row phase
  let inboundPosition := inboundPredecessorPosition row phase
  match resolveStageFlow rows row localPosition with
  | none => pure ()
  | some expected =>
      require row.srcLine (row.localPredecessorFlowId = some expected)
        "local predecessor identity does not match the stage recurrence"
  match resolveStageFlow rows row inboundPosition with
  | none => pure ()
  | some expected =>
      require row.srcLine (row.inboundPredecessorFlowId = some expected)
        "inbound predecessor identity does not match the stage recurrence"
  match row.cause with
  | .localCompletion =>
      requireEarlierRelease row (findActivatedStage rows row localPosition)
        "local predecessor stage did not activate earlier"
  | .inboundArrival =>
      requireEarlierRelease row (findActivatedStage rows row inboundPosition)
        "inbound predecessor stage did not activate earlier"

/-- A compute stage follows the same-rank stage of a compute group, or the same-rank final stage
of a collective together with the previous rank's final stage. Logged predecessors are checked. -/
def checkComputePredecessors (rows : List Row) (row : Row) : Except String Unit := do
  let previousRank := if row.rank = 0 then row.groupSize - 1 else row.rank - 1
  let isFinal (candidate : Row) (rank : Nat) : Bool :=
    candidate.stageKind = .tcp && candidate.collectivePhase = some .allGather &&
      candidate.step + 1 = candidate.groupSize && candidate.rank = rank &&
      candidate.groupSize = row.groupSize
  let local? := rows.find? fun candidate => some candidate.flowId = row.localPredecessorFlowId
  let inbound? := rows.find? fun candidate => some candidate.flowId = row.inboundPredecessorFlowId
  match local?, row.inboundPredecessorFlowId with
  | some predecessor, none =>
      require row.srcLine
        (predecessor.stageKind = .compute && predecessor.rank = row.rank &&
          predecessor.groupSize = row.groupSize)
        "compute local predecessor is not the same-rank stage of a compute group"
  | some predecessor, some _ =>
      require row.srcLine (isFinal predecessor row.rank)
        "compute local predecessor is not its rank's final collective stage"
  | none, _ => pure ()
  match inbound?, local? with
  | some inbound, some predecessor =>
      require row.srcLine
        (isFinal inbound previousRank && sameGroup inbound predecessor &&
          inbound.chunkBytes = row.inboundPredecessorBytes)
        "compute inbound predecessor is not the previous rank's final collective stage"
  | some inbound, none =>
      require row.srcLine
        (isFinal inbound previousRank && inbound.chunkBytes = row.inboundPredecessorBytes)
        "compute inbound predecessor is not the previous rank's final collective stage"
  | none, _ => pure ()
  match row.cause with
  | .localCompletion =>
      requireEarlierRelease row (local?.filter (·.activated))
        "local predecessor stage did not activate earlier"
  | .inboundArrival =>
      requireEarlierRelease row (inbound?.filter (·.activated))
        "inbound predecessor stage did not activate earlier"

def checkPredecessors (rows : List Row) (row : Row) : Except String Unit :=
  match row.stageKind, row.collectivePhase with
  | .tcp, some phase => checkTcpPredecessors rows row phase
  | .tcp, none => pure ()
  | .compute, _ => checkComputePredecessors rows row

def sameStageConfig (first second : Row) : Bool :=
  first.nodeId = second.nodeId && first.flowId = second.flowId &&
    first.chunkOffsetBytes = second.chunkOffsetBytes &&
    first.chunkBytes = second.chunkBytes &&
    first.localPredecessorFlowId = second.localPredecessorFlowId &&
    first.inboundPredecessorFlowId = second.inboundPredecessorFlowId &&
    first.inboundPredecessorBytes = second.inboundPredecessorBytes

/-- Every logged stage's local predecessor carries bytes or time, so it starts incomplete; the
inbound prerequisite starts complete only when there is none. -/
def checkInitialState (row : Row) : Except String Unit := do
  require row.srcLine (!row.beforeLocalComplete)
    "first local prerequisite state is not initial"
  require row.srcLine
    (row.beforeInboundComplete = row.inboundPredecessorFlowId.isNone &&
      row.beforeInboundBytes = 0)
    "first inbound prerequisite state is not initial"

def checkContinuity (rows : List Row) : Except String Unit := do
  let rec go (previous : List Row) : List Row → Except String Unit
    | [] => pure ()
    | row :: rest => do
        match previous.find? (sameGroup · row) with
        | none => pure ()
        | some prior =>
            require row.srcLine (sameGroupConfig prior row)
              s!"collective configuration discontinuity for collective_id={row.collectiveId}"
        for prior in previous do
          if sameGroup prior row then
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

/-- A complete trace releases every logged stage exactly once and leaves it complete. A TCP
collective logs its non-root stages, plus its roots when compute stages gate them; a compute
group logs one stage per rank. -/
def checkCoverage (rows : List Row) : Except String Unit := do
  for row in rows do
    let group := rows.filter (sameGroup row ·)
    let expected :=
      match row.stageKind, row.algorithm with
      | .tcp, some algorithm =>
          Collective.expectedActivationCount algorithm row.groupSize (group.any isRoot)
      | _, _ => row.groupSize
    require row.srcLine (distinctStageCount group = expected)
      s!"incomplete collective progress coverage for collective_id={row.collectiveId}: expected {expected}, found {distinctStageCount group}"
    let final? := rows.reverse.find? (sameStage · row)
    let final ← requireSome row.srcLine "final collective stage progress" final?
    require row.srcLine
      (final.afterLocalComplete && final.afterInboundComplete &&
        inboundDone final final.afterInboundBytes)
      "collective stage final prerequisite state is incomplete"
    let activated := group.filter (·.activated)
    require row.srcLine (activated.length = expected)
      s!"incomplete collective activation coverage for collective_id={row.collectiveId}: expected {expected}, found {activated.length}"

def checkRows (rows : List Row) : Except String Unit := do
  require 1 (!rows.isEmpty) "empty collective activation trace"
  let canonical ← canonicalize rows
  checkOrdinals canonical
  checkContinuity canonical
  for row in canonical do checkRow row
  checkCoverage canonical
  for row in canonical do checkPredecessors canonical row

end LeanGuard.P10c.CollectiveEventLog
