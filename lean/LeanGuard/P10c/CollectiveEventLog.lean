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
  | "roce" => pure .roce
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
  /-- Collective identity for a transport stage; compute-group identity for a compute stage. -/
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
  /-- Inbound rows: the arriving TCP segment or RoCE packet
  `[segmentSequence, segmentSequence + segmentBytes)` (a RoCE PSN is a byte offset). -/
  segmentSequence : Nat
  segmentBytes : Nat
  /-- Local rows caused by a transport stage: the completing ACK's cumulative acknowledgment. -/
  ackNumber : Nat
  /-- Local rows: TCP or RoCE, when the answered segment was sent; compute, when the timer was
  armed. -/
  causeOriginNs : Nat
  /-- Local rows: TCP or RoCE, the unloaded round trip of that segment and its ACK; compute, the
  duration. -/
  causeDelayNs : Nat
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
        segmentSequence := ← parseU64 (← getField idx fields "segment_sequence")
        segmentBytes := ← parseU64 (← getField idx fields "segment_bytes")
        ackNumber := ← parseU64 (← getField idx fields "ack_number")
        causeOriginNs := ← parseU64 (← getField idx fields "cause_origin_ns")
        causeDelayNs := ← parseU64 (← getField idx fields "cause_delay_ns")
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

/-- Whether a transport row is a root stage, which is logged only when a compute stage gates it. -/
def isRoot (row : Row) : Bool :=
  match row.algorithm, row.collectivePhase with
  | some algorithm, some phase => Collective.rootPosition algorithm phase row.step
  | _, _ => false

/-- The inbound prerequisite is complete when there is none or its whole chunk was delivered. -/
def inboundDone (row : Row) (bytes : Nat) : Bool :=
  row.inboundPredecessorFlowId.isNone || bytes = row.inboundPredecessorBytes

/-- Prerequisite transitions shared by transport and compute stages.

Local completion flips the local flag (TCP or RoCE: last byte acknowledged; compute: timer
fired). An inbound row certifies one arriving segment of the inbound predecessor and the
resulting advance of the receiver's in-order frontier, which may be zero (duplicate or
out-of-order segment) or, for TCP only, cover several segments at once (a filled hole).
`checkInboundReplay` replays the frontier by the predecessor's transport; `checkLocalSignal`
binds local completions to their cause. -/
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
      require row.srcLine (row.segmentSequence = 0 && row.segmentBytes = 0)
        "local completion row carries an inbound segment"
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
      require row.srcLine
        (row.segmentBytes > 0 &&
          row.segmentSequence + row.segmentBytes ≤ row.inboundPredecessorBytes)
        "inbound segment is empty or extends past the predecessor chunk"
      require row.srcLine
        (row.ackNumber = 0 && row.causeOriginNs = 0 && row.causeDelayNs = 0)
        "inbound row carries local completion fields"
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

/-- A collective stage carried by TCP or by a RoCE queue pair. The two transports share the
partition, the predecessors, the event phases and the progress rules; they differ in the
transport columns and in what a release writes (Amendment 4). -/
def checkTransportRow (row : Row) (algorithm : Collective.Algorithm) (phase : Collective.Phase) :
    Except String Unit := do
  let tcp := row.stageKind = .tcp
  require row.srcLine
    (Collective.legalPosition algorithm phase row.groupSize row.rank row.step)
    "collective algorithm, phase, rank, or step is illegal"
  require row.srcLine (row.declaredTotalBytes > 0)
    "declared collective total must be positive"
  require row.srcLine (row.groupSize ≤ row.declaredTotalBytes)
    (if tcp then "TCP collective declared total must cover every rank"
      else "RoCE collective declared total must cover every rank")
  if tcp then
    require row.srcLine
      (row.packetSizeBytes > 0 && row.intervalNs = 0 && row.durationNs = 0)
      "TCP collective stage requires a positive MSS and no pacing interval or duration"
  else
    require row.srcLine
      (row.packetSizeBytes > 0 && row.intervalNs > 0 && row.durationNs = 0)
      "RoCE collective stage requires a positive MTU and pacing interval and no duration"
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
    if tcp then
      require row.srcLine
        (Collective.tcpFirstWindow
          row.packetSizeBytes row.chunkBytes row.afterPacketsEmitted row.afterBytesEmitted)
        "first TCP window counters mismatch"
      require row.srcLine (row.afterStatus = .blocked && row.afterNextTimeNs = 0)
        "post-activation status or deadline mismatch"
    else
      require row.srcLine (row.afterPacketsEmitted = 0 && row.afterBytesEmitted = 0)
        "RoCE stage release has emitted counters: its first pacing tick is at the release instant"
      require row.srcLine
        (Collective.roceRelease row.key.timeNs row.afterStatus row.afterNextTimeNs
          row.afterPacketsEmitted row.afterBytesEmitted)
        "RoCE stage release status or first pacing tick mismatch"
  else
    checkNotReleased row

def checkComputeRow (row : Row) : Except String Unit := do
  require row.srcLine
    (row.algorithm.isNone && row.collectivePhase.isNone && row.step = 0 &&
      row.declaredTotalBytes = 0 && row.chunkOffsetBytes = 0 && row.chunkBytes = 0)
    "compute stage row carries collective fields"
  -- Amendment 5: a compute stage whose inbound predecessor is a RoCE stage carries that queue
  -- pair's MTU and pacing interval; every other compute stage writes zero in both.
  require row.srcLine (decide (row.packetSizeBytes = 0) = decide (row.intervalNs = 0))
    "compute stage inbound transport columns must be both zero or both positive (Amendment 5)"
  require row.srcLine (row.packetSizeBytes = 0 || row.inboundPredecessorFlowId.isSome)
    "compute stage without an inbound predecessor carries inbound transport columns"
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
  | .tcp, some algorithm, some phase | .roce, some algorithm, some phase =>
      checkTransportRow row algorithm phase
  | .tcp, _, _ => require row.srcLine false "TCP stage row requires an algorithm and a phase"
  | .roce, _, _ => require row.srcLine false "RoCE stage row requires an algorithm and a phase"
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

structure Position where
  phase : Collective.Phase
  rank : Nat
  step : Nat
  deriving DecidableEq, Repr

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

/-- A row's group (`sameGroup`) as a hash key. -/
abbrev GroupId := Collective.StageKind × Nat

/-- A row's stage (`sameStage`) as a hash key: its group and its position. -/
abbrev StageId := Collective.StageKind × Nat × Option Collective.Phase × Nat × Nat

def groupId (row : Row) : GroupId := (row.stageKind, row.collectiveId)

def stageId (row : Row) : StageId :=
  (row.stageKind, row.collectiveId, row.collectivePhase, row.rank, row.step)

/-- The stage of `collective`'s group at `wanted`, as a hash key. -/
def positionId (collective : Row) (wanted : Position) : StageId :=
  (collective.stageKind, collective.collectiveId, some wanted.phase, wanted.rank, wanted.step)

/-- Insert unless present, so a map built in canonical order holds each key's first row. -/
def insertFirst {α β : Type} [BEq α] [Hashable α] (map : Std.HashMap α β) (key : α) (value : β) :
    Std.HashMap α β :=
  if map.contains key then map else map.insert key value

/-! ## Keyed lookups

Each lookup of the predecessor and signal checks returns the first row of the canonical trace that
satisfies one predicate, as `List.find?` over the whole trace did (the list-scan forms are kept,
test-only, in `LeanGuard/P10c/Test/CollectiveReference.lean`). `lookupIndex` builds one map per
predicate in a single pass, inserting a key only at its first matching row, so every lookup returns
exactly the row the scan returned, and the checks that use them are unchanged. -/

structure LookupIndex where
  /-- The first row of each flow (`find? (some ·.flowId = id)`). -/
  byFlow : Std.HashMap Nat Row := ∅
  /-- The first activated row of each flow. -/
  activatedByFlow : Std.HashMap Nat Row := ∅
  /-- The first activated compute row of each flow. -/
  activatedComputeByFlow : Std.HashMap Nat Row := ∅
  /-- The first row at each stage (`atPosition`, for positioned rows). -/
  byStage : Std.HashMap StageId Row := ∅
  /-- The first activated row at each stage. -/
  activatedByStage : Std.HashMap StageId Row := ∅
  /-- For each stage, the local predecessor flow of the first non-root row of its group whose
  local predecessor position it is (the successor edge that resolves an unlogged root). -/
  successorLocal : Std.HashMap StageId (Option Nat) := ∅
  /-- The (group, flow) pairs of the trace. -/
  groupFlows : Std.HashSet (GroupId × Nat) := ∅
  /-- For each flow, the inbound byte count of the first row whose inbound predecessor it is. -/
  receivedBytes : Std.HashMap Nat Nat := ∅
  /-- For each flow, the first inbound row that completes its delivery at its receiver. -/
  delivered : Std.HashMap Nat Row := ∅

def lookupIndex (rows : List Row) : LookupIndex := Id.run do
  let mut index : LookupIndex := {}
  for row in rows do
    index := { index with
      byFlow := insertFirst index.byFlow row.flowId row
      byStage := insertFirst index.byStage (stageId row) row
      groupFlows := index.groupFlows.insert (groupId row, row.flowId) }
    if row.activated then
      index := { index with
        activatedByFlow := insertFirst index.activatedByFlow row.flowId row
        activatedByStage := insertFirst index.activatedByStage (stageId row) row }
      if row.stageKind = .compute then
        index := { index with
          activatedComputeByFlow := insertFirst index.activatedComputeByFlow row.flowId row }
    if let some phase := row.collectivePhase then
      if !isRoot row then
        index := { index with
          successorLocal := insertFirst index.successorLocal
            (positionId row (localPredecessorPosition row phase)) row.localPredecessorFlowId }
    if let some predecessor := row.inboundPredecessorFlowId then
      index := { index with
        receivedBytes := insertFirst index.receivedBytes predecessor row.inboundPredecessorBytes }
    if row.cause = .inboundArrival && row.afterInboundComplete && !row.beforeInboundComplete then
      index := { index with delivered := insertFirst index.delivered row.causeFlowId row }
  pure index

/-- The first row of a flow named by an optional identity. -/
def LookupIndex.flow? (index : LookupIndex) (flowId : Option Nat) : Option Row :=
  flowId.bind index.byFlow.get?

/-- Resolve a stage's flow, including an unlogged root via its local successor edge. -/
def resolveStageFlow (index : LookupIndex) (collective : Row) (wanted : Position) : Option Nat :=
  let key := positionId collective wanted
  match index.byStage.get? key with
  | some stage => some stage.flowId
  | none => (index.successorLocal.get? key).bind id

def requireEarlierRelease (row : Row) (predecessor : Option Row) (message : String) :
    Except String Unit :=
  match predecessor with
  | none => pure ()
  | some predecessor => require row.srcLine (compositeLT predecessor row) message

def checkTransportPredecessors (index : LookupIndex) (row : Row) (phase : Collective.Phase) :
    Except String Unit := do
  if isRoot row then
    -- A root's gate is a compute stage, never a stage of its own collective.
    require row.srcLine
      (!row.localPredecessorFlowId.any fun gate => index.groupFlows.contains (groupId row, gate))
      "a root stage's gate must be a compute stage outside its collective"
    return
  let localPosition := localPredecessorPosition row phase
  let inboundPosition := inboundPredecessorPosition row phase
  match resolveStageFlow index row localPosition with
  | none => pure ()
  | some expected =>
      require row.srcLine (row.localPredecessorFlowId = some expected)
        "local predecessor identity does not match the stage recurrence"
  match resolveStageFlow index row inboundPosition with
  | none => pure ()
  | some expected =>
      require row.srcLine (row.inboundPredecessorFlowId = some expected)
        "inbound predecessor identity does not match the stage recurrence"
  match row.cause with
  | .localCompletion =>
      requireEarlierRelease row (index.activatedByStage.get? (positionId row localPosition))
        "local predecessor stage did not activate earlier"
  | .inboundArrival =>
      requireEarlierRelease row (index.activatedByStage.get? (positionId row inboundPosition))
        "inbound predecessor stage did not activate earlier"

/-- A compute stage follows the same-rank stage of a compute group, or the same-rank final stage
of a collective together with the previous rank's final stage. Logged predecessors are checked. -/
def checkComputePredecessors (index : LookupIndex) (row : Row) : Except String Unit := do
  let previousRank := if row.rank = 0 then row.groupSize - 1 else row.rank - 1
  let isFinal (candidate : Row) (rank : Nat) : Bool :=
    candidate.stageKind.isTransport && candidate.collectivePhase = some .allGather &&
      candidate.step + 1 = candidate.groupSize && candidate.rank = rank &&
      candidate.groupSize = row.groupSize
  let local? := index.flow? row.localPredecessorFlowId
  let inbound? := index.flow? row.inboundPredecessorFlowId
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
  -- Amendment 5: a logged inbound predecessor's transport columns are the stage's own when it is
  -- a RoCE stage, and zero otherwise.
  if let some inbound := inbound? then
    require row.srcLine
      (if inbound.stageKind = .roce then
        row.packetSizeBytes = inbound.packetSizeBytes && row.intervalNs = inbound.intervalNs
      else row.packetSizeBytes = 0 && row.intervalNs = 0)
      "compute stage inbound transport columns disagree with its inbound predecessor"
  match row.cause with
  | .localCompletion =>
      requireEarlierRelease row (local?.filter (·.activated))
        "local predecessor stage did not activate earlier"
  | .inboundArrival =>
      requireEarlierRelease row (inbound?.filter (·.activated))
        "inbound predecessor stage did not activate earlier"

/-- A compute stage completes only when its timer fires. When the completed compute stage is
logged, its release row records the timer deadline (`after_next_time_ns` = release + duration), so
the successor's local completion must happen exactly then, as a phase-1 timer event. -/
def checkComputeTimerCause (index : LookupIndex) (row : Row) : Except String Unit := do
  if row.cause = .localCompletion then
    match index.activatedComputeByFlow.get? row.causeFlowId with
    | none => pure ()
    | some predecessor =>
        require row.srcLine
          (row.key.timeNs = predecessor.afterNextTimeNs && row.key.phase = 1)
          "compute local completion does not occur at its predecessor's timer deadline"

def checkPredecessors (index : LookupIndex) (row : Row) : Except String Unit := do
  checkComputeTimerCause index row
  match row.stageKind, row.collectivePhase with
  | .tcp, some phase | .roce, some phase => checkTransportPredecessors index row phase
  | .tcp, none | .roce, none => pure ()
  | .compute, _ => checkComputePredecessors index row

def segmentOf (row : Row) : Nat × Nat :=
  (row.segmentSequence, row.segmentSequence + row.segmentBytes)

/-- The receiver's in-order frontier after the half-open segments `[start, stop)` arrive, as
`tcp_receive_range` computes it: sorted by start, extended from zero through every segment that
begins at or before the frontier. -/
def frontierOf (segments : List (Nat × Nat)) : Nat :=
  let sorted :=
    (segments.toArray.qsort fun a b => a.1 < b.1 || (a.1 = b.1 && a.2 < b.2)).toList
  sorted.foldl (fun frontier segment =>
    if segment.1 ≤ frontier then max frontier segment.2 else frontier) 0


/-- The MTU of the RoCE queue pair whose packets a row's inbound segments are, or `none` for TCP
segments. A RoCE stage's inbound predecessor is a stage of its own collective (one transport per
collective), whose MTU `sameGroupConfig` makes the row's own. A compute stage names its inbound
predecessor's transport itself (schema Amendment 5): the MTU in `packet_size_bytes` when that
predecessor is a RoCE stage, zero for TCP. This holds whether the predecessor is logged or not (the
final stage of an ungated two-rank AllGather is an unlogged root); when it is logged,
`checkComputePredecessors` requires the two to agree, before the replay runs. -/
def roceInboundMtu (row : Row) : Option Nat :=
  match row.stageKind with
  | .roce => some row.packetSizeBytes
  | .tcp => none
  | .compute => if row.packetSizeBytes > 0 then some row.packetSizeBytes else none

/-- Schema Amendment 4: an inbound row of a RoCE predecessor certifies one data packet, which is
the predecessor's packet at its PSN, and the advance of the receiver's Go-back-N frontier
(`Collective.goBackNFrontier`, the `Roce.onData` frontier): the packet's size when its PSN is the
frontier, else zero, with no hole filling. Unlike TCP's, this frontier depends only on the
frontier before the packet and the packet itself, and `checkContinuity` and `checkInitialState`
chain each row's before count to the stage's previous row from zero, so checking each row against
its own before count replays the frontier in constant time per row. -/
def checkGoBackN (mtu : Nat) (row : Row) : Except String Unit := do
  require row.srcLine
    (Collective.roceSegment mtu row.inboundPredecessorBytes row.segmentSequence row.segmentBytes)
    "inbound RoCE packet is not the predecessor queue pair's packet at its PSN"
  let after :=
    Collective.goBackNFrontier row.beforeInboundBytes row.segmentSequence row.segmentBytes
  require row.srcLine
    (row.afterInboundBytes = after && row.arrivalBytes = after - row.beforeInboundBytes)
    "inbound progress does not match the receiver's Go-back-N frontier"

/-- Every segment of a pending inbound predecessor is certified in order, so the before and after
byte counts of each inbound row must equal the frontier replayed from that stage's segments: the
Go-back-N frontier for RoCE packets, TCP's in-order frontier (which merges out-of-order segments)
otherwise. -/
def checkInboundReplay (rows : List Row) (row : Row) : Except String Unit := do
  if row.cause = .inboundArrival then
    if let some mtu := roceInboundMtu row then
      return (← checkGoBackN mtu row)
    let prior :=
      (rows.filter fun candidate =>
        candidate.flowId = row.flowId && candidate.cause = .inboundArrival &&
          compositeLT candidate row).map segmentOf
    let before := frontierOf prior
    let after := frontierOf (prior ++ [segmentOf row])
    require row.srcLine
      (row.beforeInboundBytes = before && row.afterInboundBytes = after &&
        row.arrivalBytes = after - before)
      "inbound progress does not match the receiver frontier replayed from the certified segments"


/-- Whether a local completion is caused by a compute timer: a gated root's gate, or a compute
stage that follows a compute group. Every other local cause is a transport stage's completing
ACK (TCP, or a RoCE ACK: a NACK never completes, since it acknowledges the receiver's frontier
below the chunk). -/
def causeIsTimer (row : Row) : Bool :=
  match row.stageKind with
  | .tcp | .roce => isRoot row
  | .compute => row.inboundPredecessorFlowId.isNone

/-- The byte total of a transport local predecessor: from the ring recurrence for a transport
stage, or from the rows of the stage that receives it (its inbound byte count) for a compute
stage. -/
def localPredecessorTotal (index : LookupIndex) (row : Row) : Option Nat :=
  match row.stageKind, row.algorithm, row.collectivePhase with
  | .tcp, some algorithm, some phase | .roce, some algorithm, some phase =>
      let position := localPredecessorPosition row phase
      let owner :=
        Collective.stageOwner algorithm position.phase row.groupSize position.rank position.step
      some (Collective.chunkBounds row.declaredTotalBytes row.groupSize owner).2
  | _, _, _ => index.receivedBytes.get? row.causeFlowId

/-- Binds a local completion to the event that caused it.

A compute timer completes its successor exactly at arm time + duration; a logged compute
predecessor was armed at its release, an unlogged one (a root) at time zero. A transport
predecessor (TCP, or a RoCE queue pair) completes at the first ACK whose acknowledgment reaches its
byte total: the row names that
acknowledgment, which must equal the predecessor's total; the answered segment was sent after the
predecessor's release; and the ACK arrives after the receiver's frontier completed and no sooner
than the segment's send time plus the unloaded round trip of the segment and the ACK. -/
def checkLocalSignal (index : LookupIndex) (row : Row) : Except String Unit := do
  if row.cause = .localCompletion then
    let released? := index.activatedByFlow.get? row.causeFlowId
    if causeIsTimer row then
      require row.srcLine
        (row.ackNumber = 0 && row.causeDelayNs > 0 &&
          row.key.timeNs = row.causeOriginNs + row.causeDelayNs)
        "compute timer completion does not occur at arm time plus duration"
      match released? with
      | some predecessor =>
          require row.srcLine
            (predecessor.stageKind = .compute && row.causeOriginNs = predecessor.key.timeNs &&
              row.causeDelayNs = predecessor.durationNs)
            "compute timer completion does not match its predecessor's release and duration"
      | none =>
          require row.srcLine (row.causeOriginNs = 0)
            "an unlogged root compute stage is armed at time zero"
    else
      let total ← requireSome row.srcLine "local predecessor byte total"
        (localPredecessorTotal index row)
      require row.srcLine (row.ackNumber = total)
        "completing acknowledgment does not reach exactly the local predecessor's byte total"
      match released? with
      | some predecessor =>
          require row.srcLine (row.causeOriginNs ≥ predecessor.key.timeNs)
            "acknowledged segment was sent before its stage was released"
      | none => pure ()
      match index.delivered.get? row.causeFlowId with
      | some delivered =>
          require row.srcLine (compositeLT delivered row && delivered.key.timeNs < row.key.timeNs)
            "local completion precedes its predecessor's delivery at the receiver"
      | none => pure ()
      require row.srcLine
        (row.causeDelayNs > 0 && row.key.timeNs ≥ row.causeOriginNs + row.causeDelayNs)
        "local completion precedes the earliest return of the completing acknowledgment"

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

/-- What `checkContinuity` keeps of the rows before the current one (canonical positions count from
zero). -/
structure ContinuityState where
  /-- The most recent row of each group. -/
  lastOfGroup : Std.HashMap GroupId Row := ∅
  /-- For each (group, rank): the node of its rows and the position of the most recent one. -/
  rankNode : Std.HashMap (GroupId × Nat) (Nat × Nat) := ∅
  /-- For each (group, node): the rank of its rows and the position of the most recent one. -/
  nodeRank : Std.HashMap (GroupId × Nat) (Nat × Nat) := ∅
  /-- For each flow: the stage of its rows and the position of the most recent one. -/
  flowStage : Std.HashMap Nat (StageId × Nat) := ∅
  /-- The most recent row of each stage. -/
  lastOfStage : Std.HashMap StageId Row := ∅
  /-- The stages with an activated row. -/
  activated : Std.HashSet StageId := ∅

/-- The position of the most recent earlier row that breaks the group's rank-to-node mapping
against `row`, if any. -/
def ContinuityState.rankNodeBreak (state : ContinuityState) (row : Row) : Option Nat :=
  let byRank := match state.rankNode.get? (groupId row, row.rank) with
    | some (node, position) => if node = row.nodeId then none else some position
    | none => none
  let byNode := match state.nodeRank.get? (groupId row, row.nodeId) with
    | some (rank, position) => if rank = row.rank then none else some position
    | none => none
  match byRank, byNode with
  | some first, some second => some (max first second)
  | some first, none | none, some first => some first
  | none, none => none

/-- The position of the most recent earlier row of `row`'s flow at another stage, if any. -/
def ContinuityState.flowBreak (state : ContinuityState) (row : Row) : Option Nat :=
  match state.flowStage.get? row.flowId with
  | some (stage, position) => if stage = stageId row then none else some position
  | none => none

/-- Each row continues its group and its stage, in one pass in canonical order.

The list scan this replaces compared each row with every earlier row, most recent first, and for
each earlier row required first the group's rank-to-node mapping and then the flow's stage. Its
first error is therefore that of the most recent earlier row breaking either rule, with the mapping
message when one row breaks both. The keyed form finds the same row: when the pass reaches a row,
every pair of earlier rows obeys both rules (else the pass would have stopped at the later of the
two), so all earlier rows of one (group, rank) share a node, all earlier rows of one (group, node)
share a rank, and all earlier rows of one flow share a stage. The earlier rows that break the
mapping against `row` are then exactly the rows of `(group, row.rank)` when their node differs and
the rows of `(group, row.nodeId)` when their rank differs, and those that break the flow rule are
all rows of the flow when their stage differs; the most recent of each set is the stored position.
The other requirements read only the most recent row of the group or stage, or whether the stage
has activated, which the state keeps exactly. -/
def checkContinuity (rows : List Row) : Except String Unit := do
  let mut state : ContinuityState := {}
  let mut position := 0
  for row in rows do
    let group := groupId row
    let stage := stageId row
    match state.lastOfGroup.get? group with
    | none => pure ()
    | some prior =>
        require row.srcLine (sameGroupConfig prior row)
          s!"collective configuration discontinuity for collective_id={row.collectiveId}"
    match state.rankNodeBreak row, state.flowBreak row with
    | some mapping, some flow =>
        if mapping ≥ flow then
          require row.srcLine false "collective rank-to-node mapping is inconsistent"
        else
          require row.srcLine false "collective flow identity changed stage position"
    | some _, none => require row.srcLine false "collective rank-to-node mapping is inconsistent"
    | none, some _ => require row.srcLine false "collective flow identity changed stage position"
    | none, none => pure ()
    match state.lastOfStage.get? stage with
    | none => checkInitialState row
    | some prior =>
        require row.srcLine (sameStageConfig prior row)
          "collective stage configuration changed during progress"
        require row.srcLine
          (row.beforeLocalComplete = prior.afterLocalComplete &&
            row.beforeInboundComplete = prior.afterInboundComplete &&
            row.beforeInboundBytes = prior.afterInboundBytes)
          "collective stage before-state does not continue the prior after-state"
    require row.srcLine (!(state.activated.contains stage && row.activated))
      "collective stage activated more than once"
    state :=
      { lastOfGroup := state.lastOfGroup.insert group row
        rankNode := state.rankNode.insert (group, row.rank) (row.nodeId, position)
        nodeRank := state.nodeRank.insert (group, row.nodeId) (row.rank, position)
        flowStage := state.flowStage.insert row.flowId (stage, position)
        lastOfStage := state.lastOfStage.insert stage row
        activated := if row.activated then state.activated.insert stage else state.activated }
    position := position + 1

/-- What coverage needs of one group: its distinct stages, its activated rows, and whether a root
is logged. -/
structure GroupTally where
  stages : Nat := 0
  activated : Nat := 0
  hasRoot : Bool := false

/-- The per-group tallies and each stage's last row, built in one pass (linear in the trace). -/
structure CoverageIndex where
  groups : Std.HashMap GroupId GroupTally := ∅
  finals : Std.HashMap StageId Row := ∅

def coverageIndex (rows : List Row) : CoverageIndex := Id.run do
  let mut index : CoverageIndex := {}
  for row in rows do
    let tally := index.groups.getD (groupId row) {}
    let newStage := !index.finals.contains (stageId row)
    index :=
      { groups := index.groups.insert (groupId row)
          { stages := tally.stages + (if newStage then 1 else 0)
            activated := tally.activated + (if row.activated then 1 else 0)
            hasRoot := tally.hasRoot || isRoot row }
        -- Rows are canonical, so the last insertion is the stage's final row.
        finals := index.finals.insert (stageId row) row }
  pure index

/-- A complete trace releases every logged stage exactly once and leaves it complete. A transport
collective logs its non-root stages, plus its roots when compute stages gate them; a compute
group logs one stage per rank. The group tallies are computed once, so the check is linear; each
row is checked in canonical order, as by a per-row scan of its group. -/
def checkCoverage (rows : List Row) : Except String Unit := do
  let index := coverageIndex rows
  for row in rows do
    let tally := index.groups.getD (groupId row) {}
    let expected :=
      match row.stageKind, row.algorithm with
      | .tcp, some algorithm | .roce, some algorithm =>
          Collective.expectedActivationCount algorithm row.groupSize tally.hasRoot
      | _, _ => row.groupSize
    require row.srcLine (tally.stages = expected)
      s!"incomplete collective progress coverage for collective_id={row.collectiveId}: expected {expected}, found {tally.stages}"
    let final ← requireSome row.srcLine "final collective stage progress"
      (index.finals.get? (stageId row))
    require row.srcLine
      (final.afterLocalComplete && final.afterInboundComplete &&
        inboundDone final final.afterInboundBytes)
      "collective stage final prerequisite state is incomplete"
    require row.srcLine (tally.activated = expected)
      s!"incomplete collective activation coverage for collective_id={row.collectiveId}: expected {expected}, found {tally.activated}"

def checkRows (rows : List Row) : Except String Unit := do
  require 1 (!rows.isEmpty) "empty collective activation trace"
  let canonical ← canonicalize rows
  checkOrdinals canonical
  checkContinuity canonical
  for row in canonical do checkRow row
  checkCoverage canonical
  let index := lookupIndex canonical
  for row in canonical do checkPredecessors index row
  for row in canonical do checkInboundReplay canonical row
  for row in canonical do checkLocalSignal index row

end LeanGuard.P10c.CollectiveEventLog
