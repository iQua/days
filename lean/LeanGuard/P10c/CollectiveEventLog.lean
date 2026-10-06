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
  | "reduce_scatter" => pure .reduceScatter
  | "all_to_all" => pure .allToAll
  | "send_recv" => pure .sendRecv
  | other => throw s!"invalid collective algorithm: '{other}'"

def parsePhase : String → Except String Collective.Phase
  | "reduce_scatter" => pure .reduceScatter
  | "allgather" => pure .allGather
  | "all_to_all" => pure .allToAll
  | "send_recv" => pure .sendRecv
  | other => throw s!"invalid collective phase: '{other}'"

def parseChunkPolicy : String → Except String Collective.ChunkPolicy
  | "equal_remainder_last" => pure .equalRemainderLast
  | "uniform_floor" => pure .uniformFloor
  | "seeded" => pure .seeded
  | other => throw s!"invalid chunk policy: '{other}'"

def parseChannelPolicy : String → Except String Collective.ChannelPolicy
  | "ring_next" => pure .ringNext
  | "channels" => pure .channels
  | "all_pairs" => pure .allPairs
  | "pair" => pure .pair
  | other => throw s!"invalid channel policy: '{other}'"

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

/-- A `;`-separated list of flow ids; empty for none. -/
def parseFlowList (value : String) : Except String (List Nat) :=
  if value.isEmpty then pure [] else (value.splitOn ";").mapM parseU64

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
  /-- The inbound requirement: the summed totals of the inbound predecessors, zero without any. -/
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
  /-- P16 H1: the ring channel of a transport stage (zero otherwise), and its collective's chunk and
  channel policies (none for a compute stage). -/
  channel : Nat
  chunkPolicy : Option Collective.ChunkPolicy
  channelPolicy : Option Collective.ChannelPolicy
  /-- Every local and inbound predecessor, strictly ascending; a join names several. -/
  localPredecessors : List Nat
  inboundPredecessors : List Nat
  /-- The local predecessors the stage waits for, and how many had completed before and after. -/
  localRequired : Nat
  beforeLocalCompleted : Nat
  afterLocalCompleted : Nat
  /-- The cause flow's byte total: a transport stage's chunk, zero for a compute stage. -/
  causeTotalBytes : Nat
  /-- The stages of the row's collective or compute group in the image (a seeded all-to-all has
  no stage for a pair of zero bytes). -/
  groupStages : Nat
  srcLine : Nat
  deriving DecidableEq, Repr

/-- The stage's one local predecessor, unless it has none or several. -/
def Row.localOne (row : Row) : Option Nat :=
  match row.localPredecessors with
  | [flow] => some flow
  | _ => none

/-- The stage's one inbound predecessor, unless it has none or several. -/
def Row.inboundOne (row : Row) : Option Nat :=
  match row.inboundPredecessors with
  | [flow] => some flow
  | _ => none

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
        channel := ← parseU32 (← getField idx fields "channel")
        chunkPolicy := ← parseOpt parseChunkPolicy (← getField idx fields "chunk_policy")
        channelPolicy := ← parseOpt parseChannelPolicy (← getField idx fields "channel_policy")
        localPredecessors := ← parseFlowList (← getField idx fields "local_predecessors")
        inboundPredecessors := ← parseFlowList (← getField idx fields "inbound_predecessors")
        localRequired := ← parseU32 (← getField idx fields "local_required")
        beforeLocalCompleted := ← parseU32 (← getField idx fields "before_local_completed")
        afterLocalCompleted := ← parseU32 (← getField idx fields "after_local_completed")
        causeTotalBytes := ← parseU64 (← getField idx fields "cause_total_bytes")
        groupStages := ← parseU64 (← getField idx fields "group_stages")
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

/-- Whether a transport row is a root stage: it follows the groups its collective follows, and is
logged only when one of them gates it. -/
def isRoot (row : Row) : Bool :=
  match row.algorithm, row.collectivePhase with
  | some algorithm, some phase => Collective.rootPosition algorithm phase row.step
  | _, _ => false

/-- Whether a transport row is a stage that completes its collective at its rank: a ring
channel's last step of the last phase, any all-to-all pair, the send. -/
def isCompletion (row : Row) : Bool :=
  match row.algorithm, row.collectivePhase with
  | some algorithm, some phase =>
      row.stageKind.isTransport &&
        (match algorithm with
          | .allToAll | .sendRecv => true
          | _ => phase = algorithm.lastPhase && row.step + 1 = row.groupSize)
  | _, _ => false

/-- The rank a transport stage delivers to, when the row determines it: the next rank of one
ring, an all-to-all pair's destination, the receiver of a send. A channel's next rank is the
channel's order, which the row does not carry. -/
def targetRank (row : Row) : Option Nat :=
  match row.algorithm, row.channelPolicy with
  | some .allToAll, _ => some ((row.rank + row.step) % row.groupSize)
  | some .sendRecv, _ => some 1
  | some _, some .ringNext => some ((row.rank + 1) % row.groupSize)
  | _, _ => none

/-- The inbound prerequisite is complete when there is none or every byte of it was delivered
(a join: all its predecessors' bytes, summed). -/
def inboundDone (row : Row) (bytes : Nat) : Bool :=
  row.inboundPredecessors.isEmpty || bytes = row.inboundPredecessorBytes

def strictlyAscending : List Nat → Bool
  | first :: second :: rest => first < second && strictlyAscending (second :: rest)
  | _ => true

/-- Prerequisite transitions shared by transport and compute stages.

Local completion counts one more local predecessor complete (TCP or RoCE: last byte
acknowledged; compute: timer fired). An inbound row certifies one arriving segment of one inbound
predecessor and the resulting advance of that predecessor's in-order frontier at the receiver,
which may be zero (duplicate or out-of-order segment) or, for TCP only, cover several segments at
once (a filled hole); the stage's inbound byte counts sum its predecessors' frontiers.
`checkInboundReplay` replays each frontier by its predecessor's transport; `checkLocalSignal`
binds local completions to their cause. -/
def checkProgress (row : Row) : Except String Unit := do
  require row.srcLine
    (strictlyAscending row.localPredecessors && strictlyAscending row.inboundPredecessors)
    "stage predecessor lists are not strictly ascending"
  require row.srcLine (row.beforeInboundComplete = inboundDone row row.beforeInboundBytes)
    "before inbound completion flag disagrees with the delivered total"
  require row.srcLine (row.afterInboundComplete = inboundDone row row.afterInboundBytes)
    "after inbound completion flag disagrees with the delivered total"
  match row.cause with
  | .localCompletion =>
      require row.srcLine
        (row.localPredecessors.contains row.causeFlowId)
        "local completion cause does not match the local predecessor"
      require row.srcLine (row.arrivalBytes = 0)
        "local completion must not carry arrival bytes"
      require row.srcLine (row.segmentSequence = 0 && row.segmentBytes = 0)
        "local completion row carries an inbound segment"
      -- A join counts its local predecessors; any other stage has at most one.
      require row.srcLine
        (row.beforeLocalCompleted < row.localRequired &&
          row.afterLocalCompleted = row.beforeLocalCompleted + 1 &&
          row.beforeLocalComplete = false &&
          row.afterLocalComplete = (row.afterLocalCompleted = row.localRequired))
        "local completion must change the local prerequisite from incomplete to complete"
      require row.srcLine
        (row.afterInboundComplete = row.beforeInboundComplete &&
          row.afterInboundBytes = row.beforeInboundBytes)
        "local completion changed inbound prerequisite state"
  | .inboundArrival =>
      require row.srcLine
        (row.inboundPredecessors.contains row.causeFlowId)
        "inbound arrival cause does not match the inbound predecessor"
      require row.srcLine
        (row.inboundPredecessors.length > 1 || row.causeTotalBytes = row.inboundPredecessorBytes)
        "inbound cause total disagrees with the stage's inbound requirement"
      require row.srcLine
        (row.segmentBytes > 0 &&
          row.segmentSequence + row.segmentBytes ≤ row.causeTotalBytes)
        "inbound segment is empty or extends past the predecessor chunk"
      require row.srcLine
        (row.ackNumber = 0 && row.causeOriginNs = 0 && row.causeDelayNs = 0)
        "inbound row carries local completion fields"
      require row.srcLine
        (row.afterLocalComplete = row.beforeLocalComplete &&
          row.afterLocalCompleted = row.beforeLocalCompleted)
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
    (row.beforeLocalComplete = (row.beforeLocalCompleted = row.localRequired) &&
      row.afterLocalComplete = (row.afterLocalCompleted = row.localRequired) &&
      row.localRequired = row.localPredecessors.length)
    "local completion flags disagree with the local completion counts"
  require row.srcLine
    (row.activated = (row.afterLocalComplete && row.afterInboundComplete))
    "collective activated bit disagrees with prerequisite state"

def checkNotReleased (row : Row) : Except String Unit := do
  require row.srcLine (row.afterPacketsEmitted = 0 && row.afterBytesEmitted = 0)
    "nonactivated collective stage has emitted counters"
  require row.srcLine (row.afterStatus = .blocked && row.afterNextTimeNs = 0)
    "nonactivated collective status or deadline mismatch"

/-- Whether a collective's chunk and channel policies agree with its algorithm (P16 H1). -/
def policiesAgree (row : Row) (algorithm : Collective.Algorithm) : Bool :=
  match row.chunkPolicy, row.channelPolicy with
  | some chunk, some sends =>
      match algorithm with
      | .allToAll =>
          sends = .allPairs && row.channel = 0 && (chunk = .uniformFloor || chunk = .seeded)
      | .sendRecv => sends = .pair && row.channel = 0 && chunk = .equalRemainderLast
      | _ =>
          match sends with
          | .ringNext =>
              row.channel = 0 && (chunk = .equalRemainderLast || chunk = .uniformFloor)
          | .channels => chunk = .uniformFloor
          | _ => false
  | _, _ => false

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
  require row.srcLine (policiesAgree row algorithm)
    "collective chunk or channel policy is inconsistent with its algorithm"
  let owner := Collective.stageOwner algorithm phase row.groupSize row.rank row.step
  match algorithm.isRing, row.chunkPolicy, row.channelPolicy with
  | true, some .equalRemainderLast, _ =>
      let expectedBounds := Collective.chunkBounds row.declaredTotalBytes row.groupSize owner
      require row.srcLine
        (row.chunkOffsetBytes = expectedBounds.1 && row.chunkBytes = expectedBounds.2)
        "collective chunk does not match EqualRemainderLast"
  | true, _, some .ringNext =>
      -- One ring under `UniformFloor`: every message `floor(S / n)` bytes, at its owner's slot.
      let bytes := Collective.uniformFloorBytes algorithm row.declaredTotalBytes row.groupSize 1
      require row.srcLine (row.chunkBytes = bytes && row.chunkOffsetBytes = owner * bytes)
        "collective chunk does not match its chunk policy"
  | true, _, _ =>
      -- A channel's message size depends on the channel count, checked per collective by
      -- `checkCoverage`.
      pure ()
  | false, _, _ =>
      -- A seeded pair's bytes come from a routing matrix this checker does not re-derive.
      require row.srcLine
        (match algorithm, row.chunkPolicy with
          | .allToAll, some .uniformFloor =>
              row.chunkOffsetBytes = 0 &&
                row.chunkBytes =
                  Collective.uniformFloorBytes algorithm row.declaredTotalBytes row.groupSize 1
          | .sendRecv, _ =>
              row.chunkOffsetBytes = 0 && row.chunkBytes = row.declaredTotalBytes
          | _, _ => row.chunkOffsetBytes = 0 && row.chunkBytes > 0)
        "collective chunk does not match its chunk policy"
  let root := isRoot row
  if root then
    -- A root follows its collective's groups: at least one gate, and the completions of any
    -- collective it also follows (a join).
    require row.srcLine (!row.localPredecessors.isEmpty)
      "a root stage is logged only when a compute stage gates it"
    require row.srcLine
      (row.inboundPredecessors.isEmpty = decide (row.inboundPredecessorBytes = 0))
      "root inbound predecessors and byte count disagree"
  else
    require row.srcLine (row.inboundPredecessorBytes = row.chunkBytes)
      "inbound predecessor byte count does not match the stage chunk"
    require row.srcLine
      (row.localPredecessors.length = 1 && row.inboundPredecessors.length = 1)
      "a collective progress stage is missing a predecessor"
  -- Only a compute timer (a phase-1 pacing event) completes a root's gate; every other cause is a
  -- phase-0 ACK or data arrival. The cause's byte total (zero for a compute stage) tells the two
  -- apart when a root also follows a collective.
  require row.srcLine
    (row.key.phase =
      if row.cause = .localCompletion && root && row.causeTotalBytes = 0 then 1 else 0)
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
      row.declaredTotalBytes = 0 && row.chunkOffsetBytes = 0 && row.chunkBytes = 0 &&
      row.channel = 0 && row.chunkPolicy.isNone && row.channelPolicy.isNone)
    "compute stage row carries collective fields"
  -- Amendment 5: a compute stage whose inbound predecessors are RoCE stages carries their queue
  -- pairs' MTU and pacing interval; every other compute stage writes zero in both.
  require row.srcLine (decide (row.packetSizeBytes = 0) = decide (row.intervalNs = 0))
    "compute stage inbound transport columns must be both zero or both positive (Amendment 5)"
  require row.srcLine (row.packetSizeBytes = 0 || !row.inboundPredecessors.isEmpty)
    "compute stage without an inbound predecessor carries inbound transport columns"
  require row.srcLine (row.durationNs > 0)
    "compute stage duration must be positive"
  require row.srcLine (row.rank < row.groupSize)
    "compute stage rank is outside its group"
  -- A Send/Recv's receiver waits for the message alone.
  require row.srcLine (!row.localPredecessors.isEmpty || !row.inboundPredecessors.isEmpty)
    "a compute progress stage has no predecessor"
  require row.srcLine
    ((!row.inboundPredecessors.isEmpty) = decide (row.inboundPredecessorBytes > 0))
    "compute inbound predecessor and byte count disagree"
  -- A compute group's timer completes its successor in phase 1; a collective's ACK in phase 0,
  -- and inbound delivery is always a phase-0 data arrival. The cause's byte total (zero for a
  -- compute stage) tells the two local causes apart.
  require row.srcLine
    (row.key.phase =
      if row.cause = .localCompletion && row.causeTotalBytes = 0 then 1 else 0)
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

/-- A compute group's transport columns name its stages' inbound predecessors (Amendment 5), so
only its stages with inbound predecessors share them (`sameComputeTransport`); a Send/Recv's
sender stage has none. -/
def sameGroupConfig (first second : Row) : Bool :=
  first.algorithm = second.algorithm &&
    first.groupSize = second.groupSize &&
    first.declaredTotalBytes = second.declaredTotalBytes &&
    (first.stageKind = .compute ||
      first.packetSizeBytes = second.packetSizeBytes && first.intervalNs = second.intervalNs) &&
    first.durationNs = second.durationNs &&
    first.stopTimeNs = second.stopTimeNs &&
    first.chunkPolicy = second.chunkPolicy &&
    first.channelPolicy = second.channelPolicy &&
    first.groupStages = second.groupStages

def sameStage (first second : Row) : Bool :=
  sameGroup first second &&
    first.collectivePhase = second.collectivePhase && first.channel = second.channel &&
    first.rank = second.rank && first.step = second.step

structure Position where
  phase : Collective.Phase
  channel : Nat
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
  { phase := predecessor.1, channel := row.channel, rank := row.rank, step := predecessor.2 }

/-- The inbound predecessor of one ring's stage (`RingNext`): the previous rank's. -/
def inboundPredecessorPosition (row : Row) (phase : Collective.Phase) : Position :=
  let predecessor := predecessorPhaseStep row phase
  let previousRank := if row.rank = 0 then row.groupSize - 1 else row.rank - 1
  { phase := predecessor.1, channel := row.channel, rank := previousRank, step := predecessor.2 }

/-- A row's group (`sameGroup`) as a hash key. -/
abbrev GroupId := Collective.StageKind × Nat

/-- A row's stage (`sameStage`) as a hash key: its group and its position. -/
abbrev StageId := Collective.StageKind × Nat × Option Collective.Phase × Nat × Nat × Nat

def groupId (row : Row) : GroupId := (row.stageKind, row.collectiveId)

def stageId (row : Row) : StageId :=
  (row.stageKind, row.collectiveId, row.collectivePhase, row.channel, row.rank, row.step)

/-- The stage of `collective`'s group at `wanted`, as a hash key. -/
def positionId (collective : Row) (wanted : Position) : StageId :=
  (collective.stageKind, collective.collectiveId, some wanted.phase, wanted.channel, wanted.rank,
    wanted.step)

/-- Insert unless present, so a map built in canonical order holds each key's first row. -/
def insertFirst {α β : Type} [BEq α] [Hashable α] (map : Std.HashMap α β) (key : α) (value : β) :
    Std.HashMap α β :=
  if map.contains key then map else map.insert key value

/-- The byte total a stage's row states for itself as a cause: its chunk, zero for a compute
stage. -/
def ownTotal (row : Row) : Nat :=
  if row.stageKind = .compute then 0 else row.chunkBytes

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
  /-- For each cause flow, the byte total the first row it causes states. -/
  causeTotals : Std.HashMap Nat Nat := ∅
  /-- For each flow, the first inbound row at which its delivery to a receiving stage completes
  (the arrivals it caused there reach its byte total). -/
  delivered : Std.HashMap Nat Row := ∅
  /-- P16 H1: the node of each (group, rank), from its first row. -/
  rankNode : Std.HashMap (GroupId × Nat) Nat := ∅
  /-- The rank of each (group, node), from its first row. -/
  nodeRank : Std.HashMap (GroupId × Nat) Nat := ∅
  /-- Each group's ring channels (the largest channel plus one). -/
  channels : Std.HashMap GroupId Nat := ∅
  /-- Each group's distinct completion stages sourced at a node, and those delivered to a rank
  (when `targetRank` determines it). -/
  completionStages : Std.HashSet StageId := ∅
  completionsAt : Std.HashMap (GroupId × Nat) Nat := ∅
  completionsInto : Std.HashMap (GroupId × Nat) Nat := ∅

def lookupIndex (rows : List Row) : LookupIndex := Id.run do
  let mut index : LookupIndex := {}
  let mut arrived : Std.HashMap (Nat × Nat) Nat := ∅
  for row in rows do
    let group := groupId row
    index := { index with
      byFlow := insertFirst index.byFlow row.flowId row
      byStage := insertFirst index.byStage (stageId row) row
      groupFlows := index.groupFlows.insert (group, row.flowId)
      causeTotals := insertFirst index.causeTotals row.causeFlowId row.causeTotalBytes
      rankNode := insertFirst index.rankNode (group, row.rank) row.nodeId
      nodeRank := insertFirst index.nodeRank (group, row.nodeId) row.rank }
    if index.channels.getD group 0 < row.channel + 1 then
      index := { index with channels := index.channels.insert group (row.channel + 1) }
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
            (positionId row (localPredecessorPosition row phase)) row.localOne }
    if isCompletion row && !index.completionStages.contains (stageId row) then
      index := { index with
        completionStages := index.completionStages.insert (stageId row)
        completionsAt :=
          index.completionsAt.insert (group, row.nodeId)
            (index.completionsAt.getD (group, row.nodeId) 0 + 1) }
      if let some target := targetRank row then
        index := { index with
          completionsInto :=
            index.completionsInto.insert (group, target)
              (index.completionsInto.getD (group, target) 0 + 1) }
    if row.cause = .inboundArrival then
      let key := (row.flowId, row.causeFlowId)
      let before := arrived.getD key 0
      let after := before + row.arrivalBytes
      arrived := arrived.insert key after
      if before < row.causeTotalBytes && after = row.causeTotalBytes then
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

/-- A row's cause byte total agrees with every earlier row of the same cause, and with the cause
stage's own rows when they are logged (a compute stage's total is zero). -/
def checkCauseTotal (index : LookupIndex) (row : Row) : Except String Unit := do
  require row.srcLine (index.causeTotals.getD row.causeFlowId row.causeTotalBytes = row.causeTotalBytes)
    "cause byte total disagrees with an earlier row of the same cause"
  if let some cause := index.byFlow.get? row.causeFlowId then
    require row.srcLine (row.causeTotalBytes = ownTotal cause)
      "cause byte total disagrees with the cause stage"

/-- The lookups `checkEntryPredecessors` reads: keyed from a `LookupIndex` here, by list scans in
the test-only reference. -/
structure EntryLookups where
  /-- The first row of a flow. -/
  flow? : Nat → Option Row
  /-- The node of a (group, rank). -/
  rankNode? : GroupId × Nat → Option Nat
  /-- The rank of a (group, node). -/
  nodeRank? : GroupId × Nat → Option Nat
  /-- A group's ring channels. -/
  channels : GroupId → Nat
  /-- A group's distinct completion stages sourced at a node, and delivered to a rank. -/
  completionsAt : GroupId × Nat → Nat
  completionsInto : GroupId × Nat → Nat

def LookupIndex.entryLookups (index : LookupIndex) : EntryLookups :=
  { flow? := index.byFlow.get?
    rankNode? := index.rankNode.get?
    nodeRank? := index.nodeRank.get?
    channels := fun group => index.channels.getD group 1
    completionsAt := fun key => index.completionsAt.getD key 0
    completionsInto := fun key => index.completionsInto.getD key 0 }

/-- An entry stage (a compute stage, or a collective's root) follows whole groups: each local
predecessor is a compute stage of the stage's rank, or a stage that completes its collective on
the stage's host; each inbound predecessor completes its collective by delivering to that host; and
for every group a logged predecessor belongs to, the stage names all of that group's completion
stages there (one compute stage; a ring's channels, local and inbound; an all-to-all's pairs from
and to the rank; the send at the sender, the message at the receiver). Groups are checked in the
order their first logged predecessor appears in the lists. -/
def checkEntryPredecessors (lookups : EntryLookups) (row : Row) : Except String Unit := do
  let compute := row.stageKind = .compute
  let mut localCounts : Std.HashMap GroupId Nat := ∅
  let mut inboundCounts : Std.HashMap GroupId Nat := ∅
  for flow in row.localPredecessors do
    if let some predecessor := lookups.flow? flow then
      let group := groupId predecessor
      localCounts := localCounts.insert group (localCounts.getD group 0 + 1)
      if predecessor.stageKind = .compute then
        if compute then
          require row.srcLine
            (predecessor.rank = row.rank && predecessor.groupSize = row.groupSize)
            "compute local predecessor is not the same-rank stage of a compute group"
        else
          require row.srcLine (predecessor.nodeId = row.nodeId)
            "a root stage's gate is not a compute stage on its host"
      else if compute then
        require row.srcLine (isCompletion predecessor && predecessor.nodeId = row.nodeId)
          "compute local predecessor is not its rank's final collective stage"
      else
        require row.srcLine (isCompletion predecessor && predecessor.nodeId = row.nodeId)
          "a root stage's collective predecessor does not complete on its host"
  for flow in row.inboundPredecessors do
    if let some predecessor := lookups.flow? flow then
      let group := groupId predecessor
      inboundCounts := inboundCounts.insert group (inboundCounts.getD group 0 + 1)
      let delivers :=
        match targetRank predecessor with
        | some target =>
            match lookups.rankNode? (group, target) with
            | some node => node = row.nodeId
            | none => true
        | none => true
      require row.srcLine
        (isCompletion predecessor && delivers &&
          (row.inboundPredecessors.length > 1 ||
            predecessor.chunkBytes = row.inboundPredecessorBytes))
        (if compute then
          "compute inbound predecessor is not the previous rank's final collective stage"
        else "a root stage's inbound predecessor does not complete its collective at its host")
      -- Amendment 5: a compute stage's transport columns are its RoCE inbound predecessors' own,
      -- and zero after TCP.
      if compute then
        require row.srcLine
          (if predecessor.stageKind = .roce then
            row.packetSizeBytes = predecessor.packetSizeBytes &&
              row.intervalNs = predecessor.intervalNs
          else row.packetSizeBytes = 0 && row.intervalNs = 0)
          "compute stage inbound transport columns disagree with its inbound predecessor"
  let mut checked : Std.HashSet GroupId := ∅
  for flow in row.localPredecessors ++ row.inboundPredecessors do
    let some predecessor := lookups.flow? flow | continue
    let group := groupId predecessor
    if checked.contains group then continue
    checked := checked.insert group
    let localCount := localCounts.getD group 0
    let inboundCount := inboundCounts.getD group 0
    if predecessor.stageKind = .compute then
      require row.srcLine (localCount = 1 && inboundCount = 0)
        "a stage does not wait for exactly its rank's stage of a compute group"
    else
      let into :=
        if predecessor.channelPolicy = some .channels then some (lookups.channels group)
        else (lookups.nodeRank? (group, row.nodeId)).map fun rank =>
          lookups.completionsInto (group, rank)
      require row.srcLine
        (localCount = lookups.completionsAt (group, row.nodeId) &&
          into.all (· = inboundCount))
        "a stage does not wait for its predecessor collective's whole completion at its rank"

def checkTransportPredecessors (index : LookupIndex) (row : Row) (phase : Collective.Phase) :
    Except String Unit := do
  if isRoot row then
    -- A root's gate is a compute stage, never a stage of its own collective.
    require row.srcLine
      ((row.localPredecessors ++ row.inboundPredecessors).all fun gate =>
        !index.groupFlows.contains (groupId row, gate))
      "a root stage's gate must be a compute stage outside its collective"
    checkEntryPredecessors index.entryLookups row
    match row.cause with
    | .localCompletion =>
        requireEarlierRelease row (index.activatedByFlow.get? row.causeFlowId)
          "local predecessor stage did not activate earlier"
    | .inboundArrival =>
        requireEarlierRelease row (index.activatedByFlow.get? row.causeFlowId)
          "inbound predecessor stage did not activate earlier"
    return
  let localPosition := localPredecessorPosition row phase
  match resolveStageFlow index row localPosition with
  | none => pure ()
  | some expected =>
      require row.srcLine (row.localOne = some expected)
        "local predecessor identity does not match the stage recurrence"
  if row.channelPolicy = some .channels then
    -- A channel's previous rank is its own order: the inbound predecessor is the same channel's
    -- step before, on another rank, forwarding this stage's chunk (`checkChannelRings` requires
    -- each channel's previous ranks to form one ring).
    let inbound? := row.inboundOne.bind index.byFlow.get?
    if let some inbound := inbound? then
      require row.srcLine
        (sameGroup inbound row && inbound.collectivePhase = some localPosition.phase &&
          inbound.channel = row.channel && inbound.step = localPosition.step &&
          inbound.rank != row.rank && inbound.chunkBytes = row.chunkBytes &&
          inbound.chunkOffsetBytes = row.chunkOffsetBytes)
        "inbound predecessor identity does not match the stage recurrence"
    match row.cause with
    | .localCompletion =>
        requireEarlierRelease row (index.activatedByStage.get? (positionId row localPosition))
          "local predecessor stage did not activate earlier"
    | .inboundArrival =>
        requireEarlierRelease row (row.inboundOne.bind index.activatedByFlow.get?)
          "inbound predecessor stage did not activate earlier"
    return
  let inboundPosition := inboundPredecessorPosition row phase
  match resolveStageFlow index row inboundPosition with
  | none => pure ()
  | some expected =>
      require row.srcLine (row.inboundOne = some expected)
        "inbound predecessor identity does not match the stage recurrence"
  match row.cause with
  | .localCompletion =>
      requireEarlierRelease row (index.activatedByStage.get? (positionId row localPosition))
        "local predecessor stage did not activate earlier"
  | .inboundArrival =>
      requireEarlierRelease row (index.activatedByStage.get? (positionId row inboundPosition))
        "inbound predecessor stage did not activate earlier"

/-- A compute stage follows whole groups (`checkEntryPredecessors`); its cause released earlier. -/
def checkComputePredecessors (index : LookupIndex) (row : Row) : Except String Unit := do
  checkEntryPredecessors index.entryLookups row
  match row.cause with
  | .localCompletion =>
      requireEarlierRelease row (index.activatedByFlow.get? row.causeFlowId)
        "local predecessor stage did not activate earlier"
  | .inboundArrival =>
      requireEarlierRelease row (index.activatedByFlow.get? row.causeFlowId)
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
  checkCauseTotal index row
  checkComputeTimerCause index row
  match row.stageKind, row.collectivePhase with
  | .tcp, some phase | .roce, some phase => checkTransportPredecessors index row phase
  | .tcp, none | .roce, none => pure ()
  | .compute, _ => checkComputePredecessors index row

/-- The previous ranks of each channel ring form one cycle: every logged rank of a channel names
one previous rank at every step, and no two ranks name the same one. One pass in canonical order
keeps, per (collective, channel), each rank's previous rank and each previous rank's successor. -/
def checkChannelRings (index : LookupIndex) (rows : List Row) : Except String Unit := do
  let mut previous : Std.HashMap (GroupId × Nat × Nat) Nat := ∅
  let mut next : Std.HashMap (GroupId × Nat × Nat) Nat := ∅
  for row in rows do
    if row.channelPolicy = some .channels && !isRoot row then
      if let some inbound := row.inboundOne.bind index.byFlow.get? then
        let group := groupId row
        let rankKey := (group, row.channel, row.rank)
        let previousKey := (group, row.channel, inbound.rank)
        require row.srcLine
          (previous.getD rankKey inbound.rank = inbound.rank &&
            next.getD previousKey row.rank = row.rank)
          "channel ring predecessor ranks are not one ring"
        previous := previous.insert rankKey inbound.rank
        next := next.insert previousKey row.rank

def segmentOf (row : Row) : Nat × Nat :=
  (row.segmentSequence, row.segmentSequence + row.segmentBytes)

/-- The receiver's in-order frontier of one stage, replayed incrementally: `frontier` is the
frontier of the segments seen so far, as `tcp_receive_range` computes it (sorted by start, extended
from zero through every segment that begins at or before the frontier), and `pending` maps the
start of every seen segment beyond the frontier to the largest stop among the segments with that
start. The frontier is the least fixpoint of `F = max (0, max {stop | start ≤ F})`, which does not
depend on the order the segments arrive in. Every seen segment that starts at or before the frontier
also ends there, so a new segment beyond the frontier leaves it unchanged, and one at or before it
raises it to its stop and then absorbs the pending segments in start order while they reach it.
Each segment enters and leaves `pending` once: O(log n) per row. -/
structure Frontier where
  frontier : Nat := 0
  pending : Std.TreeMap Nat Nat := ∅

/-- Absorb the pending segments that start at or before `frontier`, in start order. -/
def Frontier.absorb (frontier : Nat) (pending : Std.TreeMap Nat Nat) : Frontier := Id.run do
  let mut frontier := frontier
  let mut pending := pending
  -- Each iteration erases one entry, so `pending.size + 1` iterations always suffice.
  for _ in [0:pending.size + 1] do
    match pending.minEntry? with
    | some (start, stop) =>
        if start ≤ frontier then
          frontier := max frontier stop
          pending := pending.erase start
        else
          break
    | none => break
  pure { frontier, pending }

/-- The replay after one more segment `[start, stop)`. -/
def Frontier.add (state : Frontier) (segment : Nat × Nat) : Frontier :=
  let (start, stop) := segment
  if start ≤ state.frontier then
    Frontier.absorb (max state.frontier stop) state.pending
  else
    { state with pending := state.pending.insert start (max stop (state.pending.getD start 0)) }

/-- The MTU of the RoCE queue pair whose packets a row's inbound segments are, or `none` for TCP
segments. A RoCE stage's inbound predecessor is a stage of its own collective (one transport per
collective), whose MTU `sameGroupConfig` makes the row's own. A compute stage names its inbound
predecessors' transport itself (schema Amendment 5): the MTU in `packet_size_bytes` when they are
RoCE stages, zero for TCP. This holds whether a predecessor is logged or not (the final stage of an
ungated two-rank AllGather is an unlogged root); when it is logged, `checkEntryPredecessors`
requires the two to agree. -/
def roceInboundMtu (row : Row) : Option Nat :=
  match row.stageKind with
  | .roce => some row.packetSizeBytes
  | .tcp => none
  | .compute => if row.packetSizeBytes > 0 then some row.packetSizeBytes else none

/-- Schema Amendment 4: an inbound row of a RoCE predecessor certifies one data packet, which is
the predecessor's packet at its PSN, and the advance of the receiver's Go-back-N frontier
(`Collective.goBackNFrontier`, the `Roce.onData` frontier) from `frontier`, the predecessor's
frontier replayed from its earlier packets: the packet's size when its PSN is the frontier, else
zero, with no hole filling. The stage's byte counts move by that advance from `delivered`, the sum
of its predecessors' frontiers. -/
def checkGoBackN (mtu frontier delivered : Nat) (row : Row) : Except String Unit := do
  require row.srcLine
    (Collective.roceSegment mtu row.causeTotalBytes row.segmentSequence row.segmentBytes)
    "inbound RoCE packet is not the predecessor queue pair's packet at its PSN"
  let after := Collective.goBackNFrontier frontier row.segmentSequence row.segmentBytes
  require row.srcLine
    (row.beforeInboundBytes = delivered && row.afterInboundBytes = delivered + (after - frontier) &&
      row.arrivalBytes = after - frontier)
    "inbound progress does not match the receiver's Go-back-N frontier"

/-- Every segment of a pending inbound predecessor is certified in order, so each predecessor's
frontier at the receiving stage can be replayed from its segments: the Go-back-N frontier for RoCE
packets, TCP's in-order frontier (which merges out-of-order segments) otherwise. A row's frontier
advance is its predecessor's, and the stage's before and after byte counts are the sum of its
predecessors' frontiers before and after. One pass in canonical order keeps each (stage, cause)
`Frontier` and each stage's sum, checks the row against them, and then adds the row's segment. -/
def checkInboundReplay (rows : List Row) : Except String Unit := do
  let mut replays : Std.HashMap (Nat × Nat) Frontier := ∅
  let mut delivered : Std.HashMap Nat Nat := ∅
  for row in rows do
    if row.cause = .inboundArrival then
      let key := (row.flowId, row.causeFlowId)
      let state := replays.getD key {}
      let sum := delivered.getD row.flowId 0
      match roceInboundMtu row with
      | some mtu =>
          checkGoBackN mtu state.frontier sum row
          let after := Collective.goBackNFrontier state.frontier row.segmentSequence row.segmentBytes
          replays := replays.insert key { frontier := after }
          delivered := delivered.insert row.flowId (sum + (after - state.frontier))
      | none =>
          let next := state.add (segmentOf row)
          let advance := next.frontier - state.frontier
          require row.srcLine
            (row.beforeInboundBytes = sum && row.afterInboundBytes = sum + advance &&
              row.arrivalBytes = advance)
            "inbound progress does not match the receiver frontier replayed from the certified segments"
          replays := replays.insert key next
          delivered := delivered.insert row.flowId (sum + advance)

/-- Whether a local completion is caused by a compute timer: its cause's byte total is zero. Every
other local cause is a transport stage's completing ACK (TCP, or a RoCE ACK: a NACK never
completes, since it acknowledges the receiver's frontier below the chunk). `checkCauseTotal` binds
the total to the cause stage. -/
def causeIsTimer (row : Row) : Bool :=
  row.causeTotalBytes = 0

/-- The byte total of a transport local predecessor: from the ring recurrence for a ring stage's
previous step (`EqualRemainderLast`), its own size under `UniformFloor` (every message of a ring
is one size), or the cause's stated total, bound to the cause stage by `checkCauseTotal`. -/
def localPredecessorTotal (row : Row) : Option Nat :=
  match row.stageKind, row.algorithm, row.collectivePhase with
  | .tcp, some algorithm, some phase | .roce, some algorithm, some phase =>
      if isRoot row then some row.causeTotalBytes
      else if row.chunkPolicy = some .equalRemainderLast then
        let position := localPredecessorPosition row phase
        let owner :=
          Collective.stageOwner algorithm position.phase row.groupSize position.rank position.step
        some (Collective.chunkBounds row.declaredTotalBytes row.groupSize owner).2
      else some row.chunkBytes
  | _, _, _ => some row.causeTotalBytes

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
        (localPredecessorTotal row)
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
    first.localPredecessors = second.localPredecessors &&
    first.inboundPredecessors = second.inboundPredecessors &&
    first.localRequired = second.localRequired &&
    first.inboundPredecessorBytes = second.inboundPredecessorBytes

/-- Every logged stage's local predecessor carries bytes or time, so the local prerequisite starts
incomplete unless there is none (a Send/Recv's receiver); so does the inbound prerequisite. -/
def checkInitialState (row : Row) : Except String Unit := do
  require row.srcLine (row.beforeLocalComplete = row.localPredecessors.isEmpty)
    "first local prerequisite state is not initial"
  require row.srcLine
    (row.beforeInboundComplete = row.inboundPredecessors.isEmpty &&
      row.beforeInboundBytes = 0 && row.beforeLocalCompleted = 0)
    "first inbound prerequisite state is not initial"

/-- What `checkContinuity` keeps of the rows before the current one (canonical positions count from
zero). -/
structure ContinuityState where
  /-- The most recent row of each group. -/
  lastOfGroup : Std.HashMap GroupId Row := ∅
  /-- The most recent row of each compute group with inbound predecessors. -/
  lastInboundOfGroup : Std.HashMap GroupId Row := ∅
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
    let inboundCompute := row.stageKind = .compute && !row.inboundPredecessors.isEmpty
    if inboundCompute then
      if let some prior := state.lastInboundOfGroup.get? group then
        require row.srcLine
          (prior.packetSizeBytes = row.packetSizeBytes && prior.intervalNs = row.intervalNs)
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
            row.beforeLocalCompleted = prior.afterLocalCompleted &&
            row.beforeInboundComplete = prior.afterInboundComplete &&
            row.beforeInboundBytes = prior.afterInboundBytes)
          "collective stage before-state does not continue the prior after-state"
    require row.srcLine (!(state.activated.contains stage && row.activated))
      "collective stage activated more than once"
    state :=
      { lastOfGroup := state.lastOfGroup.insert group row
        lastInboundOfGroup :=
          if inboundCompute then state.lastInboundOfGroup.insert group row
          else state.lastInboundOfGroup
        rankNode := state.rankNode.insert (group, row.rank) (row.nodeId, position)
        nodeRank := state.nodeRank.insert (group, row.nodeId) (row.rank, position)
        flowStage := state.flowStage.insert row.flowId (stage, position)
        lastOfStage := state.lastOfStage.insert stage row
        activated := if row.activated then state.activated.insert stage else state.activated }
    position := position + 1

/-- What coverage needs of one group: its distinct stages, its activated rows, whether a root is
logged, and its ring channels. -/
structure GroupTally where
  stages : Nat := 0
  activated : Nat := 0
  hasRoot : Bool := false
  channels : Nat := 0

/-- The per-group tallies, each stage's last row, and each stage's distinct inbound causes with
their summed totals, built in one pass (linear in the trace). -/
structure CoverageIndex where
  groups : Std.HashMap GroupId GroupTally := ∅
  finals : Std.HashMap StageId Row := ∅
  causes : Std.HashSet (StageId × Nat) := ∅
  causeCounts : Std.HashMap StageId Nat := ∅
  causeTotals : Std.HashMap StageId Nat := ∅

def coverageIndex (rows : List Row) : CoverageIndex := Id.run do
  let mut index : CoverageIndex := {}
  for row in rows do
    let stage := stageId row
    let tally := index.groups.getD (groupId row) {}
    let newStage := !index.finals.contains stage
    index :=
      { index with
        groups := index.groups.insert (groupId row)
          { stages := tally.stages + (if newStage then 1 else 0)
            activated := tally.activated + (if row.activated then 1 else 0)
            hasRoot := tally.hasRoot || isRoot row
            channels := max tally.channels (row.channel + 1) }
        -- Rows are canonical, so the last insertion is the stage's final row.
        finals := index.finals.insert stage row }
    if row.cause = .inboundArrival && !index.causes.contains (stage, row.causeFlowId) then
      index :=
        { index with
          causes := index.causes.insert (stage, row.causeFlowId)
          causeCounts := index.causeCounts.insert stage (index.causeCounts.getD stage 0 + 1)
          causeTotals :=
            index.causeTotals.insert stage (index.causeTotals.getD stage 0 + row.causeTotalBytes) }
  pure index

/-- A complete trace releases every logged stage exactly once and leaves it complete, each
inbound predecessor having delivered its whole total. A transport collective logs its non-root
stages, plus its roots when compute stages gate them; a compute group logs one stage per rank. The
group's stage count in the image agrees with its algorithm, except for a seeded all-to-all, whose
pairs of zero bytes have no stage: its roots are all gated, so it logs exactly that many. A
channel's messages are `UniformFloor` over the collective's channels, at their owner's slot. The
group tallies are computed once, so the check is linear; each row is checked in canonical order,
as by a per-row scan of its group. -/
def checkCoverage (rows : List Row) : Except String Unit := do
  let index := coverageIndex rows
  for row in rows do
    let tally := index.groups.getD (groupId row) {}
    let seeded := row.chunkPolicy = some .seeded
    let expected :=
      match row.stageKind, row.algorithm with
      | .tcp, some algorithm | .roce, some algorithm =>
          if seeded then row.groupStages
          else Collective.expectedActivationCountOf algorithm row.groupSize tally.channels
            tally.hasRoot
      | _, _ => row.groupSize
    let imageStages :=
      match row.stageKind, row.algorithm with
      | .tcp, some algorithm | .roce, some algorithm =>
          Collective.expectedActivationCountOf algorithm row.groupSize tally.channels true
      | _, _ => row.groupSize
    require row.srcLine
      (if seeded then row.groupStages ≤ row.groupSize * (row.groupSize - 1)
        else row.groupStages = imageStages)
      s!"collective stage count disagrees with its algorithm for collective_id={row.collectiveId}"
    require row.srcLine (tally.stages = expected)
      s!"incomplete collective progress coverage for collective_id={row.collectiveId}: expected {expected}, found {tally.stages}"
    if let (some algorithm, some .channels) := (row.algorithm, row.channelPolicy) then
      let bytes := Collective.uniformFloorBytes algorithm row.declaredTotalBytes row.groupSize
        tally.channels
      let slot := row.chunkOffsetBytes / max bytes 1
      require row.srcLine
        (row.chunkBytes = bytes && row.chunkOffsetBytes % max bytes 1 = 0 &&
          slot % tally.channels = row.channel && slot < row.groupSize * tally.channels)
        "collective chunk does not match its chunk policy"
    let final ← requireSome row.srcLine "final collective stage progress"
      (index.finals.get? (stageId row))
    require row.srcLine
      (final.afterLocalComplete && final.afterInboundComplete &&
        inboundDone final final.afterInboundBytes)
      "collective stage final prerequisite state is incomplete"
    if !row.inboundPredecessors.isEmpty then
      require row.srcLine
        (index.causeCounts.getD (stageId row) 0 = row.inboundPredecessors.length &&
          index.causeTotals.getD (stageId row) 0 = row.inboundPredecessorBytes)
        "a stage's inbound predecessors do not deliver its inbound requirement"
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
  checkChannelRings index canonical
  for row in canonical do checkPredecessors index row
  checkInboundReplay canonical
  for row in canonical do checkLocalSignal index row

end LeanGuard.P10c.CollectiveEventLog
