import LeanGuard.P10c.CollectiveEventLog

/-! Test-only reference copies of the P10c collective checker's list-scan functions, as shipped at
`26dc1d1` (P16 lane L1) and extended for P16 H1's operations and joins. The shipped checker
(`LeanGuard.P10c.CollectiveEventLog`) replaced these quadratic scans with keyed state;
`p10c_collective_diff` runs both on every campaign input and on a generated mutation set and
requires identical `Except` results. Nothing in a shipped checker imports this module. Bodies
follow the shipped checks with every keyed lookup replaced by a scan of the canonical trace; the
entry-predecessor rule (`checkEntryPredecessors`) is shared, with its lookups (`EntryLookups`)
answered by scans here. -/

namespace LeanGuard.P10c.CollectiveEventLog

open LeanGuard.Shared
open LeanGuard.P10c

def position? (row : Row) : Option Position :=
  row.collectivePhase.map fun phase =>
    { phase := phase, channel := row.channel, rank := row.rank, step := row.step }

def atPosition (collective : Row) (wanted : Position) (candidate : Row) : Bool :=
  sameGroup collective candidate && position? candidate = some wanted

def findStageReference (rows : List Row) (collective : Row) (wanted : Position) : Option Row :=
  rows.find? (atPosition collective wanted)

def findActivatedStageReference (rows : List Row) (collective : Row) (wanted : Position) : Option Row :=
  rows.find? fun candidate => atPosition collective wanted candidate && candidate.activated

def findFlowReference (rows : List Row) (flow : Nat) : Option Row :=
  rows.find? (·.flowId = flow)

def findActivatedFlowReference (rows : List Row) (flow : Nat) : Option Row :=
  rows.find? fun candidate => candidate.flowId = flow && candidate.activated

/-- Resolve a stage's flow, including an unlogged root via its local successor edge. -/
def resolveStageFlowReference (rows : List Row) (collective : Row) (wanted : Position) : Option Nat :=
  match findStageReference rows collective wanted with
  | some stage => some stage.flowId
  | none =>
      (rows.find? fun successor =>
        sameGroup collective successor && !isRoot successor &&
          (successor.collectivePhase.map (localPredecessorPosition successor ·)) = some wanted
        ).bind (·.localOne)

/-- The distinct stages among `rows`. -/
def distinctStages (rows : List Row) : Nat :=
  (rows.map stageId).eraseDups.length

def entryLookupsReference (rows : List Row) : EntryLookups :=
  { flow? := findFlowReference rows
    rankNode? := fun (group, rank) =>
      (rows.find? fun candidate => groupId candidate = group && candidate.rank = rank).map
        (·.nodeId)
    nodeRank? := fun (group, node) =>
      (rows.find? fun candidate => groupId candidate = group && candidate.nodeId = node).map
        (·.rank)
    channels := fun group =>
      match (rows.filter (groupId · = group)) with
      | [] => 1
      | members => members.foldl (fun channels candidate => max channels (candidate.channel + 1)) 0
    completionsAt := fun (group, node) =>
      distinctStages (rows.filter fun candidate =>
        groupId candidate = group && isCompletion candidate && candidate.nodeId = node)
    completionsInto := fun (group, rank) =>
      distinctStages (rows.filter fun candidate =>
        groupId candidate = group && isCompletion candidate && targetRank candidate = some rank) }

def checkTransportPredecessorsReference (rows : List Row) (row : Row) (phase : Collective.Phase) :
    Except String Unit := do
  if isRoot row then
    -- A root's gate is a compute stage, never a stage of its own collective.
    require row.srcLine
      ((row.localPredecessors ++ row.inboundPredecessors).all fun gate =>
        !rows.any fun candidate => sameGroup row candidate && candidate.flowId = gate)
      "a root stage's gate must be a compute stage outside its collective"
    checkEntryPredecessors (entryLookupsReference rows) row
    match row.cause with
    | .localCompletion =>
        requireEarlierRelease row (findActivatedFlowReference rows row.causeFlowId)
          "local predecessor stage did not activate earlier"
    | .inboundArrival =>
        requireEarlierRelease row (findActivatedFlowReference rows row.causeFlowId)
          "inbound predecessor stage did not activate earlier"
    return
  let localPosition := localPredecessorPosition row phase
  match resolveStageFlowReference rows row localPosition with
  | none => pure ()
  | some expected =>
      require row.srcLine (row.localOne = some expected)
        "local predecessor identity does not match the stage recurrence"
  if row.channelPolicy = some .channels then
    let inbound? := row.inboundOne.bind (findFlowReference rows)
    if let some inbound := inbound? then
      require row.srcLine
        (sameGroup inbound row && inbound.collectivePhase = some localPosition.phase &&
          inbound.channel = row.channel && inbound.step = localPosition.step &&
          inbound.rank != row.rank && inbound.chunkBytes = row.chunkBytes &&
          inbound.chunkOffsetBytes = row.chunkOffsetBytes)
        "inbound predecessor identity does not match the stage recurrence"
    match row.cause with
    | .localCompletion =>
        requireEarlierRelease row (findActivatedStageReference rows row localPosition)
          "local predecessor stage did not activate earlier"
    | .inboundArrival =>
        requireEarlierRelease row (row.inboundOne.bind (findActivatedFlowReference rows))
          "inbound predecessor stage did not activate earlier"
    return
  let inboundPosition := inboundPredecessorPosition row phase
  match resolveStageFlowReference rows row inboundPosition with
  | none => pure ()
  | some expected =>
      require row.srcLine (row.inboundOne = some expected)
        "inbound predecessor identity does not match the stage recurrence"
  match row.cause with
  | .localCompletion =>
      requireEarlierRelease row (findActivatedStageReference rows row localPosition)
        "local predecessor stage did not activate earlier"
  | .inboundArrival =>
      requireEarlierRelease row (findActivatedStageReference rows row inboundPosition)
        "inbound predecessor stage did not activate earlier"

def checkComputePredecessorsReference (rows : List Row) (row : Row) : Except String Unit := do
  checkEntryPredecessors (entryLookupsReference rows) row
  match row.cause with
  | .localCompletion =>
      requireEarlierRelease row (findActivatedFlowReference rows row.causeFlowId)
        "local predecessor stage did not activate earlier"
  | .inboundArrival =>
      requireEarlierRelease row (findActivatedFlowReference rows row.causeFlowId)
        "inbound predecessor stage did not activate earlier"

/-- A compute stage completes only when its timer fires. When the completed compute stage is
logged, its release row records the timer deadline (`after_next_time_ns` = release + duration), so
the successor's local completion must happen exactly then, as a phase-1 timer event. -/
def checkComputeTimerCauseReference (rows : List Row) (row : Row) : Except String Unit := do
  if row.cause = .localCompletion then
    let released? := rows.find? fun candidate =>
      candidate.flowId = row.causeFlowId &&
        (candidate.stageKind = .compute || candidate.stageKind = .notify) && candidate.activated
    match released? with
    | none => pure ()
    | some predecessor =>
        require row.srcLine
          (row.key.timeNs = predecessor.afterNextTimeNs && row.key.phase = 1)
          "compute local completion does not occur at its predecessor's timer deadline"

def checkCauseTotalReference (rows : List Row) (row : Row) : Except String Unit := do
  require row.srcLine
    ((rows.find? (·.causeFlowId = row.causeFlowId)).map (·.causeTotalBytes) =
      some row.causeTotalBytes)
    "cause byte total disagrees with an earlier row of the same cause"
  require row.srcLine ((row.causeKind = .compute) = (row.causeTotalBytes = 0))
    "a compute cause names no bytes, and every other cause names its total"
  if let some cause := findFlowReference rows row.causeFlowId then
    require row.srcLine (row.causeTotalBytes = ownTotal cause)
      "cause byte total disagrees with the cause stage"
    require row.srcLine (row.causeKind = cause.stageKind.carrier)
      "cause kind disagrees with the cause stage"

def checkPredecessorsReference (rows : List Row) (row : Row) : Except String Unit := do
  checkCauseTotalReference rows row
  checkComputeTimerCauseReference rows row
  match row.stageKind, row.collectivePhase with
  | .tcp, some phase | .roce, some phase | .notify, some phase =>
      checkTransportPredecessorsReference rows row phase
  | .tcp, none | .roce, none | .notify, none => pure ()
  | .compute, _ => checkComputePredecessorsReference rows row

/-- The channel-ring rule, by a scan of the earlier channel rows of the same channel, then one
walk per channel over its rows' (previous rank, rank) pairs, in first-row order. -/
def checkChannelRingsReference (rows : List Row) : Except String Unit := do
  let inboundRank (row : Row) : Option Nat :=
    if row.channelPolicy = some .channels && !isRoot row then
      (row.inboundOne.bind (findFlowReference rows)).map (·.rank)
    else none
  let rec go (previous : List Row) : List Row → Except String Unit
    | [] => pure ()
    | row :: rest => do
        if let some mine := inboundRank row then
          require row.srcLine
            (!previous.any fun prior =>
              sameGroup prior row && prior.channel = row.channel &&
                match inboundRank prior with
                | some theirs => (prior.rank = row.rank) != (theirs = mine)
                | none => false)
            "channel ring predecessor ranks are not one ring"
        go (row :: previous) rest
  go [] rows
  let named := rows.filter fun row => (inboundRank row).isSome
  let firsts := named.filter fun row =>
    (named.find? fun other => sameGroup other row && other.channel = row.channel) = some row
  for first in firsts do
    let pairs := named.filterMap fun row =>
      if sameGroup row first && row.channel = first.channel then
        (inboundRank row).map fun before => (before, row.rank)
      else none
    let next := pairs.foldl (fun map (before, rank) => map.insert before rank)
      (∅ : Std.HashMap Nat Nat)
    require first.srcLine (singleCycle first.groupSize next)
      "channel ring predecessor ranks are not one ring"

/-- The receiver's in-order frontier after the half-open segments `[start, stop)` arrive, as
`tcp_receive_range` computes it: sorted by start, extended from zero through every segment that
begins at or before the frontier. -/
def frontierOfReference (segments : List (Nat × Nat)) : Nat :=
  let sorted :=
    (segments.toArray.qsort fun a b => a.1 < b.1 || (a.1 = b.1 && a.2 < b.2)).toList
  sorted.foldl (fun frontier segment =>
    if segment.1 ≤ frontier then max frontier segment.2 else frontier) 0

/-- Each inbound predecessor's frontier at the receiving stage is replayed from its earlier
segments (Go-back-N for RoCE packets, TCP's merging frontier otherwise); the stage's byte counts
are the sum of its predecessors' frontiers. -/
def checkInboundReplayReference (rows : List Row) (row : Row) : Except String Unit := do
  if row.cause = .inboundArrival then
    let prior := rows.filter fun candidate =>
      candidate.flowId = row.flowId && candidate.cause = .inboundArrival &&
        compositeLT candidate row
    let fabricMtu := (rows.find? fun candidate =>
      sameGroup candidate row && candidate.stageKind = .roce).map (·.packetSizeBytes)
    -- Each cause's frontier, replayed by that cause's own replay (a row's replay depends on its
    -- cause's carrier, which every row of one cause shares).
    let frontierOf (cause : Nat) (extra : List Row) : Nat :=
      let segments := prior.filter (·.causeFlowId = cause) ++ extra
      match segments.head?.map (replayOf fabricMtu) with
      | some (.goBackN _) =>
          segments.foldl (fun frontier candidate =>
            Collective.goBackNFrontier frontier candidate.segmentSequence
              candidate.segmentBytes) 0
      | some .whole => segments.foldl (fun _ candidate => candidate.segmentBytes) 0
      | _ => frontierOfReference (segments.map segmentOf)
    let sum := ((prior.map (·.causeFlowId)).eraseDups.map (frontierOf · [])).foldl (· + ·) 0
    let before := frontierOf row.causeFlowId []
    match replayOf fabricMtu row with
    | .whole =>
        require row.srcLine
          (row.segmentSequence = 0 && row.segmentBytes = row.causeTotalBytes &&
            before == 0 && row.arrivalBytes = row.segmentBytes &&
            row.beforeInboundBytes = sum && row.afterInboundBytes = sum + row.arrivalBytes)
          "inbound stage notify does not deliver its whole chunk at once"
    | .goBackN mtu => checkGoBackN mtu before sum row
    | .tcp =>
        let after := frontierOf row.causeFlowId [row]
        require row.srcLine
          (row.beforeInboundBytes = sum && row.afterInboundBytes = sum + (after - before) &&
            row.arrivalBytes = after - before)
          "inbound progress does not match the receiver frontier replayed from the certified segments"

/-- Binds a local completion to the event that caused it (see `checkLocalSignal`). -/
def checkLocalSignalReference (rows : List Row) (row : Row) : Except String Unit := do
  if row.cause = .localCompletion then
    let released? := findActivatedFlowReference rows row.causeFlowId
    if causeIsTimer row then
      require row.srcLine
        (row.ackNumber = 0 && row.causeDelayNs > 0 &&
          row.key.timeNs = row.causeOriginNs + row.causeDelayNs)
        "compute timer completion does not occur at arm time plus duration"
      match released? with
      | some predecessor =>
          require row.srcLine
            (row.causeOriginNs = predecessor.key.timeNs &&
              (predecessor.stageKind = .compute && row.causeDelayNs = predecessor.durationNs ||
                predecessor.stageKind = .notify && row.causeDelayNs = predecessor.intervalNs))
            "compute timer completion does not match its predecessor's release and duration"
      | none =>
          require row.srcLine (row.causeKind = .notify || row.causeOriginNs = 0)
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
      -- The first inbound row at which the cause's arrivals at a receiving stage reach its total.
      let delivered? := rows.find? fun candidate =>
        candidate.cause = .inboundArrival && candidate.causeFlowId = row.causeFlowId &&
          (let upTo := rows.filter fun earlier =>
              earlier.flowId = candidate.flowId && earlier.cause = .inboundArrival &&
                earlier.causeFlowId = candidate.causeFlowId && !compositeLT candidate earlier
            let after := upTo.foldl (fun sum earlier => sum + earlier.arrivalBytes) 0
            after - candidate.arrivalBytes < candidate.causeTotalBytes &&
              after = candidate.causeTotalBytes)
      match delivered? with
      | some delivered =>
          require row.srcLine (compositeLT delivered row && delivered.key.timeNs < row.key.timeNs)
            "local completion precedes its predecessor's delivery at the receiver"
      | none => pure ()
      require row.srcLine
        (row.causeDelayNs > 0 && row.key.timeNs ≥ row.causeOriginNs + row.causeDelayNs)
        "local completion precedes the earliest return of the completing acknowledgment"

def checkContinuityReference (rows : List Row) : Except String Unit := do
  let rec go (previous : List Row) : List Row → Except String Unit
    | [] => pure ()
    | row :: rest => do
        match previous.find? (sameGroup · row) with
        | none => pure ()
        | some prior =>
            require row.srcLine (sameGroupConfig prior row)
              s!"collective configuration discontinuity for collective_id={row.collectiveId}"
        if row.sharesTransport then
          match previous.find? (fun prior =>
              sameGroup prior row && prior.stageKind = row.stageKind && prior.sharesTransport) with
          | none => pure ()
          | some prior =>
              require row.srcLine
                (prior.packetSizeBytes = row.packetSizeBytes && prior.intervalNs = row.intervalNs)
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
                row.beforeLocalCompleted = prior.afterLocalCompleted &&
                row.beforeInboundComplete = prior.afterInboundComplete &&
                row.beforeInboundBytes = prior.afterInboundBytes)
              "collective stage before-state does not continue the prior after-state"
        require row.srcLine
          (!previous.any (fun prior => sameStage prior row && prior.activated && row.activated))
          "collective stage activated more than once"
        go (row :: previous) rest
  go [] rows

def checkRowsReference (rows : List Row) : Except String Unit := do
  require 1 (!rows.isEmpty) "empty collective activation trace"
  let canonical ← canonicalize rows
  checkOrdinals canonical
  checkContinuityReference canonical
  for row in canonical do checkRow row
  checkCoverage canonical
  checkChannelRingsReference canonical
  for row in canonical do checkPredecessorsReference canonical row
  for row in canonical do checkInboundReplayReference canonical row
  for row in canonical do checkLocalSignalReference canonical row

end LeanGuard.P10c.CollectiveEventLog
