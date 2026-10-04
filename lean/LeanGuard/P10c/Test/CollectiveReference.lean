import LeanGuard.P10c.CollectiveEventLog

/-! Test-only reference copies of the P10c collective checker's list-scan functions, as shipped at
`26dc1d1` (P16 lane L1). The shipped checker (`LeanGuard.P10c.CollectiveEventLog`) replaced these
quadratic scans with keyed state; `p10c_collective_diff` runs both on every campaign input and on a
generated mutation set and requires identical `Except` results. Nothing in a shipped checker
imports this module. Bodies are verbatim apart from the `Reference` suffix on the names. -/

namespace LeanGuard.P10c.CollectiveEventLog

open LeanGuard.Shared
open LeanGuard.P10c

def findStageReference (rows : List Row) (collective : Row) (wanted : Position) : Option Row :=
  rows.find? (atPosition collective wanted)

def findActivatedStageReference (rows : List Row) (collective : Row) (wanted : Position) : Option Row :=
  rows.find? fun candidate => atPosition collective wanted candidate && candidate.activated

/-- Resolve a stage's flow, including an unlogged root via its local successor edge. -/
def resolveStageFlowReference (rows : List Row) (collective : Row) (wanted : Position) : Option Nat :=
  match findStageReference rows collective wanted with
  | some stage => some stage.flowId
  | none =>
      (rows.find? fun successor =>
        sameGroup collective successor && !isRoot successor &&
          (successor.collectivePhase.map (localPredecessorPosition successor ·)) = some wanted
        ).bind (·.localPredecessorFlowId)


def checkTransportPredecessorsReference (rows : List Row) (row : Row) (phase : Collective.Phase) :
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
  match resolveStageFlowReference rows row localPosition with
  | none => pure ()
  | some expected =>
      require row.srcLine (row.localPredecessorFlowId = some expected)
        "local predecessor identity does not match the stage recurrence"
  match resolveStageFlowReference rows row inboundPosition with
  | none => pure ()
  | some expected =>
      require row.srcLine (row.inboundPredecessorFlowId = some expected)
        "inbound predecessor identity does not match the stage recurrence"
  match row.cause with
  | .localCompletion =>
      requireEarlierRelease row (findActivatedStageReference rows row localPosition)
        "local predecessor stage did not activate earlier"
  | .inboundArrival =>
      requireEarlierRelease row (findActivatedStageReference rows row inboundPosition)
        "inbound predecessor stage did not activate earlier"

/-- A compute stage follows the same-rank stage of a compute group, or the same-rank final stage
of a collective together with the previous rank's final stage. Logged predecessors are checked. -/
def checkComputePredecessorsReference (rows : List Row) (row : Row) : Except String Unit := do
  let previousRank := if row.rank = 0 then row.groupSize - 1 else row.rank - 1
  let isFinal (candidate : Row) (rank : Nat) : Bool :=
    candidate.stageKind.isTransport && candidate.collectivePhase = some .allGather &&
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
def checkComputeTimerCauseReference (rows : List Row) (row : Row) : Except String Unit := do
  if row.cause = .localCompletion then
    let released? := rows.find? fun candidate =>
      candidate.flowId = row.causeFlowId && candidate.stageKind = .compute && candidate.activated
    match released? with
    | none => pure ()
    | some predecessor =>
        require row.srcLine
          (row.key.timeNs = predecessor.afterNextTimeNs && row.key.phase = 1)
          "compute local completion does not occur at its predecessor's timer deadline"

def checkPredecessorsReference (rows : List Row) (row : Row) : Except String Unit := do
  checkComputeTimerCauseReference rows row
  match row.stageKind, row.collectivePhase with
  | .tcp, some phase | .roce, some phase => checkTransportPredecessorsReference rows row phase
  | .tcp, none | .roce, none => pure ()
  | .compute, _ => checkComputePredecessorsReference rows row

/-- The receiver's in-order frontier after the half-open segments `[start, stop)` arrive, as
`tcp_receive_range` computes it: sorted by start, extended from zero through every segment that
begins at or before the frontier. -/
def frontierOfReference (segments : List (Nat × Nat)) : Nat :=
  let sorted :=
    (segments.toArray.qsort fun a b => a.1 < b.1 || (a.1 = b.1 && a.2 < b.2)).toList
  sorted.foldl (fun frontier segment =>
    if segment.1 ≤ frontier then max frontier segment.2 else frontier) 0


/-- Every segment of a pending inbound predecessor is certified in order, so the before and after
byte counts of each inbound row must equal the frontier replayed from that stage's segments: the
Go-back-N frontier for RoCE packets, TCP's in-order frontier (which merges out-of-order segments)
otherwise. -/
def checkInboundReplayReference (rows : List Row) (row : Row) : Except String Unit := do
  if row.cause = .inboundArrival then
    if let some mtu := roceInboundMtu row then
      return (← checkGoBackN mtu row)
    let prior :=
      (rows.filter fun candidate =>
        candidate.flowId = row.flowId && candidate.cause = .inboundArrival &&
          compositeLT candidate row).map segmentOf
    let before := frontierOfReference prior
    let after := frontierOfReference (prior ++ [segmentOf row])
    require row.srcLine
      (row.beforeInboundBytes = before && row.afterInboundBytes = after &&
        row.arrivalBytes = after - before)
      "inbound progress does not match the receiver frontier replayed from the certified segments"


/-- The byte total of a transport local predecessor: from the ring recurrence for a transport
stage, or from the rows of the stage that receives it (its inbound byte count) for a compute
stage. -/
def localPredecessorTotalReference (rows : List Row) (row : Row) : Option Nat :=
  let received :=
    (rows.find? fun candidate =>
      candidate.inboundPredecessorFlowId = some row.causeFlowId).map (·.inboundPredecessorBytes)
  match row.stageKind, row.algorithm, row.collectivePhase with
  | .tcp, some algorithm, some phase | .roce, some algorithm, some phase =>
      let position := localPredecessorPosition row phase
      let owner :=
        Collective.stageOwner algorithm position.phase row.groupSize position.rank position.step
      some (Collective.chunkBounds row.declaredTotalBytes row.groupSize owner).2
  | _, _, _ => received

/-- Binds a local completion to the event that caused it.

A compute timer completes its successor exactly at arm time + duration; a logged compute
predecessor was armed at its release, an unlogged one (a root) at time zero. A transport
predecessor (TCP, or a RoCE queue pair) completes at the first ACK whose acknowledgment reaches its
byte total: the row names that
acknowledgment, which must equal the predecessor's total; the answered segment was sent after the
predecessor's release; and the ACK arrives after the receiver's frontier completed and no sooner
than the segment's send time plus the unloaded round trip of the segment and the ACK. -/
def checkLocalSignalReference (rows : List Row) (row : Row) : Except String Unit := do
  if row.cause = .localCompletion then
    let released? := rows.find? fun candidate =>
      candidate.flowId = row.causeFlowId && candidate.activated
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
        (localPredecessorTotalReference rows row)
      require row.srcLine (row.ackNumber = total)
        "completing acknowledgment does not reach exactly the local predecessor's byte total"
      match released? with
      | some predecessor =>
          require row.srcLine (row.causeOriginNs ≥ predecessor.key.timeNs)
            "acknowledged segment was sent before its stage was released"
      | none => pure ()
      let delivered? := rows.find? fun candidate =>
        candidate.cause = .inboundArrival && candidate.causeFlowId = row.causeFlowId &&
          candidate.afterInboundComplete && !candidate.beforeInboundComplete
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

def checkRowsReference (rows : List Row) : Except String Unit := do
  require 1 (!rows.isEmpty) "empty collective activation trace"
  let canonical ← canonicalize rows
  checkOrdinals canonical
  checkContinuityReference canonical
  for row in canonical do checkRow row
  checkCoverage canonical
  for row in canonical do checkPredecessorsReference canonical row
  for row in canonical do checkInboundReplayReference canonical row
  for row in canonical do checkLocalSignalReference canonical row

end LeanGuard.P10c.CollectiveEventLog
