import Std

import LeanGuard.Shared.Check

namespace LeanGuard.Drr.Semantics

open LeanGuard.Shared

/-- Executable DRR semantics based on Shreedhar-Varghese (1996), "Efficient Fair Queueing Using Deficit Round Robin." -/
inductive Kind
  | enqueue
  | schedule
deriving DecidableEq, Repr

structure Event where
  timeNs : Nat
  eventId : Nat
  kind : Kind
  schedulerId : Nat
  classCount : Nat
  batchId : Option Nat
  packetId : Nat
  flowId : Nat
  classId : Nat
  sizeBytes : Nat
  quantumBytes : Nat
  deficitBytes : Nat
  rateBps : Nat
  currentQueue : Nat
  scanSteps : Nat
  departureTimeNs : Option Nat
deriving Repr

structure PacketInfo where
  packetId : Nat
  flowId : Nat
  sizeBytes : Nat
deriving Repr, DecidableEq

structure SchedulerState where
  classCount : Nat := 0
  rateBps : Nat := 0
  quantum : Std.HashMap Nat Nat := ∅
  deficit : Std.HashMap Nat Nat := ∅
  queues : Std.HashMap Nat (List PacketInfo) := ∅
  currentQueue : Nat := 0
  packetsWaiting : Nat := 0
  batchId : Option Nat := none
  batchClass : Option Nat := none
  batchNextStartNs : Option Nat := none
  lastTimeNs : Option Nat := none
deriving Repr

structure Global where
  schedulers : Std.HashMap Nat SchedulerState := ∅
deriving Repr

def ensureClassCount (lineNo : Nat) (st : SchedulerState) (classCount : Nat) :
    Except String SchedulerState := do
  require lineNo (classCount > 0) "class_count must be > 0"
  if st.classCount = 0 then
    pure { st with classCount := classCount }
  else
    require lineNo (st.classCount = classCount)
      s!"class_count changed: got {classCount}, expected {st.classCount}"
    pure st

def ensureRate (lineNo : Nat) (st : SchedulerState) (rateBps : Nat) :
    Except String SchedulerState := do
  require lineNo (rateBps > 0) "rate_bps must be > 0"
  if st.rateBps = 0 then
    pure { st with rateBps := rateBps }
  else
    require lineNo (st.rateBps = rateBps)
      s!"rate_bps changed: got {rateBps}, expected {st.rateBps}"
    pure st

def ensureQuantum (lineNo : Nat) (st : SchedulerState) (classId quantumBytes : Nat) :
    Except String SchedulerState := do
  require lineNo (quantumBytes > 0) s!"quantum_bytes must be > 0 for class {classId}"
  match st.quantum.get? classId with
  | none =>
      pure { st with quantum := st.quantum.insert classId quantumBytes }
  | some q => do
      require lineNo (q = quantumBytes)
        s!"quantum_bytes changed for class {classId}: got {quantumBytes}, expected {q}"
      pure st

def updateLastTime (lineNo : Nat) (st : SchedulerState) (timeNs : Nat) :
    Except String SchedulerState := do
  match st.lastTimeNs with
  | none => pure { st with lastTimeNs := some timeNs }
  | some prev => do
      require lineNo (prev <= timeNs) s!"time went backwards: {prev} > {timeNs}"
      pure { st with lastTimeNs := some timeNs }

def getQueue (st : SchedulerState) (classId : Nat) : List PacketInfo :=
  match st.queues.get? classId with
  | none => []
  | some q => q

def setQueue (st : SchedulerState) (classId : Nat) (q : List PacketInfo) : SchedulerState :=
  { st with queues := st.queues.insert classId q }

def getDeficit (st : SchedulerState) (classId : Nat) : Nat :=
  match st.deficit.get? classId with
  | none => 0
  | some d => d

def setDeficit (st : SchedulerState) (classId deficit : Nat) : SchedulerState :=
  { st with deficit := st.deficit.insert classId deficit }

def queueEmpty (st : SchedulerState) (classId : Nat) : Bool :=
  (getQueue st classId).isEmpty

def classIds (n : Nat) : List Nat :=
  let rec go (i : Nat) (acc : List Nat) : List Nat :=
    if h : i = 0 then
      acc
    else
      go (i - 1) ((i - 1) :: acc)
  go n []

def updateDeficits (lineNo : Nat) (st : SchedulerState) : Except String SchedulerState := do
  let rec go (ids : List Nat) (st : SchedulerState) : Except String SchedulerState := do
    match ids with
    | [] => pure st
    | cid :: rest => do
        let st' :=
          if queueEmpty st cid then
            setDeficit st cid 0
          else
            match st.quantum.get? cid with
            | none => st
            | some q => setDeficit st cid (getDeficit st cid + q)
        if queueEmpty st cid then
          pure ()
        else
          require lineNo (st.quantum.get? cid).isSome
            s!"missing quantum_bytes for class {cid}"
        go rest st'
  go (classIds st.classCount) st

def nextQueue (lineNo : Nat) (st : SchedulerState) : Except String SchedulerState := do
  let next := st.currentQueue + 1
  if next < st.classCount then
    pure { st with currentQueue := next }
  else
    let st' := { st with currentQueue := 0 }
    updateDeficits lineNo st'

def advanceQueue (lineNo : Nat) (st : SchedulerState) (steps : Nat) :
    Except String SchedulerState := do
  let rec go (n : Nat) (st : SchedulerState) : Except String SchedulerState := do
    match n with
    | 0 => pure st
    | n + 1 => do
        let st' ← nextQueue lineNo st
        go n st'
  go steps st

def applyBatch (lineNo : Nat) (st : SchedulerState) (batchId : Nat) (timeNs : Nat) :
    Except String SchedulerState := do
  let st' :=
    match st.batchId with
    | none => { st with batchId := some batchId, batchClass := none, batchNextStartNs := none }
    | some prev =>
        if batchId = prev then
          st
        else
          { st with batchId := some batchId, batchClass := none, batchNextStartNs := none }
  match st.batchId with
  | some prev =>
      require lineNo (batchId >= prev)
        s!"batch_id went backwards: {batchId} < {prev}"
  | none => pure ()
  match st'.batchNextStartNs with
  | none =>
      pure { st' with batchNextStartNs := some timeNs }
  | some exp => do
      require lineNo (exp = timeNs)
        s!"batch service_start mismatch: got {timeNs}, expected {exp}"
      pure st'

def stepEnqueue (lineNo : Nat) (st : SchedulerState) (e : Event) :
    Except String SchedulerState := do
  require lineNo (e.batchId.isNone) "batch_id must be empty for enqueue"
  require lineNo (e.departureTimeNs.isNone) "departure_time_ns must be empty for enqueue"
  require lineNo (e.scanSteps = 0) "scan_steps must be 0 for enqueue"
  require lineNo (e.sizeBytes > 0) "size_bytes must be > 0"
  require lineNo (e.classId < st.classCount) "class_id out of range"
  require lineNo (e.currentQueue < st.classCount) "current_queue out of range"
  require lineNo (e.currentQueue = st.currentQueue) "current_queue mismatch"

  let pkt := { packetId := e.packetId, flowId := e.flowId, sizeBytes := e.sizeBytes }
  let q := getQueue st e.classId
  let q' := q ++ [pkt]
  let st' := setQueue st e.classId q'
  let st'' := { st' with packetsWaiting := st'.packetsWaiting + 1 }

  let expDef := getDeficit st'' e.classId
  require lineNo (e.deficitBytes = expDef)
    s!"deficit_bytes mismatch: got {e.deficitBytes}, expected {expDef}"
  pure st''

def stepSchedule (lineNo : Nat) (st : SchedulerState) (e : Event) :
    Except String SchedulerState := do
  let batchId ← requireSome lineNo "batch_id" e.batchId
  let dep ← requireSome lineNo "departure_time_ns" e.departureTimeNs
  require lineNo (dep >= e.timeNs) "departure_time_ns precedes time_ns"
  require lineNo (e.sizeBytes > 0) "size_bytes must be > 0"
  require lineNo (e.classId < st.classCount) "class_id out of range"
  require lineNo (e.currentQueue < st.classCount) "current_queue out of range"
  require lineNo (st.packetsWaiting > 0) "schedule with empty queues"

  let st' ← applyBatch lineNo st batchId e.timeNs
  let st'' ← advanceQueue lineNo st' e.scanSteps
  require lineNo (st''.currentQueue = e.classId)
    s!"current_queue mismatch: got {st''.currentQueue}, expected {e.classId}"

  let batchClass :=
    match st''.batchClass with
    | none => some e.classId
    | some cid =>
        if cid = e.classId then st''.batchClass else some cid
  match st''.batchClass with
  | none => pure ()
  | some cid =>
      require lineNo (cid = e.classId)
        s!"batch class mismatch: got {e.classId}, expected {cid}"

  let q := getQueue st'' e.classId
  let head ←
    match q with
    | [] => throw s!"line {lineNo}: schedule on empty class queue"
    | h :: _ => pure h

  require lineNo (head.packetId = e.packetId) "packet_id mismatch"
  require lineNo (head.flowId = e.flowId) "flow_id mismatch"
  require lineNo (head.sizeBytes = e.sizeBytes) "size_bytes mismatch"

  let deficit := getDeficit st'' e.classId
  require lineNo (deficit > 0) "deficit_bytes must be > 0 for schedule"
  require lineNo (e.sizeBytes <= deficit) "packet size exceeds deficit"

  let q' := q.tail
  let st1 := setQueue st'' e.classId q'
  let st2 := { st1 with packetsWaiting := st1.packetsWaiting - 1 }
  let st3 := setDeficit st2 e.classId (deficit - e.sizeBytes)
  let st4 :=
    { st3 with
      batchClass := batchClass
      batchNextStartNs := some dep }

  require lineNo (e.currentQueue = st4.currentQueue) "current_queue mismatch"
  require lineNo (e.deficitBytes = getDeficit st4 e.classId)
    s!"deficit_bytes mismatch: got {e.deficitBytes}, expected {getDeficit st4 e.classId}"

  pure st4

def step (lineNo : Nat) (g : Global) (e : Event) : Except String Global := do
  let st0 := g.schedulers.getD e.schedulerId {}
  let st1 ← updateLastTime lineNo st0 e.timeNs
  let st2 ← ensureClassCount lineNo st1 e.classCount
  let st3 ← ensureRate lineNo st2 e.rateBps
  let st4 ← ensureQuantum lineNo st3 e.classId e.quantumBytes
  let st' ←
    match e.kind with
    | Kind.enqueue => stepEnqueue lineNo st4 e
    | Kind.schedule => stepSchedule lineNo st4 e
  pure { g with schedulers := g.schedulers.insert e.schedulerId st' }

end LeanGuard.Drr.Semantics
