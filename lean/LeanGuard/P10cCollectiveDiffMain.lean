import LeanGuard.P10c.Test.CollectiveReference
import LeanGuard.P10c.Test.Rng

/-! Differential and budget harness for the P10c collective checker (test-only; P16 lane L1).

* `p10c_collective_diff <events.csv>`: a drop-in for `p10c_collective_check` (same output and exit
  code) that also runs the reference checker (`checkRowsReference`, the list-scan checker of
  `26dc1d1`) and exits 3 with a `DIFFERENTIAL MISMATCH` line when the two `Except` results differ.
  `run-p10c-collective-differential.sh` runs the collective campaign through it.
* `p10c_collective_diff mutate <seed> <per-kind> <events.csv>...`: a generated mutation set. For
  every input and every mutation kind, `<per-kind>` single-edit mutants from a deterministic
  splitmix64 stream; each mutant is checked by both checkers and must give the identical result.
  Mutants are made on parsed rows, so they bypass `parseCsv` (shared by both checkers).
* `p10c_collective_diff budget <max-ratio-percent> <max-per-row> <small.csv> <large.csv>`: the
  deterministic cost budget. It counts the small allocations (`IO.getNumHeartbeats`) made by the
  shipped `checkRows` alone on each input and requires the large/small heartbeat ratio to be at
  most `<max-ratio-percent>` percent of the row ratio, and every input to stay within
  `<max-per-row>` heartbeats per row. -/

open LeanGuard.P10c
open LeanGuard.P10c.CollectiveEventLog
open LeanGuard.P10c.Test

def render (result : Except String Unit) : String :=
  match result with
  | .ok _ => "ACCEPT"
  | .error error => s!"REJECT: {error}"

/-! ## Mutations -/

inductive Kind
  | counter
  | drop
  | swapLines
  | swapOrder
  | flowId
  | predecessor
  | node
  | rank
  | renameFlow
  | time
  | segmentSwap
  | segmentShift
  | doubleEdit
  | equalStart
  deriving DecidableEq, Repr

def Kind.all : List Kind :=
  [.counter, .drop, .swapLines, .swapOrder, .flowId, .predecessor, .node, .rank, .renameFlow,
    .time, .segmentSwap, .segmentShift, .doubleEdit, .equalStart]

def Kind.name : Kind → String
  | .counter => "counter-off-by-one"
  | .drop => "dropped-row"
  | .swapLines => "swapped-adjacent-lines"
  | .swapOrder => "swapped-adjacent-event-order"
  | .flowId => "changed-flow-id"
  | .predecessor => "changed-predecessor"
  | .node => "changed-node"
  | .rank => "changed-rank"
  | .renameFlow => "renamed-flow-everywhere"
  | .time => "changed-event-time"
  | .segmentSwap => "swapped-inbound-segments"
  | .segmentShift => "shifted-inbound-segment"
  | .doubleEdit => "double-edit-mapping-and-flow"
  | .equalStart => "equal-start-duplicate-segment"

/-- Off by one in one counter-like field of one row. -/
def bumpField (row : Row) (field : Nat) (up : Bool) : Row :=
  match field with
  | 0 => { row with beforeInboundBytes := bump row.beforeInboundBytes up }
  | 1 => { row with afterInboundBytes := bump row.afterInboundBytes up }
  | 2 => { row with arrivalBytes := bump row.arrivalBytes up }
  | 3 => { row with afterPacketsEmitted := bump row.afterPacketsEmitted up }
  | 4 => { row with afterBytesEmitted := bump row.afterBytesEmitted up }
  | 5 => { row with segmentSequence := bump row.segmentSequence up }
  | 6 => { row with segmentBytes := bump row.segmentBytes up }
  | 7 => { row with ackNumber := bump row.ackNumber up }
  | 8 => { row with causeOriginNs := bump row.causeOriginNs up }
  | 9 => { row with causeDelayNs := bump row.causeDelayNs up }
  | 10 => { row with afterNextTimeNs := bump row.afterNextTimeNs up }
  | 11 => { row with ordinal := bump row.ordinal up }
  | 12 => { row with key := { row.key with timeNs := bump row.key.timeNs up } }
  | 13 => { row with key := { row.key with originSeq := bump row.key.originSeq up } }
  | 14 => { row with inboundPredecessorBytes := bump row.inboundPredecessorBytes up }
  | 15 => { row with chunkBytes := bump row.chunkBytes up }
  | 16 => { row with step := bump row.step up }
  | _ => { row with durationNs := bump row.durationNs up }

def fieldCount : Nat := 18

def maxFlow (rows : Array Row) : Nat :=
  rows.foldl (fun acc row => max acc row.flowId) 0

def renameIn (old new : Nat) (value : Nat) : Nat := if value = old then new else value

def renameOpt (old new : Nat) (value : Option Nat) : Option Nat := value.map (renameIn old new)

def isTcpInbound (row : Row) : Bool := row.cause = .inboundArrival && row.stageKind = .tcp

/-- The pairs `(a, b)`, `a < b`, of TCP inbound rows of one stage where `a` arrives beyond the
receiver's frontier (out of order) and `b`'s segment starts where `a`'s ends: the inputs of the
`equalStart` kind (fix round 1, after the reviewer's `gen_replay_eqstart.py`). -/
def equalStartPairs (rows : Array Row) : List (Nat × Nat) := Id.run do
  let inbound := (List.range rows.size).filter fun k => (rows[k]?.map isTcpInbound).getD false
  let mut pairs := []
  for a in inbound do
    let some ra := rows[a]? | continue
    if ra.segmentSequence ≤ ra.beforeInboundBytes then continue
    for b in inbound do
      let some rb := rows[b]? | continue
      if b > a && rb.flowId = ra.flowId &&
          rb.segmentSequence = ra.segmentSequence + ra.segmentBytes then
        pairs := (a, b) :: pairs
  pure pairs.reverse

/-- One mutant of `rows` (in file order), or `none` when the drawn edit does not apply.
`pairs` is `equalStartPairs rows`, computed once per input; the `equalStart` kind is exhaustive
rather than drawn: its `index`-th mutant is pair `index / 2` in order `index % 2`. -/
def mutate (rows : Array Row) (pairs : List (Nat × Nat)) (index : Nat) (kind : Kind) (g : Rng) :
    Option (Array Row) × Rng := Id.run do
  let n := rows.size
  if n < 2 then return (none, g)
  let some first := rows[0]? | return (none, g)
  let at_ := fun (k : Nat) => rows.getD k first
  let (i, g) := g.below n
  let (j, g) := g.below n
  let (choice, g) := g.below 4
  let row := at_ i
  let other := at_ j
  let fresh := maxFlow rows + 1
  match kind with
  | .counter =>
      let (field, g) := g.below fieldCount
      (some (rows.set! i (bumpField row field (choice % 2 = 0))), g)
  | .drop =>
      let kept := (rows.extract 0 i) ++
        ((rows.extract (i + 1) n).map fun later => { later with srcLine := later.srcLine - 1 })
      (some kept, g)
  | .swapLines =>
      if i + 1 ≥ n then return (none, g)
      let next := at_ (i + 1)
      (some ((rows.set! i { next with srcLine := row.srcLine }).set! (i + 1)
        { row with srcLine := next.srcLine }), g)
  | .swapOrder =>
      if i + 1 ≥ n then return (none, g)
      let next := at_ (i + 1)
      (some ((rows.set! i { row with key := next.key, ordinal := next.ordinal }).set! (i + 1)
        { next with key := row.key, ordinal := row.ordinal }), g)
  | .flowId =>
      let target := if choice = 0 then fresh else other.flowId
      if choice = 3 then
        -- The whole stage moves to the other flow id (its successors still name the old one).
        (some (rows.map fun r => if r.flowId = row.flowId then { r with flowId := target } else r), g)
      else
        (some (rows.set! i { row with flowId := target }), g)
  | .predecessor =>
      let target := if choice = 0 then fresh else other.flowId
      let isLocal := choice % 2 = 0
      -- Stage-wide, with the cause renamed on the rows it names, so the per-row progress checks
      -- still pass and the predecessor and signal passes see the edit.
      let edit (r : Row) : Row :=
        if isLocal then
          { r with
            localPredecessorFlowId := some target
            causeFlowId := if r.cause = .localCompletion then target else r.causeFlowId }
        else
          { r with
            inboundPredecessorFlowId := some target
            causeFlowId := if r.cause = .inboundArrival then target else r.causeFlowId }
      let (wide, g) := g.below 2
      if wide = 0 then
        (some (rows.set! i (edit row)), g)
      else
        (some (rows.map fun r => if r.flowId = row.flowId then edit r else r), g)
  | .node =>
      if choice = 3 then
        (some (rows.map fun r => if r.flowId = row.flowId then { r with nodeId := other.nodeId }
          else r), g)
      else
        (some (rows.set! i { row with nodeId := other.nodeId }), g)
  | .rank =>
      let target := if choice % 2 = 0 then other.rank
        else if row.groupSize = 0 then row.rank + 1 else (row.rank + 1) % row.groupSize
      if choice = 3 then
        (some (rows.map fun r => if r.flowId = row.flowId then { r with rank := target } else r), g)
      else
        (some (rows.set! i { row with rank := target }), g)
  | .renameFlow =>
      let old := row.flowId
      let target := if choice = 0 then fresh else old + fresh
      (some (rows.map fun r =>
        { r with
          flowId := renameIn old target r.flowId
          causeFlowId := renameIn old target r.causeFlowId
          localPredecessorFlowId := renameOpt old target r.localPredecessorFlowId
          inboundPredecessorFlowId := renameOpt old target r.inboundPredecessorFlowId }), g)
  | .time =>
      let neighbour := if i + 1 < n then at_ (i + 1) else at_ (i - 1)
      let timeNs := if choice = 0 then other.key.timeNs else neighbour.key.timeNs
      (some (rows.set! i { row with key := { row.key with timeNs := timeNs } }), g)
  | .segmentSwap =>
      -- Two inbound rows of one stage exchange their segments (an arrival reordering).
      let inbound := (List.range n).filter fun k => (at_ k).cause = .inboundArrival
      if inbound.isEmpty then return (none, g)
      let a := inbound[i % inbound.length]!
      let peers := inbound.filter fun k => k != a && (at_ k).flowId = (at_ a).flowId
      if peers.isEmpty then return (none, g)
      let b := peers[j % peers.length]!
      let ra := at_ a
      let rb := at_ b
      let ra' := { ra with segmentSequence := rb.segmentSequence, segmentBytes := rb.segmentBytes }
      let rb' := { rb with segmentSequence := ra.segmentSequence, segmentBytes := ra.segmentBytes }
      (some ((rows.set! a ra').set! b rb'), g)
  | .segmentShift =>
      let inbound := (List.range n).filter fun k => (at_ k).cause = .inboundArrival
      if inbound.isEmpty then return (none, g)
      let a := inbound[i % inbound.length]!
      let ra := at_ a
      let step := if choice < 2 then max ra.packetSizeBytes 1 else 1
      let sequence := if choice % 2 = 0 then ra.segmentSequence + step
        else ra.segmentSequence - min step ra.segmentSequence
      (some (rows.set! a { ra with segmentSequence := sequence }), g)
  | .doubleEdit =>
      -- Fix round 1 (review M1, after `gen_continuity.py`): row `i` breaks the rank-to-node rule
      -- against one earlier row and the flow-to-stage rule against another, at different depths,
      -- or against one row (the tie: the flow donor is the newest row of the other rank).
      if i = 0 then return (none, g)
      let sameGroup' (r : Row) := r.stageKind = row.stageKind && r.collectiveId = row.collectiveId
      let others := (List.range i).filter fun k => sameGroup' (at_ k) && (at_ k).rank != row.rank
      if others.isEmpty then return (none, g)
      let o := at_ others[j % others.length]!
      let (mode, g) := g.below 4
      let edited :=
        match mode with
        | 2 => { row with rank := o.rank }
        | 3 => { row with nodeId := (rows.foldl (fun acc (r : Row) => max acc r.nodeId) 0) + 1 }
        | _ => { row with nodeId := o.nodeId }
      let (pick, g) := g.below n
      let donor? :=
        if mode = 1 then
          ((List.range i).filter fun k => sameGroup' (at_ k) && (at_ k).rank = o.rank).getLast?.map at_
        else
          let candidates := (List.range i).filter fun k => (at_ k).flowId != row.flowId
          if candidates.isEmpty then none else some (at_ candidates[pick % candidates.length]!)
      let some donor := donor? | return (none, g)
      (some (rows.set! i { edited with flowId := donor.flowId }), g)
  | .equalStart =>
      -- Fix round 1 (review M1, after `gen_replay_eqstart.py`): two pending segments of one stage
      -- with the same start and different stops, in both orders; the stage's counters are then
      -- recomputed from the reference frontier, so that the reference can accept.
      let some (a, b) := pairs[index / 2]? | return (none, g)
      let ra := at_ a
      let rb := at_ b
      let start := ra.segmentSequence
      let short := ra.segmentBytes
      let long := ra.segmentBytes + rb.segmentBytes
      let (bytesA, bytesB) := if index % 2 = 0 then (long, short) else (short, long)
      let mut out := (rows.set! a { ra with segmentBytes := bytesA }).set! b
        { rb with segmentSequence := start, segmentBytes := bytesB }
      let total := ra.inboundPredecessorBytes
      let mut segments : List (Nat × Nat) := []
      for k in [0:n] do
        let r := out.getD k first
        if r.flowId = ra.flowId then
          let before := frontierOfReference segments
          let inbound := isTcpInbound (at_ k)
          if inbound then segments := segments ++ [segmentOf r]
          let after := frontierOfReference segments
          out := out.set! k
            { r with
              beforeInboundBytes := before
              afterInboundBytes := after
              arrivalBytes := if inbound then after - before else r.arrivalBytes
              beforeInboundComplete := before = total
              afterInboundComplete := after = total }
      (some out, g)

structure Tally where
  cases : Nat := 0
  mismatches : Nat := 0
  accepts : Nat := 0
  byKind : Std.HashMap String Nat := {}
  byMessage : Std.HashMap String Nat := {}

def mutateFile (seed perKind : Nat) (path : String) (tally : Tally) : IO Tally := do
  let content ← IO.FS.readFile path
  let rows ← match parseCsv content with
    | .ok rows => pure rows.toArray
    | .error error => throw (IO.userError s!"{path}: {error}")
  let mut tally := tally
  let mut fileCases := 0
  -- Each input gets its own stream, from the seed and the input's file name.
  let mut g := Rng.forInput seed path
  let pairs := equalStartPairs rows
  for kind in Kind.all do
    -- Every equal-start pair, in both orders; `perKind` drawn mutants of every other kind.
    let target := if kind = .equalStart then 2 * pairs.length else perKind
    let mut made := 0
    let mut attempts := 0
    while made < target && attempts < target * 20 do
      attempts := attempts + 1
      let (mutant, g') := mutate rows pairs made kind g
      g := g'
      if let some mutant := mutant then
        made := made + 1
        let shipped := render (checkRows mutant.toList)
        let reference := render (checkRowsReference mutant.toList)
        tally := { tally with
          cases := tally.cases + 1
          accepts := tally.accepts + (if shipped = "ACCEPT" then 1 else 0)
          byKind := tally.byKind.insert kind.name (tally.byKind.getD kind.name 0 + 1)
          byMessage := tally.byMessage.insert (bucket reference)
            (tally.byMessage.getD (bucket reference) 0 + 1) }
        if shipped != reference then
          tally := { tally with mismatches := tally.mismatches + 1 }
          IO.eprintln s!"DIFFERENTIAL MISMATCH: {path} {kind.name} #{made}"
          IO.eprintln s!"  shipped:   {shipped}"
          IO.eprintln s!"  reference: {reference}"
    fileCases := fileCases + made
  IO.println s!"{(System.FilePath.mk path).fileName.getD path}: rows={rows.size} mutants={fileCases}"
  pure tally

def runMutate (seed perKind : Nat) (paths : List String) : IO UInt32 := do
  let mut tally : Tally := {}
  for path in paths do
    tally ← mutateFile seed perKind path tally
  IO.println s!"seed={seed} per-kind={perKind} inputs={paths.length}"
  IO.println "mutants by kind:"
  for (name, count) in sortedEntries tally.byKind do
    IO.println s!"  {count}\t{name}"
  IO.println "mutants by first result (reference checker, line numbers removed):"
  for (name, count) in sortedEntries tally.byMessage do
    IO.println s!"  {count}\t{name}"
  IO.println s!"mutants={tally.cases} accepted={tally.accepts} rejected={tally.cases - tally.accepts} verdict-differences={tally.mismatches}"
  pure (if tally.mismatches = 0 then 0 else 3)

/-! ## Budget -/

/-- Small allocations made by the shipped `checkRows` alone (parsing excluded). -/
def heartbeatsOf (rows : List Row) : IO (Nat × String) := do
  let before ← IO.getNumHeartbeats
  let result ← IO.lazyPure fun _ => render (checkRows rows)
  let after ← IO.getNumHeartbeats
  pure (after - before, result)

def runBudget (maxRatioPercent maxPerRow : Nat) (small large : String) : IO UInt32 := do
  let load := fun (path : String) => do
    match parseCsv (← IO.FS.readFile path) with
    | .ok rows => pure rows
    | .error error => throw (IO.userError s!"{path}: {error}")
  let smallRows ← load small
  let largeRows ← load large
  let (smallBeats, smallResult) ← heartbeatsOf smallRows
  let (largeBeats, largeResult) ← heartbeatsOf largeRows
  IO.println s!"small: {small} rows={smallRows.length} heartbeats={smallBeats} {smallResult}"
  IO.println s!"large: {large} rows={largeRows.length} heartbeats={largeBeats} {largeResult}"
  let mut failures := 0
  if smallResult != "ACCEPT" || largeResult != "ACCEPT" then
    IO.eprintln "budget inputs must be accepted traces"
    failures := failures + 1
  -- heartbeat ratio ≤ maxRatioPercent% of the row ratio, in integers:
  -- largeBeats * smallRows * 100 ≤ maxRatioPercent * smallBeats * largeRows.
  let lhs := largeBeats * smallRows.length * 100
  let rhs := maxRatioPercent * smallBeats * largeRows.length
  IO.println s!"heartbeat ratio x100 = {largeBeats * 100 / max smallBeats 1}, row ratio x100 = {largeRows.length * 100 / max smallRows.length 1}, budget = {maxRatioPercent}% of the row ratio"
  if lhs > rhs then
    IO.eprintln "BUDGET EXCEEDED: heartbeats grow faster than the budgeted multiple of the rows"
    failures := failures + 1
  for (label, beats, rows) in [("small", smallBeats, smallRows.length),
      ("large", largeBeats, largeRows.length)] do
    if beats > maxPerRow * rows then
      IO.eprintln s!"BUDGET EXCEEDED: {label} input uses {beats / max rows 1} heartbeats per row (cap {maxPerRow})"
      failures := failures + 1
  pure (if failures = 0 then 0 else 1)

/-! ## Entry point -/

def usage : String :=
  "usage: p10c_collective_diff <events.csv>\n" ++
  "       p10c_collective_diff mutate <seed> <per-kind> <events.csv>...\n" ++
  "       p10c_collective_diff budget <max-ratio-percent> <max-per-row> <small.csv> <large.csv>"

def main (args : List String) : IO UInt32 := do
  match args with
  | [path] =>
      let content ← IO.FS.readFile path
      let shipped := parseCsv content >>= checkRows
      let reference := parseCsv content >>= checkRowsReference
      if render shipped != render reference then
        IO.eprintln s!"DIFFERENTIAL MISMATCH: shipped {render shipped} | reference {render reference}"
        return 3
      match shipped with
      | .ok _ =>
          IO.println "ACCEPT"
          pure 0
      | .error error =>
          IO.eprintln s!"REJECT: {error}"
          pure 1
  | "mutate" :: seed :: perKind :: paths =>
      match seed.toNat?, perKind.toNat? with
      | some seed, some perKind => runMutate seed perKind paths
      | _, _ =>
          IO.eprintln usage
          pure 2
  | ["budget", ratio, perRow, small, large] =>
      match ratio.toNat?, perRow.toNat? with
      | some ratio, some perRow => runBudget ratio perRow small large
      | _, _ =>
          IO.eprintln usage
          pure 2
  | _ =>
      IO.eprintln usage
      pure 2
