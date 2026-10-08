import LeanGuard.P10c.Test.AqmReference
import LeanGuard.P10c.Test.Rng

/-! Differential harness for the P10c AQM checker (test-only; P16 lane L1).

* `p10c_aqm_diff <aqm.csv>`: a drop-in for `p10c_aqm_check` (same output and exit code) that also
  runs the reference checker (`checkRowsReference`, the list-scan continuity check of `26dc1d1`)
  and exits 3 with a `DIFFERENTIAL MISMATCH` line when the two results differ.
* `p10c_aqm_diff mutate <seed> <per-kind> <aqm.csv>...`: per input and mutation kind, `<per-kind>`
  single-edit mutants from a deterministic stream, checked by both; made on parsed rows, so they
  bypass `parseCsv` (shared by both checkers). -/

open LeanGuard.P10c.AqmEventLog
open LeanGuard.P10c.Test

def render (result : Except String Unit) : String :=
  match result with
  | .ok _ => "ACCEPT"
  | .error error => s!"REJECT: {error}"

inductive Kind
  | counter
  | drop
  | swapLines
  | swapOrder
  | queue
  | node
  | config
  | packetKind
  deriving DecidableEq, Repr

def Kind.all : List Kind :=
  [.counter, .drop, .swapLines, .swapOrder, .queue, .node, .config, .packetKind]

def Kind.name : Kind → String
  | .counter => "counter-off-by-one"
  | .drop => "dropped-row"
  | .swapLines => "swapped-adjacent-lines"
  | .swapOrder => "swapped-adjacent-event-order"
  | .queue => "changed-queue"
  | .node => "changed-node"
  | .config => "changed-config"
  | .packetKind => "relabeled-packet-kind"

def bumpOpt (value : Option Nat) (up : Bool) : Option Nat := value.map (bump · up)

def bumpField (row : Row) (field : Nat) (up : Bool) : Row :=
  match field with
  | 0 => { row with queuedPacketsBefore := bump row.queuedPacketsBefore up }
  | 1 => { row with queuedBytesBefore := bump row.queuedBytesBefore up }
  | 2 => { row with packetSizeBytes := bump row.packetSizeBytes up }
  | 3 => { row with beforeAverageScaled := bumpOpt row.beforeAverageScaled up }
  | 4 => { row with beforeCounter := bumpOpt row.beforeCounter up }
  | 5 => { row with afterAverageScaled := bumpOpt row.afterAverageScaled up }
  | 6 => { row with afterCounter := bumpOpt row.afterCounter up }
  | 7 => { row with timeNs := bump row.timeNs up }
  | _ => { row with originSeq := bump row.originSeq up }

def bumpConfig (row : Row) (field : Nat) (up : Bool) : Row :=
  match field with
  | 0 => { row with capacity := bump row.capacity up }
  | 1 => { row with threshold := bumpOpt row.threshold up }
  | 2 => { row with minThreshold := bumpOpt row.minThreshold up }
  | 3 => { row with maxThreshold := bumpOpt row.maxThreshold up }
  | 4 => { row with maxProbabilityNumerator := bumpOpt row.maxProbabilityNumerator up }
  | _ => { row with markEcn := !row.markEcn }

def mutate (rows : Array Row) (kind : Kind) (g : Rng) : Option (Array Row) × Rng := Id.run do
  let n := rows.size
  if n = 0 then return (none, g)
  let some first := rows[0]? | return (none, g)
  let at_ := fun (k : Nat) => rows.getD k first
  let (i, g) := g.below n
  let (j, g) := g.below n
  let (choice, g) := g.below 4
  let row := at_ i
  let other := at_ j
  match kind with
  | .counter =>
      let (field, g) := g.below 9
      (some (rows.set! i (bumpField row field (choice % 2 = 0))), g)
  | .drop =>
      if n < 2 then return (none, g)
      (some ((rows.extract 0 i) ++
        ((rows.extract (i + 1) n).map fun later => { later with srcLine := later.srcLine - 1 })), g)
  | .swapLines =>
      if i + 1 ≥ n then return (none, g)
      let next := at_ (i + 1)
      (some ((rows.set! i { next with srcLine := row.srcLine }).set! (i + 1)
        { row with srcLine := next.srcLine }), g)
  | .swapOrder =>
      if i + 1 ≥ n then return (none, g)
      let next := at_ (i + 1)
      let row' :=
        { row with
          timeNs := next.timeNs
          eventPhase := next.eventPhase
          originNode := next.originNode
          originSeq := next.originSeq }
      let next' :=
        { next with
          timeNs := row.timeNs
          eventPhase := row.eventPhase
          originNode := row.originNode
          originSeq := row.originSeq }
      (some ((rows.set! i row').set! (i + 1) next'), g)
  | .queue =>
      let target := if choice = 0 then row.queueId + 1 else other.queueId
      (some (rows.set! i { row with queueId := target }), g)
  | .node =>
      let target := if choice = 0 then row.nodeId + 1 else other.nodeId
      (some (rows.set! i { row with nodeId := target }), g)
  | .config =>
      let (field, g) := g.below 6
      (some (rows.set! i (bumpConfig row field (choice % 2 = 0))), g)
  | .packetKind =>
      -- A data packet relabeled an ACK, or any other packet relabeled data.
      let kind := if row.isData then "roce_ack" else "roce_data"
      (some (rows.set! i { row with packetKind := kind }), g)

structure Tally where
  cases : Nat := 0
  mismatches : Nat := 0
  accepts : Nat := 0
  byKind : Std.HashMap String Nat := {}
  byMessage : Std.HashMap String Nat := {}

def mutateFile (seed perKind : Nat) (path : String) (tally : Tally) : IO Tally := do
  let rows ← match parseCsv (← IO.FS.readFile path) with
    | .ok rows => pure rows.toArray
    | .error error => throw (IO.userError s!"{path}: {error}")
  let mut tally := tally
  let mut fileCases := 0
  let mut g := Rng.forInput seed path
  for kind in Kind.all do
    let mut made := 0
    let mut attempts := 0
    while made < perKind && attempts < perKind * 20 do
      attempts := attempts + 1
      let (mutant, g') := mutate rows kind g
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

def usage : String :=
  "usage: p10c_aqm_diff <aqm.csv>\n" ++
  "       p10c_aqm_diff mutate <seed> <per-kind> <aqm.csv>..."

def main (args : List String) : IO UInt32 := do
  match args with
  | [path] =>
      match parseCsv (← IO.FS.readFile path) with
      | .error error =>
          IO.eprintln error
          pure 2
      | .ok rows =>
          let shipped := checkRows rows
          let reference := checkRowsReference rows
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
  | _ =>
      IO.eprintln usage
      pure 2
