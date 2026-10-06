import Std

/-! Deterministic splitmix64 stream for the test-only mutation generators (P16 lane L1). -/

namespace LeanGuard.P10c.Test

structure Rng where
  state : UInt64

def Rng.next (g : Rng) : UInt64 × Rng :=
  let s := g.state + 0x9E3779B97F4A7C15
  let z := (s ^^^ (s >>> 30)) * 0xBF58476D1CE4E5B9
  let z := (z ^^^ (z >>> 27)) * 0x94D049BB133111EB
  (z ^^^ (z >>> 31), ⟨s⟩)

/-- A value in `[0, n)` (zero when `n = 0`). -/
def Rng.below (g : Rng) (n : Nat) : Nat × Rng :=
  let (value, g) := g.next
  (if n = 0 then 0 else value.toNat % n, g)

/-- The stream for one input: from the seed and the input's file name. -/
def Rng.forInput (seed : Nat) (path : String) : Rng :=
  ⟨(seed.toUInt64 * 0x100000001B3) ^^^ (hash (System.FilePath.mk path).fileName)⟩

def bump (value : Nat) (up : Bool) : Nat :=
  if up || value = 0 then value + 1 else value - 1

/-- The message of a rendered result without its line number (and without a queue's or source's
identity in parentheses), for the histograms. -/
def bucket (rendered : String) : String :=
  let rendered := (rendered.splitOn " (node_id=").headD rendered
  match rendered.splitOn ": line " with
  | [prefix_, rest] =>
      match rest.splitOn ": " with
      | _ :: message => s!"{prefix_}: {": ".intercalate message}"
      | [] => rendered
  | _ => rendered

def sortedEntries (map : Std.HashMap String Nat) : List (String × Nat) :=
  (map.toList.toArray.qsort fun a b => a.2 > b.2 || (a.2 = b.2 && a.1 < b.1)).toList

end LeanGuard.P10c.Test
