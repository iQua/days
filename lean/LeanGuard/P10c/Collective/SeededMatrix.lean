import Std

import LeanGuard.P10c.SplitMix

/-! The seeded per-pair routing matrix of an imbalanced all-to-all (P16 H1, ruling R7), re-derived
in pure `Nat` arithmetic from the parameters a progress certificate names, exactly as the
executor's `seeded_matrix.rs` (`SeededAllToAll::bytes`) derives it: a SplitMix64 stream keyed by
`(seed, matrix, group)`, a Fisher–Yates permutation of the experts that ranks their popularity,
integer Zipf(1) (or uniform) weights, and `tokens × topk` draws per source rank by inverse
cumulative weight. Every `u64` step wraps modulo `2^64` as Rust's `wrapping_*` operations do, and
every checked step of the Rust code yields `none` here when it would overflow. -/

namespace LeanGuard.P10c.Collective.SeededMatrix

open LeanGuard.P10c.SplitMix

/-- A SplitMix64 stream. -/
structure Stream where
  state : Nat

def Stream.next (stream : Stream) : Nat × Stream :=
  let state := wrap (stream.state + golden)
  (finish state, ⟨state⟩)

/-- A draw in `0..bound` by remainder (`bound > 0`). -/
def Stream.below (stream : Stream) (bound : Nat) : Nat × Stream :=
  let (value, stream) := stream.next
  (value % bound, stream)

inductive Skew
  | uniform
  | zipf1
  deriving DecidableEq, Repr

/-- The matrix parameters a certificate row names. -/
structure Params where
  seed : Nat
  matrix : Nat
  group : Nat
  transpose : Bool
  experts : Nat
  topk : Nat
  tokens : Nat
  bytesPerCopy : Nat
  skew : Skew
  deriving DecidableEq, Repr

/-- The number of entries of the ascending `cumulative` that are at most `draw` (Rust's
`partition_point(|&bound| bound <= draw)`). -/
def partitionPoint (cumulative : Array Nat) (draw : Nat) : Nat := Id.run do
  let mut low := 0
  let mut high := cumulative.size
  -- Each iteration halves `high - low`, so `size + 1` iterations always suffice.
  for _ in [0:cumulative.size + 1] do
    if low < high then
      let middle := (low + high) / 2
      if cumulative.getD middle 0 ≤ draw then low := middle + 1 else high := middle
  pure low

/-- The bytes from rank `source` to rank `target` of an `n`-rank group, row-major
(`source * n + target`), zero on the diagonal; `none` exactly when the executor's derivation
returns `None` (experts not a positive multiple of `n`, or an overflow). -/
def bytes (params : Params) (n : Nat) : Option (Array Nat) := Id.run do
  if n = 0 || params.experts = 0 || params.experts % n != 0 then return none
  let perRank := params.experts / n
  let key := mix (mix (mix params.seed ^^^ params.matrix) ^^^ params.group)
  -- The popularity order: a Fisher–Yates permutation of the experts, from the last index down.
  let mut order := Array.range params.experts
  let mut stream : Stream := ⟨mix (key ^^^ 0x5045524d)⟩
  for step in [0:params.experts - 1] do
    let index := params.experts - 1 - step
    let (other, next) := stream.below (index + 1)
    stream := next
    order := order.swapIfInBounds index other
  -- Cumulative integer weights by expert.
  let mut cumulative : Array Nat := Array.mkEmpty params.experts
  let mut total := 0
  for popularity in order do
    let weight := match params.skew with
      | .uniform => 1
      | .zipf1 => 2 ^ 40 / (popularity + 1)
    total := total + weight
    if total > maxU64 then return none
    cumulative := cumulative.push total
  let copies := params.tokens * params.topk
  if copies > maxU64 then return none
  let mut counts : Array Nat := Array.replicate (n * n) 0
  for source in [0:n] do
    let mut draws : Stream := ⟨mix (key ^^^ (source + 1))⟩
    for _ in [0:copies] do
      let (draw, next) := draws.below total
      draws := next
      let target := partitionPoint cumulative draw / perRank
      if target != source then
        counts := counts.modify (source * n + target) (· + 1)
  let mut result : Array Nat := Array.mkEmpty (n * n)
  for source in [0:n] do
    for target in [0:n] do
      let count :=
        if params.transpose then counts.getD (target * n + source) 0
        else counts.getD (source * n + target) 0
      let pairBytes := count * params.bytesPerCopy
      if pairBytes > maxU64 then return none
      result := result.push pairBytes
  pure (some result)

end LeanGuard.P10c.Collective.SeededMatrix
