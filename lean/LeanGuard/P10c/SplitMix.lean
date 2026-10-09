/-! SplitMix64's output function in `Nat` arithmetic, mirroring the executor's one mixer
(`executor/src/splitmix.rs`): the seeded all-to-all matrix (`Collective/SeededMatrix.lean`) and the
ECN ramp's stateless draw (`Semantics.lean`, namespace `Aqm`) both derive from it. Every `u64` step
wraps modulo `2^64` as Rust's `wrapping_*` operations do. -/

namespace LeanGuard.P10c.SplitMix

def modulus : Nat := 2 ^ 64

def maxU64 : Nat := modulus - 1

def wrap (value : Nat) : Nat := value % modulus

def golden : Nat := 0x9e3779b97f4a7c15

/-- SplitMix64's output function after its increment. -/
def finish (value : Nat) : Nat :=
  let value := wrap ((value ^^^ (value >>> 30)) * 0xbf58476d1ce4e5b9)
  let value := wrap ((value ^^^ (value >>> 27)) * 0x94d049bb133111eb)
  value ^^^ (value >>> 31)

/-- `splitmix::mix`: SplitMix64's output function of one value. -/
def mix (value : Nat) : Nat := finish (wrap (value + golden))

end LeanGuard.P10c.SplitMix
