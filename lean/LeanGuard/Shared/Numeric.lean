import Std

namespace LeanGuard.Shared

def PPB : Nat := 1_000_000_000

def max0 (a : Float) : Float :=
  if a < 0.0 then 0.0 else a

def ppbToFloat (ppb : Nat) : Float :=
  (Float.ofNat ppb) / (Float.ofNat PPB)

def nsToSeconds (ns : Nat) : Float :=
  (Float.ofNat ns) / 1.0e9

def toPpb (v : Float) : Nat :=
  ((Float.round (max0 v * 1.0e9)).toUInt64).toNat

def toBps (v : Float) : Nat :=
  ((Float.round (max0 v)).toUInt64).toNat

def toNatFloor (v : Float) : Nat :=
  ((Float.floor (max0 v)).toUInt64).toNat

end LeanGuard.Shared

