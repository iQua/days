import Std

namespace LeanGuard.Shared

def require (lineNo : Nat) (cond : Bool) (msg : String) : Except String Unit :=
  if cond then
    pure ()
  else
    throw s!"line {lineNo}: {msg}"

def requireSome {α : Type} (lineNo : Nat) (name : String) : Option α → Except String α
  | none => throw s!"line {lineNo}: missing required field: {name}"
  | some v => pure v

end LeanGuard.Shared

