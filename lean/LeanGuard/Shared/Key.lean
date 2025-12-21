import Std

import LeanGuard.Shared.Check

namespace LeanGuard.Shared

def keyLt (a b : Nat × Nat) : Bool :=
  decide (a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 < b.2))

def canonicalizeRows {α : Type} (rows : List α) (key : α → Nat × Nat) (srcLine : α → Nat) :
    Except String (List α) := do
  let rowsSorted :=
    rows.toArray
      |>.qsort (fun a b => keyLt (key a) (key b))
      |>.toList

  let rec checkKeys : List α → Except String Unit
    | [] => pure ()
    | [_] => pure ()
    | a :: b :: rest => do
        if decide (key a = key b) then
          throw
            s!"duplicate key at lines {srcLine a} and {srcLine b}: (time_ns={key a |>.1}, event_id={key a |>.2})"
        require (srcLine b) (keyLt (key a) (key b)) "canonical key order violated"
        checkKeys (b :: rest)

  checkKeys rowsSorted
  pure rowsSorted

end LeanGuard.Shared

