import Std

namespace LeanGuard.Shared

structure ParsedArgs where
  coverageOut : Option String := none
  inputs : List String := []
  deriving Repr


def parseCoverageOut (args : List String) : Except String ParsedArgs := do
  let startsWithDashDash (s : String) : Bool :=
    match s.toList with
    | '-' :: '-' :: _ => true
    | _ => false
  let rec go (rest : List String) (cov : Option String) (inputsRev : List String) :
      Except String ParsedArgs := do
    match rest with
    | [] =>
        pure { coverageOut := cov, inputs := inputsRev.reverse }
    | "--coverage-out" :: tail =>
        match tail with
        | [] => throw "missing path for --coverage-out"
        | path :: tail' =>
            match cov with
            | some _ => throw "duplicate --coverage-out"
            | none => go tail' (some path) inputsRev
    | arg :: tail =>
        if startsWithDashDash arg then
          throw s!"unknown flag: {arg}"
        else
          go tail cov (arg :: inputsRev)
  go args none []

end LeanGuard.Shared
