import Std

namespace LeanGuard.Shared

structure ParsedArgs where
  coverageOut : Option String := none
  emitCoverpointCatalog : Bool := false
  inputs : List String := []
  deriving Repr


def parseCoverageOut (args : List String) : Except String ParsedArgs := do
  let startsWithDashDash (s : String) : Bool :=
    match s.toList with
    | '-' :: '-' :: _ => true
    | _ => false

  let rec go (rest : List String) (cov : Option String) (emitCatalog : Bool)
      (inputsRev : List String) : Except String ParsedArgs := do
    match rest with
    | [] =>
        pure {
          coverageOut := cov
          emitCoverpointCatalog := emitCatalog
          inputs := inputsRev.reverse
        }
    | "--coverage-out" :: tail =>
        match tail with
        | [] => throw "missing path for --coverage-out"
        | path :: tail' =>
            match cov with
            | some _ => throw "duplicate --coverage-out"
            | none => go tail' (some path) emitCatalog inputsRev
    | "--emit-coverpoint-catalog" :: tail =>
        if emitCatalog then
          throw "duplicate --emit-coverpoint-catalog"
        else
          go tail cov true inputsRev
    | arg :: tail =>
        if startsWithDashDash arg then
          throw s!"unknown flag: {arg}"
        else
          go tail cov emitCatalog (arg :: inputsRev)

  go args none false []

end LeanGuard.Shared
