import Std

namespace LeanGuard.Shared

structure CoverageState where
  points : Std.HashSet String := {}
  processedRows : Nat := 0
  seen : Std.HashSet Nat := {}
  deriving Repr

abbrev CheckOutcome := Except (String × CoverageState) CoverageState

structure CoverageReport where
  checker : String
  accept : Bool
  cover : List String
  rows : Nat
  processedRows : Nat
  error : Option String := none
  deriving Repr


def covHit (cov : CoverageState) (point : String) : CoverageState :=
  { cov with points := cov.points.insert point }


/-- Record a distinct numeric id in the observer's scratch set. Observational only:
    `seen` is never serialized (see `covList`/`CoverageReport.toJson`) and only threads
    cross-row coverage bookkeeping for observers that need it (e.g. WFQ distinct classes). -/
def covSeen (cov : CoverageState) (n : Nat) : CoverageState :=
  { cov with seen := cov.seen.insert n }


def covTick (cov : CoverageState) : CoverageState :=
  { cov with processedRows := cov.processedRows + 1 }


def covMerge (a b : CoverageState) : CoverageState :=
  let points := b.points.toList.foldl (fun acc p => acc.insert p) a.points
  let seen := b.seen.toList.foldl (fun acc n => acc.insert n) a.seen
  { points := points, processedRows := a.processedRows + b.processedRows, seen := seen }


def stringLt (a b : String) : Bool :=
  match compare a b with
  | Ordering.lt => true
  | _ => false

def covList (cov : CoverageState) : List String :=
  cov.points.toList.toArray.qsort stringLt |>.toList


def liftExcept (cov : CoverageState) (res : Except String α) : Except (String × CoverageState) α :=
  match res with
  | .ok v => pure v
  | .error e => throw (e, cov)


def escapeJson (s : String) : String :=
  s.foldl
    (fun acc c =>
      match c with
      | '\\' => acc ++ "\\\\"
      | '"' => acc ++ "\\\""
      | '\n' => acc ++ "\\n"
      | '\r' => acc ++ "\\r"
      | '\t' => acc ++ "\\t"
      | _ => acc.push c)
    ""


def jsonString (s : String) : String :=
  "\"" ++ escapeJson s ++ "\""


def jsonBool (b : Bool) : String :=
  if b then "true" else "false"


def jsonNat (n : Nat) : String :=
  toString n


def jsonArray (xs : List String) : String :=
  "[" ++ String.intercalate "," (xs.map jsonString) ++ "]"


def jsonObject (fields : List (String × String)) : String :=
  let parts := fields.map (fun (k, v) => jsonString k ++ ":" ++ v)
  "{" ++ String.intercalate "," parts ++ "}"


def CoverageReport.toJson (r : CoverageReport) : String :=
  let cover := jsonArray r.cover
  let stats := jsonObject [
    ("rows", jsonNat r.rows),
    ("processed_rows", jsonNat r.processedRows)
  ]
  let fields :=
    [ ("checker", jsonString r.checker)
    , ("accept", jsonBool r.accept)
    , ("cover", cover)
    , ("stats", stats)
    ]
  let fields :=
    match r.error with
    | none => fields
    | some e => fields ++ [("error", jsonString e)]
  jsonObject fields


def writeCoverageFile (path : String) (r : CoverageReport) : IO (Except String Unit) := do
  try
    IO.FS.writeFile path r.toJson
    pure (.ok ())
  catch e =>
    pure (.error s!"failed to write coverage: {e.toString}")

end LeanGuard.Shared
