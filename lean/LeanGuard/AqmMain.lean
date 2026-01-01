import LeanGuard.AqmEventLog

open LeanGuard.AqmEventLog

def usage : String :=
  "usage: aqm_check <path/to/aqm_events.csv>"

def main (args : List String) : IO UInt32 := do
  match args with
  | [path] =>
      let content ← IO.FS.readFile path
      match parseCsv content with
      | .error e =>
          IO.eprintln s!"REJECT: {e}"
          pure 1
      | .ok rows =>
          match checkRows rows with
          | .ok _ =>
              IO.println "ACCEPT"
              pure 0
          | .error e =>
              IO.eprintln s!"REJECT: {e}"
              pure 1
  | _ =>
      IO.eprintln usage
      pure 2
