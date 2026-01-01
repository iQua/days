import LeanGuard.DrrEventLog

open LeanGuard.DrrEventLog

def usage : String :=
  "usage: drr_check <path/to/drr_events.csv>"

def main (args : List String) : IO UInt32 := do
  match args with
  | [path] =>
      let content ← IO.FS.readFile path
      match parseCsv content with
      | .error e =>
          IO.eprintln e
          pure 2
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
