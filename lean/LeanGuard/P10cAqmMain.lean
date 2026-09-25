import LeanGuard.P10c.AqmEventLog

open LeanGuard.P10c.AqmEventLog

def main (args : List String) : IO UInt32 := do
  match args with
  | [path] =>
      let content ← IO.FS.readFile path
      match parseCsv content with
      | .error error =>
          IO.eprintln error
          pure 2
      | .ok rows =>
          match checkRows rows with
          | .ok _ =>
              IO.println "ACCEPT"
              pure 0
          | .error error =>
              IO.eprintln s!"REJECT: {error}"
              pure 1
  | _ =>
      IO.eprintln "usage: p10c_aqm_check <path/to/aqm_events.csv>"
      pure 2
