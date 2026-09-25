import LeanGuard.P10c.DcqcnEventLog

open LeanGuard.P10c.DcqcnEventLog

def usage : String :=
  "usage: p10c_dcqcn_check <events.csv>"

def main (args : List String) : IO UInt32 := do
  match args with
  | [path] =>
      let content ← IO.FS.readFile path
      match parseCsv content >>= checkRows with
      | .ok _ =>
          IO.println "ACCEPT"
          pure 0
      | .error error =>
          IO.eprintln s!"REJECT: {error}"
          pure 1
  | _ =>
      IO.eprintln usage
      pure 2
