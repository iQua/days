import LeanGuard.P10c.RoceEventLog

open LeanGuard.P10c.RoceEventLog

def usage : String :=
  "usage: p10c_roce_check receiver <roce_receiver.csv>"

def report (result : Except String Unit) : IO UInt32 :=
  match result with
  | .ok _ => do
      IO.println "ACCEPT"
      pure 0
  | .error error => do
      IO.eprintln s!"REJECT: {error}"
      pure 1

def main (args : List String) : IO UInt32 := do
  match args with
  | ["receiver", path] =>
      let content ← IO.FS.readFile path
      report (parseReceiverCsv content >>= checkReceiverRows)
  | _ =>
      IO.eprintln usage
      pure 2
