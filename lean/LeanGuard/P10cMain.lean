import LeanGuard.P10c.MechanismEventLog

open LeanGuard.P10c.MechanismEventLog

def usage : String :=
  "usage: p10c_mechanisms_check <rate|pfc|drr|wrr> <events.csv>"

def main (args : List String) : IO UInt32 := do
  match args with
  | [mechanism, path] =>
      let content ← IO.FS.readFile path
      let result :=
        match mechanism with
        | "rate" => RateLog.parseCsv content >>= RateLog.checkRows
        | "pfc" => PfcLog.parseCsv content >>= PfcLog.checkRows
        | "drr" => DrrLog.parseCsv content >>= DrrLog.checkRows
        | "wrr" => WrrLog.parseCsv content >>= WrrLog.checkRows
        | other => .error s!"invalid mechanism: {other}"
      match result with
      | .ok _ =>
          IO.println "ACCEPT"
          pure 0
      | .error error =>
          IO.eprintln s!"REJECT: {error}"
          pure 1
  | _ =>
      IO.eprintln usage
      pure 2
