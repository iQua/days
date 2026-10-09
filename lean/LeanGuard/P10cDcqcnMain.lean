import LeanGuard.P10c.DcqcnEventLog

open LeanGuard.P10c.DcqcnEventLog

def usage : String :=
  "usage: p10c_dcqcn_check <dcqcn.csv>\n" ++
  "       p10c_dcqcn_check trace <dcqcn.csv> <cnp_arrivals.csv>\n" ++
  "  trace mode also joins the run's CNP arrivals (dcqcn_cnp_arrivals_csv) to the unreliable\n" ++
  "  flows' feedback rows: each CNP before its flow's freeze is exactly one feedback row."

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
  | [path] =>
      let content ← IO.FS.readFile path
      report (parseCsv content >>= checkRows)
  | ["trace", dcqcnPath, cnpPath] =>
      let dcqcn ← IO.FS.readFile dcqcnPath
      let cnps ← IO.FS.readFile cnpPath
      report do
        let rows ← parseCsv dcqcn
        checkRows rows
        let arrivals ← inRole "cnp" (parseCnpCsv cnps)
        checkCnpJoin rows arrivals
  | _ =>
      IO.eprintln usage
      pure 2
