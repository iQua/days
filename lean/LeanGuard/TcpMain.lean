import LeanGuard.Shared.Cli
import LeanGuard.Shared.Coverage
import LeanGuard.TcpEventLog

open LeanGuard.Shared
open LeanGuard.TcpEventLog

def usage : String := "usage: tcp_check [--coverage-out <path>] <path/to/tcp_events.csv>"

def main (args : List String) : IO UInt32 := do
  match parseCoverageOut args with
  | .error _ => IO.eprintln usage; pure 2
  | .ok parsed =>
      match parsed.inputs with
      | [path] =>
          let content ← IO.FS.readFile path
          match parseCsv content with
          | .error error => IO.eprintln error; pure 2
          | .ok rows =>
              match checkRowsWithCoverage rows with
              | .ok coverage =>
                  let report : CoverageReport :=
                    { checker := "tcp_check", accept := true, cover := covList coverage,
                      rows := rows.length, processedRows := coverage.processedRows }
                  match parsed.coverageOut with
                  | none => IO.println "ACCEPT"; pure 0
                  | some out =>
                      match ← writeCoverageFile out report with
                      | .ok _ => IO.println "ACCEPT"; pure 0
                      | .error error => IO.eprintln error; pure 2
              | .error (error, coverage) =>
                  let report : CoverageReport :=
                    { checker := "tcp_check", accept := false, cover := covList coverage,
                      rows := rows.length, processedRows := coverage.processedRows,
                      error := some error }
                  match parsed.coverageOut with
                  | none => IO.eprintln s!"REJECT: {error}"; pure 1
                  | some out =>
                      match ← writeCoverageFile out report with
                      | .ok _ => IO.eprintln s!"REJECT: {error}"; pure 1
                      | .error writeError => IO.eprintln writeError; pure 2
      | _ => IO.eprintln usage; pure 2
