import LeanGuard.DcqcnEventLog
import LeanGuard.Shared.Cli
import LeanGuard.Shared.Coverage

open LeanGuard.DcqcnEventLog
open LeanGuard.Shared

def usage : String :=
  "usage: dcqcn_check [--coverage-out <path>] <path/to/dcqcn_events.csv>"

def main (args : List String) : IO UInt32 := do
  match parseCoverageOut args with
  | .error _ =>
      IO.eprintln usage
      pure 2
  | .ok parsed =>
      match parsed.inputs with
      | [path] =>
          let content ← IO.FS.readFile path
          match parseCsv content with
          | .error e =>
              let report : CoverageReport :=
                { checker := "dcqcn_check"
                  accept := false
                  cover := []
                  rows := 0
                  processedRows := 0
                  error := some e }
              match parsed.coverageOut with
              | none =>
                  IO.eprintln e
                  pure 2
              | some out =>
                  match (← writeCoverageFile out report) with
                  | .ok _ =>
                      IO.eprintln e
                      pure 2
                  | .error we =>
                      IO.eprintln we
                      pure 2
          | .ok rows =>
              let rowCount := rows.length
              match checkRowsWithCoverage rows with
              | .ok cov =>
                  let report : CoverageReport :=
                    { checker := "dcqcn_check"
                      accept := true
                      cover := covList cov
                      rows := rowCount
                      processedRows := cov.processedRows }
                  match parsed.coverageOut with
                  | none =>
                      IO.println "ACCEPT"
                      pure 0
                  | some out =>
                      match (← writeCoverageFile out report) with
                      | .ok _ =>
                          IO.println "ACCEPT"
                          pure 0
                      | .error we =>
                          IO.eprintln we
                          pure 2
              | .error (e, cov) =>
                  let report : CoverageReport :=
                    { checker := "dcqcn_check"
                      accept := false
                      cover := covList cov
                      rows := rowCount
                      processedRows := cov.processedRows
                      error := some e }
                  match parsed.coverageOut with
                  | none =>
                      IO.eprintln s!"REJECT: {e}"
                      pure 1
                  | some out =>
                      match (← writeCoverageFile out report) with
                      | .ok _ =>
                          IO.eprintln s!"REJECT: {e}"
                          pure 1
                      | .error we =>
                          IO.eprintln we
                          pure 2
      | _ =>
          IO.eprintln usage
          pure 2
