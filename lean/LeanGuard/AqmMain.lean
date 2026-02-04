import LeanGuard.AqmEventLog
import LeanGuard.Shared.Cli
import LeanGuard.Shared.Coverage

open LeanGuard.AqmEventLog
open LeanGuard.Shared

def usage : String :=
  "usage:\n"
  ++ "  aqm_check [--coverage-out <path>] <path/to/aqm_events.csv>\n"
  ++ "  aqm_check --emit-coverpoint-catalog"

def main (args : List String) : IO UInt32 := do
  match parseCoverageOut args with
  | .error _ =>
      IO.eprintln usage
      pure 2
  | .ok parsed =>
      if parsed.emitCoverpointCatalog then
        for cp in sortStrings coverpointCatalog do
          IO.println cp
        pure 0
      else
        match parsed.inputs with
        | [path] =>
            let content ← IO.FS.readFile path
            match parseCsv content with
            | .error e =>
                let report : CoverageReport :=
                  { checker := "aqm_check"
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
                      { checker := "aqm_check"
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
                      { checker := "aqm_check"
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
