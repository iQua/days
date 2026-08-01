import LeanGuard.Shared.Cli
import LeanGuard.Shared.Coverage
import LeanGuard.SpEventLog

open LeanGuard.Shared
open LeanGuard.SpEventLog

def usage : String :=
    "usage: sp_check [--coverage-out <path>] <path/to/sp_events.csv>"

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
            | .error error =>
                let report : CoverageReport :=
                    { checker := "sp_check"
                      accept := false
                      cover := []
                      rows := 0
                      processedRows := 0
                      error := some error }
                match parsed.coverageOut with
                | none =>
                    IO.eprintln error
                    pure 2
                | some output =>
                    match (← writeCoverageFile output report) with
                    | .ok _ =>
                        IO.eprintln error
                        pure 2
                    | .error writeError =>
                        IO.eprintln writeError
                        pure 2
            | .ok rows =>
                let rowCount := rows.length
                match checkRowsWithCoverage rows with
                | .ok coverage =>
                    let report : CoverageReport :=
                        { checker := "sp_check"
                          accept := true
                          cover := covList coverage
                          rows := rowCount
                          processedRows := coverage.processedRows }
                    match parsed.coverageOut with
                    | none =>
                        IO.println "ACCEPT"
                        pure 0
                    | some output =>
                        match (← writeCoverageFile output report) with
                        | .ok _ =>
                            IO.println "ACCEPT"
                            pure 0
                        | .error writeError =>
                            IO.eprintln writeError
                            pure 2
                | .error (error, coverage) =>
                    let report : CoverageReport :=
                        { checker := "sp_check"
                          accept := false
                          cover := covList coverage
                          rows := rowCount
                          processedRows := coverage.processedRows
                          error := some error }
                    match parsed.coverageOut with
                    | none =>
                        IO.eprintln s!"REJECT: {error}"
                        pure 1
                    | some output =>
                        match (← writeCoverageFile output report) with
                        | .ok _ =>
                            IO.eprintln s!"REJECT: {error}"
                            pure 1
                        | .error writeError =>
                            IO.eprintln writeError
                            pure 2
        | _ =>
            IO.eprintln usage
            pure 2
