import LeanGuard.P10c.DcqcnEventLog
import LeanGuard.P10c.RoceEventLog
import LeanGuard.P10c.RoceStages

open LeanGuard.P10c.RoceEventLog

def usage : String :=
  "usage: p10c_roce_check receiver <roce_receiver.csv>\n" ++
  "       p10c_roce_check sender <roce_sender.csv> <dcqcn.csv> --pfc <pfc.csv> [--horizon-ns <ns>] [<stop_time_ns>]\n" ++
  "       p10c_roce_check trace <roce_sender.csv> <roce_receiver.csv> <dcqcn.csv> --pfc <pfc.csv> [--horizon-ns <ns>] [--collective <collective.csv>] [<stop_time_ns>]\n" ++
  "  <dcqcn.csv> is the dcqcn_transitions_csv of the same run: each queue pair's controller\n" ++
  "  transitions, at the sender rows that make them (the sender's rate and status).\n" ++
  "  <pfc.csv> is the pfc_transitions_csv of the same run (header-only without PFC); it is required\n" ++
  "  (the sender log's data_class, schema Amendment 3), and the pauses and resumes are checked\n" ++
  "  against its host PAUSE and RESUME records.\n" ++
  "  --horizon-ns <ns> checks a prefix of a run: the logs hold exactly its events with\n" ++
  "  time_ns < <ns>; pending events at or after it need not have fired.\n" ++
  "  --collective <collective.csv> (trace mode) is the collective_transitions_csv of the same run:\n" ++
  "  each RoCE stage's queue pair starts at its release and completes its successor's prerequisite.\n" ++
  "  The sender log must carry the full P16 sender schema (every column the executor writes); a\n" ++
  "  missing column is named and rejected. No other sender format is read.\n" ++
  "  <stop_time_ns> is the image's stop_time_ns, which no CSV records; when omitted, one stop\n" ++
  "  time must fit every armed and stopped pacer decision in the log."

/-- The optional arguments after the logs, in any order: `--pfc <path>`, `--horizon-ns <ns>`,
`--collective <path>` (trace mode only) and a stop time, each at most once. -/
structure Options where
  pfc : Option String := none
  horizon : Option Nat := none
  collective : Option String := none
  stop : Option Nat := none

def parseOptions : List String → Option Options
  | [] => some {}
  | "--pfc" :: path :: rest => do
      let options ← parseOptions rest
      if options.pfc.isSome then none else some { options with pfc := some path }
  | "--collective" :: path :: rest => do
      let options ← parseOptions rest
      if options.collective.isSome then none else some { options with collective := some path }
  | "--horizon-ns" :: value :: rest => do
      let horizon ← value.toNat?
      let options ← parseOptions rest
      if options.horizon.isSome then none else some { options with horizon := some horizon }
  | value :: rest => do
      let stop ← value.toNat?
      let options ← parseOptions rest
      if options.stop.isSome then none else some { options with stop := some stop }

def readPfc (path : Option String) :
    IO (Except String (Option (List LeanGuard.P10c.MechanismEventLog.PfcLog.Row))) := do
  match path with
  | none => pure (.ok none)
  | some path =>
      let content ← IO.FS.readFile path
      pure (inRole "pfc" (some <$> LeanGuard.P10c.MechanismEventLog.PfcLog.parseCsv content))

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
  | "sender" :: senderPath :: dcqcnPath :: rest =>
      match parseOptions rest with
      | none =>
          IO.eprintln usage
          pure 2
      | some options =>
          -- The collective joins need the receiver log too: trace mode only.
          if options.collective.isSome then
            IO.eprintln usage
            return 2
          let sender ← IO.FS.readFile senderPath
          let dcqcn ← IO.FS.readFile dcqcnPath
          let pfc ← readPfc options.pfc
          report do
            let senderRows ← inRole "sender" (parseSenderCsv sender)
            let dcqcnRows ← inRole "dcqcn" (LeanGuard.P10c.DcqcnEventLog.parseCsv dcqcn)
            checkSenderRows options.stop options.horizon senderRows dcqcnRows (← pfc)
  | "trace" :: senderPath :: receiverPath :: dcqcnPath :: rest =>
      match parseOptions rest with
      | none =>
          IO.eprintln usage
          pure 2
      | some options =>
          let sender ← IO.FS.readFile senderPath
          let receiver ← IO.FS.readFile receiverPath
          let dcqcn ← IO.FS.readFile dcqcnPath
          let pfc ← readPfc options.pfc
          let collective ←
            match options.collective with
            | none => pure none
            | some path => some <$> IO.FS.readFile path
          report do
            let senderRows ← inRole "sender" (parseSenderCsv sender)
            let receiverRows ← inRole "receiver" (parseReceiverCsv receiver)
            let dcqcnRows ← inRole "dcqcn" (LeanGuard.P10c.DcqcnEventLog.parseCsv dcqcn)
            checkTrace options.stop options.horizon senderRows receiverRows dcqcnRows (← pfc)
            if let some content := collective then
              let collectiveRows ←
                inRole "collective" (LeanGuard.P10c.CollectiveEventLog.parseCsv content)
              LeanGuard.P10c.RoceStages.checkStages options.horizon collectiveRows senderRows
  | _ =>
      IO.eprintln usage
      pure 2
