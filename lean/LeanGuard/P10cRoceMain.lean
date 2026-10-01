import LeanGuard.P10c.DcqcnEventLog
import LeanGuard.P10c.RoceEventLog

open LeanGuard.P10c.RoceEventLog

def usage : String :=
  "usage: p10c_roce_check receiver <roce_receiver.csv>\n" ++
  "       p10c_roce_check sender <roce_sender.csv> <dcqcn.csv> [--pfc <pfc.csv>] [<stop_time_ns>]\n" ++
  "       p10c_roce_check trace <roce_sender.csv> <roce_receiver.csv> <dcqcn.csv> [--pfc <pfc.csv>] [<stop_time_ns>]\n" ++
  "  <dcqcn.csv> is the dcqcn_transitions_csv of the same run (the sender's rate and status).\n" ++
  "  <pfc.csv> is the pfc_transitions_csv of the same run; it is required when the sender log\n" ++
  "  has the data_class column (schema Amendment 3), and the pauses and resumes are checked\n" ++
  "  against its host PAUSE and RESUME records.\n" ++
  "  <stop_time_ns> is the image's stop_time_ns, which no CSV records; when omitted, one stop\n" ++
  "  time must fit every armed and stopped pacer decision in the log."

/-- The optional arguments after the logs: `--pfc <path>` and a stop time, in either order. -/
def parseOptions : List String → Option (Option String × Option Nat)
  | [] => some (none, none)
  | "--pfc" :: path :: rest => do
      let (pfc, stop) ← parseOptions rest
      if pfc.isSome then none else some (some path, stop)
  | [value] => value.toNat?.map (fun stop => (none, some stop))
  | value :: rest => do
      let stop ← value.toNat?
      let (pfc, later) ← parseOptions rest
      if later.isSome then none else some (pfc, some stop)

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
      | some (pfcPath, stop) =>
          let sender ← IO.FS.readFile senderPath
          let dcqcn ← IO.FS.readFile dcqcnPath
          let pfc ← readPfc pfcPath
          report do
            let senderRows ← inRole "sender" (parseSenderCsv sender)
            let dcqcnRows ← inRole "dcqcn" (LeanGuard.P10c.DcqcnEventLog.parseCsv dcqcn)
            checkSenderRows stop senderRows dcqcnRows (← pfc)
  | "trace" :: senderPath :: receiverPath :: dcqcnPath :: rest =>
      match parseOptions rest with
      | none =>
          IO.eprintln usage
          pure 2
      | some (pfcPath, stop) =>
          let sender ← IO.FS.readFile senderPath
          let receiver ← IO.FS.readFile receiverPath
          let dcqcn ← IO.FS.readFile dcqcnPath
          let pfc ← readPfc pfcPath
          report do
            let senderRows ← inRole "sender" (parseSenderCsv sender)
            let receiverRows ← inRole "receiver" (parseReceiverCsv receiver)
            let dcqcnRows ← inRole "dcqcn" (LeanGuard.P10c.DcqcnEventLog.parseCsv dcqcn)
            checkTrace stop senderRows receiverRows dcqcnRows (← pfc)
  | _ =>
      IO.eprintln usage
      pure 2
