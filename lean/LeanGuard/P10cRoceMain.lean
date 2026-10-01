import LeanGuard.P10c.DcqcnEventLog
import LeanGuard.P10c.RoceEventLog

open LeanGuard.P10c.RoceEventLog

def usage : String :=
  "usage: p10c_roce_check receiver <roce_receiver.csv>\n" ++
  "       p10c_roce_check sender <roce_sender.csv> <dcqcn.csv> [<stop_time_ns>]\n" ++
  "       p10c_roce_check trace <roce_sender.csv> <roce_receiver.csv> <dcqcn.csv> [<stop_time_ns>]\n" ++
  "  <dcqcn.csv> is the dcqcn_transitions_csv of the same run (the sender's rate and status).\n" ++
  "  <stop_time_ns> is the image's stop_time_ns, which no CSV records; when omitted, one stop\n" ++
  "  time must fit every armed and stopped pacer decision in the log."

def parseStop : List String → Option (Option Nat)
  | [] => some none
  | [value] => value.toNat?.map some
  | _ => none

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
      match parseStop rest with
      | none =>
          IO.eprintln usage
          pure 2
      | some stop =>
          let sender ← IO.FS.readFile senderPath
          let dcqcn ← IO.FS.readFile dcqcnPath
          report do
            let senderRows ← inRole "sender" (parseSenderCsv sender)
            let dcqcnRows ← inRole "dcqcn" (LeanGuard.P10c.DcqcnEventLog.parseCsv dcqcn)
            checkSenderRows stop senderRows dcqcnRows
  | "trace" :: senderPath :: receiverPath :: dcqcnPath :: rest =>
      match parseStop rest with
      | none =>
          IO.eprintln usage
          pure 2
      | some stop =>
          let sender ← IO.FS.readFile senderPath
          let receiver ← IO.FS.readFile receiverPath
          let dcqcn ← IO.FS.readFile dcqcnPath
          report do
            let senderRows ← inRole "sender" (parseSenderCsv sender)
            let receiverRows ← inRole "receiver" (parseReceiverCsv receiver)
            let dcqcnRows ← inRole "dcqcn" (LeanGuard.P10c.DcqcnEventLog.parseCsv dcqcn)
            checkTrace stop senderRows receiverRows dcqcnRows
  | _ =>
      IO.eprintln usage
      pure 2
