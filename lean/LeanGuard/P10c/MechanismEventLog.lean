import DaysExecutor.Event
import LeanGuard.P10c.Semantics
import LeanGuard.Shared.Csv

namespace LeanGuard.P10c.MechanismEventLog

open LeanGuard.Shared
open LeanGuard.P10c.Semantics

def parseBit (value : String) : Except String Bool :=
  match value with
  | "0" => pure false
  | "1" => pure true
  | other => throw s!"invalid bit: '{other}'"

def parseStatus : String → Except String Rate.Status
  | "scheduled" => pure .scheduled
  | "blocked" => pure .blocked
  | "finished" => pure .finished
  | "stopped" => pure .stopped
  | other => throw s!"invalid rate status: '{other}'"

def parseNatList (value : String) : Except String (List Nat) := do
  if value = "" then
    pure []
  else
    value.splitOn ";" |>.mapM parseNat

structure Packet where
  id : Nat
  flow : Nat
  sizeBytes : Nat
deriving DecidableEq, Repr

def parsePacket (value : String) : Except String Packet := do
  match value.splitOn ":" with
  | [id, flow, sizeBytes] =>
      pure { id := ← parseNat id, flow := ← parseNat flow, sizeBytes := ← parseNat sizeBytes }
  | _ => throw s!"invalid scheduler packet: '{value}'"

def parsePackets (value : String) : Except String (List Packet) := do
  if value = "" then
    pure []
  else
    value.splitOn ";" |>.mapM parsePacket

def indexedMap (values : List Nat) : Std.HashMap Nat Nat :=
  (values.foldl
    (fun (entry : Nat × Std.HashMap Nat Nat) value =>
      (entry.1 + 1, entry.2.insert entry.1 value))
    (0, {})).2

def parseRows
    (content : String)
    (parseRow : Nat → Std.HashMap String Nat → Array String → Except String α) :
    Except String (List α) := do
  let lines :=
    content.splitOn "\n" |>.map stripCR |>.map String.trim |>.filter (· != "")
  match lines with
  | [] => throw "empty CSV"
  | header :: data =>
      let idx := mkIndex (splitCsvLine header)
      let rec go (lineNo : Nat) (remaining : List String) (rows : List α) := do
        match remaining with
        | [] => pure rows.reverse
        | line :: rest =>
            let row ← parseRow lineNo idx (splitCsvLine line).toArray
            go (lineNo + 1) rest (row :: rows)
      go 2 data []

namespace RateLog

structure Row where
  key : DaysExecutor.EventKey
  nodeId : Nat
  flowId : Nat
  payloadId : Nat
  stopTimeNs : Nat
  currentPacketSizeBytes : Nat
  pacingIntervalNs : Nat
  packetSizeBytes : Nat
  totalBytes : Nat
  rateNumeratorBitsPerSecond : Nat
  rateDenominator : Nat
  beforePacketsEmitted : Nat
  beforeBytesEmitted : Nat
  beforeCreditQuanta : Nat
  beforeStatus : Rate.Status
  beforeNextTimeNs : Nat
  afterPacketsEmitted : Nat
  afterBytesEmitted : Nat
  afterCreditQuanta : Nat
  afterStatus : Rate.Status
  afterNextTimeNs : Nat
  srcLine : Nat
deriving DecidableEq, Repr

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let result : Except String Row := do
    pure
      { key :=
          { timeNs := ← parseNat (← getField idx fields "time_ns")
            phase := ← parseNat (← getField idx fields "event_phase")
            originNode := ← parseNat (← getField idx fields "event_origin_node")
            originSeq := ← parseNat (← getField idx fields "event_origin_sequence") }
        nodeId := ← parseNat (← getField idx fields "node_id")
        flowId := ← parseNat (← getField idx fields "flow_id")
        payloadId := ← parseNat (← getField idx fields "payload_id")
        stopTimeNs := ← parseNat (← getField idx fields "stop_time_ns")
        currentPacketSizeBytes := ← parseNat (← getField idx fields "current_packet_size_bytes")
        pacingIntervalNs := ← parseNat (← getField idx fields "pacing_interval_ns")
        packetSizeBytes := ← parseNat (← getField idx fields "packet_size_bytes")
        totalBytes := ← parseNat (← getField idx fields "total_bytes")
        rateNumeratorBitsPerSecond :=
          ← parseNat (← getField idx fields "rate_numerator_bits_per_second")
        rateDenominator := ← parseNat (← getField idx fields "rate_denominator")
        beforePacketsEmitted := ← parseNat (← getField idx fields "before_packets_emitted")
        beforeBytesEmitted := ← parseNat (← getField idx fields "before_bytes_emitted")
        beforeCreditQuanta := ← parseNat (← getField idx fields "before_credit_quanta")
        beforeStatus := ← parseStatus (← getField idx fields "before_status")
        beforeNextTimeNs := ← parseNat (← getField idx fields "before_next_time_ns")
        afterPacketsEmitted := ← parseNat (← getField idx fields "after_packets_emitted")
        afterBytesEmitted := ← parseNat (← getField idx fields "after_bytes_emitted")
        afterCreditQuanta := ← parseNat (← getField idx fields "after_credit_quanta")
        afterStatus := ← parseStatus (← getField idx fields "after_status")
        afterNextTimeNs := ← parseNat (← getField idx fields "after_next_time_ns")
        srcLine := lineNo }
  match result with
  | .ok row => pure row
  | .error error => throw s!"line {lineNo}: {error}"

def sameSource (first second : Row) : Bool :=
  first.nodeId = second.nodeId && first.flowId = second.flowId

def sameConfig (first second : Row) : Bool :=
  first.stopTimeNs = second.stopTimeNs &&
    first.pacingIntervalNs = second.pacingIntervalNs &&
    first.packetSizeBytes = second.packetSizeBytes &&
    first.totalBytes = second.totalBytes &&
    first.rateNumeratorBitsPerSecond = second.rateNumeratorBitsPerSecond &&
    first.rateDenominator = second.rateDenominator

def continuous (first second : Row) : Bool :=
  first.afterPacketsEmitted = second.beforePacketsEmitted &&
    first.afterBytesEmitted = second.beforeBytesEmitted &&
    first.afterCreditQuanta = second.beforeCreditQuanta &&
    first.afterStatus = second.beforeStatus &&
    first.afterNextTimeNs = second.beforeNextTimeNs

def checkRow (row : Row) : Except String Unit := do
  require row.srcLine (row.key.phase = 1) "rate pacing certificate must have phase 1"
  require row.srcLine (row.pacingIntervalNs > 0) "pacing interval must be positive"
  require row.srcLine (row.packetSizeBytes > 0) "packet size must be positive"
  require row.srcLine (row.totalBytes > row.beforeBytesEmitted) "rate tick is already finished"
  require row.srcLine
    (row.beforeStatus = .scheduled || row.beforeStatus = .blocked)
    "rate tick must begin scheduled or blocked"
  require row.srcLine (row.beforeNextTimeNs = row.key.timeNs)
    "rate event time does not match before-state deadline"
  let expectedPacket := min row.packetSizeBytes (row.totalBytes - row.beforeBytesEmitted)
  require row.srcLine (row.currentPacketSizeBytes = expectedPacket)
    "rate current packet size mismatch"
  let before : Rate.State :=
    { rateNumeratorBitsPerSecond := row.rateNumeratorBitsPerSecond
      rateDenominator := row.rateDenominator
      pacingIntervalNs := row.pacingIntervalNs
      packetSizeBytes := row.packetSizeBytes
      totalBytes := row.totalBytes
      emittedBytes := row.beforeBytesEmitted
      creditQuanta := row.beforeCreditQuanta }
  let expected := Rate.tick before row.currentPacketSizeBytes row.key.timeNs row.stopTimeNs
  let emittedPacket := if expected.emittedBytes = 0 then 0 else 1
  require row.srcLine
    (row.afterPacketsEmitted = row.beforePacketsEmitted + emittedPacket)
    "rate packet counter mismatch"
  require row.srcLine
    (row.afterBytesEmitted = expected.state.emittedBytes &&
      row.afterCreditQuanta = expected.state.creditQuanta && row.afterStatus = expected.nextStatus)
    "rate after-state mismatch"
  let expectedTime := expected.nextTimeNs.getD row.beforeNextTimeNs
  require row.srcLine (row.afterNextTimeNs = expectedTime) "rate next deadline mismatch"

def canonicalize (rows : List Row) : Except String (List Row) := do
  let sorted := rows.toArray.qsort (fun a b => decide (a.key < b.key)) |>.toList
  let rec check : List Row → Except String Unit
    | [] | [_] => pure ()
    | first :: second :: rest => do
        require second.srcLine (first.key < second.key) "duplicate canonical event key"
        check (second :: rest)
  check sorted
  pure sorted

def checkRows (rows : List Row) : Except String Unit := do
  let rows ← canonicalize rows
  let rec continuity (previous : List Row) : List Row → Except String Unit
    | [] => pure ()
    | row :: rest => do
        match previous.find? (sameSource · row) with
        | none => pure ()
        | some prior =>
            require row.srcLine (sameConfig prior row)
              s!"rate config discontinuity for source (node_id={row.nodeId}, flow_id={row.flowId})"
            require row.srcLine (continuous prior row)
              s!"rate state discontinuity for source (node_id={row.nodeId}, flow_id={row.flowId})"
        continuity (row :: previous.filter (fun prior => !sameSource prior row)) rest
  continuity [] rows
  for row in rows do checkRow row

def parseCsv (content : String) : Except String (List Row) :=
  parseRows content parseRow

end RateLog

namespace PfcLog

inductive Kind
  | threshold
  | control
deriving DecidableEq, Repr

inductive OccupancyAction
  | admit
  | drain
deriving DecidableEq, Repr

def parseKind : String → Except String Kind
  | "threshold" => pure .threshold
  | "control" => pure .control
  | other => throw s!"invalid PFC row kind: '{other}'"

def parseOccupancyAction : String → Except String OccupancyAction
  | "admit" => pure .admit
  | "drain" => pure .drain
  | other => throw s!"invalid PFC occupancy action: '{other}'"

def parseControl : String → Except String Pfc.Control
  | "pause" => pure .pause
  | "resume" => pure .resume
  | other => throw s!"invalid PFC control action: '{other}'"

structure Row where
  key : DaysExecutor.EventKey
  nodeId : Nat
  queueId : Nat
  kind : Kind
  controlledLink : Nat
  controller : Option Nat
  priority : Nat
  xonBytes : Option Nat
  xoffBytes : Option Nat
  bufferCapacityBytes : Option Nat
  amountBytes : Option Nat
  occupancyAction : Option OccupancyAction
  beforeOccupancyBytes : Option Nat
  beforeAsserted : Option Bool
  afterOccupancyBytes : Option Nat
  afterAsserted : Option Bool
  controlAction : Option Pfc.Control
  beforeControllers : List Nat
  afterControllers : List Nat
  srcLine : Nat
deriving DecidableEq, Repr

def parseOptBit (value : String) : Except String (Option Bool) :=
  parseOpt parseBit value

def parseOptOccupancyAction (value : String) : Except String (Option OccupancyAction) :=
  parseOpt parseOccupancyAction value

def parseOptControl (value : String) : Except String (Option Pfc.Control) :=
  parseOpt parseControl value

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let result : Except String Row := do
    pure
      { key :=
          { timeNs := ← parseNat (← getField idx fields "time_ns")
            phase := ← parseNat (← getField idx fields "event_phase")
            originNode := ← parseNat (← getField idx fields "event_origin_node")
            originSeq := ← parseNat (← getField idx fields "event_origin_sequence") }
        nodeId := ← parseNat (← getField idx fields "node_id")
        queueId := ← parseNat (← getField idx fields "queue_id")
        kind := ← parseKind (← getField idx fields "kind")
        controlledLink := ← parseNat (← getField idx fields "controlled_link")
        controller := ← parseOpt parseNat (← getField idx fields "controller")
        priority := ← parseNat (← getField idx fields "priority")
        xonBytes := ← parseOpt parseNat (← getField idx fields "xon_bytes")
        xoffBytes := ← parseOpt parseNat (← getField idx fields "xoff_bytes")
        bufferCapacityBytes :=
          ← parseOpt parseNat (← getField idx fields "buffer_capacity_bytes")
        amountBytes := ← parseOpt parseNat (← getField idx fields "amount_bytes")
        occupancyAction :=
          ← parseOptOccupancyAction (← getField idx fields "occupancy_action")
        beforeOccupancyBytes :=
          ← parseOpt parseNat (← getField idx fields "before_occupancy_bytes")
        beforeAsserted := ← parseOptBit (← getField idx fields "before_asserted")
        afterOccupancyBytes :=
          ← parseOpt parseNat (← getField idx fields "after_occupancy_bytes")
        afterAsserted := ← parseOptBit (← getField idx fields "after_asserted")
        controlAction := ← parseOptControl (← getField idx fields "control_action")
        beforeControllers := ← parseNatList (← getField idx fields "before_controllers")
        afterControllers := ← parseNatList (← getField idx fields "after_controllers")
        srcLine := lineNo }
  match result with
  | .ok row => pure row
  | .error error => throw s!"line {lineNo}: {error}"

def requireSome (line : Nat) (name : String) : Option α → Except String α
  | some value => pure value
  | none => throw s!"line {line}: missing required field: {name}"

def sameMonitor (first second : Row) : Bool :=
  first.kind = .threshold && second.kind = .threshold &&
    first.nodeId = second.nodeId && first.queueId = second.queueId &&
    first.controlledLink = second.controlledLink && first.priority = second.priority

def sameControlledQueue (first second : Row) : Bool :=
  first.kind = .control && second.kind = .control &&
    first.nodeId = second.nodeId && first.queueId = second.queueId &&
    first.controlledLink = second.controlledLink && first.priority = second.priority

def strictlyIncreasing : List Nat → Bool
  | [] | [_] => true
  | first :: second :: rest => first < second && strictlyIncreasing (second :: rest)

def checkThreshold (row : Row) : Except String Unit := do
  let xon ← requireSome row.srcLine "xon_bytes" row.xonBytes
  let xoff ← requireSome row.srcLine "xoff_bytes" row.xoffBytes
  let capacity ← requireSome row.srcLine "buffer_capacity_bytes" row.bufferCapacityBytes
  let amount ← requireSome row.srcLine "amount_bytes" row.amountBytes
  let occupancyAction ←
    requireSome row.srcLine "occupancy_action" row.occupancyAction
  let beforeOccupancy ←
    requireSome row.srcLine "before_occupancy_bytes" row.beforeOccupancyBytes
  let beforeAsserted ← requireSome row.srcLine "before_asserted" row.beforeAsserted
  let afterOccupancy ←
    requireSome row.srcLine "after_occupancy_bytes" row.afterOccupancyBytes
  let afterAsserted ← requireSome row.srcLine "after_asserted" row.afterAsserted
  require row.srcLine
    (row.controller.isNone && row.beforeControllers.isEmpty && row.afterControllers.isEmpty)
    "PFC threshold row contains control-arrival state"
  require row.srcLine (xon < xoff && xoff ≤ capacity) "invalid PFC threshold configuration"
  let occupancy ←
    match occupancyAction with
    | .admit => pure (beforeOccupancy + amount)
    | .drain => do
        require row.srcLine (amount ≤ beforeOccupancy) "PFC drain exceeds occupancy"
        pure (beforeOccupancy - amount)
  require row.srcLine (occupancy ≤ capacity) "PFC occupancy exceeds capacity"
  let before : Pfc.ThresholdState :=
    { xonBytes := xon, xoffBytes := xoff, asserted := beforeAsserted }
  let (after, emitted) := Pfc.occupancyTransition before occupancy
  require row.srcLine
    (afterOccupancy = occupancy && afterAsserted = after.asserted && row.controlAction = emitted)
    "PFC threshold after-state mismatch"
  require row.srcLine
    (row.key.phase = if occupancyAction = .admit then 0 else 2)
    "PFC occupancy transition has the wrong event phase"

def checkControl (row : Row) : Except String Unit := do
  let controller ← requireSome row.srcLine "controller" row.controller
  let action ← requireSome row.srcLine "control_action" row.controlAction
  require row.srcLine (row.key.phase = 0) "PFC control arrival must have phase 0"
  require row.srcLine (controller = row.key.originNode)
    "PFC controller does not match event origin"
  require row.srcLine
    (row.xonBytes.isNone && row.xoffBytes.isNone && row.bufferCapacityBytes.isNone &&
      row.amountBytes.isNone && row.occupancyAction.isNone &&
      row.beforeOccupancyBytes.isNone && row.beforeAsserted.isNone &&
      row.afterOccupancyBytes.isNone && row.afterAsserted.isNone)
    "PFC control row contains threshold-monitor state"
  require row.srcLine
    (strictlyIncreasing row.beforeControllers && strictlyIncreasing row.afterControllers)
    "PFC controller sets must be sorted and duplicate-free"
  let mask : Pfc.PauseMask := ({} : Pfc.PauseMask).insert row.priority row.beforeControllers
  let after := Pfc.applyControl mask row.priority controller action
  require row.srcLine (after.getD row.priority [] = row.afterControllers)
    "PFC controller-set after-state mismatch"

def checkRow (row : Row) : Except String Unit := do
  require row.srcLine (row.priority < 8) "PFC priority must be below 8"
  match row.kind with
  | .threshold => checkThreshold row
  | .control => checkControl row

def canonicalize (rows : List Row) : Except String (List Row) := do
  let sorted := rows.toArray.qsort (fun a b => decide (a.key < b.key)) |>.toList
  let rec check : List Row → Except String Unit
    | [] | [_] => pure ()
    | first :: second :: rest => do
        require second.srcLine (first.key < second.key) "duplicate canonical event key"
        check (second :: rest)
  check sorted
  pure sorted

def checkRows (rows : List Row) : Except String Unit := do
  let rows ← canonicalize rows
  let rec continuity (previous : List Row) : List Row → Except String Unit
    | [] => pure ()
    | row :: rest => do
        match previous.find? (sameMonitor · row) with
        | none => pure ()
        | some prior =>
            require row.srcLine
              (prior.xonBytes = row.xonBytes && prior.xoffBytes = row.xoffBytes &&
                prior.bufferCapacityBytes = row.bufferCapacityBytes)
              s!"PFC threshold config discontinuity for monitor (node_id={row.nodeId}, queue_id={row.queueId}, controlled_link={row.controlledLink}, priority={row.priority})"
            require row.srcLine
              (prior.afterOccupancyBytes = row.beforeOccupancyBytes &&
                prior.afterAsserted = row.beforeAsserted)
              s!"PFC threshold state discontinuity for monitor (node_id={row.nodeId}, queue_id={row.queueId}, controlled_link={row.controlledLink}, priority={row.priority})"
        match previous.find? (sameControlledQueue · row) with
        | none => pure ()
        | some prior =>
            require row.srcLine (prior.afterControllers = row.beforeControllers)
              s!"PFC controller-set discontinuity for queue (node_id={row.nodeId}, queue_id={row.queueId}, controlled_link={row.controlledLink}, priority={row.priority})"
        continuity (row :: previous.filter (fun prior =>
          !sameMonitor prior row && !sameControlledQueue prior row)) rest
  continuity [] rows
  for row in rows do checkRow row

def parseCsv (content : String) : Except String (List Row) :=
  parseRows content parseRow

end PfcLog

namespace DrrLog

structure Row where
  key : DaysExecutor.EventKey
  nodeId : Nat
  queueId : Nat
  classCount : Nat
  quantaBytes : List Nat
  beforeDeficitsBytes : List Nat
  beforeCurrentClass : Nat
  scanSteps : Nat
  eligiblePackets : List Packet
  selectedPayload : Nat
  afterDeficitsBytes : List Nat
  afterCurrentClass : Nat
  srcLine : Nat
deriving DecidableEq, Repr

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let result : Except String Row := do
    pure
      { key :=
          { timeNs := ← parseNat (← getField idx fields "time_ns")
            phase := ← parseNat (← getField idx fields "event_phase")
            originNode := ← parseNat (← getField idx fields "event_origin_node")
            originSeq := ← parseNat (← getField idx fields "event_origin_sequence") }
        nodeId := ← parseNat (← getField idx fields "node_id")
        queueId := ← parseNat (← getField idx fields "queue_id")
        classCount := ← parseNat (← getField idx fields "class_count")
        quantaBytes := ← parseNatList (← getField idx fields "quanta_bytes")
        beforeDeficitsBytes :=
          ← parseNatList (← getField idx fields "before_deficits_bytes")
        beforeCurrentClass := ← parseNat (← getField idx fields "before_current_class")
        scanSteps := ← parseNat (← getField idx fields "scan_steps")
        eligiblePackets := ← parsePackets (← getField idx fields "eligible_packets")
        selectedPayload := ← parseNat (← getField idx fields "selected_payload")
        afterDeficitsBytes := ← parseNatList (← getField idx fields "after_deficits_bytes")
        afterCurrentClass := ← parseNat (← getField idx fields "after_current_class")
        srcLine := lineNo }
  match result with
  | .ok row => pure row
  | .error error => throw s!"line {lineNo}: {error}"

def sameQueue (first second : Row) : Bool :=
  first.nodeId = second.nodeId && first.queueId = second.queueId

def checkRow (row : Row) : Except String Unit := do
  require row.srcLine (row.key.phase = 2) "DRR service certificate must have phase 2"
  require row.srcLine (row.classCount > 0) "DRR class count must be positive"
  require row.srcLine
    (row.quantaBytes.length = row.classCount &&
      row.beforeDeficitsBytes.length = row.classCount &&
      row.afterDeficitsBytes.length = row.classCount)
    "DRR state vector length mismatch"
  require row.srcLine (row.quantaBytes.all (· > 0)) "DRR quanta must be positive"
  let initial : Drr.State :=
    { classCount := row.classCount
      quanta := indexedMap row.quantaBytes
      deficits := indexedMap row.beforeDeficitsBytes
      currentClass := row.beforeCurrentClass }
  let initial := row.eligiblePackets.foldl
    (fun state packet => Drr.enqueue state
      { id := packet.id, flow := packet.flow, sizeBytes := packet.sizeBytes }) initial
  match Drr.schedule row.srcLine row.scanSteps initial with
  | .error error => throw error
  | .ok (after, selected) =>
      require row.srcLine (selected.id = row.selectedPayload) "DRR selected payload mismatch"
      let deficits := (List.range row.classCount).map (Drr.deficit after)
      require row.srcLine
        (deficits = row.afterDeficitsBytes && after.currentClass = row.afterCurrentClass)
        "DRR after-state mismatch"

def canonicalize (rows : List Row) : Except String (List Row) := do
  let sorted := rows.toArray.qsort (fun a b => decide (a.key < b.key)) |>.toList
  let rec check : List Row → Except String Unit
    | [] | [_] => pure ()
    | first :: second :: rest => do
        require second.srcLine (first.key < second.key) "duplicate canonical event key"
        check (second :: rest)
  check sorted
  pure sorted

def checkRows (rows : List Row) : Except String Unit := do
  let rows ← canonicalize rows
  let rec continuity (previous : List Row) : List Row → Except String Unit
    | [] => pure ()
    | row :: rest => do
        match previous.find? (sameQueue · row) with
        | none => pure ()
        | some prior =>
            require row.srcLine
              (prior.classCount = row.classCount && prior.quantaBytes = row.quantaBytes)
              s!"DRR config discontinuity for queue (node_id={row.nodeId}, queue_id={row.queueId})"
            require row.srcLine
              (prior.afterDeficitsBytes = row.beforeDeficitsBytes &&
                prior.afterCurrentClass = row.beforeCurrentClass)
              s!"DRR state discontinuity for queue (node_id={row.nodeId}, queue_id={row.queueId})"
        continuity (row :: previous.filter (fun prior => !sameQueue prior row)) rest
  continuity [] rows
  for row in rows do checkRow row

def parseCsv (content : String) : Except String (List Row) :=
  parseRows content parseRow

end DrrLog

namespace WrrLog

structure Row where
  key : DaysExecutor.EventKey
  nodeId : Nat
  queueId : Nat
  classCount : Nat
  weights : List Nat
  beforePacketsSent : List Nat
  beforeCurrentClass : Nat
  eligiblePackets : List Packet
  selectedPayload : Nat
  afterPacketsSent : List Nat
  afterCurrentClass : Nat
  srcLine : Nat
deriving DecidableEq, Repr

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let result : Except String Row := do
    pure
      { key :=
          { timeNs := ← parseNat (← getField idx fields "time_ns")
            phase := ← parseNat (← getField idx fields "event_phase")
            originNode := ← parseNat (← getField idx fields "event_origin_node")
            originSeq := ← parseNat (← getField idx fields "event_origin_sequence") }
        nodeId := ← parseNat (← getField idx fields "node_id")
        queueId := ← parseNat (← getField idx fields "queue_id")
        classCount := ← parseNat (← getField idx fields "class_count")
        weights := ← parseNatList (← getField idx fields "weights")
        beforePacketsSent := ← parseNatList (← getField idx fields "before_packets_sent")
        beforeCurrentClass := ← parseNat (← getField idx fields "before_current_class")
        eligiblePackets := ← parsePackets (← getField idx fields "eligible_packets")
        selectedPayload := ← parseNat (← getField idx fields "selected_payload")
        afterPacketsSent := ← parseNatList (← getField idx fields "after_packets_sent")
        afterCurrentClass := ← parseNat (← getField idx fields "after_current_class")
        srcLine := lineNo }
  match result with
  | .ok row => pure row
  | .error error => throw s!"line {lineNo}: {error}"

def sameQueue (first second : Row) : Bool :=
  first.nodeId = second.nodeId && first.queueId = second.queueId

def sent (state : Wrr.State) (classId : Nat) : Nat :=
  state.sent.getD classId 0

def checkRow (row : Row) : Except String Unit := do
  require row.srcLine (row.key.phase = 2) "WRR service certificate must have phase 2"
  require row.srcLine (row.classCount > 0) "WRR class count must be positive"
  require row.srcLine
    (row.weights.length = row.classCount &&
      row.beforePacketsSent.length = row.classCount &&
      row.afterPacketsSent.length = row.classCount)
    "WRR state vector length mismatch"
  require row.srcLine (row.weights.all (· > 0)) "WRR weights must be positive"
  let initial : Wrr.State :=
    { classCount := row.classCount
      weights := indexedMap row.weights
      sent := indexedMap row.beforePacketsSent
      currentClass := row.beforeCurrentClass }
  let initial := row.eligiblePackets.foldl
    (fun state packet => Wrr.enqueue state
      { id := packet.id, flow := packet.flow, sizeBytes := packet.sizeBytes }) initial
  match Wrr.schedule row.srcLine initial with
  | .error error => throw error
  | .ok (after, selected) =>
      require row.srcLine (selected.id = row.selectedPayload) "WRR selected payload mismatch"
      let packetsSent := (List.range row.classCount).map (sent after)
      require row.srcLine
        (packetsSent = row.afterPacketsSent && after.currentClass = row.afterCurrentClass)
        "WRR after-state mismatch"

def canonicalize (rows : List Row) : Except String (List Row) := do
  let sorted := rows.toArray.qsort (fun a b => decide (a.key < b.key)) |>.toList
  let rec check : List Row → Except String Unit
    | [] | [_] => pure ()
    | first :: second :: rest => do
        require second.srcLine (first.key < second.key) "duplicate canonical event key"
        check (second :: rest)
  check sorted
  pure sorted

def checkRows (rows : List Row) : Except String Unit := do
  let rows ← canonicalize rows
  let rec continuity (previous : List Row) : List Row → Except String Unit
    | [] => pure ()
    | row :: rest => do
        match previous.find? (sameQueue · row) with
        | none => pure ()
        | some prior =>
            require row.srcLine
              (prior.classCount = row.classCount && prior.weights = row.weights)
              s!"WRR config discontinuity for queue (node_id={row.nodeId}, queue_id={row.queueId})"
            require row.srcLine
              (prior.afterPacketsSent = row.beforePacketsSent &&
                prior.afterCurrentClass = row.beforeCurrentClass)
              s!"WRR state discontinuity for queue (node_id={row.nodeId}, queue_id={row.queueId})"
        continuity (row :: previous.filter (fun prior => !sameQueue prior row)) rest
  continuity [] rows
  for row in rows do checkRow row

def parseCsv (content : String) : Except String (List Row) :=
  parseRows content parseRow

end WrrLog

/-!
The Days AGO WFQ certificate (`wfq_transitions_csv`): one row per enqueue (phase 0), service start
(`select`, phase 2) and service completion (`complete`, phase 1) at a WFQ egress queue, and an
`initial` row (event-key columns empty) for a queue that does not start empty and idle.

The checker replays each queue from its `initial` row, or from the empty initial state; every
row's `before` state must equal the replayed state, and its `after` state, virtual start and
finish tag must equal the reference update. It tracks the waiting packets, their tags and their
PFC classes itself:
- a packet's PFC class is fixed for its lifetime: its `enqueue` row (or the `initial` row) gives
  it, and every listing must repeat it; on a queue without a PFC monitor (`pfc_monitor = 0`) every
  class is zero and nothing is ever paused;
- a `select` row must list exactly the waiting packets, with their tags and classes, and serve the
  least finish tag (the earliest enqueue among equal tags) among those whose class is not paused;
- on a queue with a PFC monitor the paused set must be the queue's pause state in the run's PFC
  certificate (`p10c_mechanisms_check pfc`, checked first) at the select's event key: the
  controller sets of its `control` rows before that key, from the starting paused set of the
  `initial` row (a priority it lists as paused has a non-empty controller set before its first
  `control` row, every other an empty one).
-/
namespace WfqLog

inductive Kind
  | initial
  | enqueue
  | select
  | complete
  deriving DecidableEq, Repr

def parseKind : String → Except String Kind
  | "initial" => pure .initial
  | "enqueue" => pure .enqueue
  | "select" => pure .select
  | "complete" => pure .complete
  | other => throw s!"invalid WFQ kind: '{other}'"

def phase : Kind → Nat
  | .enqueue => 0
  | .complete => 1
  | .select => 2
  | .initial => 0

def kindName : Kind → String
  | .initial => "initial"
  | .enqueue => "enqueue"
  | .select => "select"
  | .complete => "complete"

/-- An exact rational `numerator/denominator`, in lowest terms with a positive denominator. -/
def parseRat (value : String) : Except String Rat := do
  match value.splitOn "/" with
  | [numerator, denominator] =>
      let numerator ← parseNat numerator
      let denominator ← parseNat denominator
      if denominator = 0 then throw s!"zero denominator: '{value}'"
      if Nat.gcd numerator denominator ≠ 1 then throw s!"rational not in lowest terms: '{value}'"
      pure ((numerator : Rat) / (denominator : Rat))
  | _ => throw s!"invalid rational: '{value}'"

def parseRatList (value : String) : Except String (List Rat) := do
  if value = "" then pure [] else value.splitOn ";" |>.mapM parseRat

structure QueuedPacket where
  payload : Nat
  flow : Nat
  sizeBytes : Nat
  pfcPriority : Nat
  finish : Rat
  deriving DecidableEq, Repr

def parseQueuedPacket (value : String) : Except String QueuedPacket := do
  match value.splitOn ":" with
  | [payload, flow, sizeBytes, pfcPriority, finish] =>
      pure
        { payload := ← parseNat payload
          flow := ← parseNat flow
          sizeBytes := ← parseNat sizeBytes
          pfcPriority := ← parseNat pfcPriority
          finish := ← parseRat finish }
  | _ => throw s!"invalid WFQ queued packet: '{value}'"

def parseQueuedPackets (value : String) : Except String (List QueuedPacket) := do
  if value = "" then pure [] else value.splitOn ";" |>.mapM parseQueuedPacket

def parseState (idx : Std.HashMap String Nat) (fields : Array String) (side : String) :
    Except String Wfq.State := do
  pure
    { virtualTime := ← parseRat (← getField idx fields s!"{side}_virtual_time")
      lastUpdatedNs := ← parseNat (← getField idx fields s!"{side}_last_updated_ns")
      finishTimes := ← parseRatList (← getField idx fields s!"{side}_finish_tags")
      activePackets := ← parseNatList (← getField idx fields s!"{side}_active_packets") }

structure Row where
  key : DaysExecutor.EventKey
  kind : Kind
  nodeId : Nat
  queueId : Nat
  rateBps : Nat
  weights : List Nat
  pfcMonitor : Bool
  payload : Option Nat
  flow : Option Nat
  sizeBytes : Option Nat
  pfcPriority : Option Nat
  virtualStart : Option Rat
  finish : Option Rat
  before : Wfq.State
  queuedPackets : List QueuedPacket
  pausedPriorities : List Nat
  after : Wfq.State
  srcLine : Nat

def parseKey (kind : Kind) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String DaysExecutor.EventKey := do
  let columns := ["time_ns", "event_phase", "event_origin_node", "event_origin_sequence"]
  let values ← columns.mapM (getField idx fields)
  match kind, values with
  | .initial, values => do
      unless values.all (· = "") do throw "an initial row has no event key"
      pure { timeNs := 0, phase := 0, originNode := 0, originSeq := 0 }
  | _, [timeNs, phase, originNode, originSeq] =>
      pure
        { timeNs := ← parseNat timeNs
          phase := ← parseNat phase
          originNode := ← parseNat originNode
          originSeq := ← parseNat originSeq }
  | _, _ => throw "invalid event key"

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let result : Except String Row := do
    let kind ← parseKind (← getField idx fields "kind")
    pure
      { key := ← parseKey kind idx fields
        kind
        nodeId := ← parseNat (← getField idx fields "node_id")
        queueId := ← parseNat (← getField idx fields "queue_id")
        rateBps := ← parseNat (← getField idx fields "rate_bps")
        weights := ← parseNatList (← getField idx fields "weights")
        pfcMonitor := ← parseBit (← getField idx fields "pfc_monitor")
        payload := ← parseOpt parseNat (← getField idx fields "payload")
        flow := ← parseOpt parseNat (← getField idx fields "flow_id")
        sizeBytes := ← parseOpt parseNat (← getField idx fields "size_bytes")
        pfcPriority := ← parseOpt parseNat (← getField idx fields "pfc_priority")
        virtualStart := ← parseOpt parseRat (← getField idx fields "virtual_start")
        finish := ← parseOpt parseRat (← getField idx fields "finish_tag")
        before := ← parseState idx fields "before"
        queuedPackets := ← parseQueuedPackets (← getField idx fields "queued_packets")
        pausedPriorities := ← parseNatList (← getField idx fields "paused_priorities")
        after := ← parseState idx fields "after"
        srcLine := lineNo }
  match result with
  | .ok row => pure row
  | .error error => throw s!"line {lineNo}: {error}"

/-- A packet the queue holds: waiting, or in service. -/
structure Tagged where
  flow : Nat
  sizeBytes : Nat
  pfcPriority : Nat
  finish : Rat
  order : Nat
  deriving Repr

/-- One egress queue's replay: its configuration, its scheduler state and its packets. -/
structure QueueReplay where
  rateBps : Nat
  weights : List Nat
  pfcMonitor : Bool
  state : Wfq.State
  waiting : Std.HashMap Nat Tagged := {}
  inService : Option (Nat × Tagged) := none
  enqueued : Nat := 0

/-- One PFC priority's pause state at a queue: its controller set, or paused by a set the run
starts with and the PFC certificate has not yet named. -/
inductive PauseState
  | controllers (set : List Nat)
  | pausedAtStart
  deriving DecidableEq, Repr

def PauseState.paused : PauseState → Bool
  | .controllers set => !set.isEmpty
  | .pausedAtStart => true

/-- The replay of every WFQ queue and of the pause state of its PFC priorities. -/
structure Replay where
  queues : Std.HashMap (Nat × Nat) QueueReplay := {}
  pauses : Std.HashMap (Nat × Nat × Nat) PauseState := {}

def strictlyIncreasing : List Nat → Bool
  | first :: second :: rest => first < second && strictlyIncreasing (second :: rest)
  | _ => true

def queueLabel (row : Row) : String :=
  s!"queue (node_id={row.nodeId}, queue_id={row.queueId})"

def requireField (row : Row) (name : String) : Option α → Except String α
  | some value => pure value
  | none => throw s!"line {row.srcLine}: WFQ {kindName row.kind} row has no {name}"

/-- A packet's PFC class on a queue: any class with a monitor, zero without one. -/
def checkClass (row : Row) (monitor : Bool) (pfcPriority : Nat) : Except String Unit :=
  require row.srcLine (pfcPriority < 8 && (monitor || pfcPriority = 0))
    s!"WFQ PFC class {pfcPriority} on {queueLabel row}, which {if monitor then "has" else "has no"} PFC monitor"

/-- The listed packets are exactly the waiting packets, with their flows, sizes, tags and classes;
returns those whose class is not paused. -/
def checkListing (row : Row) (queue : QueueReplay) : Except String (List Wfq.Waiting) := do
  require row.srcLine (row.queuedPackets.length = queue.waiting.size)
    s!"WFQ select lists {row.queuedPackets.length} packets; {queueLabel row} has {queue.waiting.size} waiting"
  let (_, listed) ← row.queuedPackets.foldlM
    (fun (seen, listed) (packet : QueuedPacket) => do
      require row.srcLine (!seen.contains packet.payload)
        s!"WFQ select lists payload {packet.payload} twice"
      let tagged ←
        match queue.waiting.get? packet.payload with
        | none => throw s!"line {row.srcLine}: WFQ select lists payload {packet.payload}, which is not waiting"
        | some tagged => pure tagged
      require row.srcLine
        (tagged.flow = packet.flow && tagged.sizeBytes = packet.sizeBytes &&
          tagged.finish = packet.finish)
        s!"WFQ select lists payload {packet.payload} with a flow, size or finish tag other than its enqueue's"
      require row.srcLine (tagged.pfcPriority = packet.pfcPriority)
        s!"WFQ select lists payload {packet.payload} with PFC class {packet.pfcPriority}, but it was enqueued with class {tagged.pfcPriority}"
      let listed :=
        if row.pausedPriorities.contains packet.pfcPriority then listed
        else { payload := packet.payload, finish := packet.finish, order := tagged.order } :: listed
      pure (seen.insert packet.payload, listed))
    ((∅ : Std.HashSet Nat), ([] : List Wfq.Waiting))
  pure listed.reverse

/-- The paused priorities of a queue in the replayed pause state, ascending. -/
def pausedAt (pauses : Std.HashMap (Nat × Nat × Nat) PauseState) (node queue : Nat) : List Nat :=
  (List.range 8).filter fun priority =>
    (pauses.getD (node, queue, priority) (.controllers [])).paused

def step (pauses : Std.HashMap (Nat × Nat × Nat) PauseState) (queue : QueueReplay) (row : Row) :
    Except String QueueReplay := do
  let classId := (← requireField row "flow_id" row.flow) % row.weights.length
  let payload ← requireField row "payload" row.payload
  let flow ← requireField row "flow_id" row.flow
  let sizeBytes ← requireField row "size_bytes" row.sizeBytes
  match row.kind with
  | .initial => throw s!"line {row.srcLine}: a second initial row for {queueLabel row}"
  | .enqueue => do
      require row.srcLine (row.queuedPackets.isEmpty && row.pausedPriorities.isEmpty)
        "WFQ enqueue row lists queued packets or paused priorities"
      let pfcPriority ← requireField row "pfc_priority" row.pfcPriority
      checkClass row queue.pfcMonitor pfcPriority
      require row.srcLine
        (!queue.waiting.contains payload && (queue.inService.map (·.1) != some payload))
        s!"WFQ payload {payload} is enqueued twice"
      let (after, start, finish) ←
        Wfq.enqueue row.srcLine row.rateBps row.weights queue.state classId sizeBytes
          row.key.timeNs
      require row.srcLine (row.virtualStart = some start) "WFQ virtual start mismatch"
      require row.srcLine (row.finish = some finish) "WFQ finish tag mismatch"
      require row.srcLine (row.after = after) "WFQ enqueue after-state mismatch"
      pure
        { queue with
          state := after
          waiting :=
            queue.waiting.insert payload
              { flow, sizeBytes, pfcPriority, finish, order := queue.enqueued }
          enqueued := queue.enqueued + 1 }
  | .select => do
      require row.srcLine (row.virtualStart.isNone && row.pfcPriority.isNone)
        "WFQ select row has a virtual start or a PFC class column"
      require row.srcLine (strictlyIncreasing row.pausedPriorities)
        "WFQ paused priorities must be strictly increasing"
      let paused := pausedAt pauses row.nodeId row.queueId
      require row.srcLine (queue.pfcMonitor || row.pausedPriorities.isEmpty)
        s!"WFQ select on {queueLabel row}, which has no PFC monitor, claims paused priorities"
      require row.srcLine (row.pausedPriorities = paused)
        s!"WFQ select claims paused priorities {row.pausedPriorities}; the PFC certificate pauses {paused} on {queueLabel row}"
      require row.srcLine (queue.inService.isNone)
        s!"WFQ select while {queueLabel row} is transmitting"
      require row.srcLine (row.after = queue.state) "WFQ select changes the scheduler state"
      let eligible ← checkListing row queue
      let served ←
        match Wfq.least eligible with
        | none => throw s!"line {row.srcLine}: WFQ select with no eligible packet"
        | some served => pure served
      require row.srcLine (served.payload = payload)
        s!"WFQ served payload {payload}, but the least finish tag among the eligible packets is payload {served.payload}"
      let tagged ←
        match queue.waiting.get? payload with
        | none => throw s!"line {row.srcLine}: WFQ served payload {payload} is not waiting"
        | some tagged => pure tagged
      require row.srcLine
        (tagged.flow = flow && tagged.sizeBytes = sizeBytes && some tagged.finish = row.finish)
        "WFQ served packet differs from its enqueue"
      pure
        { queue with
          waiting := queue.waiting.erase payload
          inService := some (payload, tagged) }
  | .complete => do
      require row.srcLine
        (row.virtualStart.isNone && row.pfcPriority.isNone && row.queuedPackets.isEmpty &&
          row.pausedPriorities.isEmpty)
        "WFQ complete row has a virtual start, a PFC class, queued packets or paused priorities"
      let tagged ←
        match queue.inService with
        | some (inService, tagged) =>
            if inService = payload then pure tagged
            else throw s!"line {row.srcLine}: WFQ completes payload {payload}, but payload {inService} is in service"
        | none => throw s!"line {row.srcLine}: WFQ completes payload {payload} with nothing in service"
      require row.srcLine
        (tagged.flow = flow && tagged.sizeBytes = sizeBytes && some tagged.finish = row.finish)
        "WFQ completed packet differs from its enqueue"
      let after ←
        Wfq.complete row.srcLine row.rateBps row.weights queue.state classId row.key.timeNs
      require row.srcLine (row.after = after) "WFQ complete after-state mismatch"
      pure { queue with state := after, inService := none }

def checkShape (row : Row) : Except String Unit := do
  require row.srcLine (row.key.phase = phase row.kind || row.kind = .initial)
    s!"WFQ {kindName row.kind} row must have phase {phase row.kind}"
  let classCount := row.weights.length
  require row.srcLine (classCount > 0 && row.weights.all (· > 0))
    "WFQ weights must be nonempty and positive"
  require row.srcLine (row.rateBps > 0) "WFQ rate must be positive"
  require row.srcLine
    (row.before.finishTimes.length = classCount && row.before.activePackets.length = classCount &&
      row.after.finishTimes.length = classCount && row.after.activePackets.length = classCount)
    "WFQ state vector length mismatch"

/-- A queue's starting state: its waiting packets in queue order and its in-service packet, whose
counts by class must be the active counts, and its starting paused priorities. -/
def checkInitial (replay : Replay) (row : Row) : Except String Replay := do
  checkShape row
  let id := (row.nodeId, row.queueId)
  require row.srcLine (!replay.queues.contains id) s!"two initial rows for {queueLabel row}"
  require row.srcLine (row.before = row.after) "WFQ initial row changes its state"
  require row.srcLine (row.virtualStart.isNone) "WFQ initial row has a virtual start"
  require row.srcLine (strictlyIncreasing row.pausedPriorities && row.pausedPriorities.all (· < 8))
    "WFQ paused priorities must be strictly increasing PFC priorities"
  require row.srcLine (row.pfcMonitor || row.pausedPriorities.isEmpty)
    s!"WFQ initial row of {queueLabel row}, which has no PFC monitor, claims paused priorities"
  let inService ← match row.payload with
    | none => do
        require row.srcLine
          (row.flow.isNone && row.sizeBytes.isNone && row.pfcPriority.isNone && row.finish.isNone)
          "WFQ initial row describes an in-service packet without its payload"
        pure none
    | some payload => do
        let tagged : Tagged :=
          { flow := ← requireField row "flow_id" row.flow
            sizeBytes := ← requireField row "size_bytes" row.sizeBytes
            pfcPriority := ← requireField row "pfc_priority" row.pfcPriority
            finish := ← requireField row "finish_tag" row.finish
            order := 0 }
        checkClass row row.pfcMonitor tagged.pfcPriority
        pure (some (payload, tagged))
  let (waiting, enqueued) ← row.queuedPackets.foldlM
    (fun (waiting, order) (packet : QueuedPacket) => do
      require row.srcLine
        (!waiting.contains packet.payload && (inService.map (·.1) != some packet.payload))
        s!"WFQ initial row lists payload {packet.payload} twice"
      checkClass row row.pfcMonitor packet.pfcPriority
      pure
        (waiting.insert packet.payload
          { flow := packet.flow
            sizeBytes := packet.sizeBytes
            pfcPriority := packet.pfcPriority
            finish := packet.finish
            order : Tagged },
          order + 1))
    ((∅ : Std.HashMap Nat Tagged), 0)
  let classCount := row.weights.length
  let held := (inService.map (·.2.flow)).toList ++ row.queuedPackets.map (·.flow)
  let counts := (List.range classCount).map fun classId =>
    (held.filter (· % classCount = classId)).length
  require row.srcLine (counts = row.after.activePackets)
    s!"WFQ initial row of {queueLabel row}: its packets by class {counts} are not its active counts"
  let pauses := row.pausedPriorities.foldl
    (fun pauses priority => pauses.insert (row.nodeId, row.queueId, priority) .pausedAtStart)
    replay.pauses
  pure
    { queues :=
        replay.queues.insert id
          { rateBps := row.rateBps
            weights := row.weights
            pfcMonitor := row.pfcMonitor
            state := row.after
            waiting
            inService
            enqueued }
      pauses }

/-- Applies one `control` row of the PFC certificate to a WFQ queue's pause state. -/
def applyControl (monitors : Std.HashMap (Nat × Nat) Bool) (replay : Replay) (control : PfcLog.Row) :
    Except String Replay := do
  match monitors.get? (control.nodeId, control.queueId) with
  | none => pure replay
  | some false =>
      throw s!"pfc line {control.srcLine}: a PFC control row for WFQ queue (node_id={control.nodeId}, queue_id={control.queueId}), which has no PFC monitor"
  | some true =>
      let id := (control.nodeId, control.queueId, control.priority)
      match replay.pauses.getD id (.controllers []) with
      | .controllers set =>
          require control.srcLine (set = control.beforeControllers)
            s!"pfc: controllers {control.beforeControllers} before this row, but the WFQ queue's pause state holds {set}"
      | .pausedAtStart =>
          require control.srcLine (!control.beforeControllers.isEmpty)
            "pfc: the WFQ initial row starts this priority paused, but no controller pauses it"
      pure { replay with pauses := replay.pauses.insert id (.controllers control.afterControllers) }

def checkRow (monitors : Std.HashMap (Nat × Nat) Bool) (replay : Replay) (row : Row) :
    Except String Replay := do
  checkShape row
  let id := (row.nodeId, row.queueId)
  let queue := replay.queues.getD id
    { rateBps := row.rateBps
      weights := row.weights
      pfcMonitor := monitors.getD id false
      state := Wfq.initial row.weights.length }
  require row.srcLine
    (queue.rateBps = row.rateBps && queue.weights = row.weights && queue.pfcMonitor = row.pfcMonitor)
    s!"WFQ config discontinuity for {queueLabel row}"
  require row.srcLine (row.before = queue.state)
    s!"WFQ state discontinuity for {queueLabel row}"
  let queue ← step replay.pauses queue row
  pure { replay with queues := replay.queues.insert id queue }

def canonicalize (rows : List Row) : Except String (List Row) := do
  let sorted := rows.toArray.qsort (fun a b => decide (a.key < b.key)) |>.toList
  let rec check : List Row → Except String Unit
    | [] | [_] => pure ()
    | first :: second :: rest => do
        require second.srcLine (first.key < second.key) "duplicate canonical event key"
        check (second :: rest)
  check sorted
  pure sorted

/-- Replays `rows`, joining every queue with a PFC monitor to `pfc`, the run's PFC certificate. -/
def checkRows (rows : List Row) (pfc : Option (List PfcLog.Row)) : Except String Unit := do
  let initial := rows.filter (·.kind = .initial)
  let events ← canonicalize (rows.filter (·.kind != .initial))
  let monitors ← rows.foldlM
    (fun (monitors : Std.HashMap (Nat × Nat) Bool) row => do
      let id := (row.nodeId, row.queueId)
      require row.srcLine (monitors.getD id row.pfcMonitor = row.pfcMonitor)
        s!"WFQ config discontinuity for {queueLabel row}"
      pure (monitors.insert id row.pfcMonitor))
    {}
  let controls ← match pfc with
    | none => do
        match rows.find? (·.pfcMonitor) with
        | some row =>
            throw s!"line {row.srcLine}: {queueLabel row} has a PFC monitor: pass the run's PFC certificate"
        | none => pure []
    | some pfcRows => do
        PfcLog.checkRows pfcRows
        let sorted ← PfcLog.canonicalize pfcRows
        pure (sorted.filter (·.kind = .control))
  let replay ← initial.foldlM checkInitial {}
  let rec go (replay : Replay) (controls : List PfcLog.Row) : List Row → Except String Unit
    | [] => pure ()
    | row :: rest => do
        let (due, later) := controls.span (fun control => decide (control.key < row.key))
        let replay ← due.foldlM (applyControl monitors) replay
        let replay ← checkRow monitors replay row
        go replay later rest
  go replay controls events

def parseCsv (content : String) : Except String (List Row) :=
  parseRows content parseRow

end WfqLog

end LeanGuard.P10c.MechanismEventLog
