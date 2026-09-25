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

end LeanGuard.P10c.MechanismEventLog
