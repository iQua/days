import Std

namespace LeanGuard.P10c.Roce

/-!
Exact semantics of a RoCE queue pair's reliability layer (P15).

A queue pair is a reliable DCQCN flow: its Mellanox-form controller stays DCQCN's own
(`LeanGuard.P10c.Dcqcn`); this module specifies the Go-back-N layer on top of it. The rules mirror
`days-gpu/evidence/P15/qp-design.md` §5 (steps 1-11, same-instant rules S1-S6) and its Phase 2
amendments, with P16's ECN echo (`days-gpu/evidence/P16/dcqcn-design.md` §3, rulings D4-D6): the
receiver sends no CNP, and each ACK or NACK echoes the CE mark of the data packet that triggered
it. The record views mirror the schema pinned in `days-gpu/plans/briefs/p15/qp-schema.md` with
its Amendment 6.

A PSN is the byte offset of a packet's first byte. Packet `psn` is `min(mtu, total - psn)` bytes,
and every PSN the sender can name is a packet boundary (§1).

All quantities are naturals with explicit `u64`/`u128` bounds where the executor's arithmetic is
fixed-width; no floating point appears anywhere.
-/

def maxU64 : Nat := 2 ^ 64 - 1

def maxU128 : Nat := 2 ^ 128 - 1

/-! ## Receiver (§2.3, §5 step 9) -/

structure ReceiverConfig where
  totalBytes : Nat
  ackEveryPackets : Nat
  nackIntervalNs : Nat
  duplicateAck : Bool
  ackSizeBytes : Nat
  deriving DecidableEq, Repr

/-- The rate-limit mark of the last NACK sent: its frontier and its time. -/
structure NackMark where
  expectedPsn : Nat
  timeNs : Nat
  deriving DecidableEq, Repr

structure ReceiverState where
  expectedPsn : Nat
  packetsSinceAck : Nat
  lastNack : Option NackMark
  deriving DecidableEq, Repr

/-- What the receiver answers one data arrival with (`RoceReceiverAction`). -/
inductive Action
  | ack
  | duplicateAck
  | nack
  | nackSuppressed
  | none
  deriving DecidableEq, Repr

def Action.sendsFeedback : Action → Bool
  | .ack | .duplicateAck | .nack => true
  | .nackSuppressed | .none => false

/-- One data arrival at the receiver. -/
structure DataArrival where
  psn : Nat
  bytes : Nat
  ce : Bool
  deriving DecidableEq, Repr

structure ReceiverResult where
  state : ReceiverState
  action : Action
  /-- The frontier an ACK or NACK carries, when one is sent. -/
  feedbackAcknowledgment : Option Nat
  /-- The CE mark an ACK or NACK echoes, when one is sent: the arriving packet's (ruling D5). -/
  feedbackCeEcho : Option Bool
  deriving DecidableEq, Repr

def validReceiverConfig (config : ReceiverConfig) : Bool :=
  config.totalBytes > 0 &&
    config.totalBytes ≤ maxU64 &&
    config.ackEveryPackets ≥ 1 &&
    config.ackEveryPackets ≤ maxU64 &&
    config.nackIntervalNs ≤ maxU64 &&
    config.ackSizeBytes ≥ 1 &&
    config.ackSizeBytes ≤ maxU64

/-- §7 invariant 11, restricted to what one receiver sees. -/
def validReceiverState (config : ReceiverConfig) (state : ReceiverState) : Bool :=
  state.expectedPsn ≤ config.totalBytes &&
    state.packetsSinceAck < config.ackEveryPackets &&
    state.lastNack.all (fun mark => mark.expectedPsn ≤ state.expectedPsn && mark.timeNs ≤ maxU64)

def initialReceiver : ReceiverState :=
  { expectedPsn := 0, packetsSinceAck := 0, lastNack := none }

/--
The NACK rate limit (§5 step 9): a NACK goes out unless the last NACK carried the same frontier
less than `nack_interval_ns` ago. Exact arithmetic: a deadline beyond `u64` is never reached.
-/
def nackAdmitted (config : ReceiverConfig) (state : ReceiverState) (timeNs : Nat) : Bool :=
  match state.lastNack with
  | none => true
  | some mark =>
      mark.expectedPsn != state.expectedPsn || mark.timeNs + config.nackIntervalNs ≤ timeNs

/--
One data arrival (§5 step 9, as amended by P16 ruling D4: no notification point).

* In order: the frontier advances; an ACK goes out every `ack_every_packets` packets and at the
  last byte.
* Below the frontier (a duplicate): an immediate ACK of the frontier when `duplicate_ack` (D4, D7),
  else a silent drop.
* Above it (out of order, dropped by Go-back-N): a NACK of the frontier unless rate-limited.

Every ACK or NACK restarts the ACK cadence and echoes the packet's CE mark (D5); a suppressed NACK
and a silent drop change nothing and echo nothing.
-/
def onData (config : ReceiverConfig) (state : ReceiverState) (timeNs : Nat)
    (packet : DataArrival) : ReceiverResult :=
  let (reliable, action) :=
    if packet.psn = state.expectedPsn then
      let expected := state.expectedPsn + packet.bytes
      let count := state.packetsSinceAck + 1
      let next := { state with expectedPsn := expected, packetsSinceAck := count }
      if count ≥ config.ackEveryPackets || expected = config.totalBytes then
        (next, Action.ack)
      else
        (next, Action.none)
    else if packet.psn < state.expectedPsn then
      if config.duplicateAck then (state, Action.duplicateAck) else (state, Action.none)
    else if nackAdmitted config state timeNs then
      ({ state with
          lastNack := some { expectedPsn := state.expectedPsn, timeNs := timeNs } },
        Action.nack)
    else
      (state, Action.nackSuppressed)
  let final :=
    if action.sendsFeedback then { reliable with packetsSinceAck := 0 } else reliable
  { state := final
    action := action
    feedbackAcknowledgment := if action.sendsFeedback then some final.expectedPsn else none
    feedbackCeEcho := if action.sendsFeedback then some packet.ce else none }

/-! ## Sender (§2.2, §5 steps 1-8) -/

/-- The pacer as the sender CSV names it. -/
inductive Pacer
  | armed
  | parked
  | stopped
  deriving DecidableEq, Repr

/-- `GeneratorStatus`. -/
inductive Status
  | scheduled
  | blocked
  | finished
  | stopped
  deriving DecidableEq, Repr

/-- A queue pair's congestion control (P17 lane nocc, qp-schema Amendment 7). -/
inductive CongestionControl
  /-- The Mellanox-form DCQCN controller, joined from the DCQCN log. -/
  | dcqcn
  /-- No congestion control: the pair paces at its configured rate and has no controller, so the
  DCQCN log holds no row of it and an ECN echo changes nothing. -/
  | none
  deriving DecidableEq, Repr

structure SenderConfig where
  mtuBytes : Nat
  totalBytes : Nat
  pacingIntervalNs : Nat
  firstPacingTimeNs : Nat
  /-- The fixed retransmission timeout; `0` means the timeout is off (§5 step 11). -/
  rtoNs : Nat
  /-- P16 ruling D7 (Amendment 6): the window in bytes; `0` means no window. -/
  windowBytes : Nat
  /-- The window scales with the controller's rate (SimAI `m_var_win`). -/
  variableWindow : Bool
  /-- The controller's maximum rate, which scales a variable window. -/
  maximumRateBps : Nat
  /-- The controller's configured initial rate, the pair's rate while its controller is pristine
  (fix round 1, review F3). -/
  initialRateBps : Nat
  /-- Amendment 7 (P17): DCQCN, or no congestion control. -/
  congestionControl : CongestionControl
  deriving DecidableEq, Repr

/--
The sender state exactly as one record views it (`RoceSenderView`).

* `rtoDeadlineNs` is present iff a timeout is armed: the timeout is on and data is outstanding.
* `nextTickNs` is present iff the pacer is not parked: the pending tick while armed, the
  beyond-stop tick while stopped. A parked pacer's last departure is never read again (a restart
  overwrites it), so the view is the complete state of the reliability layer.
-/
structure SenderState where
  nextPsn : Nat
  sndUna : Nat
  /-- The high-water mark: first transmissions only. -/
  bytesEmitted : Nat
  packetsEmitted : Nat
  creditQuanta : Nat
  rtoDeadlineNs : Option Nat
  pacer : Pacer
  nextTickNs : Option Nat
  status : Status
  deriving DecidableEq, Repr

/-- The data packet a pacing tick sends. -/
structure Emission where
  psn : Nat
  bytes : Nat
  retransmission : Bool
  deriving DecidableEq, Repr

def creditScale : Nat := 8 * 1_000_000_000

/-- §1: packet `psn` is `min(mtu, total - psn)` bytes. -/
def packetSize (config : SenderConfig) (psn : Nat) : Nat :=
  min config.mtuBytes (config.totalBytes - psn)

/-- §2.2: a packet costs `size * 8 * 10^9` credit quanta (the rate denominator is always one). -/
def packetCost (sizeBytes : Nat) : Nat :=
  sizeBytes * creditScale

/-- §2.2: one tick credits `current_rate_bps * pacing_interval_ns` quanta. -/
def tickCredit (config : SenderConfig) (rateBps : Nat) : Nat :=
  rateBps * config.pacingIntervalNs

/-- A PSN the sender can name: a multiple of the MTU, or the total (§1). -/
def boundary (config : SenderConfig) (psn : Nat) : Bool :=
  psn % config.mtuBytes = 0 || psn = config.totalBytes

def onGrid (config : SenderConfig) (timeNs : Nat) : Bool :=
  config.firstPacingTimeNs ≤ timeNs &&
    (timeNs - config.firstPacingTimeNs) % config.pacingIntervalNs = 0

def validSenderConfig (config : SenderConfig) : Bool :=
  config.mtuBytes > 0 &&
    config.mtuBytes ≤ maxU64 &&
    config.totalBytes > 0 &&
    config.totalBytes ≤ maxU64 &&
    config.pacingIntervalNs > 0 &&
    config.pacingIntervalNs ≤ maxU64 &&
    config.firstPacingTimeNs ≤ maxU64 &&
    config.rtoNs ≤ maxU64 &&
    config.windowBytes ≤ maxU64 &&
    config.maximumRateBps ≤ maxU64 &&
    (!config.variableWindow || (config.windowBytes > 0 && config.maximumRateBps > 0)) &&
    -- Amendment 7: a pair without congestion control has one fixed rate and no variable window.
    (config.congestionControl = .dcqcn ||
      (config.initialRateBps = config.maximumRateBps && !config.variableWindow))

/-- §7 invariants 2, 3, 4 and 5, as far as one sender's record shows them. -/
def validSenderState (config : SenderConfig) (state : SenderState) : Bool :=
  state.sndUna ≤ state.nextPsn &&
    state.nextPsn ≤ state.bytesEmitted &&
    state.bytesEmitted ≤ config.totalBytes &&
    boundary config state.sndUna &&
    boundary config state.nextPsn &&
    boundary config state.bytesEmitted &&
    state.packetsEmitted = (state.bytesEmitted + config.mtuBytes - 1) / config.mtuBytes &&
    state.creditQuanta ≤ maxU128 &&
    state.rtoDeadlineNs.isSome = (config.rtoNs != 0 && state.sndUna < state.bytesEmitted) &&
    state.rtoDeadlineNs.all (· ≤ maxU64) &&
    state.nextTickNs.isSome = (state.pacer != .parked) &&
    state.nextTickNs.all (fun tick => tick ≤ maxU64 && onGrid config tick) &&
    (state.pacer = .stopped) == (state.status = .stopped) &&
    (state.status = .finished) == (state.sndUna = config.totalBytes) &&
    (state.pacer != .parked || state.status = .blocked || state.status = .finished)

/--
P16 ruling D7 (SimAI `GetWin`): the window at controller rate `rateBps`: `windowBytes`, or with a
variable window `max(1, floor(windowBytes × rate / maximum))`; `none` without a window.
-/
def window (config : SenderConfig) (rateBps : Nat) : Option Nat :=
  if config.windowBytes = 0 then none
  else if config.variableWindow then
    some (max 1 (config.windowBytes * rateBps / config.maximumRateBps))
  else some config.windowBytes

/-- The window binds (SimAI `IsWinBound`): `next_psn - snd_una ≥ w`. -/
def windowBound (config : SenderConfig) (rateBps : Nat) (state : SenderState) : Bool :=
  (window config rateBps).any (fun w => state.nextPsn - state.sndUna ≥ w)

/--
The status an armed pacer's pending tick predicts (`roce::armed_status`): `scheduled` when the
window, if any, is open and one more tick of credit at the controller's current rate covers the
next packet, else `blocked`. The executor adds in `u128`; a sum beyond it predicts `blocked`. A
pending tick with nothing left to send is `blocked`.
-/
def armedStatus (config : SenderConfig) (rateBps : Nat) (state : SenderState) : Status :=
  if state.nextPsn ≥ config.totalBytes || windowBound config rateBps state then
    .blocked
  else
    let credit := state.creditQuanta + tickCredit config rateBps
    if credit ≤ maxU128 && packetCost (packetSize config state.nextPsn) ≤ credit then
      .scheduled
    else
      .blocked

/--
§5 step 2 (amended): the status is recomputed at every queue-pair transition, at the controller's
rate as of that transition (P16 ruling D2; `roce::settled_status`): `finished` once every byte is acknowledged, the
armed tick's prediction, or the parked status as it stands.
-/
def settle (config : SenderConfig) (rateBps : Nat) (state : SenderState)
    (parkedStatus : Status) : SenderState :=
  let status :=
    if state.sndUna ≥ config.totalBytes then .finished
    else if state.pacer = .armed then armedStatus config rateBps state
    else parkedStatus
  -- The CSV names a pacer that is not armed `stopped` exactly when its status is `stopped`;
  -- `finished` therefore turns a stopped pacer into a parked one, with no tick shown.
  let pacer :=
    if state.pacer = .armed then Pacer.armed
    else if status = .stopped then .stopped
    else .parked
  { state with
    status := status
    pacer := pacer
    nextTickNs := if pacer = .parked then none else state.nextTickNs }

/--
Ruling C6: the rate at which an armed status turns from `blocked` to `scheduled`, when the status
depends on the rate at all (`armedStatus` of an armed, unfinished pacer with a packet to send):
`scheduled` iff `rate ≥` this threshold, since `credit + rate × interval ≥ cost` is monotone in
the rate. The caller uses it only while the pair's credit is still zero, where the `u128` bound
of `armedStatus` cannot bind (`rate × interval < 2^128`).

A window (ruling D7) adds a second monotone condition. A variable window is open iff
`floor(W × rate / maximum) ≥ outstanding + 1`, iff `rate ≥ ceil((outstanding + 1) × maximum / W)`
(always, with nothing outstanding), so the threshold is the larger of the two; a fixed window
that binds predicts `blocked` at every rate (`none`: compared exactly). With nothing outstanding,
as before any pair's first credit, no window binds.
-/
def statusThreshold (config : SenderConfig) (state : SenderState) : Option Nat :=
  if state.pacer = .armed && state.sndUna < config.totalBytes &&
      state.nextPsn < config.totalBytes then
    let cost := packetCost (packetSize config state.nextPsn)
    let credit :=
      if cost ≤ state.creditQuanta then 0
      else (cost - state.creditQuanta + config.pacingIntervalNs - 1) / config.pacingIntervalNs
    let outstanding := state.nextPsn - state.sndUna
    if config.windowBytes = 0 || outstanding = 0 then some credit
    else if config.variableWindow then
      some (max credit
        (((outstanding + 1) * config.maximumRateBps + config.windowBytes - 1) / config.windowBytes))
    else if outstanding ≥ config.windowBytes then none
    else some credit
  else
    none

/-- §5 step 1: the state before the first pacing tick, at `first_pacing_time_ns`. -/
def initialSender (config : SenderConfig) (rateBps : Nat) : SenderState :=
  let state : SenderState :=
    { nextPsn := 0
      sndUna := 0
      bytesEmitted := 0
      packetsEmitted := 0
      creditQuanta := 0
      rtoDeadlineNs := none
      pacer := .armed
      nextTickNs := some config.firstPacingTimeNs
      status := .blocked }
  { state with status := armedStatus config rateBps state }

/-- §5 step 3 (D3): the first grid point strictly after `timeNs`. -/
def restartTime (config : SenderConfig) (timeNs : Nat) : Nat :=
  if timeNs < config.firstPacingTimeNs then
    config.firstPacingTimeNs
  else
    config.firstPacingTimeNs +
      ((timeNs - config.firstPacingTimeNs) / config.pacingIntervalNs + 1) * config.pacingIntervalNs

/--
The result of a sender transition. `stopQuery` is the one tick time the transition compared with
the scenario's stop time, if any; the transition took `withinStop` as that comparison's answer.
-/
structure SenderResult where
  state : SenderState
  emission : Option Emission
  stopQuery : Option Nat
  deriving DecidableEq, Repr

/--
§5 step 3: a rewind restarts a pacer that is not armed and has packets to send again, on the next
grid point after `timeNs`; beyond the stop time it stays `stopped` there. A completed queue pair
never restarts.
-/
def restart (config : SenderConfig) (timeNs : Nat) (withinStop : Bool) (state : SenderState) :
    SenderState × Option Nat :=
  if state.pacer = .armed || state.nextPsn ≥ config.totalBytes ||
      state.sndUna ≥ config.totalBytes then
    (state, none)
  else
    let tick := restartTime config timeNs
    if withinStop then
      ({ state with pacer := .armed, nextTickNs := some tick }, some tick)
    else
      ({ state with pacer := .stopped, nextTickNs := some tick, status := .stopped }, some tick)

/-- The timeout deadline as the view shows it: armed iff the timeout is on and data is outstanding. -/
def withDeadline (config : SenderConfig) (state : SenderState) (deadline : Option Nat) :
    SenderState :=
  { state with
    rtoDeadlineNs :=
      if config.rtoNs != 0 && state.sndUna < state.bytesEmitted then deadline else none }

/--
§5 step 2: a pacing tick at `timeNs` of an armed pacer. `rateBps` is the controller rate the tick
credits; `rateAfterBytes` is the rate after the tick's `on_bytes_emitted` (equal to `rateBps`
when nothing is sent), which predicts the next tick's status.
-/
def onTick (config : SenderConfig) (rateBps rateAfterBytes timeNs : Nat) (withinStop : Bool)
    (state : SenderState) : SenderResult :=
  let total := config.totalBytes
  let (sent, emission) :=
    if state.nextPsn < total then
      let credited := state.creditQuanta + tickCredit config rateBps
      let psn := state.nextPsn
      let size := packetSize config psn
      let cost := packetCost size
      if cost ≤ credited then
        let retransmission := psn < state.bytesEmitted
        let outstandingBefore := state.sndUna < state.bytesEmitted
        let next := psn + size
        let fresh : SenderState :=
          { state with
            creditQuanta := credited - cost
            nextPsn := next
            bytesEmitted := if retransmission then state.bytesEmitted else next
            packetsEmitted := if retransmission then state.packetsEmitted else state.packetsEmitted + 1 }
        let deadline :=
          if config.rtoNs != 0 && !outstandingBefore then some (timeNs + config.rtoNs)
          else state.rtoDeadlineNs
        (withDeadline config fresh deadline,
          some { psn := psn, bytes := size, retransmission := retransmission })
      else
        ({ state with creditQuanta := credited }, none)
    else
      (state, none)
  let (scheduled, query, parkedStatus) :=
    if sent.nextPsn < total then
      let tick := timeNs + config.pacingIntervalNs
      if withinStop then
        ({ sent with pacer := Pacer.armed, nextTickNs := some tick }, some tick, Status.blocked)
      else
        ({ sent with pacer := Pacer.stopped, nextTickNs := some tick }, some tick, Status.stopped)
    else
      ({ sent with pacer := Pacer.parked, nextTickNs := none }, none, Status.blocked)
  { state := settle config rateAfterBytes scheduled parkedStatus
    emission := emission
    stopQuery := query }

/--
Schema Amendment 1 (host-link PFC, ruling H1 (b); `hostpfc-design.md` §3.3): a pacing tick that
finds its queue pair's data class paused on its host's egress sends nothing, adds no credit, and
parks the pacer, so no tick is pending until a restart. The status settles as for any parked
pacer.
-/
def onPausedTick (config : SenderConfig) (rateBps : Nat) (state : SenderState) : SenderResult :=
  { state := settle config rateBps { state with pacer := .parked, nextTickNs := none } .blocked
    emission := none
    stopQuery := none }

/--
P16 ruling D7 (Amendment 6): a pacing tick that finds the window closed at the rate as of the tick
sends nothing, adds no credit and parks, as a paused tick does; only an ACK or NACK that moves
`snd_una`, or a timeout, restarts it (`onFeedback`, `onTimeout`), never a host RESUME. The caller
checks the window.
-/
def onWindowBlockedTick (config : SenderConfig) (rateBps : Nat) (state : SenderState) :
    SenderResult :=
  { state := settle config rateBps { state with pacer := .parked, nextTickNs := none } .blocked
    emission := none
    stopQuery := none }

/--
Schema Amendment 2: a PFC RESUME at `timeNs` restarts a pacer that a paused tick parked, on its
next grid point strictly after `timeNs` (D3), or leaves it `stopped` beyond the stop time. The
caller checks that the pair was parked by a pause and that a restart happened (`stopQuery`).
-/
def onResume (config : SenderConfig) (rateBps timeNs : Nat) (withinStop : Bool)
    (state : SenderState) : SenderResult :=
  let (restarted, query) := restart config timeNs withinStop state
  { state := settle config rateBps restarted restarted.status
    emission := none
    stopQuery := query }

/--
§5 steps 4 and 5: an ACK or NACK carrying `acknowledgment` at `timeNs`.

* Stale (`a ≤ snd_una` for an ACK, `e < snd_una` for a NACK): nothing changes.
* Otherwise the cumulative acknowledgment advances, `next_psn` follows it (an old copy may have
  advanced it past a rewound `next_psn`), a NACK rewinds `next_psn` to it, the timeout restarts
  while data is outstanding and disarms otherwise, and a pacer that is not armed restarts.
-/
def onFeedback (config : SenderConfig) (rateBps timeNs acknowledgment : Nat) (nack : Bool)
    (withinStop : Bool) (state : SenderState) : SenderResult :=
  let stale := if nack then acknowledgment < state.sndUna else acknowledgment ≤ state.sndUna
  if stale then
    { state := settle config rateBps state state.status, emission := none, stopQuery := none }
  else
    let sndUna := max state.sndUna acknowledgment
    let nextPsn := if nack then sndUna else max state.nextPsn sndUna
    let advanced := { state with sndUna := sndUna, nextPsn := nextPsn }
    let timed := withDeadline config advanced (some (timeNs + config.rtoNs))
    let (restarted, query) := restart config timeNs withinStop timed
    { state := settle config rateBps restarted restarted.status
      emission := none
      stopQuery := query }

/--
§5 step 6: the retransmission timeout fires (the caller checks it is armed for `timeNs`): rewind
to `snd_una`, re-arm one fixed timeout later, restart a pacer that is not armed.
-/
def onTimeout (config : SenderConfig) (rateBps timeNs : Nat) (withinStop : Bool)
    (state : SenderState) : SenderResult :=
  let rewound := withDeadline config { state with nextPsn := state.sndUna }
    (some (timeNs + config.rtoNs))
  let (restarted, query) := restart config timeNs withinStop rewound
  { state := settle config rateBps restarted restarted.status
    emission := none
    stopQuery := query }

end LeanGuard.P10c.Roce
