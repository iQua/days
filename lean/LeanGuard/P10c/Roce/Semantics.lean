import Std

namespace LeanGuard.P10c.Roce

/-!
Exact semantics of a RoCE queue pair's reliability layer (P15).

A queue pair is a reliable DCQCN flow: the DCQCN controller, its control tick and its CNP stay
DCQCN's own (`LeanGuard.P10c.Dcqcn`); this module specifies the Go-back-N layer on top of it.
The rules mirror `days-gpu/evidence/P15/qp-design.md` §5 (steps 1-11, same-instant rules S1-S6)
and its Phase 2 amendments; the record views mirror the schema pinned in
`days-gpu/plans/briefs/p15/qp-schema.md`.

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
  cnpIntervalNs : Nat
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
  lastCnpNs : Option Nat
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
  cnpSent : Bool
  /-- The frontier an ACK or NACK carries, when one is sent. -/
  feedbackAcknowledgment : Option Nat
  deriving DecidableEq, Repr

def validReceiverConfig (config : ReceiverConfig) : Bool :=
  config.totalBytes > 0 &&
    config.totalBytes ≤ maxU64 &&
    config.ackEveryPackets ≥ 1 &&
    config.ackEveryPackets ≤ maxU64 &&
    config.nackIntervalNs ≤ maxU64 &&
    config.ackSizeBytes ≥ 1 &&
    config.ackSizeBytes ≤ maxU64 &&
    config.cnpIntervalNs ≤ maxU64

/-- §7 invariant 11, restricted to what one receiver sees. -/
def validReceiverState (config : ReceiverConfig) (state : ReceiverState) : Bool :=
  state.expectedPsn ≤ config.totalBytes &&
    state.packetsSinceAck < config.ackEveryPackets &&
    state.lastNack.all (fun mark => mark.expectedPsn ≤ state.expectedPsn && mark.timeNs ≤ maxU64) &&
    state.lastCnpNs.all (· ≤ maxU64)

def initialReceiver : ReceiverState :=
  { expectedPsn := 0, packetsSinceAck := 0, lastNack := none, lastCnpNs := none }

/--
The DCQCN notification point (shared with DCQCN receivers, `scalar.rs` `host_dcqcn_data_arrival`):
a CE-marked packet sends a CNP when no CNP was sent yet, or when the CNP interval has elapsed.
The executor computes the deadline with `checked_add`; a deadline beyond `u64` keeps the interval
closed.
-/
def cnpAdmitted (config : ReceiverConfig) (state : ReceiverState) (timeNs : Nat) : Bool :=
  match state.lastCnpNs with
  | none => true
  | some last =>
      last + config.cnpIntervalNs ≤ maxU64 && last + config.cnpIntervalNs ≤ timeNs

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
One data arrival (§5 step 9): the notification point decides the CNP first (S1), then the
reliability layer.

* In order: the frontier advances; an ACK goes out every `ack_every_packets` packets and at the
  last byte.
* Below the frontier (a duplicate): an immediate ACK of the frontier when `duplicate_ack` (D4, D7),
  else a silent drop.
* Above it (out of order, dropped by Go-back-N): a NACK of the frontier unless rate-limited.

Every ACK or NACK restarts the ACK cadence; a suppressed NACK and a silent drop change nothing.
-/
def onData (config : ReceiverConfig) (state : ReceiverState) (timeNs : Nat)
    (packet : DataArrival) : ReceiverResult :=
  let cnpSent := packet.ce && cnpAdmitted config state timeNs
  let afterCnp := if cnpSent then { state with lastCnpNs := some timeNs } else state
  let (reliable, action) :=
    if packet.psn = afterCnp.expectedPsn then
      let expected := afterCnp.expectedPsn + packet.bytes
      let count := afterCnp.packetsSinceAck + 1
      let next := { afterCnp with expectedPsn := expected, packetsSinceAck := count }
      if count ≥ config.ackEveryPackets || expected = config.totalBytes then
        (next, Action.ack)
      else
        (next, Action.none)
    else if packet.psn < afterCnp.expectedPsn then
      if config.duplicateAck then (afterCnp, Action.duplicateAck) else (afterCnp, Action.none)
    else if nackAdmitted config afterCnp timeNs then
      ({ afterCnp with
          lastNack := some { expectedPsn := afterCnp.expectedPsn, timeNs := timeNs } },
        Action.nack)
    else
      (afterCnp, Action.nackSuppressed)
  let final :=
    if action.sendsFeedback then { reliable with packetsSinceAck := 0 } else reliable
  { state := final
    action := action
    cnpSent := cnpSent
    feedbackAcknowledgment := if action.sendsFeedback then some final.expectedPsn else none }

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

structure SenderConfig where
  mtuBytes : Nat
  totalBytes : Nat
  pacingIntervalNs : Nat
  firstPacingTimeNs : Nat
  /-- The fixed retransmission timeout; `0` means the timeout is off (§5 step 11). -/
  rtoNs : Nat
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
    config.rtoNs ≤ maxU64

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
The status an armed pacer's pending tick predicts (`roce::armed_status`): `scheduled` when one
more tick of credit at the controller's current rate covers the next packet, else `blocked`. The
executor adds in `u128`; a sum beyond it predicts `blocked`. A pending tick with nothing left to
send is `blocked`.
-/
def armedStatus (config : SenderConfig) (rateBps : Nat) (state : SenderState) : Status :=
  if state.nextPsn ≥ config.totalBytes then
    .blocked
  else
    let credit := state.creditQuanta + tickCredit config rateBps
    if credit ≤ maxU128 && packetCost (packetSize config state.nextPsn) ≤ credit then
      .scheduled
    else
      .blocked

/--
§5 step 2 (amended): the status is recomputed at every queue-pair transition, the CNP and the
control tick included (`roce::settled_status`): `finished` once every byte is acknowledged, the
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

/-- §5 steps 7 and 8: a CNP or a control tick changes the rate; the status is recomputed. -/
def onRateChange (config : SenderConfig) (rateBps : Nat) (state : SenderState) : SenderState :=
  settle config rateBps state state.status

end LeanGuard.P10c.Roce
