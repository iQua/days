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

end LeanGuard.P10c.Roce
