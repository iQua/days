import Std

namespace DaysExecutor

/-- Stable LP identifier mirroring `executor/src/event.rs:3-10` (`NodeId`). -/
abbrev NodeId := Nat

/-- Stable directed-link identifier mirroring `executor/src/event.rs:12-17` (`LinkId`). -/
abbrev LinkId := Nat

/-- Stable event-payload identifier mirroring `executor/src/event.rs:26-31` (`PayloadId`). -/
abbrev PayloadId := Nat

/--
Canonical event key in the exact Rust field order, mirroring
`executor/src/event.rs:51-62` (`EventKey`).
-/
structure EventKey where
  timeNs : Nat
  phase : Nat
  originNode : Nat
  originSeq : Nat
  deriving DecidableEq, Repr, Ord

/--
Proposition-valued lexicographic non-strict order corresponding to the derived comparator from
`executor/src/event.rs:51-62`.
-/
def EventKey.lexLE (a b : EventKey) : Prop :=
  a.timeNs < b.timeNs ∨
    (a.timeNs = b.timeNs ∧
      (a.phase < b.phase ∨
        (a.phase = b.phase ∧
          (a.originNode < b.originNode ∨
            (a.originNode = b.originNode ∧ a.originSeq ≤ b.originSeq)))))

/-- Relational `≤` for the canonical key, mirroring `EventKey`'s Rust total order. -/
instance eventKeyLE : LE EventKey := ⟨EventKey.lexLE⟩

/-- Decidable canonical non-strict ordering, matching fixed-width Rust key comparison. -/
instance (a b : EventKey) : Decidable (a ≤ b) := by
  change Decidable (EventKey.lexLE a b)
  unfold EventKey.lexLE
  infer_instance

/--
Totality of the canonical key order required to select Rust's least `EventKey`
(`executor/src/scalar.rs:344-350`).
-/
theorem EventKey.le_total (a b : EventKey) : a ≤ b ∨ b ≤ a := by
  change EventKey.lexLE a b ∨ EventKey.lexLE b a
  unfold EventKey.lexLE
  omega

/--
Reflexivity of the canonical key order used by the least-key queue at
`executor/src/scalar.rs:341-350`.
-/
theorem EventKey.le_refl (a : EventKey) : a ≤ a := by
  change EventKey.lexLE a a
  unfold EventKey.lexLE
  omega

/--
Transitivity of the canonical key order used by the ordered future-event lists at
`executor/src/scalar.rs:341-359`.
-/
theorem EventKey.le_trans {a b c : EventKey} (hab : a ≤ b) (hbc : b ≤ c) : a ≤ c := by
  change EventKey.lexLE a b at hab
  change EventKey.lexLE b c at hbc
  change EventKey.lexLE a c
  unfold EventKey.lexLE at hab hbc ⊢
  omega

/--
Antisymmetry of the canonical key order required for unique least-key execution, mirroring the
duplicate-key rejection at `executor/src/scalar.rs:352-355`.
-/
theorem EventKey.le_antisymm {a b : EventKey} (hab : a ≤ b) (hba : b ≤ a) : a = b := by
  cases a with
  | mk aT aP aO aS =>
    cases b with
    | mk bT bP bO bS =>
      change
        aT < bT ∨
          (aT = bT ∧
            (aP < bP ∨
              (aP = bP ∧ (aO < bO ∨ (aO = bO ∧ aS ≤ bS))))) at hab
      change
        bT < aT ∨
          (bT = aT ∧
            (bP < aP ∨
              (bP = aP ∧ (bO < aO ∨ (bO = aO ∧ bS ≤ aS))))) at hba
      have hT : aT = bT := by omega
      subst bT
      have hP : aP = bP := by omega
      subst bP
      have hO : aO = bO := by omega
      subst bO
      have hS : aS = bS := by omega
      subst bS
      rfl

/--
Lawful total-order witness for the relation used by the scalar and safe-horizon queues at
`executor/src/scalar.rs:341-359` and `executor/src/safe_horizon.rs:170`.
-/
instance : Std.IsLinearOrder EventKey where
  le_refl := EventKey.le_refl
  le_trans _ _ _ := EventKey.le_trans
  le_antisymm _ _ := EventKey.le_antisymm
  le_total := EventKey.le_total

/--
Proposition-valued strict lexicographic order corresponding to Rust's derived `Ord` at
`executor/src/event.rs:51-62`.
-/
def EventKey.lexLT (a b : EventKey) : Prop :=
  a.timeNs < b.timeNs ∨
    (a.timeNs = b.timeNs ∧
      (a.phase < b.phase ∨
        (a.phase = b.phase ∧
          (a.originNode < b.originNode ∨
            (a.originNode = b.originNode ∧ a.originSeq < b.originSeq)))))

/-- Relational `<` for the canonical key, mirroring `EventKey`'s Rust total order. -/
instance eventKeyLT : LT EventKey := ⟨EventKey.lexLT⟩

/-- Decidable canonical strict ordering, matching fixed-width Rust key comparison. -/
instance (a b : EventKey) : Decidable (a < b) := by
  change Decidable (EventKey.lexLT a b)
  unfold EventKey.lexLT
  infer_instance

/--
Trichotomy of the strict canonical key order used by Rust's ordered maps at
`executor/src/scalar.rs:341-359`.
-/
theorem EventKey.lt_trichotomy (a b : EventKey) : a < b ∨ a = b ∨ b < a := by
  change EventKey.lexLT a b ∨ a = b ∨ EventKey.lexLT b a
  unfold EventKey.lexLT
  cases a with
  | mk aT aP aO aS =>
    cases b with
    | mk bT bP bO bS =>
      simp only [mk.injEq]
      omega

/--
Irreflexivity of the strict canonical key order used by unique future-event lists at
`executor/src/scalar.rs:341-359`.
-/
theorem EventKey.lt_irrefl (key : EventKey) : ¬ key < key := by
  change ¬ EventKey.lexLT key key
  unfold EventKey.lexLT
  omega

/--
Transitivity of the strict canonical key order used by scalar and per-LP future-event lists at
`executor/src/scalar.rs:341-359` and `executor/src/safe_horizon.rs:381-448`.
-/
theorem EventKey.lt_trans {a b c : EventKey} (hab : a < b) (hbc : b < c) : a < c := by
  change EventKey.lexLT a b at hab
  change EventKey.lexLT b c at hbc
  change EventKey.lexLT a c
  unfold EventKey.lexLT at hab hbc ⊢
  omega

namespace EventKey

/--
The all-zero suffix boundary used for Rust's half-open time horizon
`executor/src/safe_horizon.rs:147-155`.
-/
def timeBoundary (timeNs : Nat) : EventKey :=
  { timeNs, phase := 0, originNode := 0, originSeq := 0 }

/--
Strict key comparison with `(H, 0, 0, 0)` is exactly Rust's time-only `< H` drain test at
`executor/src/safe_horizon.rs:392-395`.
-/
theorem lt_timeBoundary_iff (key : EventKey) (timeNs : Nat) :
    key < timeBoundary timeNs ↔ key.timeNs < timeNs := by
  change EventKey.lexLT key (timeBoundary timeNs) ↔ key.timeNs < timeNs
  simp only [EventKey.lexLT, timeBoundary]
  omega

end EventKey

/--
Closed executor event-kind set mirroring `executor/src/event.rs:64-76` (`EventKind`).
-/
inductive EventKind where
  | packetArrival
  | txReady
  | txComplete
  | remoteArrival
  deriving DecidableEq, Repr, Ord

/--
Canonical equal-time phase dispatch mirroring `executor/src/event.rs:78-88` (`event_phase`).
-/
def eventPhase : EventKind → Nat
  | .packetArrival | .remoteArrival => 0
  | .txComplete => 1
  | .txReady => 2

/--
Persistent fixed-width semantic event mirroring `executor/src/event.rs:90-101` (`Event`).
-/
structure Event where
  key : EventKey
  target : NodeId
  kind : EventKind
  payload : PayloadId
  deriving DecidableEq, Repr

/--
Inclusive scenario endpoint predicate mirroring `executor/src/scalar.rs:344-347`; it is
intentionally distinct from a round horizon.
-/
def withinInclusiveStop (stopTimeNs : Nat) (event : Event) : Prop :=
  event.key.timeNs ≤ stopTimeNs

/--
Exclusive endpoint corresponding to Rust's successful `u128(stop_time_ns) + 1` conversion at
`executor/src/safe_horizon.rs:231-236`.
-/
def stopExclusive (stopTimeNs : Nat) : Nat :=
  stopTimeNs + 1

/--
Half-open per-target key-bound predicate mirroring `executor/src/safe_horizon.rs:392-395`.
-/
def belowBound (bounds : NodeId → Nat) (event : Event) : Prop :=
  event.key < EventKey.timeBoundary (bounds event.target)

/--
Decidability of Rust's half-open per-LP drain predicate at
`executor/src/safe_horizon.rs:392-395`.
-/
instance (bounds : NodeId → Nat) (event : Event) :
    Decidable (belowBound bounds event) := by
  unfold belowBound
  infer_instance

/--
The constant time-bound specialization of `belowBound`, matching Rust's global half-open horizon
at `executor/src/safe_horizon.rs:264-271`.
-/
theorem belowConstantTimeBound_iff (horizon : Nat) (event : Event) :
    belowBound (fun _ => horizon) event ↔ event.key.timeNs < horizon := by
  unfold belowBound
  exact EventKey.lt_timeBoundary_iff event.key horizon

end DaysExecutor
