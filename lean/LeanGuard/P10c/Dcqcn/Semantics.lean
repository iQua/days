import Std

namespace LeanGuard.P10c.Dcqcn

/-! The Mellanox-form DCQCN reaction point of the executor (P16), the controller of HPCC's ns-3
(`CC_MODE 1`) and SimAI: `days-gpu/evidence/P16/simai-dcqcn-spec.md` §7, pinned record schema
`days-gpu/plans/briefs/p16/dcqcn-schema.md`.

The eager machine has three periodic timers: the alpha update every `alphaIntervalNs` from the
first feedback, the rate-decrease check on the grid `t0 + D + 1 + m D`, and the rate-increase timer
every `increaseIntervalNs` from the last cut. At equal time a feedback arrival precedes the timers,
and the timers fire in the order alpha, increase, decrease. The executor applies them lazily; the
transitions below are the eager machine's instants applied in that order before an exclusive bound,
which is what each transition record must show.

The arithmetic is exact `Nat` arithmetic in the direct rational forms (alpha in Q63, `2^63` is one):
it shares no algebraic identity with the executor's u64 code. This is intentionally separate from
`LeanGuard.Dcqcn.Semantics`, which specifies the legacy simulator's floating-point controller. -/

def maxU64 : Nat := 2 ^ 64 - 1

/-- `alpha = 1` in Q63. -/
def alphaOne : Nat := 2 ^ 63

structure Config where
  initialRateBps : Nat
  minimumRateBps : Nat
  maximumRateBps : Nat
  additiveRateBps : Nat
  hyperRateBps : Nat
  gQ63 : Nat
  alphaIntervalNs : Nat
  decreaseIntervalNs : Nat
  increaseIntervalNs : Nat
  fastRecoverySteps : Nat
  clampTargetRate : Bool
  deriving DecidableEq, Repr

structure State where
  alphaQ63 : Nat
  currentRateBps : Nat
  targetRateBps : Nat
  nextAlphaNs : Nat
  nextDecreaseNs : Nat
  nextIncreaseNs : Nat
  stage : Nat
  armed : Bool
  alphaPending : Bool
  decreasePending : Bool
  increaseArmed : Bool
  deriving DecidableEq, Repr

/-- What one transition applied (the record's diagnostic counts). -/
structure Counts where
  alphaTicks : Nat := 0
  increaseFires : Nat := 0
  decreaseCuts : Nat := 0
  deriving DecidableEq, Repr

def Counts.add (left right : Counts) : Counts :=
  { alphaTicks := left.alphaTicks + right.alphaTicks
    increaseFires := left.increaseFires + right.increaseFires
    decreaseCuts := left.decreaseCuts + right.decreaseCuts }

/-- `t + interval`, where an instant at or beyond `u64::MAX` never fires. -/
def later (time interval : Nat) : Nat := min maxU64 (time + interval)

/-- The lowest rate a controller reaches: `2 * floor((minimum - 1) / 2)`. -/
def rateFloor (config : Config) : Nat := 2 * ((config.minimumRateBps - 1) / 2)

def validConfig (config : Config) : Bool :=
  3 ≤ config.minimumRateBps &&
    config.minimumRateBps ≤ config.initialRateBps &&
    config.initialRateBps ≤ config.maximumRateBps &&
    config.maximumRateBps ≤ maxU64 &&
    config.additiveRateBps ≤ maxU64 &&
    config.hyperRateBps ≤ maxU64 &&
    config.gQ63 ≤ alphaOne &&
    0 < config.alphaIntervalNs && config.alphaIntervalNs ≤ maxU64 &&
    0 < config.decreaseIntervalNs && config.decreaseIntervalNs ≤ maxU64 &&
    0 < config.increaseIntervalNs && config.increaseIntervalNs ≤ maxU64 &&
    config.fastRecoverySteps < 2 ^ 32 - 1

def pristine (config : Config) : State :=
  { alphaQ63 := alphaOne
    currentRateBps := config.initialRateBps
    targetRateBps := config.initialRateBps
    nextAlphaNs := 0
    nextDecreaseNs := 0
    nextIncreaseNs := 0
    stage := 0
    armed := false
    alphaPending := false
    decreasePending := false
    increaseArmed := false }

def validState (config : Config) (state : State) : Bool :=
  state.alphaQ63 ≤ alphaOne &&
    state.stage ≤ config.fastRecoverySteps + 1 &&
    rateFloor config ≤ state.currentRateBps &&
    state.currentRateBps ≤ config.maximumRateBps &&
    rateFloor config ≤ state.targetRateBps &&
    state.targetRateBps ≤ config.maximumRateBps &&
    state.nextAlphaNs ≤ maxU64 &&
    state.nextDecreaseNs ≤ maxU64 &&
    state.nextIncreaseNs ≤ maxU64 &&
    (state.armed || state == pristine config)

/-- One alpha update: `floor(((S - g) alpha + [pending] g S) / S)`. -/
def alphaStep (config : Config) (alpha : Nat) (pending : Bool) : Nat :=
  ((alphaOne - config.gQ63) * alpha + (if pending then config.gQ63 * alphaOne else 0)) / alphaOne

/-- `ticks` pure decays; decay is monotone and floors to zero, after which it is the identity. -/
def decay (config : Config) : Nat → Nat → Nat
  | 0, alpha => alpha
  | ticks + 1, alpha => if alpha = 0 then 0 else decay config ticks (alphaStep config alpha false)

/-- Every alpha tick with time at most `time`. -/
def alphaThrough (config : Config) (state : State) (time : Nat) : State × Nat :=
  if time < state.nextAlphaNs || state.nextAlphaNs = maxU64 then (state, 0)
  else
    let ticks := (time - state.nextAlphaNs) / config.alphaIntervalNs + 1
    let first := alphaStep config state.alphaQ63 state.alphaPending
    ({ state with
        alphaQ63 := decay config (ticks - 1) first
        alphaPending := false
        nextAlphaNs := min maxU64 (state.nextAlphaNs + ticks * config.alphaIntervalNs) },
      ticks)

/-- The rate-decrease check at `time`, with a decrease pending: `floor(R (2^64 - alpha) / 2^64)`. -/
def decreaseCheck (config : Config) (state : State) (time : Nat) : State :=
  let target :=
    if config.clampTargetRate || state.stage ≠ 0 then state.currentRateBps else state.targetRateBps
  let cut := state.currentRateBps * (2 ^ 64 - state.alphaQ63) / 2 ^ 64
  { state with
    nextDecreaseNs := later time config.decreaseIntervalNs
    targetRateBps := target
    currentRateBps := max config.minimumRateBps cut
    stage := 0
    decreasePending := false
    increaseArmed := true
    nextIncreaseNs := later time config.increaseIntervalNs }

/-- The rate-increase timer at `time`: fast recovery, one additive step at `F`, hyper beyond. -/
def increaseFire (config : Config) (state : State) (time : Nat) : State :=
  let steps := config.fastRecoverySteps
  let target :=
    if state.stage = steps then
      min config.maximumRateBps (state.targetRateBps + config.additiveRateBps)
    else if steps < state.stage then
      min config.maximumRateBps (state.targetRateBps + config.hyperRateBps)
    else state.targetRateBps
  { state with
    nextIncreaseNs := later time config.increaseIntervalNs
    targetRateBps := target
    currentRateBps := state.currentRateBps / 2 + target / 2
    stage := if state.stage ≤ steps then state.stage + 1 else state.stage }

def increaseDue (state : State) : Nat :=
  if state.increaseArmed then state.nextIncreaseNs else maxU64

def decreaseDue (state : State) : Nat :=
  if state.decreasePending then state.nextDecreaseNs else maxU64

/-- Every rate-increase and rate-decrease instant before `bound`, increase first at equal time,
with the alpha ticks each cut reads. An instant at `maxU64` never fires (the executor's bounds never
exceed `u64::MAX`), and a zero increase interval, which `validConfig` rejects, applies nothing.

It terminates: a decrease check clears `decreasePending`, which nothing here sets again, so it
fires at most once; an increase fire moves its instant strictly later while it stays below
`bound`. -/
def materialize (config : Config) (state : State) (bound : Nat) (counts : Counts := {}) :
    State × Counts :=
  if _hstop : bound ≤ min (increaseDue state) (decreaseDue state) ∨
      maxU64 ≤ min (increaseDue state) (decreaseDue state) ∨ config.increaseIntervalNs = 0 then
    (state, counts)
  else if _hinc : increaseDue state ≤ decreaseDue state then
    materialize config (increaseFire config state (increaseDue state)) bound
      { counts with increaseFires := counts.increaseFires + 1 }
  else
    let ticked := alphaThrough config state (decreaseDue state)
    materialize config (decreaseCheck config ticked.1 (decreaseDue state)) bound
      { counts with
        alphaTicks := counts.alphaTicks + ticked.2
        decreaseCuts := counts.decreaseCuts + 1 }
termination_by (if state.decreasePending then bound + 1 else 0) + (bound - increaseDue state)
decreasing_by
  · -- An increase fire: the increase timer is armed below `maxU64` and moves strictly later.
    have harmed : state.increaseArmed = true := by
      cases h : state.increaseArmed
      · simp [increaseDue, h] at _hstop _hinc; omega
      · rfl
    simp only [increaseDue, harmed, if_true] at _hstop _hinc ⊢
    simp only [increaseFire, harmed, if_true, later]
    split <;> omega
  · -- A decrease check: it was pending, and it clears the flag.
    have hpending : state.decreasePending = true := by
      cases h : state.decreasePending
      · simp [decreaseDue, h] at _hstop _hinc
        omega
      · rfl
    simp only [hpending, if_true, decreaseCheck, Bool.false_eq_true, if_false]
    omega

/-- The first instant of the decrease grid at or after `time`. -/
def firstDecreaseAtOrAfter (config : Config) (state : State) (time : Nat) : Nat :=
  let anchor := state.nextDecreaseNs
  if time ≤ anchor || anchor = maxU64 then anchor
  else
    let interval := config.decreaseIntervalNs
    let steps := (time - anchor + interval - 1) / interval
    min maxU64 (anchor + steps * interval)

/-- A feedback (an echoing ACK or NACK, or a CNP) at `time`, a phase-0 transition. -/
def onFeedback (config : Config) (state : State) (time : Nat) : State × Counts :=
  if !state.armed then
    ({ state with
        armed := true
        alphaQ63 := alphaOne
        alphaPending := false
        decreasePending := true
        nextAlphaNs := later time config.alphaIntervalNs
        nextDecreaseNs := later (later time config.decreaseIntervalNs) 1 },
      {})
  else
    let (materialized, counts) := materialize config state time
    let (ticked, ticks) :=
      if time = 0 then (materialized, 0) else alphaThrough config materialized (time - 1)
    let pending := { ticked with alphaPending := true }
    let opened :=
      if pending.decreasePending then pending
      else
        { pending with
          decreasePending := true
          nextDecreaseNs := firstDecreaseAtOrAfter config pending time }
    (opened, { counts with alphaTicks := counts.alphaTicks + ticks })

/-- The freeze at the transition that completes the flow: every instant before `bound`, alpha
included, and the decrease grid at its first instant at or after the bound. -/
def settle (config : Config) (state : State) (bound : Nat) : State × Counts :=
  if !state.armed then (state, {})
  else
    let (materialized, counts) := materialize config state bound
    let (ticked, ticks) :=
      if bound = 0 then (materialized, 0) else alphaThrough config materialized (bound - 1)
    ({ ticked with nextDecreaseNs := firstDecreaseAtOrAfter config ticked bound },
      { counts with alphaTicks := counts.alphaTicks + ticks })

end LeanGuard.P10c.Dcqcn
