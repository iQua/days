import Std

namespace LeanGuard.P10c.Dcqcn

/-- Executor DCQCN uses one part-per-billion scale and integer bit/s rates.

This is intentionally separate from `LeanGuard.Dcqcn.Semantics`, which specifies the
legacy simulator's floating-point controller. The executor extends that lineage with
an explicit target rate, staged recovery, and byte-triggered increase opportunities.
Every division below is the sole floor operation for its complete rational expression.
-/
def fractionScale : Nat := 1_000_000_000

def maxU64 : Nat := 2 ^ 64 - 1

def stageLength : Nat := 5

inductive Stage
  | fastRecovery
  | additive
  | hyper
  deriving DecidableEq, Repr

structure Config where
  initialRateBps : Nat
  minimumRateBps : Nat
  maximumRateBps : Nat
  additiveRateBps : Nat
  hyperRateBps : Nat
  gPpb : Nat
  decreasePpb : Nat
  cnpIntervalNs : Nat
  controlIntervalNs : Nat
  increaseByteThreshold : Nat
  deriving DecidableEq, Repr

structure State where
  alphaPpb : Nat
  currentRateBps : Nat
  targetRateBps : Nat
  stage : Stage
  stageSteps : Nat
  bytesSinceIncrease : Nat
  cnpSeen : Bool
  lastCnpNs : Option Nat
  nextControlTimeNs : Nat
  deriving DecidableEq, Repr

structure Result where
  state : State
  acted : Bool
  deriving DecidableEq, Repr

def validConfig (config : Config) : Bool :=
  config.minimumRateBps > 0 &&
    config.minimumRateBps ≤ config.initialRateBps &&
    config.initialRateBps ≤ config.maximumRateBps &&
    config.maximumRateBps ≤ maxU64 &&
    config.additiveRateBps ≤ maxU64 &&
    config.hyperRateBps ≤ maxU64 &&
    config.gPpb ≤ fractionScale &&
    config.decreasePpb ≤ fractionScale &&
    config.cnpIntervalNs ≤ maxU64 &&
    config.controlIntervalNs > 0 &&
    config.controlIntervalNs ≤ maxU64 &&
    config.increaseByteThreshold > 0 &&
    config.increaseByteThreshold ≤ maxU64

def validStage (state : State) : Bool :=
  match state.stage with
  | .fastRecovery | .additive => state.stageSteps < stageLength
  | .hyper => state.stageSteps = 0

def validState (config : Config) (state : State) : Bool :=
  state.alphaPpb ≤ fractionScale &&
    config.minimumRateBps ≤ state.currentRateBps &&
    state.currentRateBps ≤ config.maximumRateBps &&
    config.minimumRateBps ≤ state.targetRateBps &&
    state.targetRateBps ≤ config.maximumRateBps &&
    validStage state &&
    state.bytesSinceIncrease ≤ maxU64 &&
    state.nextControlTimeNs ≤ maxU64 &&
    (!state.cnpSeen || state.lastCnpNs.isSome) &&
    state.lastCnpNs.all (· ≤ maxU64)

def initialCompatible (config : Config) (state : State) : Bool :=
  state.alphaPpb = 0 &&
    state.currentRateBps = config.initialRateBps &&
    state.targetRateBps = config.initialRateBps &&
    state.stage = .hyper &&
    state.stageSteps = 0 &&
    state.bytesSinceIncrease = 0 &&
    !state.cnpSeen &&
    state.lastCnpNs.isNone &&
    state.nextControlTimeNs + config.controlIntervalNs ≤ maxU64

def alphaOnCnp (config : Config) (alphaPpb : Nat) : Nat :=
  ((fractionScale - config.gPpb) * alphaPpb + config.gPpb * fractionScale) /
    fractionScale

def alphaOnQuietTick (config : Config) (alphaPpb : Nat) : Nat :=
  ((fractionScale - config.gPpb) * alphaPpb) / fractionScale

def decreasedRate (config : Config) (rateBps alphaPpb : Nat) : Nat :=
  let square := fractionScale * fractionScale
  let factor := square - config.decreasePpb * alphaPpb
  max config.minimumRateBps ((rateBps * factor) / square)

def averageRate (current target : Nat) : Nat :=
  (current + target) / 2

def clampIncrease (maximum current increment : Nat) : Nat :=
  min maximum (current + increment)

def increase (config : Config) (state : State) : State :=
  match state.stage with
  | .fastRecovery =>
      let steps := state.stageSteps + 1
      { state with
        currentRateBps := averageRate state.currentRateBps state.targetRateBps
        stage := if steps = stageLength then .additive else .fastRecovery
        stageSteps := if steps = stageLength then 0 else steps }
  | .additive =>
      let target := clampIncrease config.maximumRateBps state.targetRateBps config.additiveRateBps
      let steps := state.stageSteps + 1
      { state with
        currentRateBps := averageRate state.currentRateBps target
        targetRateBps := target
        stage := if steps = stageLength then .hyper else .additive
        stageSteps := if steps = stageLength then 0 else steps }
  | .hyper =>
      let target := clampIncrease config.maximumRateBps state.targetRateBps config.hyperRateBps
      { state with
        currentRateBps := averageRate state.currentRateBps target
        targetRateBps := target
        stageSteps := 0 }

def acceptsCnp (config : Config) (state : State) (timeNs : Nat) : Bool :=
  match state.lastCnpNs with
  | none => true
  | some last => last + config.cnpIntervalNs ≤ timeNs

def onCnp (config : Config) (state : State) (timeNs : Nat) : Result :=
  if acceptsCnp config state timeNs then
    let alpha := alphaOnCnp config state.alphaPpb
    { state :=
        { state with
          alphaPpb := alpha
          currentRateBps := decreasedRate config state.currentRateBps alpha
          targetRateBps := state.currentRateBps
          stage := .fastRecovery
          stageSteps := 0
          bytesSinceIncrease := 0
          cnpSeen := true
          lastCnpNs := some timeNs }
      acted := true }
  else
    { state, acted := false }

def onControl (config : Config) (state : State) (timeNs : Nat) : Result :=
  let scheduled := { state with nextControlTimeNs := timeNs + config.controlIntervalNs }
  if state.cnpSeen then
    { state := { scheduled with cnpSeen := false }, acted := false }
  else
    let quiet := { scheduled with alphaPpb := alphaOnQuietTick config state.alphaPpb }
    { state := increase config quiet, acted := true }

def onBytes (config : Config) (state : State) (emittedBytes : Nat) : Result :=
  let bytes := state.bytesSinceIncrease + emittedBytes
  if !state.cnpSeen && config.increaseByteThreshold ≤ bytes then
    { state := increase config { state with bytesSinceIncrease := 0 }, acted := true }
  else
    { state := { state with bytesSinceIncrease := bytes }, acted := false }

end LeanGuard.P10c.Dcqcn
