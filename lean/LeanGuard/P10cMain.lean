import LeanGuard.P10c.Semantics

open LeanGuard.P10c.Semantics

def aqmChecks : Bool :=
  let config : Aqm.ThresholdConfig :=
    { unit := .packets, capacity := 4, threshold := 2 }
  let red : Aqm.RedState :=
    { unit := .packets
      capacity := 10
      minThreshold := 1
      maxThreshold := 2
      maxProbabilityNumerator := 1
      maxProbabilityDenominator := 1
      averageScaled := 2 * Aqm.averageScale
      counter := 9
      markEcn := true }
  let (redAfter, redAction) := Aqm.redDecision red 2 0 100
  Aqm.thresholdDecision config 1 0 100 = .mark &&
    Aqm.thresholdDecision config 4 0 100 = .drop &&
    Aqm.thresholdDecision config 0 0 100 = .enqueue &&
    redAction = .mark && redAfter.counter = 0

def rateChecks : Bool :=
  let state : Rate.State :=
    { rateNumeratorBitsPerSecond := 8_000_000_000
      rateDenominator := 1
      pacingIntervalNs := 1_000
      packetSizeBytes := 1_000
      totalBytes := 2_000
      emittedBytes := 0
      creditQuanta := 0 }
  let first := Rate.tick state 1_000 1_000 10_000
  let second := Rate.tick first.state first.nextPacketBytes 2_000 10_000
  let blockedState : Rate.State :=
    { rateNumeratorBitsPerSecond := 1
      rateDenominator := 1
      pacingIntervalNs := 1
      packetSizeBytes := 1
      totalBytes := 1
      emittedBytes := 0
      creditQuanta := 0 }
  let blocked := Rate.tick blockedState 1 1 10
  first.emittedBytes = 1_000 && first.nextStatus = .scheduled &&
    second.emittedBytes = 1_000 && second.nextStatus = .finished &&
    second.nextTimeNs.isNone && blocked.emittedBytes = 0 && blocked.nextStatus = .blocked

def pfcChecks : Bool :=
  let threshold : Pfc.ThresholdState :=
    { xonBytes := 4, xoffBytes := 8, asserted := false }
  let (paused, pause) := Pfc.occupancyTransition threshold 8
  let (resumed, resume) := Pfc.occupancyTransition paused 4
  let earlyResume := Pfc.applyControl {} 3 .resume
  let firstPause := Pfc.applyControl earlyResume 3 .pause
  let duplicatePause := Pfc.applyControl firstPause 3 .pause
  let finalResume := Pfc.applyControl duplicatePause 3 .resume
  pause = some .pause && resume = some .resume && !resumed.asserted &&
    Pfc.eligible earlyResume 3 && !Pfc.eligible firstPause 3 &&
    !Pfc.eligible duplicatePause 3 && Pfc.eligible finalResume 3

def drrChecks : Bool :=
  let initial : Drr.State :=
    { classCount := 2
      quanta := Std.HashMap.emptyWithCapacity.insert 0 100 |>.insert 1 200 }
  let initial := Drr.enqueue initial { id := 10, flow := 0, sizeBytes := 150 }
  match Drr.schedule 1 3 initial, Drr.schedule 1 4 initial with
  | .error _, .ok (state, packet) =>
      packet.id = 10 && state.currentClass = 0 && Drr.deficit state 0 = 50
  | _, _ => false

def wrrChecks : Bool :=
  let initial : Wrr.State :=
    { classCount := 2
      weights := Std.HashMap.emptyWithCapacity.insert 0 1 |>.insert 1 2 }
  let initial := Wrr.enqueue initial { id := 10, flow := 0, sizeBytes := 100 }
  let initial := Wrr.enqueue initial { id := 11, flow := 0, sizeBytes := 100 }
  let initial := Wrr.enqueue initial { id := 20, flow := 1, sizeBytes := 100 }
  match Wrr.schedule 1 initial with
  | .error _ => false
  | .ok (afterFirst, first) =>
      match Wrr.schedule 2 afterFirst with
      | .error _ => false
      | .ok (afterSecond, second) =>
          match Wrr.schedule 3 afterSecond with
          | .error _ => false
          | .ok (_, third) => first.id = 10 && second.id = 20 && third.id = 11

def main : IO UInt32 := do
  let checks :=
    [("aqm", aqmChecks), ("rate", rateChecks), ("pfc", pfcChecks),
      ("drr", drrChecks), ("wrr", wrrChecks)]
  let failures := checks.filter (fun check => !check.2)
  for (name, passed) in checks do
    IO.println s!"{if passed then "ok" else "FAIL"}: {name}"
  if failures.isEmpty then
    IO.println s!"P10c exact mechanism checks: {checks.length}"
    pure 0
  else
    pure 1
