import Std

import LeanGuard.Shared.Numeric

namespace LeanGuard.Cubic.Semantics

open LeanGuard.Shared

structure Params where
  flowId : Nat
  mssBytes : Nat
  betaPpb : Nat
  cPpb : Nat
  tcpFriendly : Bool
  fastConvergence : Bool
  initCwndBytes : Nat
  initSsthreshBytes : Nat
deriving DecidableEq, Repr

structure State where
  p : Params
  cwndSegs : Float
  ssthreshSegs : Float
  wMaxSegs : Float
  wLastMaxSegs : Float
  srtt : Float := 0.0
  epochStartNs : Option Nat := none
  kZero : Bool := true
  lastTimeNs : Option Nat := none
deriving Repr

def okParams (p : Params) : Bool :=
  p.mssBytes > 0
    && p.betaPpb > 0
    && p.betaPpb < PPB
    && p.cPpb > 0
    && p.initCwndBytes > 0
    && p.initSsthreshBytes > 0

def updateSrtt (srtt rtt : Float) : Float :=
  if srtt == 0.0 then
    rtt
  else
    (1.0 - 0.125) * srtt + 0.125 * rtt

def bytesToSegs (bytes mss : Nat) : Float :=
  (Float.ofNat bytes) / (Float.ofNat mss)

def encodeBytes (segs : Float) (mss : Nat) : Nat :=
  toNatFloor (segs * Float.ofNat mss)

def maxCwndSegs : Float := 2.0e6

def cubicK (st : State) (beta c : Float) : Float :=
  if st.kZero || decide (st.wMaxSegs <= 0.0) then
    0.0
  else
    Float.cbrt (st.wMaxSegs * (1.0 - beta) / c)

def cubicWindow (st : State) (beta c : Float) (t : Float) : Float :=
  c * (Float.pow (t - cubicK st beta c) 3.0) + st.wMaxSegs

def clampCwnd (cwnd : Float) : Float :=
  let c := if cwnd < 1.0 then 1.0 else cwnd
  if c > maxCwndSegs then maxCwndSegs else c

/-- Apply CUBIC congestion-avoidance update at time `nowNs` with RTT `rttNs` (ns). -/
def cubicUpdate (st : State) (nowNs rttNs : Nat) : State :=
  let p := st.p
  let beta := ppbToFloat p.betaPpb
  let c := ppbToFloat p.cPpb
  let rtt := nsToSeconds rttNs
  let srtt := if st.srtt > 0.0 then st.srtt else rtt
  let epochStartNs := st.epochStartNs.getD nowNs
  let wMaxSegs := if st.wMaxSegs == 0.0 then st.cwndSegs else st.wMaxSegs
  let kZero := if st.wMaxSegs == 0.0 then true else st.kZero
  let t := nsToSeconds (nowNs - epochStartNs)
  let st' := { st with wMaxSegs := wMaxSegs, kZero := kZero }
  let wCubicT := cubicWindow st' beta c t
  let wEst :=
    wMaxSegs * beta + (3.0 * (1.0 - beta) / (1.0 + beta)) * (t / srtt)
  let cwndNext :=
    if p.tcpFriendly && wCubicT < wEst then
      wEst
    else
      let wTarget := cubicWindow st' beta c (t + srtt)
      let denom := if st.cwndSegs < 1.0 then 1.0 else st.cwndSegs
      st.cwndSegs + (wTarget - st.cwndSegs) / denom
  { st' with
    cwndSegs := clampCwnd cwndNext
    srtt := srtt
    epochStartNs := some epochStartNs }

def onCongestion (st : State) (nowNs : Nat) : State :=
  let p := st.p
  let beta := ppbToFloat p.betaPpb
  let wMaxCur := st.cwndSegs
  let (wMax', wLast') :=
    if p.fastConvergence && st.wLastMaxSegs > 0.0 && wMaxCur < st.wLastMaxSegs then
      (wMaxCur * (1.0 + beta) / 2.0, wMaxCur)
    else
      (wMaxCur, wMaxCur)
  let reduced := wMaxCur * beta
  let ssthresh := if reduced < 2.0 then 2.0 else reduced
  let cwnd' := clampCwnd reduced
  { st with
    cwndSegs := cwnd'
    ssthreshSegs := ssthresh
    wMaxSegs := wMax'
    wLastMaxSegs := wLast'
    srtt := st.srtt
    epochStartNs := some nowNs
    kZero := false }

def onTimeout (st : State) : State :=
  let p := st.p
  let beta := ppbToFloat p.betaPpb
  let reduced := st.cwndSegs * beta
  let ssthresh := if reduced < 2.0 then 2.0 else reduced
  { st with
    cwndSegs := 1.0
    ssthreshSegs := ssthresh
    wMaxSegs := 0.0
    wLastMaxSegs := 0.0
    epochStartNs := none
    kZero := true }

end LeanGuard.Cubic.Semantics

