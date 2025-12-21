import Std

import LeanGuard.Shared.Numeric

namespace LeanGuard.Dcqcn.Semantics

open LeanGuard.Shared

/-- Parameters expected to remain constant for a given source endpoint. -/
structure SrcParams where
  flowId : Nat
  cnpIntervalNs : Nat
  gPpb : Nat
  miPpb : Nat
  initRateBps : Nat
  minRateBps : Nat
  maxRateBps : Nat
  aiRateBps : Nat
  haiRateBps : Nat
deriving DecidableEq, Repr

structure SrcState where
  p : SrcParams
  alpha : Float := 0.0
  rateBps : Float
  cnpSeen : Bool := false
  lastCnpNs : Option Nat := none
  lastTimeNs : Option Nat := none
deriving Repr

def okParams (p : SrcParams) : Bool :=
  p.gPpb ≤ PPB
    && p.miPpb ≤ PPB
    && p.minRateBps ≤ p.initRateBps
    && p.initRateBps ≤ p.maxRateBps

def fmax (a b : Float) : Float :=
  if a < b then b else a

def fmin (a b : Float) : Float :=
  if a < b then a else b

def initialSrcState (p : SrcParams) : SrcState :=
  { p := p
    alpha := 0.0
    rateBps := Float.ofNat p.initRateBps
    cnpSeen := false
    lastCnpNs := none
    lastTimeNs := none }

/-- Apply DCQCN source updates after receiving a CNP at time `t` (ns). -/
def srcAfterCnp (s : SrcState) (t : Nat) : SrcState :=
  let p := s.p
  let applied :=
    match s.lastCnpNs with
    | none => true
    | some last => last + p.cnpIntervalNs ≤ t
  if applied then
    let g := ppbToFloat p.gPpb
    let mi := ppbToFloat p.miPpb
    let α := (1.0 - g) * s.alpha + g
    let decrease := 1.0 - mi * α
    let decreasedRate := s.rateBps * decrease
    let r := fmax decreasedRate (Float.ofNat p.minRateBps)
    { s with
      alpha := α
      rateBps := r
      cnpSeen := true
      lastCnpNs := some t
      lastTimeNs := some t }
  else
    { s with lastTimeNs := some t }

/-- Apply DCQCN source updates after a timer tick at time `t` (ns). -/
def srcAfterTimer (s : SrcState) (t : Nat) : SrcState :=
  let p := s.p
  let g := ppbToFloat p.gPpb
  if s.cnpSeen then
    { s with cnpSeen := false, lastTimeNs := some t }
  else
    let α := (1.0 - g) * s.alpha
    let inc := if α < 0.1 then Float.ofNat p.haiRateBps else Float.ofNat p.aiRateBps
    let r := fmin (s.rateBps + inc) (Float.ofNat p.maxRateBps)
    { s with alpha := α, rateBps := r, cnpSeen := false, lastTimeNs := some t }

end LeanGuard.Dcqcn.Semantics

