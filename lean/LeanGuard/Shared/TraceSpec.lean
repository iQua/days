import Std

import LeanGuard.Shared.Coverage

namespace LeanGuard.Shared

universe u v

/--
Protocol-independent interface for replaying a canonicalized trace.

The executable checker supplies a row type, replay state, initial state, and
one partial transition function. Protocol modules instantiate this structure
with their existing checker step functions.
-/
structure TraceSpec where
  Row : Type u
  State : Type v
  init : State
  step : State → Row → Except String State

namespace TraceSpec

variable (spec : TraceSpec)

/-- Pure deterministic replay over an already-canonical row sequence. -/
def replayM : spec.State → List spec.Row → Except String spec.State
  | s, [] => .ok s
  | s, r :: rs =>
      match spec.step s r with
      | .error e => .error e
      | .ok s' => replayM s' rs

/-- Declarative replay relation induced by the executable step function. -/
inductive Replay : spec.State → List spec.Row → spec.State → Prop where
  | nil {s} : Replay s [] s
  | cons {s r s' rs s''} :
      spec.step s r = .ok s' →
      Replay s' rs s'' →
      Replay s (r :: rs) s''

/--
Replay with a coverage observer. The observer is deliberately observational:
it sees the pre-state and row, updates coverage, and cannot alter replay state
or acceptance.
-/
def replayWithObserverM
    (observe : CoverageState → spec.State → spec.Row → CoverageState) :
    spec.State → List spec.Row → CoverageState →
      Except (String × CoverageState) (spec.State × CoverageState)
  | s, [], cov => .ok (s, cov)
  | s, r :: rs, cov =>
      let cov' := observe cov s r
      match spec.step s r with
      | .error e => .error (e, cov')
      | .ok s' => replayWithObserverM observe s' rs cov'

theorem replayM_sound {s : spec.State} {rows : List spec.Row} {s' : spec.State} :
    replayM spec s rows = .ok s' → Replay spec s rows s' := by
  induction rows generalizing s with
  | nil =>
      intro h
      simp [replayM] at h
      cases h
      exact Replay.nil
  | cons r rs ih =>
      intro h
      simp [replayM] at h
      cases hstep : spec.step s r with
      | error e =>
          simp [hstep] at h
      | ok s1 =>
          simp [hstep] at h
          exact Replay.cons hstep (ih h)

theorem replayWithObserverM_sound
    (observe : CoverageState → spec.State → spec.Row → CoverageState)
    {s : spec.State} {rows : List spec.Row} {cov : CoverageState}
    {s' : spec.State} {cov' : CoverageState} :
    replayWithObserverM spec observe s rows cov = .ok (s', cov') →
      Replay spec s rows s' := by
  induction rows generalizing s cov with
  | nil =>
      intro h
      simp [replayWithObserverM] at h
      rcases h with ⟨hs, _⟩
      cases hs
      exact Replay.nil
  | cons r rs ih =>
      intro h
      simp [replayWithObserverM] at h
      cases hstep : spec.step s r with
      | error e =>
          simp [hstep] at h
      | ok s1 =>
          simp [hstep] at h
          exact Replay.cons hstep (ih h)

theorem Replay.preserves
    {Inv : spec.State → Prop}
    (hstep : ∀ {s r s'}, Inv s → spec.step s r = .ok s' → Inv s')
    {s : spec.State} {rows : List spec.Row} {s' : spec.State} :
    Replay spec s rows s' → Inv s → Inv s' := by
  intro h
  induction h with
  | nil =>
      intro hs
      exact hs
  | cons hrow htail ih =>
      intro hs
      exact ih (hstep hs hrow)

theorem replayM_preserves
    {Inv : spec.State → Prop}
    (hstep : ∀ {s r s'}, Inv s → spec.step s r = .ok s' → Inv s')
    {s : spec.State} {rows : List spec.Row} {s' : spec.State} :
    replayM spec s rows = .ok s' → Inv s → Inv s' := by
  intro h hs
  exact Replay.preserves spec hstep (replayM_sound spec h) hs

theorem replayWithObserverM_preserves
    (observe : CoverageState → spec.State → spec.Row → CoverageState)
    {Inv : spec.State → Prop}
    (hstep : ∀ {s r s'}, Inv s → spec.step s r = .ok s' → Inv s')
    {s : spec.State} {rows : List spec.Row} {cov : CoverageState}
    {s' : spec.State} {cov' : CoverageState} :
    replayWithObserverM spec observe s rows cov = .ok (s', cov') →
      Inv s → Inv s' := by
  intro h hs
  exact Replay.preserves spec hstep (replayWithObserverM_sound spec observe h) hs

end TraceSpec

end LeanGuard.Shared
