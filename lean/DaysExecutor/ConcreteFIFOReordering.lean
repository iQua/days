import DaysExecutor.EagerSelectionWitness
import DaysExecutor.IntraRoundReordering

namespace DaysExecutor

theorem tinyQueueCanonicalRoundEmissions_payload :
    ∀ parent child,
      RecordedEmissionEdge queueCounterexampleRoundTwoEmissions parent child →
        parent.payload = child.payload := by
  intro parent child hedge
  simp [RecordedEmissionEdge, queueCounterexampleRoundTwoEmissions] at hedge
  rcases hedge with ⟨rfl, rfl⟩ | ⟨rfl, rfl⟩ <;>
    rfl

/--
The canonical FIFO transition discharges the scoped F5 commutation premise through the formal
materialized-step diamond for its actual service-start emission trace.
-/
theorem tinyQueue_independentStepsCommute :
    IndependentStepsCommute
      queueCounterexampleImage
      (tinyQueueTransition false)
      queueCounterexampleRoundTwoEmissions :=
  acceptedModel_independentStepsCommute
    queueCounterexampleImage
    (tinyQueueTransition false)
    queueCounterexampleRoundTwoEmissions
    (tinyQueue_accepted_model false)
    tinyQueueCanonicalRoundEmissions_payload

/--
License in hand: every conflict-respecting permutation of a canonical FIFO round with the fixture's
actual emission delta has an execution and preserves the complete normalized machine result.
-/
theorem tinyQueue_conflictRespectingReorderings_preserve_results
    (bounds : BoundFamily)
    (cut : Event → Prop)
    (start : RoundState TinyQueueStateFamily)
    (drainedEvents : List Event)
    (roundFinish : RoundState TinyQueueStateFamily)
    (canonicalOrder : List Event)
    (canonicalFinish : MachineState TinyQueueStateFamily)
    (candidateOrder : List Event)
    (hround :
      SafeHorizonRound queueCounterexampleImage
        (tinyQueueTransition false) bounds cut
        start drainedEvents roundFinish)
    (hcanonical :
      CanonicalSerialRestricted queueCounterexampleImage
        (tinyQueueTransition false) cut start.machine
        canonicalOrder canonicalFinish)
    (hdelta :
      RoundEmissionDelta start.machine canonicalFinish
        queueCounterexampleRoundTwoEmissions)
    (hmembership :
      ∀ event, event ∈ canonicalOrder ↔ event ∈ drainedEvents)
    (hpreserves :
      PreservesRequiredIntraRoundOrder
        queueCounterexampleRoundTwoEmissions
        drainedEvents candidateOrder) :
    ∃ candidateFinish,
      ExecutionInOrder queueCounterexampleImage
        (tinyQueueTransition false) start.machine
        candidateOrder candidateFinish ∧
      SameMachineResult queueCounterexampleImage
        canonicalFinish candidateFinish := by
  exact
    (f5IntraRoundReordering_proved
      queueCounterexampleImage (tinyQueueTransition false)).1
      (tinyQueue_accepted_model false)
      bounds cut start drainedEvents roundFinish
      canonicalOrder canonicalFinish candidateOrder
      queueCounterexampleRoundTwoEmissions
      hround hcanonical hdelta tinyQueue_independentStepsCommute
      hmembership hpreserves

end DaysExecutor
