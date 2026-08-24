import DaysExecutor.DecisionPointWitnesses
import DaysExecutor.EagerSelectionWitness
import DaysExecutor.WitnessChecks

namespace DaysExecutor

theorem committedServiceMultiplicityCounterexamples_proved :
    CommittedServiceMultiplicityCounterexamples := by
  exact ⟨committedServiceDuplicatorCheck_true,
    committedServicePartialEraserCheck_true⟩

theorem f4DecisionPointScope_proved
    (image : SimulationImage State)
    (transition : TransitionRelation State) :
    F4DecisionPointScope image transition := by
  exact
    ⟨fun _ =>
      ⟨f2RoundSerializability_core image transition,
        f3RunComposition_proved image transition⟩,
      reachableEagerSelectionCountermodel_proved,
      privatePayloadSmugglerCountermodel_structural,
      committedServiceErasurePreemptorCountermodel_structural,
      committedServiceMultiplicityCounterexamples_proved⟩

end DaysExecutor
