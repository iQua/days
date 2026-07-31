import DaysExecutor.RunComposition

namespace DaysExecutor

set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

private theorem privatePayload_actualStart :
    ActualServiceStartDiscipline tinyQueuePrivatePayloadTransition := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, _, rfl⟩
  unfold tinyQueuePrivatePayloadResult tinyQueueTransitionResult
  cases hkind : event.kind <;>
    simp [hkind, tinyQueueSelectedPacket, tinyQueueDecisions]
  case txReady =>
    cases hcommitted : state.committedService with
    | nil =>
        cases hqueue : state.serviceQueue <;>
          simp [hcommitted, hqueue, tinyQueueSelectedPacket,
            tinyQueueDecisions]
    | cons head tail =>
        simp [hcommitted, tinyQueueSelectedPacket, tinyQueueDecisions]

private theorem erasure_actualStart :
    ActualServiceStartDiscipline tinyQueueErasurePreemptingTransition := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, _, rfl⟩
  unfold tinyQueueErasurePreemptingResult tinyQueueTransitionResult
  cases hkind : event.kind <;>
    simp [hkind, tinyQueueSelectedPacket, tinyQueueDecisions]
  case txReady =>
    cases hcommitted : state.committedService with
    | nil =>
        cases hqueue : state.serviceQueue <;>
          simp [hcommitted, hqueue, tinyQueueSelectedPacket,
            tinyQueueDecisions]
    | cons head tail =>
        simp [hcommitted, tinyQueueSelectedPacket, tinyQueueDecisions]

private theorem erase_ne_append_singleton
    (payload packet : PayloadId)
    (values : List PayloadId)
    (hmem : payload ∈ values) :
    values.erase payload ≠ values ++ [packet] := by
  intro heq
  have hlength := congrArg List.length heq
  simp [List.length_erase_of_mem hmem] at hlength

@[simp] private theorem tinyQueueArrivalState_committedService
    (eager : Bool)
    (state : RoleState TinyQueueStateFamily kind)
    (packet : PayloadId) :
  (tinyQueueArrivalState eager state packet).committedService =
      state.committedService := by
  unfold tinyQueueArrivalState
  split <;> dsimp <;> split <;> rfl

private theorem privatePayload_decisionTrace :
    ServiceDecisionTraceComplete tinyQueuePrivatePayloadTransition := by
  intro node event state result htransition packet
  rcases htransition with ⟨_, _, _, hcompletion, rfl⟩
  unfold SelectionIntroduced tinyQueuePrivatePayloadResult
    tinyQueueTransitionResult
  cases hkind : event.kind
  · simp [hkind, tinyQueueDecisions]
  · cases hcommitted : state.committedService with
    | nil =>
        cases hqueue : state.serviceQueue with
        | nil =>
            simp [hkind, hcommitted, hqueue, tinyQueueSelectedPacket,
              tinyQueueReadyState, tinyQueueDecisions]
        | cons selected rest =>
            simp [hkind, hcommitted, hqueue, tinyQueueSelectedPacket,
              tinyQueueReadyState, tinyQueueDecisions]
    | cons head tail =>
        simp [hkind, hcommitted, tinyQueueSelectedPacket,
          tinyQueueReadyState, tinyQueueDecisions]
  · have hmem := hcompletion hkind
    constructor
    · intro hselection
      exact False.elim
        (erase_ne_append_singleton event.payload packet
          state.committedService hmem hselection)
    · rintro ⟨decision, hdecision, _⟩
      simpa [hkind, tinyQueueDecisions] using hdecision
  · constructor
    · intro hselection
      have hlength := congrArg List.length hselection
      simp at hlength
    · rintro ⟨decision, hdecision, _⟩
      simpa [hkind, tinyQueueDecisions] using hdecision
  · constructor
    · intro hselection
      have hlength := congrArg List.length hselection
      simp at hlength
    · rintro ⟨decision, hdecision, _⟩
      simpa [hkind, tinyQueueDecisions] using hdecision

private theorem erasure_decisionTrace :
    ServiceDecisionTraceComplete tinyQueueErasurePreemptingTransition := by
  intro node event state result htransition packet
  rcases htransition with ⟨_, _, _, hcompletion, rfl⟩
  unfold SelectionIntroduced tinyQueueErasurePreemptingResult
    tinyQueueTransitionResult
  cases hkind : event.kind
  · simp [hkind, tinyQueueDecisions]
  · cases hcommitted : state.committedService with
    | nil =>
        cases hqueue : state.serviceQueue with
        | nil =>
            simp [hkind, hcommitted, hqueue, tinyQueueSelectedPacket,
              tinyQueueReadyState, tinyQueueDecisions]
        | cons selected rest =>
            simp [hkind, hcommitted, hqueue, tinyQueueSelectedPacket,
              tinyQueueReadyState, tinyQueueDecisions]
    | cons head tail =>
        simp [hkind, hcommitted, tinyQueueSelectedPacket,
          tinyQueueReadyState, tinyQueueDecisions]
  · have hmem := hcompletion hkind
    constructor
    · intro hselection
      exact False.elim
        (erase_ne_append_singleton event.payload packet
          state.committedService hmem hselection)
    · rintro ⟨decision, hdecision, _⟩
      simpa [hkind, tinyQueueDecisions] using hdecision
  · constructor
    · intro hselection
      have hlength := congrArg List.length hselection
      simp at hlength
    · rintro ⟨decision, hdecision, _⟩
      simpa [hkind, tinyQueueDecisions] using hdecision
  · constructor
    · intro hselection
      have hlength := congrArg List.length hselection
      simp at hlength
    · rintro ⟨decision, hdecision, _⟩
      simpa [hkind, tinyQueueDecisions] using hdecision

private theorem privatePayload_nonPreemptive :
    CommittedServiceNonPreemptive tinyQueuePrivatePayloadTransition := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, hcompletion, rfl⟩
  unfold CommittedServiceTransitionValid tinyQueuePrivatePayloadResult
    tinyQueueTransitionResult
  cases hkind : event.kind
  · simp [hkind, tinyQueueDecisions]
  · cases hcommitted : state.committedService with
    | nil =>
        cases hqueue : state.serviceQueue with
        | nil =>
            simp [hkind, hcommitted, hqueue, tinyQueueSelectedPacket,
              tinyQueueReadyState, tinyQueueDecisions]
        | cons selected rest =>
            simp [hkind, hcommitted, hqueue, tinyQueueSelectedPacket,
              tinyQueueReadyState, tinyQueueDecisions]
    | cons head tail =>
        simp [hkind, hcommitted, tinyQueueSelectedPacket,
          tinyQueueReadyState, tinyQueueDecisions]
  · exact ⟨fun hnodup => hnodup.erase _, hcompletion hkind, rfl⟩
  · simp [hkind, tinyQueueDecisions]
  · simp [hkind, tinyQueueDecisions]

private def privatePayloadWitnessNode : NodeDescriptor :=
  { id := 1, kind := .switch, stateSlot := 0 }

private def privatePayloadWitnessState :
    RoleState TinyQueueStateFamily .switch :=
  { privateState :=
      { capacity := 1
        hiddenReservation := some 21
        accepted := []
        dropped := [] }
    serviceQueue := [12]
    committedService := [] }

private def privatePayloadAlternateState :
    RoleState TinyQueueStateFamily .switch :=
  { privatePayloadWitnessState with
    privateState :=
      { privatePayloadWitnessState.privateState with
        hiddenReservation := none } }

private theorem privatePayload_witnessTransition :
    tinyQueuePrivatePayloadTransition
      privatePayloadWitnessNode
      queueCounterexampleReady
      privatePayloadWitnessState
      (tinyQueuePrivatePayloadResult
        privatePayloadWitnessNode queueCounterexampleReady
        privatePayloadWitnessState) := by
  simp [tinyQueuePrivatePayloadTransition, privatePayloadWitnessNode,
    queueCounterexampleReady, queueCounterexampleImage, roleSupports]

private theorem privatePayload_alternateTransition
    (result : TransitionResult TinyQueueStateFamily .switch)
    (htransition :
      tinyQueuePrivatePayloadTransition
        privatePayloadWitnessNode
        queueCounterexampleReady
        privatePayloadAlternateState
        result) :
    result =
      tinyQueuePrivatePayloadResult
        privatePayloadWitnessNode queueCounterexampleReady
        privatePayloadAlternateState := by
  exact htransition.2.2.2.2

private theorem privatePayload_emissionsMismatch :
    ¬ ServiceDecisionEmissionsMatch tinyQueuePrivatePayloadTransition := by
  intro hmatch
  have hwitness :=
    hmatch privatePayloadWitnessNode queueCounterexampleReady
      privatePayloadWitnessState
      (tinyQueuePrivatePayloadResult
        privatePayloadWitnessNode queueCounterexampleReady
        privatePayloadWitnessState)
      privatePayload_witnessTransition
  exact (by decide : ¬ ServiceDecisionChildrenMatch
    (tinyQueuePrivatePayloadResult
      privatePayloadWitnessNode queueCounterexampleReady
      privatePayloadWitnessState)) hwitness

private theorem privatePayload_privateRelevant :
    ¬ TxReadySelectionPrivateIrrelevant tinyQueuePrivatePayloadTransition := by
  intro hirrelevant
  obtain ⟨alternateResult, halternate, hsame⟩ :=
    hirrelevant privatePayloadWitnessNode queueCounterexampleReady
      privatePayloadWitnessState
      (tinyQueuePrivatePayloadResult
        privatePayloadWitnessNode queueCounterexampleReady
        privatePayloadWitnessState)
      privatePayload_witnessTransition rfl
      privatePayloadAlternateState.privateState
  have halternateState :
      ({ privateState := privatePayloadAlternateState.privateState
         serviceQueue := privatePayloadWitnessState.serviceQueue
         committedService := privatePayloadWitnessState.committedService } :
        RoleState TinyQueueStateFamily .switch) =
        privatePayloadAlternateState := by
    rfl
  rw [halternateState] at halternate
  rw [privatePayload_alternateTransition alternateResult halternate] at hsame
  exact (by decide : ¬ SameServiceSelectionResult
    (tinyQueuePrivatePayloadResult
      privatePayloadWitnessNode queueCounterexampleReady
      privatePayloadWitnessState)
    (tinyQueuePrivatePayloadResult
      privatePayloadWitnessNode queueCounterexampleReady
      privatePayloadAlternateState)) hsame

private theorem privatePayload_incomplete :
    ¬ CompleteActualServiceStartDiscipline
      tinyQueuePrivatePayloadTransition := by
  intro hcomplete
  exact privatePayload_emissionsMismatch hcomplete.2.2.2.1

private theorem erasure_privateIrrelevant :
    TxReadySelectionPrivateIrrelevant
      tinyQueueErasurePreemptingTransition := by
  intro node event state result htransition hready privateState
  rcases htransition with
    ⟨hnode, htarget, hrole, hcompletion, rfl⟩
  let alternateState : RoleState TinyQueueStateFamily node.kind :=
    { privateState
      serviceQueue := state.serviceQueue
      committedService := state.committedService }
  let alternateResult :=
    tinyQueueErasurePreemptingResult node event alternateState
  refine ⟨alternateResult, ?_, ?_⟩
  · exact ⟨hnode, htarget, hrole,
      fun hcomplete => False.elim (by
        rw [hready] at hcomplete
        cases hcomplete), rfl⟩
  · unfold SameServiceSelectionResult alternateResult alternateState
      tinyQueueErasurePreemptingResult tinyQueueTransitionResult
    simp only [hready, ↓reduceIte]
    cases hcommitted : state.committedService with
    | nil =>
        cases hqueue : state.serviceQueue with
        | nil =>
            simp [hcommitted, hqueue, tinyQueueSelectedPacket,
              tinyQueueReadyState, tinyQueueDecisions,
              stateReferenceIncrements, stateReferenceConsumptions,
              ownedRoleStateReferences]
        | cons selected rest =>
            simp [hcommitted, hqueue, tinyQueueSelectedPacket,
              tinyQueueReadyState, tinyQueueDecisions,
              stateReferenceIncrements, stateReferenceConsumptions,
              ownedRoleStateReferences]
    | cons head tail =>
        simp [hcommitted, tinyQueueSelectedPacket, tinyQueueReadyState,
          tinyQueueDecisions, stateReferenceIncrements,
          stateReferenceConsumptions, ownedRoleStateReferences]

private theorem erasure_emissionsMatch :
    ServiceDecisionEmissionsMatch
      tinyQueueErasurePreemptingTransition := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, _, rfl⟩
  unfold ServiceDecisionChildrenMatch tinyQueueErasurePreemptingResult
    tinyQueueTransitionResult
  cases hkind : event.kind <;>
    simp [hkind, tinyQueueSelectedPacket, tinyQueueServiceChildren,
      tinyQueueDecisions]
  case txReady =>
    cases hcommitted : state.committedService with
    | nil =>
        cases hqueue : state.serviceQueue <;>
          simp [hcommitted, hqueue, tinyQueueSelectedPacket,
            tinyQueueServiceChildren, tinyQueueDecisions]
    | cons head tail =>
        simp [hcommitted, tinyQueueSelectedPacket,
          tinyQueueServiceChildren, tinyQueueDecisions]

private def erasureWitnessNode : NodeDescriptor :=
  { id := 1, kind := .switch, stateSlot := 0 }

private def erasureWitnessEvent : Event :=
  { queueCounterexampleArrival with payload := 30 }

private def erasureWitnessState :
    RoleState TinyQueueStateFamily .switch :=
  { privateState := queueCounterexamplePrivateState
    serviceQueue := [21]
    committedService := [12] }

private theorem erasure_witnessTransition :
    tinyQueueErasurePreemptingTransition
      erasureWitnessNode erasureWitnessEvent erasureWitnessState
      (tinyQueueErasurePreemptingResult
        erasureWitnessNode erasureWitnessEvent erasureWitnessState) := by
  simp [tinyQueueErasurePreemptingTransition, erasureWitnessNode,
    erasureWitnessEvent, queueCounterexampleArrival,
    queueCounterexampleImage, roleSupports]

private theorem erasure_notNonPreemptive :
    ¬ CommittedServiceNonPreemptive
      tinyQueueErasurePreemptingTransition := by
  intro hnonPreemptive
  have hwitness :=
    hnonPreemptive erasureWitnessNode erasureWitnessEvent
      erasureWitnessState
      (tinyQueueErasurePreemptingResult
        erasureWitnessNode erasureWitnessEvent erasureWitnessState)
      erasure_witnessTransition
  exact (by decide : ¬ CommittedServiceTransitionValid
    erasureWitnessEvent
    (tinyQueueErasurePreemptingResult
      erasureWitnessNode erasureWitnessEvent erasureWitnessState).decisions
    erasureWitnessState
    (tinyQueueErasurePreemptingResult
      erasureWitnessNode erasureWitnessEvent
      erasureWitnessState).nextState) hwitness

private theorem erasure_incomplete :
    ¬ CompleteActualServiceStartDiscipline
      tinyQueueErasurePreemptingTransition := by
  intro hcomplete
  exact erasure_notNonPreemptive hcomplete.2.2.2.2

/--
The private-payload fixture satisfies the older actual-start, decision-trace, and persistence
clauses, while its concrete `TxReady` result violates both payload binding and private
irrelevance.
-/
theorem privatePayloadSmugglerCountermodel_structural :
    PrivatePayloadSmugglerCountermodel := by
  exact ⟨rfl, privatePayload_actualStart,
    privatePayload_decisionTrace, privatePayload_nonPreemptive,
    privatePayload_emissionsMismatch, privatePayload_privateRelevant,
    privatePayload_incomplete⟩

/--
The erasure fixture retains actual-start, exact decision tracing, private irrelevance, and
decision/child payload binding, but its concrete arrival silently clears committed service.
-/
theorem committedServiceErasurePreemptorCountermodel_structural :
    CommittedServiceErasurePreemptorCountermodel := by
  exact ⟨rfl, erasure_actualStart, erasure_decisionTrace,
    erasure_privateIrrelevant, erasure_emissionsMatch,
    erasure_notNonPreemptive, erasure_incomplete⟩

end DaysExecutor
