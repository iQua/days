import DaysExecutor.RunComposition
import DaysExecutor.TinyQueueEnabled
import DaysExecutor.Proofs

namespace DaysExecutor

/-!
Structural proof of the reachable eager-selection fixture.  Concrete finite image checks are kept
separate from the universally quantified transition contracts.
-/

private theorem queueCounterexample_static :
    StaticImageWellFormed queueCounterexampleImage := by
  unfold StaticImageWellFormed
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_,
    ?_, ?_, ?_, ?_, ?_⟩
  · simp [UniqueNodeIds, queueCounterexampleImage]
  · constructor
    · intro node hnode
      simp [queueCounterexampleImage] at hnode
      rcases hnode with rfl | rfl | rfl
      all_goals native_decide
    · intro kind slot
      cases kind <;>
        simp [queueCounterexampleImage] <;>
        omega
  · intro kind state hstate
    cases kind <;>
      simp [queueCounterexampleImage] at hstate
    all_goals simp_all
  · unfold UniqueEventKeys
    native_decide
  · unfold InitialEventsOrdered
    native_decide
  · unfold UniqueLinkIds
    native_decide
  · unfold UniqueChannelRoutes
    native_decide
  · intro event hevent
    simp [queueCounterexampleImage, queueCounterexampleStartPending] at hevent
    rcases hevent with rfl | rfl
    · exact
        ⟨{ id := 1, kind := .switch, stateSlot := 0 },
          by simp [queueCounterexampleImage], rfl⟩
    · exact
        ⟨{ id := 1, kind := .switch, stateSlot := 0 },
          by simp [queueCounterexampleImage], rfl⟩
  · intro event hevent
    simp [queueCounterexampleImage, queueCounterexampleStartPending] at hevent
    rcases hevent with rfl | rfl
    · exact
        ⟨rfl,
          ⟨{ id := 0, kind := .host, stateSlot := 0 },
            by simp [queueCounterexampleImage], rfl⟩⟩
    · exact
        ⟨rfl,
          ⟨{ id := 1, kind := .switch, stateSlot := 0 },
            by simp [queueCounterexampleImage], rfl⟩⟩
  · intro payload
    exact ⟨rfl, rfl⟩
  · constructor
    · intro node hnode
      simp [queueCounterexampleImage] at hnode
      rcases hnode with rfl | rfl | rfl
      all_goals
        simp [queueCounterexampleImage, DescriptorStoreSorted,
          ownedReferenceFixtureEntry, queueCounterexampleDescriptor]
    · intro node hnode state hstate
      simp [queueCounterexampleImage] at hnode
      rcases hnode with rfl | rfl | rfl
      · simp [queueCounterexampleImage, stateAt?, listGet?] at hstate
        subst state
        simp [OwnedReferencesMatchStore, initialOwnedReferencesFor,
          ownedRoleStateReferences, queueCounterexampleImage,
          queueCounterexampleStartPending, queueCounterexampleArrival,
          queueCounterexampleReady]
      · simp [queueCounterexampleImage, stateAt?, listGet?] at hstate
        subst state
        unfold OwnedReferencesMatchStore
        constructor
        · intro reference hreference
          simp [initialOwnedReferencesFor, ownedRoleStateReferences,
            queueCounterexampleImage, queueCounterexampleStartPending,
            queueCounterexampleArrival, queueCounterexampleReady,
            ownedEventReference, ownedQueueReference,
            ownedReferenceFixtureEntry, queueCounterexampleDescriptor] at hreference
          rcases hreference with rfl | rfl | rfl <;> decide
        · intro entry hentry owner howner
          simp [queueCounterexampleImage, ownedReferenceFixtureEntry] at hentry
          rcases hentry with rfl | rfl
          · simp at howner
            rcases howner with rfl | rfl <;> decide
          · simp at howner
            subst owner
            decide
      · simp [queueCounterexampleImage, stateAt?, listGet?] at hstate
        subst state
        simp [OwnedReferencesMatchStore, initialOwnedReferencesFor,
          ownedRoleStateReferences, queueCounterexampleImage,
          queueCounterexampleStartPending, queueCounterexampleArrival,
          queueCounterexampleReady]
  · intro node hnode event hevent horigin
    simp [queueCounterexampleImage] at hnode
    simp [queueCounterexampleImage, queueCounterexampleStartPending] at hevent
    rcases hnode with rfl | rfl | rfl <;>
      rcases hevent with rfl | rfl <;>
      simp [queueCounterexampleImage, queueCounterexampleArrival,
        queueCounterexampleReady] at horigin ⊢
  · unfold PositiveLinkRates
    native_decide
  · intro link hlink
    simp [queueCounterexampleImage] at hlink
    rcases hlink with rfl | rfl | rfl
    all_goals
      constructor
      · first
        | exact
            ⟨{ id := 0, kind := .host, stateSlot := 0 },
              by simp [queueCounterexampleImage], rfl⟩
        | exact
            ⟨{ id := 1, kind := .switch, stateSlot := 0 },
              by simp [queueCounterexampleImage], rfl⟩
        | exact
            ⟨{ id := 2, kind := .host, stateSlot := 1 },
              by simp [queueCounterexampleImage], rfl⟩
      · first
        | exact
            ⟨{ id := 0, kind := .host, stateSlot := 0 },
              by simp [queueCounterexampleImage], rfl⟩
        | exact
            ⟨{ id := 1, kind := .switch, stateSlot := 0 },
              by simp [queueCounterexampleImage], rfl⟩
        | exact
            ⟨{ id := 2, kind := .host, stateSlot := 1 },
              by simp [queueCounterexampleImage], rfl⟩
  · unfold PositiveChannelBounds
    native_decide
  · intro channel hchannel
    simp [queueCounterexampleImage] at hchannel
    rcases hchannel with rfl | rfl
    all_goals
      constructor
      · first
        | exact
            ⟨{ id := 0, kind := .host, stateSlot := 0 },
              by simp [queueCounterexampleImage], rfl⟩
        | exact
            ⟨{ id := 1, kind := .switch, stateSlot := 0 },
              by simp [queueCounterexampleImage], rfl⟩
      · first
        | exact
            ⟨{ id := 1, kind := .switch, stateSlot := 0 },
              by simp [queueCounterexampleImage], rfl⟩
        | exact
            ⟨{ id := 2, kind := .host, stateSlot := 1 },
              by simp [queueCounterexampleImage], rfl⟩
  · intro channel hchannel
    simp [queueCounterexampleImage] at hchannel
    rcases hchannel with rfl | rfl
    · exact
        ⟨{ id := 0, source := 0, physicalTarget := 1,
            rateBps := 8_000_000_000, propagationNs := 4 },
          by simp [queueCounterexampleImage], rfl, rfl, rfl⟩
    · exact
        ⟨{ id := 1, source := 1, physicalTarget := 2,
            rateBps := 8_000_000_000, propagationNs := 0 },
          by simp [queueCounterexampleImage], rfl, rfl, rfl⟩

private theorem queueCounterexample_initial_roles :
    InitialEventsRoleCorrect queueCounterexampleImage := by
  simp [InitialEventsRoleCorrect, queueCounterexampleImage,
    queueCounterexampleStartPending, queueCounterexampleArrival,
    queueCounterexampleReady, roleSupports]

private theorem tinyQueueTransition_deterministic (eager : Bool) :
    TransitionDeterministic (tinyQueueTransition eager) := by
  intro node event state left right hleft hright
  exact hleft.2.2.2.2.trans hright.2.2.2.2.symm

private theorem tinyQueueTransition_role_correct (eager : Bool) :
    TransitionRoleCorrect (tinyQueueTransition eager) := by
  intro node event state result htransition
  exact ⟨htransition.2.1, htransition.2.2.1⟩

private theorem tinyQueueTransition_generated_roles (eager : Bool) :
    GeneratedEventsRoleCorrect
      queueCounterexampleImage (tinyQueueTransition eager) := by
  intro node event state result htransition child hchild
  rcases htransition with ⟨hnode, _, _, _, rfl⟩
  simp [queueCounterexampleImage] at hnode
  rcases hnode with rfl | rfl | rfl
  all_goals
    by_cases hkind : event.kind = .txReady
    · simp only [tinyQueueTransitionResult, hkind, ↓reduceIte] at hchild
      cases hselected : tinyQueueSelectedPacket eager state with
      | none =>
          simp [tinyQueueServiceChildren, hselected] at hchild
      | some packet =>
          simp [tinyQueueServiceChildren, hselected] at hchild
          rcases hchild with rfl | rfl <;>
            simp [queueCounterexampleImage, roleSupports]
    · simp [tinyQueueTransitionResult, hkind,
        tinyQueueServiceChildren] at hchild

private theorem tinyQueueTransition_children_advance (eager : Bool) :
    ChildrenAdvanceParent (tinyQueueTransition eager) := by
  intro node event state result htransition child hchild
  rcases htransition with ⟨_, _, _, _, rfl⟩
  by_cases hkind : event.kind = .txReady
  · simp only [tinyQueueTransitionResult, hkind, ↓reduceIte] at hchild
    cases hselected : tinyQueueSelectedPacket eager state with
    | none =>
        simp [tinyQueueServiceChildren, hselected] at hchild
    | some packet =>
        simp [tinyQueueServiceChildren, hselected] at hchild
        rcases hchild with rfl | rfl
        all_goals
          change EventKey.lexLT event.key _
          unfold EventKey.lexLT
          exact Or.inl (Nat.lt_succ_self _)
  · simp [tinyQueueTransitionResult, hkind,
      tinyQueueServiceChildren] at hchild

private theorem tinyQueueTransition_children_origin (eager : Bool) :
    ChildrenUseOwnerOrigin (tinyQueueTransition eager) := by
  intro node event state result htransition child hchild
  rcases htransition with ⟨_, _, _, _, rfl⟩
  by_cases hkind : event.kind = .txReady
  · simp only [tinyQueueTransitionResult, hkind, ↓reduceIte] at hchild
    cases hselected : tinyQueueSelectedPacket eager state with
    | none =>
        simp [tinyQueueServiceChildren, hselected] at hchild
    | some packet =>
        simp [tinyQueueServiceChildren, hselected] at hchild
        rcases hchild with rfl | rfl <;> rfl
  · simp [tinyQueueTransitionResult, hkind,
      tinyQueueServiceChildren] at hchild

private theorem tinyQueueTransition_children_phase (eager : Bool) :
    ChildrenUseCanonicalPhase (tinyQueueTransition eager) := by
  intro node event state result htransition child hchild
  rcases htransition with ⟨_, _, _, _, rfl⟩
  by_cases hkind : event.kind = .txReady
  · simp only [tinyQueueTransitionResult, hkind, ↓reduceIte] at hchild
    cases hselected : tinyQueueSelectedPacket eager state with
    | none =>
        simp [tinyQueueServiceChildren, hselected] at hchild
    | some packet =>
        simp [tinyQueueServiceChildren, hselected] at hchild
        rcases hchild with rfl | rfl <;> rfl
  · simp [tinyQueueTransitionResult, hkind,
      tinyQueueServiceChildren] at hchild

private theorem tinyQueueTransition_children_unique (eager : Bool) :
    TransitionChildrenHaveUniqueKeys (tinyQueueTransition eager) := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, _, rfl⟩
  unfold UniqueEventKeys
  by_cases hkind : event.kind = .txReady
  · simp only [tinyQueueTransitionResult, hkind, ↓reduceIte]
    cases hselected : tinyQueueSelectedPacket eager state with
    | none =>
        simp [tinyQueueServiceChildren, hselected]
    | some packet =>
        simp [tinyQueueServiceChildren, hselected]
  · simp [tinyQueueTransitionResult, hkind,
      tinyQueueServiceChildren]

private theorem tinyQueueTransition_remote_coverage (eager : Bool) :
    RemoteEmissionCoverage
      queueCounterexampleImage (tinyQueueTransition eager) := by
  intro node event state result htransition child hchild hremote
  rcases htransition with ⟨hnode, _, _, _, rfl⟩
  simp [queueCounterexampleImage] at hnode
  rcases hnode with rfl | rfl | rfl
  · by_cases hkind : event.kind = .txReady
    · simp only [tinyQueueTransitionResult, hkind, ↓reduceIte] at hchild
      cases hselected : tinyQueueSelectedPacket eager state with
      | none => simp [tinyQueueServiceChildren, hselected] at hchild
      | some packet =>
          simp [tinyQueueServiceChildren, hselected] at hchild
          rcases hchild with rfl | rfl <;> simp at hremote
    · simp [tinyQueueTransitionResult, hkind,
        tinyQueueServiceChildren] at hchild
  · by_cases hkind : event.kind = .txReady
    · simp only [tinyQueueTransitionResult, hkind, ↓reduceIte] at hchild
      cases hselected : tinyQueueSelectedPacket eager state with
      | none => simp [tinyQueueServiceChildren, hselected] at hchild
      | some packet =>
          simp [tinyQueueServiceChildren, hselected] at hchild
          rcases hchild with rfl | rfl
          · simp at hremote
          · exact
              ⟨{ source := 1, target := 2, link := 1,
                  eventKind := .remoteArrival, minDelayNs := 1 },
                by simp [queueCounterexampleImage], rfl, rfl, rfl⟩
    · simp [tinyQueueTransitionResult, hkind,
        tinyQueueServiceChildren] at hchild
  · by_cases hkind : event.kind = .txReady
    · simp only [tinyQueueTransitionResult, hkind, ↓reduceIte] at hchild
      cases hselected : tinyQueueSelectedPacket eager state with
      | none => simp [tinyQueueServiceChildren, hselected] at hchild
      | some packet =>
          simp [tinyQueueServiceChildren, hselected] at hchild
          rcases hchild with rfl | rfl <;> simp at hremote
    · simp [tinyQueueTransitionResult, hkind,
        tinyQueueServiceChildren] at hchild

set_option maxRecDepth 10000 in
private theorem tinyQueueTransition_bound_sound (eager : Bool) :
    CertifiedBoundSoundness
      queueCounterexampleImage (tinyQueueTransition eager) := by
  intro node event state result htransition child hchild hremote
    channel hchannel hsource htarget hchannelKind
  rcases htransition with ⟨hnode, _, _, _, rfl⟩
  have hnode' :
      node = { id := 0, kind := .host, stateSlot := 0 } ∨
        node = { id := 1, kind := .switch, stateSlot := 0 } ∨
          node = { id := 2, kind := .host, stateSlot := 1 } := by
    simpa [queueCounterexampleImage] using hnode
  rcases hnode' with rfl | rfl | rfl
  · by_cases hkind : event.kind = .txReady
    · simp only [tinyQueueTransitionResult, hkind, ↓reduceIte] at hchild
      cases hselected : tinyQueueSelectedPacket eager state with
      | none => simp [tinyQueueServiceChildren, hselected] at hchild
      | some packet =>
          simp [tinyQueueServiceChildren, hselected] at hchild
          rcases hchild with rfl | rfl <;> simp at hremote
    · simp [tinyQueueTransitionResult, hkind,
        tinyQueueServiceChildren] at hchild
  · by_cases hkind : event.kind = .txReady
    · simp only [tinyQueueTransitionResult, hkind, ↓reduceIte] at hchild
      cases hselected : tinyQueueSelectedPacket eager state with
      | none => simp [tinyQueueServiceChildren, hselected] at hchild
      | some packet =>
          simp [tinyQueueServiceChildren, hselected] at hchild
          rcases hchild with rfl | rfl
          · simp at hremote
          · simp [queueCounterexampleImage] at hchannel
            rcases hchannel with rfl | rfl
            · simp at hsource
            · exact
                ⟨{ id := 1, source := 1, physicalTarget := 2,
                    rateBps := 8_000_000_000, propagationNs := 0 },
                  by simp [queueCounterexampleImage], rfl, rfl,
                  by
                    rw [show
                      queueCounterexampleImage.payloadBytes packet = 1 from rfl]
                    native_decide,
                  by
                    change event.key.timeNs + 1 ≤ event.key.timeNs + 1
                    exact Nat.le_refl _⟩
    · change child ∈
        tinyQueueServiceChildren
          { id := 1, kind := .switch, stateSlot := 0 }
          event
          (if event.kind = .txReady then
            tinyQueueSelectedPacket eager state else none) at hchild
      simp [hkind, tinyQueueServiceChildren] at hchild
  · by_cases hkind : event.kind = .txReady
    · simp only [tinyQueueTransitionResult, hkind, ↓reduceIte] at hchild
      cases hselected : tinyQueueSelectedPacket eager state with
      | none => simp [tinyQueueServiceChildren, hselected] at hchild
      | some packet =>
          simp [tinyQueueServiceChildren, hselected] at hchild
          rcases hchild with rfl | rfl <;> simp at hremote
    · change child ∈
        tinyQueueServiceChildren
          { id := 2, kind := .host, stateSlot := 1 }
          event
          (if event.kind = .txReady then
            tinyQueueSelectedPacket eager state else none) at hchild
      simp [hkind, tinyQueueServiceChildren] at hchild

private theorem listBagDifference_mem
    [BEq α] [LawfulBEq α]
    (source removed : List α)
    {item : α}
    (hitem : item ∈ listBagDifference source removed) :
    item ∈ source := by
  induction removed generalizing source with
  | nil =>
      exact hitem
  | cons head tail ih =>
      simp only [listBagDifference, List.foldl_cons] at hitem
      exact List.mem_of_mem_erase (ih (source.erase head) hitem)

private theorem ownedRoleStateReference_oracle
    (node : NodeDescriptor)
    (state : RoleState TinyQueueStateFamily node.kind)
    (reference : OwnedPacketReference)
    (hreference :
      reference ∈
        ownedRoleStateReferences queueCounterexampleImage node state) :
    reference.descriptor =
      queueCounterexampleImage.packetDescriptor reference.descriptor.id := by
  unfold ownedRoleStateReferences at hreference
  rw [List.mem_append] at hreference
  rcases hreference with hqueue | hservice
  · rcases List.mem_map.mp hqueue with ⟨payload, _, rfl⟩
    rfl
  · rcases List.mem_map.mp hservice with ⟨payload, _, rfl⟩
    rfl

private theorem tinyQueueTransition_descriptors (eager : Bool) :
    TransitionDescriptorEffectsCoherent
      queueCounterexampleImage (tinyQueueTransition eager) := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, _, rfl⟩
  constructor
  · intro reference hreference
    unfold tinyQueueTransitionResult at hreference
    exact ownedRoleStateReference_oracle node _ reference
      (listBagDifference_mem _ _ hreference)
  constructor
  · intro reference hreference
    unfold tinyQueueTransitionResult at hreference
    rcases List.mem_cons.mp hreference with rfl | hreference
    · rfl
    · exact ownedRoleStateReference_oracle node state reference
        (listBagDifference_mem _ _ hreference)
  · intro descriptor hdescriptor
    simp [tinyQueueTransitionResult] at hdescriptor

private theorem tinyQueueTransition_observation_keys (eager : Bool) :
    TransitionObservationsUseEventKey (tinyQueueTransition eager) := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, _, rfl⟩
  simp [ObservationRecordsUseEventKey, tinyQueueTransitionResult]

private theorem tinyQueueTransition_axioms (eager : Bool) :
    TransitionAxioms
      queueCounterexampleImage (tinyQueueTransition eager) := by
  exact
    ⟨tinyQueueTransition_deterministic eager,
      tinyQueueTransition_role_correct eager,
      tinyQueueTransition_generated_roles eager,
      tinyQueueTransition_children_advance eager,
      tinyQueueTransition_children_origin eager,
      tinyQueueTransition_children_phase eager,
      tinyQueueTransition_children_unique eager,
      tinyQueueTransition_remote_coverage eager,
      tinyQueueTransition_bound_sound eager,
      tinyQueueTransition_descriptors eager,
      tinyQueueTransition_observation_keys eager⟩

private theorem tinyQueue_actual_service_start (eager : Bool) :
    ActualServiceStartDiscipline (tinyQueueTransition eager) := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, _, rfl⟩
  by_cases hkind : event.kind = .txReady
  · simp only [tinyQueueTransitionResult, hkind, ↓reduceIte]
    cases hselected : tinyQueueSelectedPacket eager state with
    | none =>
        simp [tinyQueueDecisions, hselected]
    | some packet =>
        simp [tinyQueueDecisions, hselected, hkind]
  · simp [tinyQueueTransitionResult, hkind, tinyQueueDecisions]

private theorem tinyQueue_decision_emissions (eager : Bool) :
    ServiceDecisionEmissionsMatch (tinyQueueTransition eager) := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, _, rfl⟩
  unfold ServiceDecisionChildrenMatch
  by_cases hkind : event.kind = .txReady
  · simp only [tinyQueueTransitionResult, hkind, ↓reduceIte]
    cases hselected : tinyQueueSelectedPacket eager state with
    | none =>
        simp [tinyQueueServiceChildren, tinyQueueDecisions, hselected]
    | some packet =>
        simp [tinyQueueServiceChildren, tinyQueueDecisions, hselected]
  · simp [tinyQueueTransitionResult, hkind, tinyQueueServiceChildren,
      tinyQueueDecisions]

private theorem list_ne_append_singleton (items : List α) (item : α) :
    items ≠ items ++ [item] := by
  intro heq
  have hlength := congrArg List.length heq
  simp at hlength

private theorem erase_ne_append_singleton_of_mem
    [BEq α] [LawfulBEq α]
    (items : List α)
    (removed item : α)
    (hmem : removed ∈ items) :
    items.erase removed ≠ items ++ [item] := by
  intro heq
  have hlength := congrArg List.length heq
  rw [List.length_erase_of_mem hmem] at hlength
  simp at hlength

private theorem tinyQueue_decision_trace (eager : Bool) :
    ServiceDecisionTraceComplete (tinyQueueTransition eager) := by
  intro node event state result htransition packet
  rcases htransition with ⟨_, _, _, hcompletion, rfl⟩
  cases hkind : event.kind with
  | packetArrival =>
      simp [SelectionIntroduced, tinyQueueTransitionResult, hkind,
        tinyQueueDecisions]
  | remoteArrival =>
      simp [SelectionIntroduced, tinyQueueTransitionResult, hkind,
        tinyQueueDecisions, tinyQueueArrivalState,
        list_ne_append_singleton]
      split <;> split <;> exact list_ne_append_singleton _ _
  | txReady =>
      cases hcommitted : state.committedService with
      | nil =>
          cases eager with
          | false =>
              cases hqueue : state.serviceQueue with
              | nil =>
                  simp [SelectionIntroduced, tinyQueueTransitionResult,
                    tinyQueueSelectedPacket, tinyQueueReadyState,
                    tinyQueueDecisions, hkind, hcommitted, hqueue]
              | cons head tail =>
                  simp [SelectionIntroduced, tinyQueueTransitionResult,
                    tinyQueueSelectedPacket, tinyQueueReadyState,
                    tinyQueueDecisions, hkind, hcommitted, hqueue]
          | true =>
              cases hhidden : state.privateState.hiddenReservation with
              | none =>
                  cases hqueue : state.serviceQueue with
                  | nil =>
                      simp [SelectionIntroduced, tinyQueueTransitionResult,
                        tinyQueueSelectedPacket, tinyQueueReadyState,
                        tinyQueueDecisions, hkind, hcommitted, hhidden, hqueue]
                  | cons head tail =>
                      simp [SelectionIntroduced, tinyQueueTransitionResult,
                        tinyQueueSelectedPacket, tinyQueueReadyState,
                        tinyQueueDecisions, hkind, hcommitted, hhidden, hqueue]
              | some hidden =>
                  simp [SelectionIntroduced, tinyQueueTransitionResult,
                    tinyQueueSelectedPacket, tinyQueueReadyState,
                    tinyQueueDecisions, hkind, hcommitted, hhidden]
      | cons head tail =>
          simp [SelectionIntroduced, tinyQueueTransitionResult,
            tinyQueueSelectedPacket, tinyQueueReadyState,
            tinyQueueDecisions, hkind, hcommitted]
  | txComplete =>
      have hpresent : event.payload ∈ state.committedService :=
        hcompletion (by simpa using hkind)
      simp [SelectionIntroduced, tinyQueueTransitionResult, hkind,
        tinyQueueDecisions,
        erase_ne_append_singleton_of_mem _ _ _ hpresent]

private theorem tinyQueue_committed_nonpreemptive (eager : Bool) :
    CommittedServiceNonPreemptive (tinyQueueTransition eager) := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, hcompletion, rfl⟩
  unfold CommittedServiceTransitionValid
  cases hkind : event.kind with
  | packetArrival =>
      simp [tinyQueueTransitionResult, hkind, tinyQueueDecisions]
  | remoteArrival =>
      simp [tinyQueueTransitionResult, hkind, tinyQueueDecisions,
        tinyQueueArrivalState]
      split <;> split <;> exact ⟨fun h => h, rfl⟩
  | txReady =>
      cases hcommitted : state.committedService with
      | nil =>
          cases eager with
          | false =>
              cases hqueue : state.serviceQueue with
              | nil =>
                  simp [tinyQueueTransitionResult, tinyQueueSelectedPacket,
                    tinyQueueReadyState, tinyQueueDecisions, hkind,
                    hcommitted, hqueue]
              | cons head tail =>
                  simp [tinyQueueTransitionResult, tinyQueueSelectedPacket,
                    tinyQueueReadyState, tinyQueueDecisions, hkind,
                    hcommitted, hqueue]
          | true =>
              cases hhidden : state.privateState.hiddenReservation with
              | none =>
                  cases hqueue : state.serviceQueue with
                  | nil =>
                      simp [tinyQueueTransitionResult,
                        tinyQueueSelectedPacket, tinyQueueReadyState,
                        tinyQueueDecisions, hkind, hcommitted, hhidden, hqueue]
                  | cons head tail =>
                      simp [tinyQueueTransitionResult,
                        tinyQueueSelectedPacket, tinyQueueReadyState,
                        tinyQueueDecisions, hkind, hcommitted, hhidden, hqueue]
              | some hidden =>
                  simp [tinyQueueTransitionResult, tinyQueueSelectedPacket,
                    tinyQueueReadyState, tinyQueueDecisions, hkind,
                    hcommitted, hhidden]
      | cons head tail =>
          simp [tinyQueueTransitionResult, tinyQueueSelectedPacket,
            tinyQueueReadyState, tinyQueueDecisions, hkind, hcommitted]
  | txComplete =>
      have hpresent : event.payload ∈ state.committedService :=
        hcompletion (by simpa using hkind)
      simp only [tinyQueueTransitionResult, hkind, tinyQueueDecisions,
        ↓reduceIte, hpresent, true_and]
      exact ⟨fun hnodup => hnodup.erase _, trivial⟩

private theorem tinyQueue_canonical_private_irrelevant :
    TxReadySelectionPrivateIrrelevant (tinyQueueTransition false) := by
  intro node event state result htransition hkind privateState
  rcases htransition with
    ⟨hnode, htarget, hsupport, _, rfl⟩
  let alternateState : RoleState TinyQueueStateFamily node.kind :=
    { privateState
      serviceQueue := state.serviceQueue
      committedService := state.committedService }
  let alternateResult :=
    tinyQueueTransitionResult false node event alternateState
  refine ⟨alternateResult, ?_, ?_⟩
  · exact ⟨hnode, htarget, hsupport,
      fun hcomplete => False.elim (by simp [hkind] at hcomplete),
      rfl⟩
  · unfold SameServiceSelectionResult alternateResult alternateState
    cases hcommitted : state.committedService with
    | nil =>
        cases hqueue : state.serviceQueue with
        | nil =>
            simp [tinyQueueTransitionResult, tinyQueueSelectedPacket,
              tinyQueueReadyState, hkind, hcommitted, hqueue,
              stateReferenceIncrements, stateReferenceConsumptions,
              ownedRoleStateReferences]
        | cons head tail =>
            simp [tinyQueueTransitionResult, tinyQueueSelectedPacket,
              tinyQueueReadyState, hkind, hcommitted, hqueue,
              stateReferenceIncrements, stateReferenceConsumptions,
              ownedRoleStateReferences]
    | cons head tail =>
        simp [tinyQueueTransitionResult, tinyQueueSelectedPacket,
          tinyQueueReadyState, hkind, hcommitted,
          stateReferenceIncrements, stateReferenceConsumptions,
          ownedRoleStateReferences]

theorem tinyQueue_eager_private_relevant :
    ¬ TxReadySelectionPrivateIrrelevant (tinyQueueTransition true) := by
  intro hirrelevant
  let node : NodeDescriptor :=
    { id := 1, kind := .switch, stateSlot := 0 }
  let smuggled : RoleState TinyQueueStateFamily .switch :=
    { privateState :=
        { capacity := 1
          hiddenReservation := some 12
          accepted := [21]
          dropped := [] }
      serviceQueue := [21]
      committedService := [] }
  let alternatePrivate : TinyQueuePrivateState :=
    { smuggled.privateState with hiddenReservation := none }
  let result :=
    tinyQueueTransitionResult true node queueCounterexampleReady smuggled
  have htransition :
      tinyQueueTransition true node queueCounterexampleReady smuggled result := by
    exact ⟨by simp [node, queueCounterexampleImage],
      by rfl, by simp [node, queueCounterexampleReady, roleSupports],
      by simp [queueCounterexampleReady], rfl⟩
  obtain ⟨alternateResult, halternate, hsame⟩ :=
    hirrelevant node queueCounterexampleReady smuggled result
      htransition rfl alternatePrivate
  have halternateResult :
      alternateResult =
        tinyQueueTransitionResult true node queueCounterexampleReady
          { privateState := alternatePrivate
            serviceQueue := smuggled.serviceQueue
            committedService := smuggled.committedService } :=
    halternate.2.2.2.2
  rw [halternateResult] at hsame
  have hnot :
      ¬ SameServiceSelectionResult result
        (tinyQueueTransitionResult true node queueCounterexampleReady
          { privateState := alternatePrivate
            serviceQueue := smuggled.serviceQueue
            committedService := smuggled.committedService }) := by
    native_decide
  exact hnot hsame

private theorem tinyQueue_complete_canonical :
    CompleteActualServiceStartDiscipline (tinyQueueTransition false) :=
  ⟨tinyQueue_actual_service_start false,
    tinyQueue_decision_trace false,
    tinyQueue_canonical_private_irrelevant,
    tinyQueue_decision_emissions false,
    tinyQueue_committed_nonpreemptive false⟩

private theorem tinyQueue_complete_eager_fails :
    ¬ CompleteActualServiceStartDiscipline (tinyQueueTransition true) := by
  intro hcomplete
  exact tinyQueue_eager_private_relevant hcomplete.2.2.1

theorem queueCounterexample_initial_machine :
    InitialMachine queueCounterexampleImage queueCounterexampleMachine := by
  rcases queueCounterexample_static with
    ⟨_, _, _, hkeys, _, _, _, _, hkeyCanonical, _, hstores,
      hreserved, _, _, _, _, _⟩
  refine ⟨rfl, rfl, rfl, rfl, rfl, rfl, rfl, rfl, ?_, ?_⟩
  · intro node hnode
    simp [queueCounterexampleImage] at hnode
    rcases hnode with rfl | rfl | rfl
    all_goals
      constructor
      · simp [queueCounterexampleMachine, queueCounterexampleLocalState,
          queueCounterexampleImage, stateAt?, listGet?]
      · rfl
  · unfold MachineWellFormed
    refine ⟨?_, hkeys, ?_, ?_, ?_, ?_, ?_⟩
    · unfold queueCounterexampleMachine
      unfold canonicalizeEvents
      apply canonicalPending_insertEvents
      · simp [CanonicalPending]
      · exact hkeys
      · intro child _ pending hpending
        simp at hpending
    · intro key hkey
      rcases List.mem_map.mp hkey with ⟨event, hevent, rfl⟩
      obtain ⟨hphase, origin, horigin, heq⟩ :=
        hkeyCanonical event hevent
      exact
        ⟨by
          apply hreserved origin horigin event hevent
          exact heq.symm,
        origin, horigin, heq.symm⟩
    · intro event hevent
      unfold queueCounterexampleMachine at hevent
      unfold canonicalizeEvents at hevent
      rw [mem_insertEvents_iff] at hevent
      have hinitial : event ∈ queueCounterexampleImage.initialEvents := by
        simpa using hevent
      exact
        ⟨List.mem_map_of_mem hinitial,
          queueCounterexample_initial_roles event hinitial⟩
    · intro node hnode
      exact hstores.1 node hnode
    · intro node hnode
      have hstate :
          stateAt? queueCounterexampleImage node =
            some (queueCounterexampleMachine.localState node) := by
        simp [queueCounterexampleImage] at hnode
        rcases hnode with rfl | rfl | rfl
        all_goals
          simp [queueCounterexampleMachine, queueCounterexampleLocalState,
            queueCounterexampleImage, stateAt?, listGet?]
      exact hstores.2 node hnode
        (queueCounterexampleMachine.localState node) hstate
    · simp [DescriptorListCoherent, queueCounterexampleMachine]

private def queueCounterexampleNode : NodeDescriptor :=
  { id := 1, kind := .switch, stateSlot := 0 }

private def queueCounterexampleAfterArrival (eager : Bool) :
    MachineState TinyQueueStateFamily :=
  materializeScalarResult
    queueCounterexampleImage queueCounterexampleNode
    queueCounterexampleArrival
    (tinyQueueTransitionResult eager queueCounterexampleNode
      queueCounterexampleArrival
      (queueCounterexampleMachine.localState queueCounterexampleNode))
    queueCounterexampleMachine

private def queueCounterexampleFinish (eager : Bool) :
    MachineState TinyQueueStateFamily :=
  materializeScalarResult
    queueCounterexampleImage queueCounterexampleNode
    queueCounterexampleReady
    (tinyQueueTransitionResult eager queueCounterexampleNode
      queueCounterexampleReady
      ((queueCounterexampleAfterArrival eager).localState
        queueCounterexampleNode))
    (queueCounterexampleAfterArrival eager)

private theorem queueCounterexample_arrival_step (eager : Bool) :
    AvailableEventStep
        queueCounterexampleImage (tinyQueueTransition eager)
        queueCounterexampleArrival queueCounterexampleMachine
        (queueCounterexampleAfterArrival eager) ∧
      MachineWellFormed queueCounterexampleImage
        (queueCounterexampleAfterArrival eager) := by
  rcases queueCounterexample_initial_machine with
    ⟨_, _, _, _, _, _, _, _, _, hwellFormed⟩
  apply materializeScalarResult_available
    queueCounterexampleImage (tinyQueueTransition eager)
    queueCounterexample_static.1
    (by intro payload; exact ⟨rfl, rfl⟩)
    (tinyQueueTransition_generated_roles eager)
    (tinyQueueTransition_descriptors eager)
    queueCounterexampleNode
    (by simp [queueCounterexampleNode, queueCounterexampleImage])
    queueCounterexampleArrival
    (tinyQueueTransitionResult eager queueCounterexampleNode
      queueCounterexampleArrival
      (queueCounterexampleMachine.localState queueCounterexampleNode))
    queueCounterexampleMachine hwellFormed
  · native_decide
  · rfl
  · exact
      ⟨by simp [queueCounterexampleNode, queueCounterexampleImage],
        rfl, by simp [queueCounterexampleNode, queueCounterexampleArrival,
          roleSupports],
        by simp [queueCounterexampleArrival], rfl⟩
  · simp [tinyQueueTransitionResult, tinyQueueServiceChildren,
      queueCounterexampleArrival, ChildrenUseOriginSequence]
  · simp [tinyQueueTransitionResult, tinyQueueServiceChildren,
      queueCounterexampleArrival]
  · simp [tinyQueueTransitionResult, tinyQueueServiceChildren,
      queueCounterexampleArrival]
  · rfl
  · simp [ReferenceIncrementsValid, tinyQueueTransitionResult,
      queueCounterexampleArrival]
  · cases eager <;>
      native_decide

private theorem queueCounterexample_ready_step (eager : Bool) :
    AvailableEventStep
        queueCounterexampleImage (tinyQueueTransition eager)
        queueCounterexampleReady (queueCounterexampleAfterArrival eager)
        (queueCounterexampleFinish eager) ∧
      MachineWellFormed queueCounterexampleImage
        (queueCounterexampleFinish eager) := by
  have hbefore := (queueCounterexample_arrival_step eager).2
  apply materializeScalarResult_available
    queueCounterexampleImage (tinyQueueTransition eager)
    queueCounterexample_static.1
    (by intro payload; exact ⟨rfl, rfl⟩)
    (tinyQueueTransition_generated_roles eager)
    (tinyQueueTransition_descriptors eager)
    queueCounterexampleNode
    (by simp [queueCounterexampleNode, queueCounterexampleImage])
    queueCounterexampleReady
    (tinyQueueTransitionResult eager queueCounterexampleNode
      queueCounterexampleReady
      ((queueCounterexampleAfterArrival eager).localState
        queueCounterexampleNode))
    (queueCounterexampleAfterArrival eager) hbefore
  · cases eager <;> native_decide
  · rfl
  · exact
      ⟨by simp [queueCounterexampleNode, queueCounterexampleImage],
        rfl, by simp [queueCounterexampleNode, queueCounterexampleReady,
          roleSupports],
        by simp [queueCounterexampleReady], rfl⟩
  · cases eager <;>
      simp [queueCounterexampleAfterArrival, materializeScalarResult,
        tinyQueueTransitionResult, tinyQueueArrivalState,
        tinyQueueReservePrivately, tinyQueueSelectedPacket,
        tinyQueueServiceChildren, queueCounterexampleArrival,
        queueCounterexampleReady, queueCounterexampleNode,
        queueCounterexampleMachine, queueCounterexampleLocalState,
        queueCounterexamplePrivateState, queueCounterexampleImage]
      <;> simp [ChildrenUseOriginSequence]
  · cases eager <;>
      native_decide
  · cases eager <;>
      native_decide
  · rfl
  · exact List.Perm.refl _
  · cases eager <;> native_decide

theorem queueCounterexample_execution (eager : Bool) :
    ExecutionInOrder
      queueCounterexampleImage
      (tinyQueueTransition eager)
      queueCounterexampleMachine
      [queueCounterexampleArrival, queueCounterexampleReady]
      (queueCounterexampleFinish eager) := by
  exact .step (queueCounterexample_arrival_step eager).1
    (.step (queueCounterexample_ready_step eager).1 (.refl _))

/-!
Concrete safe-horizon execution witness.

The first round uses the constant horizon `6` and drains only the arrival at time `5`. The second
uses the exclusive endpoint `11`, drains the `TxReady` at time `10`, and leaves its two children at
time `11`. The local completion remains in the switch future while the remote arrival is buffered
as an envelope and transferred to the terminal host by the canonical exchange.
-/

private def queueCounterexampleTerminalNode : NodeDescriptor :=
  { id := 2, kind := .host, stateSlot := 1 }

private def queueCounterexampleSourceNode : NodeDescriptor :=
  { id := 0, kind := .host, stateSlot := 0 }

def queueCounterexampleCompletion : Event :=
  { key :=
      { timeNs := 11
        phase := eventPhase .txComplete
        originNode := 1
        originSeq := 1 }
    target := 1
    kind := .txComplete
    payload := 12 }

def queueCounterexampleRemote : Event :=
  { key :=
      { timeNs := 11
        phase := eventPhase .remoteArrival
        originNode := 1
        originSeq := 2 }
    target := 2
    kind := .remoteArrival
    payload := 12 }

def queueCounterexampleRoundTwoEmissions : List (Event × Event) :=
  [(queueCounterexampleReady, queueCounterexampleCompletion),
    (queueCounterexampleReady, queueCounterexampleRemote)]

def queueCounterexampleRemoteEnvelope : RemoteEnvelope :=
  { source := 1
    event := queueCounterexampleRemote
    packet := queueCounterexampleDescriptor 12 }

private def queueCounterexampleRoundOneBounds : BoundFamily :=
  fun _ => 6

private def queueCounterexampleRoundTwoBounds : BoundFamily :=
  fun _ => 11

private def queueCounterexampleRoundStart :
    RoundState TinyQueueStateFamily :=
  { machine := queueCounterexampleMachine
    outboxes := fun _ => [] }

private def queueCounterexampleRoundMiddle :
    RoundState TinyQueueStateFamily :=
  { machine := queueCounterexampleAfterArrival false
    outboxes := fun _ => [] }

private def queueCounterexampleAfterReadyDrainMachine :
    MachineState TinyQueueStateFamily :=
  let before := queueCounterexampleAfterArrival false
  let result :=
    tinyQueueTransitionResult false queueCounterexampleNode
      queueCounterexampleReady
      (before.localState queueCounterexampleNode)
  let scalar :=
    materializeScalarResult queueCounterexampleImage queueCounterexampleNode
      queueCounterexampleReady result before
  { scalar with
    packetStore := fun target =>
      if target.id = queueCounterexampleNode.id then
        holdEmittedChildReferences queueCounterexampleImage
          queueCounterexampleNode.id result.children
          (applyPacketEffects result (before.packetStore target))
      else
        before.packetStore target
    pending :=
      insertEvents
        (localChildren queueCounterexampleNode.id result.children)
        (before.pending.erase queueCounterexampleReady) }

private def queueCounterexampleAfterReadyDrain :
    RoundState TinyQueueStateFamily :=
  { machine := queueCounterexampleAfterReadyDrainMachine
    outboxes := fun source =>
      if source = queueCounterexampleNode.id then
        [queueCounterexampleRemoteEnvelope]
      else
        [] }

private def queueCounterexampleRoundFinish :
    RoundState TinyQueueStateFamily :=
  { machine := queueCounterexampleFinish false
    outboxes := fun _ => [] }

private theorem roundReferencesHeld_empty
    (machine : MachineState TinyQueueStateFamily)
    (hwellFormed :
      MachineWellFormed queueCounterexampleImage machine) :
    RoundReferencesHeld queueCounterexampleImage
      { machine := machine, outboxes := fun _ => [] } := by
  intro node hnode
  simpa [RoundReferencesHeld] using
    hwellFormed.2.2.2.2.2.1 node hnode

private theorem postExchangeStart_empty
    (machine : MachineState TinyQueueStateFamily)
    (hwellFormed :
      MachineWellFormed queueCounterexampleImage machine) :
    PostExchangeStart queueCounterexampleImage
      { machine := machine, outboxes := fun _ => [] } := by
  exact
    ⟨by intro node _; rfl,
      hwellFormed,
      roundReferencesHeld_empty machine hwellFormed⟩

private theorem queueCounterexample_ready_children :
    (tinyQueueTransitionResult false queueCounterexampleNode
      queueCounterexampleReady
      ((queueCounterexampleAfterArrival false).localState
        queueCounterexampleNode)).children =
      [queueCounterexampleCompletion, queueCounterexampleRemote] := by
  decide

private theorem queueCounterexample_afterArrival_pending :
    (queueCounterexampleAfterArrival false).pending =
      [queueCounterexampleReady] := by
  decide

private theorem queueCounterexample_initial_pending :
    queueCounterexampleMachine.pending =
      [queueCounterexampleArrival, queueCounterexampleReady] := by
  decide

private theorem queueCounterexample_afterReadyDrain_pending :
    queueCounterexampleAfterReadyDrainMachine.pending =
      [queueCounterexampleCompletion] := by
  decide

private theorem queueCounterexample_finish_pending :
    (queueCounterexampleFinish false).pending =
      [queueCounterexampleRemote, queueCounterexampleCompletion] := by
  decide

private theorem queueCounterexample_switch_dispatch :
    tinyQueueTransition false queueCounterexampleNode
      queueCounterexampleReady
      ((queueCounterexampleAfterArrival false).localState
        queueCounterexampleNode)
      (tinyQueueTransitionResult false queueCounterexampleNode
        queueCounterexampleReady
        ((queueCounterexampleAfterArrival false).localState
          queueCounterexampleNode)) := by
  exact
    ⟨by simp [queueCounterexampleNode, queueCounterexampleImage],
      rfl,
      by simp [queueCounterexampleNode, queueCounterexampleReady,
        roleSupports],
      by simp [queueCounterexampleReady],
      rfl⟩

private theorem queueCounterexample_host_dispatch :
    tinyQueueTransition false queueCounterexampleTerminalNode
      queueCounterexampleRemote
      ((queueCounterexampleFinish false).localState
        queueCounterexampleTerminalNode)
      (tinyQueueTransitionResult false queueCounterexampleTerminalNode
        queueCounterexampleRemote
        ((queueCounterexampleFinish false).localState
          queueCounterexampleTerminalNode)) := by
  exact
    ⟨by simp [queueCounterexampleTerminalNode, queueCounterexampleImage],
      rfl,
      by simp [queueCounterexampleTerminalNode, queueCounterexampleRemote,
        roleSupports],
      by simp [queueCounterexampleRemote],
      rfl⟩

private theorem queueCounterexample_arrival_localRoundStep :
    LocalRoundStep queueCounterexampleImage (tinyQueueTransition false)
      queueCounterexampleRoundOneBounds queueCounterexampleNode
      queueCounterexampleRoundStart queueCounterexampleArrival
      queueCounterexampleRoundMiddle := by
  have havailable := (queueCounterexample_arrival_step false).1
  rcases havailable with
    ⟨hevent, node, hnode, result, htarget, htransition, _,
      hallocates, happlies, hcoherent, _, hpending, hemissions⟩
  have hnodeEq : node = queueCounterexampleNode := by
    apply node_eq_of_unique_ids
      queueCounterexample_static.1 hnode
      (by simp [queueCounterexampleNode, queueCounterexampleImage])
    simpa [queueCounterexampleNode, queueCounterexampleArrival] using
      htarget.symm
  subst node
  have hresultEq :
      result =
        tinyQueueTransitionResult false queueCounterexampleNode
          queueCounterexampleArrival
          (queueCounterexampleMachine.localState
            queueCounterexampleNode) :=
    htransition.2.2.2.2
  subst result
  have hchildren :
      (tinyQueueTransitionResult false queueCounterexampleNode
        queueCounterexampleArrival
        (queueCounterexampleMachine.localState
          queueCounterexampleNode)).children = [] := by
    decide
  rcases happlies with
    ⟨hlocal, hstore, hother, houtput, hconsumptions, hincrements⟩
  have happliesRound :
      AppliesTransitionResult queueCounterexampleImage
        queueCounterexampleNode queueCounterexampleArrival
        (tinyQueueTransitionResult false queueCounterexampleNode
          queueCounterexampleArrival
          (queueCounterexampleMachine.localState
            queueCounterexampleNode))
        queueCounterexampleMachine
        (queueCounterexampleAfterArrival false) := by
    refine ⟨hlocal, ?_, ?_, houtput, hconsumptions, hincrements⟩
    · simpa [hchildren] using hstore
    · intro other hotherMem hotherId
      have hsame := hother other hotherMem hotherId
      simpa [hchildren] using hsame
  have hafterWellFormed := (queueCounterexample_arrival_step false).2
  refine
    ⟨by simp [queueCounterexampleNode, queueCounterexampleImage],
      ?_,
      (tinyQueueTransitionResult false queueCounterexampleNode
        queueCounterexampleArrival
        (queueCounterexampleRoundStart.machine.localState
          queueCounterexampleNode)),
      ?_, ?_, ?_, ?_,
      hcoherent, ?_, ?_, ?_, ?_, [], ?_, ?_, ?_⟩
  · refine ⟨?_, ?_, ?_⟩
    · simpa [queueCounterexampleRoundStart] using hevent
    · constructor
      · rfl
      · decide
    · intro other hother _
      have hreadyLater :
          ¬ queueCounterexampleReady.key ≤
            queueCounterexampleArrival.key := by
        decide
      have hcases :
          other = queueCounterexampleArrival ∨
            other = queueCounterexampleReady := by
        simpa [queueCounterexampleRoundStart,
          queueCounterexampleMachine, queueCounterexampleImage,
          queueCounterexampleStartPending, canonicalizeEvents,
          insertEvents, insertEvent, hreadyLater] using hother
      rcases hcases with rfl | rfl <;> decide
  · simpa [queueCounterexampleRoundStart] using htransition
  · change FreshEventKeys
      (tinyQueueTransitionResult false queueCounterexampleNode
        queueCounterexampleArrival
        (queueCounterexampleMachine.localState
          queueCounterexampleNode)).children _
    rw [hchildren]
    simp [FreshEventKeys]
  · simpa [queueCounterexampleRoundStart,
      queueCounterexampleRoundMiddle] using hallocates
  · simpa [queueCounterexampleRoundStart,
      queueCounterexampleRoundMiddle] using happliesRound
  · change ChildDescriptorsAvailable queueCounterexampleImage
      queueCounterexampleNode.id
      ((queueCounterexampleAfterArrival false).packetStore
        queueCounterexampleNode)
      (tinyQueueTransitionResult false queueCounterexampleNode
        queueCounterexampleArrival
        (queueCounterexampleMachine.localState
          queueCounterexampleNode)).children
    rw [hchildren]
    intro reference hreference
    simp at hreference
  · simpa [queueCounterexampleRoundStart,
      queueCounterexampleRoundMiddle, hchildren] using hpending
  · simpa [queueCounterexampleRoundStart,
      queueCounterexampleRoundMiddle, hchildren] using hemissions
  · simpa [queueCounterexampleRoundMiddle] using
      roundReferencesHeld_empty
        (queueCounterexampleAfterArrival false) hafterWellFormed
  · change RemoteEnvelopesFromStore queueCounterexampleImage
      queueCounterexampleNode.id
      ((queueCounterexampleAfterArrival false).packetStore
        queueCounterexampleNode)
      (remoteChildren queueCounterexampleNode.id
        (tinyQueueTransitionResult false queueCounterexampleNode
          queueCounterexampleArrival
          (queueCounterexampleMachine.localState
            queueCounterexampleNode)).children) []
    rw [hchildren]
    trivial
  · simp [queueCounterexampleRoundStart,
      queueCounterexampleRoundMiddle]
  · intro other _ _
    simp [queueCounterexampleRoundStart,
      queueCounterexampleRoundMiddle]

private theorem queueCounterexample_ready_localRoundStep :
    LocalRoundStep queueCounterexampleImage (tinyQueueTransition false)
      queueCounterexampleRoundTwoBounds queueCounterexampleNode
      queueCounterexampleRoundMiddle queueCounterexampleReady
      queueCounterexampleAfterReadyDrain := by
  have havailable := (queueCounterexample_ready_step false).1
  rcases havailable with
    ⟨hevent, node, hnode, result, htarget, htransition, _,
      hallocates, happlies, _, _, hpending, hemissions⟩
  have hnodeEq : node = queueCounterexampleNode := by
    apply node_eq_of_unique_ids
      queueCounterexample_static.1 hnode
      (by simp [queueCounterexampleNode, queueCounterexampleImage])
    simpa [queueCounterexampleNode, queueCounterexampleReady] using
      htarget.symm
  subst node
  have hresultEq :
      result =
        tinyQueueTransitionResult false queueCounterexampleNode
          queueCounterexampleReady
          ((queueCounterexampleAfterArrival false).localState
            queueCounterexampleNode) :=
    htransition.2.2.2.2
  subst result
  rcases happlies with
    ⟨hlocal, _, _, houtput, hconsumptions, hincrements⟩
  have happliesRound :
      AppliesTransitionResult queueCounterexampleImage
        queueCounterexampleNode queueCounterexampleReady
        (tinyQueueTransitionResult false queueCounterexampleNode
          queueCounterexampleReady
          ((queueCounterexampleAfterArrival false).localState
            queueCounterexampleNode))
        (queueCounterexampleAfterArrival false)
        queueCounterexampleAfterReadyDrainMachine := by
    refine ⟨?_, ?_, ?_, ?_, hconsumptions, hincrements⟩
    · simpa [queueCounterexampleAfterReadyDrainMachine] using hlocal
    · simp [queueCounterexampleAfterReadyDrainMachine]
    · intro other hother hotherId
      have hotherNe : other ≠ queueCounterexampleNode := by
        intro heq
        subst other
        exact hotherId rfl
      constructor
      · simp [queueCounterexampleAfterReadyDrainMachine,
          materializeScalarResult, hotherNe]
      · simp [queueCounterexampleAfterReadyDrainMachine, hotherId]
    · simpa [queueCounterexampleAfterReadyDrainMachine] using houtput
  have hchildren := queueCounterexample_ready_children
  have hcpuStore :
      queueCounterexampleAfterReadyDrainMachine.packetStore
          queueCounterexampleNode =
        [ownedReferenceFixtureEntry 12
          [(ownedEnvelopeReference
              queueCounterexampleRemoteEnvelope).owner,
            (ownedEventReference queueCounterexampleImage
              queueCounterexampleCompletion).owner,
            (ownedInServiceReference queueCounterexampleImage
              queueCounterexampleNode.id 12).owner]] := by
    decide
  have hsourceCoherent :
      DescriptorStoreCoherent queueCounterexampleImage
        (queueCounterexampleAfterReadyDrainMachine.packetStore
          queueCounterexampleNode) := by
    rw [hcpuStore]
    simp [DescriptorStoreCoherent, DescriptorStoreSorted,
      ownedReferenceFixtureEntry, ownedEnvelopeReference,
      ownedEventReference, ownedInServiceReference,
      queueCounterexampleRemoteEnvelope, queueCounterexampleCompletion,
      queueCounterexampleImage, queueCounterexampleDescriptor]
  have hroundHeld :
      RoundReferencesHeld queueCounterexampleImage
        queueCounterexampleAfterReadyDrain := by
    intro owner howner
    simp [queueCounterexampleImage] at howner
    rcases howner with rfl | rfl | rfl
    all_goals
      decide
  refine
    ⟨by simp [queueCounterexampleNode, queueCounterexampleImage],
      ?_,
      (tinyQueueTransitionResult false queueCounterexampleNode
        queueCounterexampleReady
        (queueCounterexampleRoundMiddle.machine.localState
          queueCounterexampleNode)),
      ?_, ?_, ?_, ?_, hsourceCoherent, ?_, ?_, ?_,
      hroundHeld, [queueCounterexampleRemoteEnvelope],
      ?_, ?_, ?_⟩
  · refine ⟨?_, ?_, ?_⟩
    · simpa [queueCounterexampleRoundMiddle] using hevent
    · constructor
      · rfl
      · decide
    · intro other hother _
      have heq : other = queueCounterexampleReady := by
        change other ∈
          (queueCounterexampleAfterArrival false).pending at hother
        rw [queueCounterexample_afterArrival_pending] at hother
        simpa using hother
      subst other
      exact EventKey.le_refl _
  · simpa [queueCounterexampleRoundMiddle] using htransition
  · intro child hchild other hother heq
    change child ∈
      (tinyQueueTransitionResult false queueCounterexampleNode
        queueCounterexampleReady
        ((queueCounterexampleAfterArrival false).localState
          queueCounterexampleNode)).children at hchild
    rw [queueCounterexample_ready_children] at hchild
    have hotherEq : other = queueCounterexampleReady := by
      change other ∈
        (queueCounterexampleAfterArrival false).pending at hother
      rw [queueCounterexample_afterArrival_pending] at hother
      simpa using hother
    subst other
    rcases List.mem_cons.mp hchild with hchildEq | htail
    · subst child
      exact
        (by decide :
          queueCounterexampleCompletion.key ≠
            queueCounterexampleReady.key) heq
    · have hchildEq := List.mem_singleton.mp htail
      subst child
      exact
        (by decide :
          queueCounterexampleRemote.key ≠
            queueCounterexampleReady.key) heq
  · simpa [queueCounterexampleRoundMiddle,
      queueCounterexampleAfterReadyDrain,
      queueCounterexampleAfterReadyDrainMachine] using hallocates
  · simpa [queueCounterexampleRoundMiddle,
      queueCounterexampleAfterReadyDrain] using happliesRound
  · change ChildDescriptorsAvailable queueCounterexampleImage
      queueCounterexampleNode.id
      (queueCounterexampleAfterReadyDrainMachine.packetStore
        queueCounterexampleNode)
      (tinyQueueTransitionResult false queueCounterexampleNode
        queueCounterexampleReady
        ((queueCounterexampleAfterArrival false).localState
          queueCounterexampleNode)).children
    rw [queueCounterexample_ready_children]
    unfold ChildDescriptorsAvailable
    rw [hcpuStore]
    decide
  · simp [queueCounterexampleRoundMiddle,
      queueCounterexampleAfterReadyDrain,
      queueCounterexampleAfterReadyDrainMachine,
      queueCounterexample_ready_children,
      localChildren, queueCounterexampleNode,
      queueCounterexampleCompletion, queueCounterexampleRemote]
  · simpa [queueCounterexampleRoundMiddle,
      queueCounterexampleAfterReadyDrain,
      queueCounterexampleAfterReadyDrainMachine] using hemissions
  · change RemoteEnvelopesFromStore queueCounterexampleImage
      queueCounterexampleNode.id
      (queueCounterexampleAfterReadyDrainMachine.packetStore
        queueCounterexampleNode)
      (remoteChildren queueCounterexampleNode.id
        (tinyQueueTransitionResult false queueCounterexampleNode
          queueCounterexampleReady
          ((queueCounterexampleAfterArrival false).localState
            queueCounterexampleNode)).children)
      [queueCounterexampleRemoteEnvelope]
    rw [queueCounterexample_ready_children]
    rw [hcpuStore]
    simp [RemoteEnvelopesFromStore, remoteChildren,
      queueCounterexampleNode, queueCounterexampleCompletion,
      queueCounterexampleRemote, queueCounterexampleRemoteEnvelope,
      ownedReferenceFixtureEntry, ownedEnvelopeReference,
      RemoteEnvelope.Coherent, queueCounterexampleImage,
      queueCounterexampleDescriptor, ownedReferenceCount]
  · simp [queueCounterexampleRoundMiddle,
      queueCounterexampleAfterReadyDrain, queueCounterexampleNode]
  · intro other _ hotherId
    simp [queueCounterexampleRoundMiddle,
      queueCounterexampleAfterReadyDrain, hotherId]

private theorem queueCounterexample_roundOne_drain :
    SequentialRoundDrain queueCounterexampleImage
      (tinyQueueTransition false) queueCounterexampleRoundOneBounds
      queueCounterexampleRoundStart [queueCounterexampleArrival]
      queueCounterexampleRoundMiddle := by
  have hsourceDone :
      SequentialDrainLP queueCounterexampleImage
        (tinyQueueTransition false) queueCounterexampleRoundOneBounds
        queueCounterexampleSourceNode queueCounterexampleRoundStart []
        queueCounterexampleRoundStart := by
    apply SequentialDrainLP.done
    intro event hevent heligible
    rw [show queueCounterexampleRoundStart.machine.pending =
      [queueCounterexampleArrival, queueCounterexampleReady] by
        exact queueCounterexample_initial_pending] at hevent
    rcases List.mem_cons.mp hevent with rfl | htail
    · exact
        (show queueCounterexampleArrival.target ≠
          queueCounterexampleSourceNode.id by decide) heligible.1
    · have heq := List.mem_singleton.mp htail
      subst event
      exact
        (show queueCounterexampleReady.target ≠
          queueCounterexampleSourceNode.id by decide) heligible.1
  have hswitchDone :
      SequentialDrainLP queueCounterexampleImage
        (tinyQueueTransition false) queueCounterexampleRoundOneBounds
        queueCounterexampleNode queueCounterexampleRoundMiddle []
        queueCounterexampleRoundMiddle := by
    apply SequentialDrainLP.done
    intro event hevent heligible
    change event ∈ (queueCounterexampleAfterArrival false).pending at hevent
    rw [queueCounterexample_afterArrival_pending] at hevent
    have heq := List.mem_singleton.mp hevent
    subst event
    exact
      (show ¬ belowBound queueCounterexampleRoundOneBounds
        queueCounterexampleReady by decide) heligible.2
  have hswitchDrain :
      SequentialDrainLP queueCounterexampleImage
        (tinyQueueTransition false) queueCounterexampleRoundOneBounds
        queueCounterexampleNode queueCounterexampleRoundStart
        [queueCounterexampleArrival] queueCounterexampleRoundMiddle :=
    .step queueCounterexample_arrival_localRoundStep hswitchDone
  have hterminalDone :
      SequentialDrainLP queueCounterexampleImage
        (tinyQueueTransition false) queueCounterexampleRoundOneBounds
        queueCounterexampleTerminalNode queueCounterexampleRoundMiddle []
        queueCounterexampleRoundMiddle := by
    apply SequentialDrainLP.done
    intro event hevent heligible
    change event ∈ (queueCounterexampleAfterArrival false).pending at hevent
    rw [queueCounterexample_afterArrival_pending] at hevent
    have heq := List.mem_singleton.mp hevent
    subst event
    exact
      (show queueCounterexampleReady.target ≠
        queueCounterexampleTerminalNode.id by decide) heligible.1
  refine ⟨[0, 1, 2], by decide, ?_⟩
  simpa using
    (DrainLPsInOrder.cons
      (by simp [queueCounterexampleSourceNode,
        queueCounterexampleImage])
      hsourceDone
      (DrainLPsInOrder.cons
        (by simp [queueCounterexampleNode, queueCounterexampleImage])
        hswitchDrain
        (DrainLPsInOrder.cons
          (by simp [queueCounterexampleTerminalNode,
            queueCounterexampleImage])
          hterminalDone
          (DrainLPsInOrder.nil queueCounterexampleRoundMiddle))))

private theorem queueCounterexample_roundTwo_drain :
    SequentialRoundDrain queueCounterexampleImage
      (tinyQueueTransition false) queueCounterexampleRoundTwoBounds
      queueCounterexampleRoundMiddle [queueCounterexampleReady]
      queueCounterexampleAfterReadyDrain := by
  have hsourceDone :
      SequentialDrainLP queueCounterexampleImage
        (tinyQueueTransition false) queueCounterexampleRoundTwoBounds
        queueCounterexampleSourceNode queueCounterexampleRoundMiddle []
        queueCounterexampleRoundMiddle := by
    apply SequentialDrainLP.done
    intro event hevent heligible
    change event ∈ (queueCounterexampleAfterArrival false).pending at hevent
    rw [queueCounterexample_afterArrival_pending] at hevent
    have heq := List.mem_singleton.mp hevent
    subst event
    exact
      (show queueCounterexampleReady.target ≠
        queueCounterexampleSourceNode.id by decide) heligible.1
  have hswitchDone :
      SequentialDrainLP queueCounterexampleImage
        (tinyQueueTransition false) queueCounterexampleRoundTwoBounds
        queueCounterexampleNode queueCounterexampleAfterReadyDrain []
        queueCounterexampleAfterReadyDrain := by
    apply SequentialDrainLP.done
    intro event hevent heligible
    change event ∈ queueCounterexampleAfterReadyDrainMachine.pending at hevent
    rw [queueCounterexample_afterReadyDrain_pending] at hevent
    have heq := List.mem_singleton.mp hevent
    subst event
    exact
      (show ¬ belowBound queueCounterexampleRoundTwoBounds
        queueCounterexampleCompletion by decide) heligible.2
  have hswitchDrain :
      SequentialDrainLP queueCounterexampleImage
        (tinyQueueTransition false) queueCounterexampleRoundTwoBounds
        queueCounterexampleNode queueCounterexampleRoundMiddle
        [queueCounterexampleReady] queueCounterexampleAfterReadyDrain :=
    .step queueCounterexample_ready_localRoundStep hswitchDone
  have hterminalDone :
      SequentialDrainLP queueCounterexampleImage
        (tinyQueueTransition false) queueCounterexampleRoundTwoBounds
        queueCounterexampleTerminalNode
        queueCounterexampleAfterReadyDrain [] queueCounterexampleAfterReadyDrain := by
    apply SequentialDrainLP.done
    intro event hevent heligible
    change event ∈ queueCounterexampleAfterReadyDrainMachine.pending at hevent
    rw [queueCounterexample_afterReadyDrain_pending] at hevent
    have heq := List.mem_singleton.mp hevent
    subst event
    exact
      (show queueCounterexampleCompletion.target ≠
        queueCounterexampleTerminalNode.id by decide) heligible.1
  refine ⟨[0, 1, 2], by decide, ?_⟩
  simpa using
    (DrainLPsInOrder.cons
      (by simp [queueCounterexampleSourceNode,
        queueCounterexampleImage])
      hsourceDone
      (DrainLPsInOrder.cons
        (by simp [queueCounterexampleNode, queueCounterexampleImage])
        hswitchDrain
        (DrainLPsInOrder.cons
          (by simp [queueCounterexampleTerminalNode,
            queueCounterexampleImage])
          hterminalDone
          (DrainLPsInOrder.nil queueCounterexampleAfterReadyDrain))))

private theorem queueCounterexample_roundOne_exchange :
    CompleteCanonicalExchange queueCounterexampleImage
      queueCounterexampleRoundMiddle queueCounterexampleRoundMiddle := by
  have hwellFormed := (queueCounterexample_arrival_step false).2
  have hheld :
      RoundReferencesHeld queueCounterexampleImage
        queueCounterexampleRoundMiddle := by
    simpa [queueCounterexampleRoundMiddle] using
      roundReferencesHeld_empty
        (queueCounterexampleAfterArrival false) hwellFormed
  refine ⟨[], ?_, by simp, by simp, ?_, ?_, rfl, rfl, rfl,
    rfl, rfl, rfl, rfl, rfl, hheld, ?_⟩
  · simp [flattenedOutboxes, queueCounterexampleImage,
      queueCounterexampleRoundMiddle]
  · intro node _ reference hreference
    simp at hreference
  · intro node _
    simp [queueCounterexampleRoundMiddle,
      installRemoteEnvelopesFor, consumeRemoteEnvelopesFor]
  · intro node _
    rfl

private theorem queueCounterexample_roundTwo_exchange :
    CompleteCanonicalExchange queueCounterexampleImage
      queueCounterexampleAfterReadyDrain queueCounterexampleRoundFinish := by
  have hfinishWellFormed := (queueCounterexample_ready_step false).2
  have hfinishHeld :
      RoundReferencesHeld queueCounterexampleImage
        queueCounterexampleRoundFinish := by
    simpa [queueCounterexampleRoundFinish] using
      roundReferencesHeld_empty
        (queueCounterexampleFinish false) hfinishWellFormed
  refine
    ⟨[queueCounterexampleRemoteEnvelope],
      ?_, by simp, ?_, ?_, ?_, ?_, rfl, rfl, rfl, rfl, rfl,
      rfl, rfl, hfinishHeld, ?_⟩
  · simp [flattenedOutboxes, queueCounterexampleImage,
      queueCounterexampleAfterReadyDrain, queueCounterexampleNode]
  · intro envelope henvelope
    have heq := List.mem_singleton.mp henvelope
    subst envelope
    rfl
  · intro node hnode
    simp [queueCounterexampleImage] at hnode
    rcases hnode with rfl | rfl | rfl
    all_goals decide
  · intro node hnode
    simp [queueCounterexampleImage] at hnode
    rcases hnode with rfl | rfl | rfl
    all_goals
      constructor
      · rfl
      · decide
  · decide
  · intro node _
    rfl

private def queueCounterexampleRoundOneCut (event : Event) : Prop :=
  event = queueCounterexampleArrival

private def queueCounterexampleRoundTwoCut (event : Event) : Prop :=
  event = queueCounterexampleReady

private theorem recordedReachable_nil_iff
    (startPending : List Event)
    (event : Event) :
    RecordedReachableEvent [] startPending event ↔
      event ∈ startPending := by
  constructor
  · intro hreachable
    cases hreachable with
    | seed hmember =>
        exact hmember
    | child _ hedge =>
        simp [RecordedEmissionEdge] at hedge
  · intro hmember
    exact .seed hmember

private theorem noRecordedCausalBefore_nil
    (parent child : Event) :
    ¬ RecordedCausalBefore [] parent child := by
  intro hcausal
  induction hcausal with
  | direct hedge =>
      simp [RecordedEmissionEdge] at hedge
  | tail hedge _ _ =>
      simp [RecordedEmissionEdge] at hedge

private theorem queueCounterexample_roundTwo_reachable_cases
    (event : Event)
    (hreachable :
      RecordedReachableEvent queueCounterexampleRoundTwoEmissions
        [queueCounterexampleReady] event) :
    event = queueCounterexampleReady ∨
      event = queueCounterexampleCompletion ∨
        event = queueCounterexampleRemote := by
  induction hreachable with
  | seed hmember =>
      exact Or.inl (List.mem_singleton.mp hmember)
  | child _ hedge _ =>
      simp [RecordedEmissionEdge,
        queueCounterexampleRoundTwoEmissions] at hedge
      rcases hedge with ⟨_, rfl⟩ | ⟨_, rfl⟩
      · exact Or.inr (Or.inl rfl)
      · exact Or.inr (Or.inr rfl)

private theorem queueCounterexample_roundTwo_causal_target_cases
    {parent child : Event}
    (hcausal :
      RecordedCausalBefore queueCounterexampleRoundTwoEmissions
        parent child) :
    child = queueCounterexampleCompletion ∨
      child = queueCounterexampleRemote := by
  induction hcausal with
  | direct hedge =>
      simp [RecordedEmissionEdge,
        queueCounterexampleRoundTwoEmissions] at hedge
      rcases hedge with ⟨_, rfl⟩ | ⟨_, rfl⟩
      · exact Or.inl rfl
      · exact Or.inr rfl
  | tail _ _ ih =>
      exact ih

private theorem queueCounterexample_noCausalBeforeReady
    (parent : Event) :
    ¬ RecordedCausalBefore queueCounterexampleRoundTwoEmissions
      parent queueCounterexampleReady := by
  intro hcausal
  have hcases :=
    queueCounterexample_roundTwo_causal_target_cases hcausal
  rcases hcases with hcompletion | hremote
  · exact
      (show queueCounterexampleReady ≠
        queueCounterexampleCompletion by decide) hcompletion
  · exact
      (show queueCounterexampleReady ≠
        queueCounterexampleRemote by decide) hremote

private theorem queueCounterexample_roundOne_cut :
    DrainedConsistentCut []
      [queueCounterexampleArrival, queueCounterexampleReady]
      [queueCounterexampleArrival]
      queueCounterexampleRoundOneBounds
      queueCounterexampleRoundOneCut := by
  refine ⟨⟨?_, ?_, ?_⟩, ?_, ?_⟩
  · intro event hcut
    subst event
    exact .seed (by simp)
  · intro later hlater earlier hearlier _ hkey
    subst later
    have hcases :
        earlier = queueCounterexampleArrival ∨
          earlier = queueCounterexampleReady := by
      have hmember :=
        (recordedReachable_nil_iff
          [queueCounterexampleArrival, queueCounterexampleReady]
          earlier).mp hearlier
      simpa using hmember
    rcases hcases with rfl | rfl
    · rfl
    · have hnot :
          ¬ queueCounterexampleReady.key <
            queueCounterexampleArrival.key := by
        decide
      exact (hnot hkey).elim
  · intro parent child hcausal _
    exact (noRecordedCausalBefore_nil parent child hcausal).elim
  · intro event
    simp [queueCounterexampleRoundOneCut]
  · intro event
    constructor
    · intro hcut
      subst event
      exact ⟨.seed (by simp), by decide⟩
    · rintro ⟨hreachable, hbelow⟩
      have hmember :=
        (recordedReachable_nil_iff
          [queueCounterexampleArrival, queueCounterexampleReady]
          event).mp hreachable
      have hcases :
          event = queueCounterexampleArrival ∨
            event = queueCounterexampleReady := by
        simpa using hmember
      rcases hcases with rfl | rfl
      · rfl
      · exact
          ((show ¬ belowBound queueCounterexampleRoundOneBounds
            queueCounterexampleReady by decide) hbelow).elim

private theorem queueCounterexample_roundTwo_cut :
    DrainedConsistentCut queueCounterexampleRoundTwoEmissions
      [queueCounterexampleReady]
      [queueCounterexampleReady]
      queueCounterexampleRoundTwoBounds
      queueCounterexampleRoundTwoCut := by
  refine ⟨⟨?_, ?_, ?_⟩, ?_, ?_⟩
  · intro event hcut
    subst event
    exact .seed (by simp)
  · intro later hlater earlier hearlier htarget hkey
    subst later
    have hcases :=
      queueCounterexample_roundTwo_reachable_cases earlier hearlier
    rcases hcases with rfl | rfl | rfl
    · rfl
    · exact
        ((show ¬ queueCounterexampleCompletion.key <
          queueCounterexampleReady.key by decide) hkey).elim
    · exact
        ((show queueCounterexampleRemote.target ≠
          queueCounterexampleReady.target by decide) htarget).elim
  · intro parent child hcausal hcut
    subst child
    exact
      (queueCounterexample_noCausalBeforeReady parent hcausal).elim
  · intro event
    simp [queueCounterexampleRoundTwoCut]
  · intro event
    constructor
    · intro hcut
      subst event
      exact ⟨.seed (by simp), by decide⟩
    · rintro ⟨hreachable, hbelow⟩
      have hcases :=
        queueCounterexample_roundTwo_reachable_cases event hreachable
      rcases hcases with rfl | rfl | rfl
      · rfl
      · exact
          ((show ¬ belowBound queueCounterexampleRoundTwoBounds
            queueCounterexampleCompletion by decide) hbelow).elim
      · exact
          ((show ¬ belowBound queueCounterexampleRoundTwoBounds
            queueCounterexampleRemote by decide) hbelow).elim

theorem queueCounterexample_results_differ :
    ¬ SameMachineResult queueCounterexampleImage
      (queueCounterexampleFinish false)
      (queueCounterexampleFinish true) := by
  intro hsame
  have hlocal :=
    hsame.1 queueCounterexampleNode
      (by simp [queueCounterexampleNode, queueCounterexampleImage])
  have hqueue :=
    congrArg
      (fun state => state.serviceQueue)
      hlocal
  have himpossible : ([] : List PayloadId) = [21] := by
    simpa [queueCounterexampleFinish, queueCounterexampleAfterArrival,
      materializeScalarResult, tinyQueueTransitionResult,
      tinyQueueArrivalState, tinyQueueReservePrivately,
      tinyQueueSelectedPacket, tinyQueueReadyState,
      queueCounterexampleArrival, queueCounterexampleReady,
      queueCounterexampleNode, queueCounterexampleMachine,
      queueCounterexampleLocalState, queueCounterexamplePrivateState,
      queueCounterexampleImage] using hqueue
  simp at himpossible

private theorem queueCounterexample_constant_bounds
    (haccepted :
      AcceptedModel queueCounterexampleImage (tinyQueueTransition true)) :
    ConstantGlobalBoundsValid
      queueCounterexampleImage
      (tinyQueueTransition true)
      queueCounterexampleMachine := by
  let start : RoundState TinyQueueStateFamily :=
    { machine := queueCounterexampleMachine
      outboxes := fun _ => [] }
  apply constantGlobalBoundsValid
    queueCounterexampleImage (tinyQueueTransition true)
    haccepted start
  rcases queueCounterexample_initial_machine with
    ⟨_, _, _, _, _, _, _, _, _, hwellFormed⟩
  refine ⟨?_, hwellFormed, ?_⟩
  · intro node hnode
    rfl
  · intro node hnode
    simp only [start, RoundState.machine, RoundState.outboxes,
      List.map_nil, List.append_nil]
    exact hwellFormed.2.2.2.2.2.1 node hnode

theorem tinyQueue_accepted_model (eager : Bool) :
    AcceptedModel queueCounterexampleImage (tinyQueueTransition eager) :=
  ⟨queueCounterexample_static,
    queueCounterexample_initial_roles,
    tinyQueueTransition_axioms eager,
    tinyQueueTransition_enabledOnReachable eager⟩

private theorem queueCounterexample_roundOne :
    SafeHorizonRound queueCounterexampleImage
      (tinyQueueTransition false) queueCounterexampleRoundOneBounds
      queueCounterexampleRoundOneCut queueCounterexampleRoundStart
      [queueCounterexampleArrival] queueCounterexampleRoundMiddle := by
  have hinitial := queueCounterexample_initial_machine
  have hstart :
      PostExchangeStart queueCounterexampleImage
        queueCounterexampleRoundStart := by
    simpa [queueCounterexampleRoundStart] using
      postExchangeStart_empty queueCounterexampleMachine
        hinitial.2.2.2.2.2.2.2.2.2
  have hglobal :
      globalHorizon queueCounterexampleImage
        queueCounterexampleRoundStart.machine = 6 := by
    decide
  have hvalid :
      BoundFamilyValid (tinyQueueTransition false)
        queueCounterexampleRoundStart.machine
        queueCounterexampleRoundOneBounds := by
    have hconstant :=
      constantGlobalBoundsValid queueCounterexampleImage
        (tinyQueueTransition false) (tinyQueue_accepted_model false)
        queueCounterexampleRoundStart hstart
    simpa [ConstantGlobalBoundsValid, constantGlobalBounds,
      queueCounterexampleRoundOneBounds, hglobal] using hconstant
  have hprogress :
      BoundFamilyMakesProgress queueCounterexampleImage
        queueCounterexampleRoundStart.machine
        queueCounterexampleRoundOneBounds := by
    intro _
    exact
      ⟨queueCounterexampleArrival,
        by
          change queueCounterexampleArrival ∈
            queueCounterexampleMachine.pending
          rw [queueCounterexample_initial_pending]
          simp,
        by decide⟩
  have hwithin :
      BoundFamilyWithinStop queueCounterexampleImage
        queueCounterexampleRoundOneBounds := by
    intro node _
    change 6 ≤ 11
    decide
  refine ⟨hstart, hvalid, hprogress, hwithin,
    queueCounterexampleRoundMiddle,
    queueCounterexample_roundOne_drain, [], ?_, ?_,
    queueCounterexample_roundOne_exchange, ?_⟩
  · unfold RoundEmissionDelta
    decide
  · simpa [queueCounterexampleRoundStart,
      queueCounterexampleRoundMiddle] using
      queueCounterexample_roundOne_cut
  · simpa [queueCounterexampleRoundMiddle] using
      postExchangeStart_empty
        (queueCounterexampleAfterArrival false)
        (queueCounterexample_arrival_step false).2

private theorem queueCounterexample_roundTwo :
    SafeHorizonRound queueCounterexampleImage
      (tinyQueueTransition false) queueCounterexampleRoundTwoBounds
      queueCounterexampleRoundTwoCut queueCounterexampleRoundMiddle
      [queueCounterexampleReady] queueCounterexampleRoundFinish := by
  have hstart :
      PostExchangeStart queueCounterexampleImage
        queueCounterexampleRoundMiddle := by
    simpa [queueCounterexampleRoundMiddle] using
      postExchangeStart_empty
        (queueCounterexampleAfterArrival false)
        (queueCounterexample_arrival_step false).2
  have hglobal :
      globalHorizon queueCounterexampleImage
        queueCounterexampleRoundMiddle.machine = 11 := by
    decide
  have hvalid :
      BoundFamilyValid (tinyQueueTransition false)
        queueCounterexampleRoundMiddle.machine
        queueCounterexampleRoundTwoBounds := by
    have hconstant :=
      constantGlobalBoundsValid queueCounterexampleImage
        (tinyQueueTransition false) (tinyQueue_accepted_model false)
        queueCounterexampleRoundMiddle hstart
    simpa [ConstantGlobalBoundsValid, constantGlobalBounds,
      queueCounterexampleRoundTwoBounds, hglobal] using hconstant
  have hprogress :
      BoundFamilyMakesProgress queueCounterexampleImage
        queueCounterexampleRoundMiddle.machine
        queueCounterexampleRoundTwoBounds := by
    intro _
    exact
      ⟨queueCounterexampleReady,
        by
          change queueCounterexampleReady ∈
            (queueCounterexampleAfterArrival false).pending
          rw [queueCounterexample_afterArrival_pending]
          simp,
        by decide⟩
  have hwithin :
      BoundFamilyWithinStop queueCounterexampleImage
        queueCounterexampleRoundTwoBounds := by
    intro node _
    change 11 ≤ 11
    decide
  refine ⟨hstart, hvalid, hprogress, hwithin,
    queueCounterexampleAfterReadyDrain,
    queueCounterexample_roundTwo_drain,
    queueCounterexampleRoundTwoEmissions, ?_, ?_,
    queueCounterexample_roundTwo_exchange, ?_⟩
  · unfold RoundEmissionDelta
    decide
  · simpa [queueCounterexampleRoundMiddle] using
      queueCounterexample_roundTwo_cut
  · simpa [queueCounterexampleRoundFinish] using
      postExchangeStart_empty
        (queueCounterexampleFinish false)
        (queueCounterexample_ready_step false).2

private theorem queueCounterexample_canonicalThroughStop :
    CanonicalSerialThroughStop queueCounterexampleImage
      (tinyQueueTransition false) queueCounterexampleMachine
      [queueCounterexampleArrival, queueCounterexampleReady]
      (queueCounterexampleFinish false) := by
  have harrivalLeast :
      IsLeastEligible
        (withinInclusiveStop queueCounterexampleImage.stopTimeNs)
        queueCounterexampleArrival queueCounterexampleMachine.pending := by
    refine ⟨?_, by decide, ?_⟩
    · rw [queueCounterexample_initial_pending]
      simp
    · intro other hother _
      rw [queueCounterexample_initial_pending] at hother
      rcases List.mem_cons.mp hother with rfl | htail
      · exact EventKey.le_refl _
      · have heq := List.mem_singleton.mp htail
        subst other
        decide
  have hreadyLeast :
      IsLeastEligible
        (withinInclusiveStop queueCounterexampleImage.stopTimeNs)
        queueCounterexampleReady
        (queueCounterexampleAfterArrival false).pending := by
    refine ⟨?_, by decide, ?_⟩
    · rw [queueCounterexample_afterArrival_pending]
      simp
    · intro other hother _
      rw [queueCounterexample_afterArrival_pending] at hother
      have heq := List.mem_singleton.mp hother
      subst other
      exact EventKey.le_refl _
  refine ⟨.step
    ⟨harrivalLeast, (queueCounterexample_arrival_step false).1⟩
    (.step
      ⟨hreadyLeast, (queueCounterexample_ready_step false).1⟩
      (.refl _)), ?_⟩
  intro event hevent hwithin
  rw [queueCounterexample_finish_pending] at hevent
  rcases List.mem_cons.mp hevent with rfl | htail
  · exact
      (show ¬ withinInclusiveStop
        queueCounterexampleImage.stopTimeNs
        queueCounterexampleRemote by decide) hwithin
  · have heq := List.mem_singleton.mp htail
    subst event
    exact
      (show ¬ withinInclusiveStop
        queueCounterexampleImage.stopTimeNs
        queueCounterexampleCompletion by decide) hwithin

/--
Concrete non-vacuity witness for the F2/F3 premise surface. Two actual constant-bound rounds start
from the accepted three-LP initial machine. The second switch step leaves a local completion and a
descriptor-carrying remote envelope; canonical exchange transfers that envelope to the terminal
host and produces exactly the canonical scalar endpoint through the inclusive stop.
-/
theorem tinyQueue_multiRound_heterogeneous_witness :
    AcceptedModel queueCounterexampleImage (tinyQueueTransition false) ∧
      CompleteActualServiceStartDiscipline (tinyQueueTransition false) ∧
      ∃ start middle afterReadyDrain finish :
          RoundState TinyQueueStateFamily,
        InitialMachine queueCounterexampleImage start.machine ∧
          PostExchangeStart queueCounterexampleImage start ∧
          ReachablePostExchangeStart queueCounterexampleImage
            (tinyQueueTransition false) start ∧
          SafeHorizonRound queueCounterexampleImage
            (tinyQueueTransition false) (fun _ => 6)
            (fun event => event = queueCounterexampleArrival)
            start [queueCounterexampleArrival] middle ∧
          ReachablePostExchangeStart queueCounterexampleImage
            (tinyQueueTransition false) middle ∧
          SequentialRoundDrain queueCounterexampleImage
            (tinyQueueTransition false) (fun _ => 11)
            middle [queueCounterexampleReady] afterReadyDrain ∧
          afterReadyDrain.machine.pending =
            [queueCounterexampleCompletion] ∧
          afterReadyDrain.outboxes 1 =
            [queueCounterexampleRemoteEnvelope] ∧
          CompleteCanonicalExchange queueCounterexampleImage
            afterReadyDrain finish ∧
          SafeHorizonRound queueCounterexampleImage
            (tinyQueueTransition false) (fun _ => 11)
            (fun event => event = queueCounterexampleReady)
            middle [queueCounterexampleReady] finish ∧
          SafeHorizonRounds queueCounterexampleImage
            (tinyQueueTransition false) start
            [(fun _ => 6), (fun _ => 11)] finish ∧
          StoppedThroughInclusiveStop queueCounterexampleImage finish ∧
          CanonicalSerialThroughStop queueCounterexampleImage
            (tinyQueueTransition false) start.machine
            [queueCounterexampleArrival, queueCounterexampleReady]
            finish.machine ∧
          finish.machine = queueCounterexampleFinish false ∧
          (∃ switchResult,
            tinyQueueTransition false
              { id := 1, kind := .switch, stateSlot := 0 }
              queueCounterexampleReady
              (middle.machine.localState
                { id := 1, kind := .switch, stateSlot := 0 })
              switchResult) ∧
          ∃ hostResult,
            tinyQueueTransition false
              { id := 2, kind := .host, stateSlot := 1 }
              queueCounterexampleRemote
              (finish.machine.localState
                { id := 2, kind := .host, stateSlot := 1 })
              hostResult := by
  refine ⟨tinyQueue_accepted_model false,
    tinyQueue_complete_canonical,
    queueCounterexampleRoundStart,
    queueCounterexampleRoundMiddle,
    queueCounterexampleAfterReadyDrain,
    queueCounterexampleRoundFinish, ?_⟩
  have hinitial :
      InitialMachine queueCounterexampleImage
        queueCounterexampleRoundStart.machine := by
    simpa [queueCounterexampleRoundStart] using
      queueCounterexample_initial_machine
  have hstart :
      PostExchangeStart queueCounterexampleImage
        queueCounterexampleRoundStart :=
    queueCounterexample_roundOne.1
  have hreachableStart :
      ReachablePostExchangeStart queueCounterexampleImage
        (tinyQueueTransition false) queueCounterexampleRoundStart :=
    ⟨queueCounterexampleRoundStart, [], hinitial, hstart, .refl _⟩
  have hreachableMiddle :
      ReachablePostExchangeStart queueCounterexampleImage
        (tinyQueueTransition false) queueCounterexampleRoundMiddle :=
    reachablePostExchangeStartAfterRound_proved
      queueCounterexampleImage (tinyQueueTransition false)
      queueCounterexampleRoundOneBounds queueCounterexampleRoundOneCut
      queueCounterexampleRoundStart [queueCounterexampleArrival]
      queueCounterexampleRoundMiddle hreachableStart
      queueCounterexample_roundOne
  have hrounds :
      SafeHorizonRounds queueCounterexampleImage
        (tinyQueueTransition false) queueCounterexampleRoundStart
        [queueCounterexampleRoundOneBounds,
          queueCounterexampleRoundTwoBounds]
        queueCounterexampleRoundFinish :=
    .step queueCounterexample_roundOne
      (.step queueCounterexample_roundTwo (.refl _))
  refine ⟨hinitial, hstart, hreachableStart,
    queueCounterexample_roundOne, hreachableMiddle,
    queueCounterexample_roundTwo_drain,
    queueCounterexample_afterReadyDrain_pending, rfl,
    queueCounterexample_roundTwo_exchange,
    queueCounterexample_roundTwo, hrounds, ?_,
    ?_, rfl, ?_, ?_⟩
  · intro event hevent hwithin
    change event ∈ (queueCounterexampleFinish false).pending at hevent
    rw [queueCounterexample_finish_pending] at hevent
    rcases List.mem_cons.mp hevent with rfl | htail
    · exact
        (show ¬ withinInclusiveStop
          queueCounterexampleImage.stopTimeNs
          queueCounterexampleRemote by decide) hwithin
    · have heq := List.mem_singleton.mp htail
      subst event
      exact
        (show ¬ withinInclusiveStop
          queueCounterexampleImage.stopTimeNs
          queueCounterexampleCompletion by decide) hwithin
  · simpa [queueCounterexampleRoundStart,
      queueCounterexampleRoundFinish] using
      queueCounterexample_canonicalThroughStop
  · exact
      ⟨tinyQueueTransitionResult false queueCounterexampleNode
          queueCounterexampleReady
          ((queueCounterexampleAfterArrival false).localState
            queueCounterexampleNode),
        by
          simpa [queueCounterexampleNode,
            queueCounterexampleRoundMiddle] using
            queueCounterexample_switch_dispatch⟩
  · exact
      ⟨tinyQueueTransitionResult false queueCounterexampleTerminalNode
          queueCounterexampleRemote
          ((queueCounterexampleFinish false).localState
            queueCounterexampleTerminalNode),
        by
          simpa [queueCounterexampleTerminalNode,
            queueCounterexampleRoundFinish] using
            queueCounterexample_host_dispatch⟩

theorem reachableEagerSelectionCountermodel_proved :
    ReachableEagerSelectionCountermodel := by
  refine ⟨by
      unfold EagerSelectionCounterexampleShape
      native_decide,
    by
      unfold eagerSelectionPrivateSmugglingCheck
      native_decide,
    tinyQueue_accepted_model false,
    tinyQueue_accepted_model true,
    queueCounterexample_initial_machine,
    queueCounterexample_constant_bounds (tinyQueue_accepted_model true),
    tinyQueue_complete_canonical,
    tinyQueue_actual_service_start true,
    tinyQueue_decision_trace true,
    tinyQueue_decision_emissions true,
    tinyQueue_committed_nonpreemptive true,
    tinyQueue_eager_private_relevant,
    tinyQueue_complete_eager_fails,
    ?_⟩
  exact
    ⟨queueCounterexampleFinish false,
      queueCounterexampleFinish true,
      queueCounterexample_execution false,
      queueCounterexample_execution true,
      queueCounterexample_results_differ⟩

end DaysExecutor
