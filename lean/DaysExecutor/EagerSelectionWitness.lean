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
