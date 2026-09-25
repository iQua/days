import DaysExecutor.ExecutionSort

namespace DaysExecutor

/-- An allocated event absent from pending cannot be reintroduced by a scalar step. -/
theorem availableEventStep_preserves_absent_allocated
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (ghost : Event)
    (before after : MachineState State)
    (hallocated : ghost.key ∈ before.allocatedKeys)
    (habsent : ghost ∉ before.pending)
    (hstep : AvailableEventStep image transition event before after) :
    ghost ∉ after.pending ∧ ghost.key ∈ after.allocatedKeys := by
  rcases hstep with
    ⟨_, node, _, result, _, _, _, hallocates, _, _, _, hpending, _⟩
  have hfresh := hallocates.2.2.1
  have hallocatedAfter := hallocates.2.2.2.1
  constructor
  · intro hghost
    rw [hpending, mem_insertEvents_iff] at hghost
    rcases hghost with hchild | hremaining
    · exact (hfresh ghost hchild) hallocated
    · exact habsent (List.mem_of_mem_erase hremaining)
  · rw [hallocatedAfter]
    exact List.mem_append_left _ hallocated

/-- The processed event is absent immediately after its scalar step and remains allocated. -/
theorem availableEventStep_removes_processed
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (event : Event)
    (before after : MachineState State)
    (hbefore : MachineWellFormed image before)
    (hstep : AvailableEventStep image transition event before after) :
    event ∉ after.pending ∧ event.key ∈ after.allocatedKeys := by
  rcases hstep with
    ⟨hevent, node, _, result, _, _, _, hallocates, _, _, _, hpending, _⟩
  have hallocated := (hbefore.2.2.2.1 event hevent).1
  have hfresh := hallocates.2.2.1
  have hallocatedAfter := hallocates.2.2.2.1
  constructor
  · intro heventAfter
    rw [hpending, mem_insertEvents_iff] at heventAfter
    rcases heventAfter with hchild | herased
    · exact (hfresh event hchild) hallocated
    · exact (canonicalPending_nodup hbefore.1).not_mem_erase herased
  · rw [hallocatedAfter]
    exact List.mem_append_left _ hallocated

/-- An absent allocated event remains absent throughout an execution. -/
theorem executionInOrder_preserves_absent_allocated
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (ghost : Event)
    (before after : MachineState State)
    (events : List Event)
    (hbefore : MachineWellFormed image before)
    (hallocated : ghost.key ∈ before.allocatedKeys)
    (habsent : ghost ∉ before.pending)
    (hexecution :
      ExecutionInOrder image transition before events after) :
    ghost ∉ after.pending ∧ ghost.key ∈ after.allocatedKeys := by
  induction hexecution with
  | refl =>
      exact ⟨habsent, hallocated⟩
  | step first rest ih =>
      have hmiddle :=
        availableEventStep_preserves_machineWellFormed image transition
          hunique horacle hgenerated hdescriptors _ _ _ hbefore first
      have hfirst :=
        availableEventStep_preserves_absent_allocated image transition
          ghost _ _ hallocated habsent first
      exact ih hmiddle hfirst.2 hfirst.1

/-- Every event in a well-formed execution is absent from the final pending queue. -/
theorem executionInOrder_events_not_pending
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (before after : MachineState State)
    (events : List Event)
    (hbefore : MachineWellFormed image before)
    (hexecution :
      ExecutionInOrder image transition before events after) :
    ∀ event ∈ events, event ∉ after.pending := by
  induction hexecution with
  | refl =>
      simp
  | @step event before middle events after first rest ih =>
      have hmiddle :=
        availableEventStep_preserves_machineWellFormed image transition
          hunique horacle hgenerated hdescriptors event before middle
          hbefore first
      have hremoved :=
        availableEventStep_removes_processed image transition event
          before middle hbefore first
      have hheadAbsent :=
        executionInOrder_preserves_absent_allocated image transition
          hunique horacle hgenerated hdescriptors event middle after events
          hmiddle hremoved.2 hremoved.1 rest
      intro candidate hcandidate
      rcases List.mem_cons.mp hcandidate with rfl | htail
      · exact hheadAbsent.1
      · exact ih hmiddle candidate htail

/-- A pending event not named by an execution remains pending at its endpoint. -/
theorem pending_mem_of_not_mem_execution
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (candidate : Event)
    (before after : MachineState State)
    (events : List Event)
    (hbefore : MachineWellFormed image before)
    (hcandidate : candidate ∈ before.pending)
    (hnotExecuted : candidate ∉ events)
    (hexecution :
      ExecutionInOrder image transition before events after) :
    candidate ∈ after.pending := by
  induction hexecution with
  | refl =>
      exact hcandidate
  | @step event before middle events after first rest ih =>
      have hmiddle :=
        availableEventStep_preserves_machineWellFormed image transition
          hunique horacle hgenerated hdescriptors event before middle
          hbefore first
      have hne : candidate ≠ event := by
        intro heq
        exact hnotExecuted (by simp [heq])
      rcases first with
        ⟨_, node, _, result, _, _, _, _, _, _, _, hpending, _⟩
      have hmiddleMem : candidate ∈ middle.pending := by
        rw [hpending, mem_insertEvents_iff]
        right
        exact (canonicalPending_nodup hbefore.1).mem_erase_iff.mpr
          ⟨hne, hcandidate⟩
      have hnotTail : candidate ∉ events := by
        intro hmem
        exact hnotExecuted (List.mem_cons_of_mem event hmem)
      exact ih hmiddle hmiddleMem hnotTail

/--
A strictly key-ordered execution which exhausts its cut is exactly the canonical restricted
execution of that cut.
-/
theorem executionInOrder_to_canonicalRestricted
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (cut : Event → Prop)
    (before after : MachineState State)
    (events : List Event)
    (hbefore : MachineWellFormed image before)
    (hordered : KeyOrdered events)
    (hallCut : ∀ event ∈ events, cut event)
    (hnoFinal : NoEligibleEvent cut after.pending)
    (hexecution :
      ExecutionInOrder image transition before events after) :
    CanonicalSerialRestricted image transition cut before events after := by
  constructor
  · induction hexecution with
    | refl =>
        exact .refl _
    | @step event before middle events after first rest ih =>
        have hmiddle :=
          availableEventStep_preserves_machineWellFormed image transition
            hunique horacle hgenerated hdescriptors event before middle
            hbefore first
        have horderedParts := List.pairwise_cons.mp hordered
        have hleast : IsLeastEligible cut event before.pending := by
          refine ⟨first.1, hallCut event List.mem_cons_self, ?_⟩
          intro other hother hotherCut
          by_cases heq : other = event
          · subst other
            exact EventKey.le_refl event.key
          · have hotherExecuted : other ∈ event :: events := by
              by_cases hmem : other ∈ event :: events
              · exact hmem
              · exact False.elim (hnoFinal other
                  (pending_mem_of_not_mem_execution image transition
                    hunique horacle hgenerated hdescriptors other
                    before after (event :: events) hbefore hother hmem
                    (.step first rest))
                  hotherCut)
            rcases List.mem_cons.mp hotherExecuted with heq' | htail
            · exact False.elim (heq heq')
            · exact EventKey.lt_implies_le
                (horderedParts.1 other htail)
        exact .step ⟨hleast, first⟩
          (ih hmiddle horderedParts.2
            (fun candidate hmem =>
              hallCut candidate (List.mem_cons_of_mem event hmem))
            hnoFinal)
  · exact hnoFinal

end DaysExecutor
