import DaysExecutor.ScalarStep
import DaysExecutor.RoundMaterialization

namespace DaysExecutor

/-- Exact-reference holding is invariant under permutation of the requested owner bag. -/
theorem packetReferencesHeld_perm
    {left right : List OwnedPacketReference}
    {store : List PacketStoreEntry}
    (hperm : left.Perm right)
    (hheld : PacketReferencesHeld right store) :
    PacketReferencesHeld left store := by
  intro reference hreference
  rw [hperm.count_eq reference]
  exact hheld reference (hperm.subset hreference)

/-- Pending children of a well-formed machine have exactly one target-side pending owner. -/
theorem childReferencesAvailable_of_wellFormed
    (image : SimulationImage State)
    (horacle : DescriptorOracleWellFormed image)
    (machine : MachineState State)
    (children : List Event)
    (hwellFormed : MachineWellFormed image machine)
    (hchildren : ∀ child ∈ children, child ∈ machine.pending) :
    ChildReferencesAvailableAtTargets image machine children := by
  intro child hchild
  have hpending := hchildren child hchild
  rcases hwellFormed.2.2.2.1 child hpending with
    ⟨_, target, htarget, htargetEq, _⟩
  have howned :
      ownedEventReference image child ∈
        machineOwnedReferencesFor image machine target := by
    unfold machineOwnedReferencesFor
    apply List.mem_append_left
    apply List.mem_map.mpr
    exact ⟨child,
      List.mem_filter.mpr
        ⟨hpending, by simpa only [decide_eq_true_eq] using htargetEq⟩,
      rfl⟩
  have hnodup :=
    machineOwnedReferencesFor_nodup image machine hwellFormed
      target htarget horacle
  have hcount :
      (machineOwnedReferencesFor image machine target).count
        (ownedEventReference image child) = 1 := by
    rw [hnodup.count, if_pos howned]
  have hmatch := hwellFormed.2.2.2.2.2.1 target htarget
  refine ⟨target, htarget, htargetEq.symm, ?_⟩
  rw [← hmatch.1 (ownedEventReference image child) howned]
  exact hcount

/--
One CPU-local step has a scalar immediate-insertion counterpart and preserves the replay progress
invariant. This is the executable induction step for a complete safe-prefix drain.
-/
theorem localRoundStep_materializes_scalar
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (hdeterministic : TransitionDeterministic transition)
    (bounds : BoundFamily)
    (node : NodeDescriptor)
    (cpuBefore cpuAfter : RoundState State)
    (scalarBefore : MachineState State)
    (event : Event)
    (hprogress : RoundScalarProgress image cpuBefore scalarBefore)
    (hcpu :
      LocalRoundStep image transition bounds node cpuBefore event cpuAfter) :
    ∃ scalarAfter,
      AvailableEventStep image transition event scalarBefore scalarAfter ∧
      RoundScalarProgress image cpuAfter scalarAfter := by
  have hprogressSaved := hprogress
  have hcpuSaved := hcpu
  rcases hprogress with
    ⟨hscalarBeforeWellFormed, huniverse, hlocal, hsummary,
      hobserved, hdepartures, harrivals, hpendingCover, hcursors,
      hallocated, hemissions⟩
  rcases hcpu with
    ⟨hnode, hleast, result, htransition, hfreshUniverse,
      hcpuAllocates, hcpuApplies, hcpuSourceCoherent, _,
      hcpuPending, hcpuEmissions, hroundHeld, emittedRemote,
      hremoteFrom, hsourceOutbox, hotherOutboxes⟩
  rcases hcpuAllocates with
    ⟨hsequence, hchildKeys, hchildFreshAllocated, hallocatedAfter,
      hcursorNode, hcursorOther⟩
  rcases hcpuApplies with
    ⟨hcpuState, hcpuStore, hcpuOther, hcpuOutput,
      hcpuConsumptions, hcpuIncrements⟩
  let scalarAfter :=
    materializeLocalScalarStep image node event result
      scalarBefore cpuAfter
  have heventScalar : event ∈ scalarBefore.pending := by
    apply (hpendingCover event).mpr
    unfold roundEventUniverse
    exact List.mem_append_left _ hleast.1
  have hscalarTransition :
      transition node event (scalarBefore.localState node) result := by
    rw [← hlocal node hnode]
    exact htransition
  have hfreshScalar :
      FreshEventKeys result.children (scalarBefore.pending.erase event) := by
    intro child hchild other hother
    apply hfreshUniverse child hchild other
    apply (hpendingCover other).mp
    exact List.mem_of_mem_erase hother
  have hscalarAllocates :
      AllocatesChildrenInOrder node result.children
        scalarBefore scalarAfter := by
    refine ⟨?_, hchildKeys, ?_, ?_, ?_, ?_⟩
    · simpa [congrArg (fun cursor => cursor node.id) hcursors] using hsequence
    · intro child hchild hmem
      apply hchildFreshAllocated child hchild
      rw [hallocated]
      exact hmem
    · unfold scalarAfter materializeLocalScalarStep
      simp only
      rw [hallocatedAfter, hallocated]
    · unfold scalarAfter materializeLocalScalarStep
      simp only
      rw [hcursorNode, congrArg (fun cursor => cursor node.id) hcursors]
    · intro origin horigin
      unfold scalarAfter materializeLocalScalarStep
      simp only
      rw [hcursorOther origin horigin,
        congrArg (fun cursor => cursor origin) hcursors]
  have hscalarConsumptions :
      ReferenceConsumptionsValid image node event
        (scalarBefore.localState node) result
        (scalarBefore.packetStore node) := by
    constructor
    · simpa [hlocal node hnode] using hcpuConsumptions.1
    · apply packetReferencesHeld_perm hcpuConsumptions.1
      simpa [hlocal node hnode] using
        (structuralConsumptionsHeld_of_machineWellFormed
          image scalarBefore hscalarBeforeWellFormed node hnode
          event heventScalar hleast.2.1.1 result.nextState)
  have hscalarIncrements :
      ReferenceIncrementsValid image node
        (scalarBefore.localState node) result := by
    simpa [hlocal node hnode] using hcpuIncrements
  have hafterCanonical :
      CanonicalPending
        (insertEvents result.children
          (scalarBefore.pending.erase event)) :=
    canonicalPending_insertEvents result.children
      (scalarBefore.pending.erase event)
      (canonicalPending_erase event scalarBefore.pending
        hscalarBeforeWellFormed.1)
      hchildKeys hfreshScalar
  have hnewStateNodup :
      (ownedRoleStateReferences image node result.nextState).Nodup := by
    rw [← hcpuState]
    exact ownedRoleStateReferences_nodup_of_round image cpuAfter
      node hnode hcpuSourceCoherent hroundHeld
  have hdescriptorResult :=
    hdescriptors node event (scalarBefore.localState node)
      result hscalarTransition
  have hsourceCoherent :
      DescriptorStoreCoherent image (scalarAfter.packetStore node) := by
    unfold scalarAfter materializeLocalScalarStep
    simp only
    simp
    exact scalar_source_store_coherent image horacle node event result
      scalarBefore hscalarBeforeWellFormed hnode heventScalar
      hleast.2.1.1 hscalarConsumptions hscalarIncrements
      hdescriptorResult.1 hdescriptorResult.2.1 hchildKeys
      (by
        intro child hchild
        rw [← hallocated]
        exact hchildFreshAllocated child hchild)
      hafterCanonical hnewStateNodup
  have hscalarApplies :
      AppliesScalarTransitionResult image node event result
        scalarBefore scalarAfter := by
    refine ⟨?_, ?_, ?_, ?_, hscalarConsumptions,
      hscalarIncrements⟩
    · unfold scalarAfter materializeLocalScalarStep
      simp only
      exact hcpuState
    · unfold scalarAfter materializeLocalScalarStep
      simp only
      simp
    · intro other hother hid
      constructor
      · unfold scalarAfter materializeLocalScalarStep
        simp only
        exact (hcpuOther other hother hid).1.trans (hlocal other hother)
      · unfold scalarAfter materializeLocalScalarStep
        simp only
        rw [if_neg hid]
    · rcases hcpuOutput with
        ⟨hcpuSummary, hcpuObserved, hcpuDepartures, hcpuArrivals⟩
      refine ⟨?_, ?_, ?_, ?_⟩
      · unfold scalarAfter materializeLocalScalarStep
        simp only
        rw [hcpuSummary, hsummary]
      · unfold scalarAfter materializeLocalScalarStep
        simp only
        rw [hcpuObserved, hobserved]
      · unfold scalarAfter materializeLocalScalarStep
        simp only
        rw [hcpuDepartures, hdepartures]
      · unfold scalarAfter materializeLocalScalarStep
        simp only
        rw [hcpuArrivals, harrivals]
  have hscalarEmissions :
      scalarAfter.emissions =
        scalarBefore.emissions ++
          (result.children.map fun child => (event, child)) := by
    unfold scalarAfter materializeLocalScalarStep
    simp only
    rw [hcpuEmissions, hemissions]
  have hcore :
      AvailableEventStepCore image transition event
        scalarBefore scalarAfter :=
    ⟨heventScalar, node, hnode, result, hleast.2.1.1,
      hscalarTransition, hfreshScalar, hscalarAllocates,
      hscalarApplies, hsourceCoherent, rfl, hscalarEmissions⟩
  have hscalarAfterWellFormed :=
    availableEventStepCore_preserves_machineWellFormed
      image transition hunique horacle hgenerated hdescriptors
      event scalarBefore scalarAfter hscalarBeforeWellFormed hcore
  have hchildrenAvailable :
      ChildReferencesAvailableAtTargets image scalarAfter
        result.children := by
    apply childReferencesAvailable_of_wellFormed image horacle
      scalarAfter result.children hscalarAfterWellFormed
    intro child hchild
    unfold scalarAfter materializeLocalScalarStep
    simp only
    rw [mem_insertEvents_iff]
    exact Or.inl hchild
  have havailable :
      AvailableEventStep image transition event
        scalarBefore scalarAfter :=
    ⟨heventScalar, node, hnode, result, hleast.2.1.1,
      hscalarTransition, hfreshScalar, hscalarAllocates,
      hscalarApplies, hsourceCoherent, hchildrenAvailable,
      rfl, hscalarEmissions⟩
  refine ⟨scalarAfter, havailable, ?_⟩
  exact roundScalarProgress_step image transition hdeterministic hunique
    bounds node cpuBefore cpuAfter scalarBefore scalarAfter event
    hprogressSaved hcpuSaved havailable hscalarAfterWellFormed

/-- Caller-supplied executions compose by list concatenation. -/
theorem executionInOrder_append
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    {before middle after : MachineState State}
    {first later : List Event}
    (hfirst :
      ExecutionInOrder image transition before first middle)
    (hlater :
      ExecutionInOrder image transition middle later after) :
    ExecutionInOrder image transition before (first ++ later) after := by
  induction hfirst with
  | refl =>
      exact hlater
  | step head tail ih =>
      exact .step head (ih hlater)

/-- A complete drain of one LP has a scalar immediate-insertion replay. -/
theorem sequentialDrainLP_materializes_scalar
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (hdeterministic : TransitionDeterministic transition)
    (bounds : BoundFamily)
    (node : NodeDescriptor)
    (cpuBefore cpuAfter : RoundState State)
    (events : List Event)
    (scalarBefore : MachineState State)
    (hprogress : RoundScalarProgress image cpuBefore scalarBefore)
    (hdrain :
      SequentialDrainLP image transition bounds node
        cpuBefore events cpuAfter) :
    ∃ scalarAfter,
      ExecutionInOrder image transition scalarBefore events scalarAfter ∧
      RoundScalarProgress image cpuAfter scalarAfter := by
  induction hdrain generalizing scalarBefore with
  | done =>
      exact ⟨scalarBefore, .refl _, hprogress⟩
  | @step before event middle events after first rest ih =>
      obtain ⟨scalarMiddle, hfirst, hmiddleProgress⟩ :=
        localRoundStep_materializes_scalar image transition
          hunique horacle hgenerated hdescriptors hdeterministic
          bounds node before middle scalarBefore event hprogress first
      obtain ⟨scalarAfter, hrest, hafterProgress⟩ :=
        ih scalarMiddle hmiddleProgress
      exact ⟨scalarAfter, .step hfirst hrest, hafterProgress⟩

/-- A chosen whole-LP drain order has a scalar replay in exactly its recorded event order. -/
theorem drainLPsInOrder_materializes_scalar
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (hdeterministic : TransitionDeterministic transition)
    (bounds : BoundFamily)
    (order : List NodeId)
    (cpuBefore cpuAfter : RoundState State)
    (events : List Event)
    (scalarBefore : MachineState State)
    (hprogress : RoundScalarProgress image cpuBefore scalarBefore)
    (hdrain :
      DrainLPsInOrder image transition bounds order
        cpuBefore events cpuAfter) :
    ∃ scalarAfter,
      ExecutionInOrder image transition scalarBefore events scalarAfter ∧
      RoundScalarProgress image cpuAfter scalarAfter := by
  induction hdrain generalizing scalarBefore with
  | nil =>
      exact ⟨scalarBefore, .refl _, hprogress⟩
  | cons hnode first rest ih =>
      obtain ⟨scalarMiddle, hfirst, hmiddleProgress⟩ :=
        sequentialDrainLP_materializes_scalar image transition
          hunique horacle hgenerated hdescriptors hdeterministic
          bounds _ _ _ _ scalarBefore
          hprogress first
      obtain ⟨scalarAfter, hrest, hafterProgress⟩ :=
        ih scalarMiddle hmiddleProgress
      exact ⟨scalarAfter,
        executionInOrder_append image transition hfirst hrest,
        hafterProgress⟩

/--
The complete recorded CPU drain has a scalar replay; completing exchange changes only ownership
placement, so the scalar endpoint has strong owner-preserving provenance to the round endpoint.
-/
theorem sequentialRoundDrain_materializes_scalar
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (hdeterministic : TransitionDeterministic transition)
    (bounds : BoundFamily)
    (start afterDrain finish : RoundState State)
    (events : List Event)
    (hstart : PostExchangeStart image start)
    (hdrain :
      SequentialRoundDrain image transition bounds
        start events afterDrain)
    (hexchange : CompleteCanonicalExchange image afterDrain finish)
    (hfinish : PostExchangeStart image finish) :
    ∃ scalarFinish,
      ExecutionInOrder image transition start.machine events scalarFinish ∧
      StrongMachineReplay image scalarFinish finish.machine := by
  rcases hdrain with ⟨order, _, horderedDrain⟩
  obtain ⟨scalarFinish, hexecution, hprogress⟩ :=
    drainLPsInOrder_materializes_scalar image transition
      hunique horacle hgenerated hdescriptors hdeterministic
      bounds order start afterDrain events start.machine
      (roundScalarProgress_start image start hstart) horderedDrain
  have hpendingExchange :=
    completeCanonicalExchange_pending_mem_iff image afterDrain
      finish hexchange
  have hpending :
      scalarFinish.pending = finish.machine.pending := by
    apply canonicalPending_eq_of_mem_iff
      hprogress.1.1 hfinish.2.1.1
    intro event
    rw [hprogress.2.2.2.2.2.2.2.1 event,
      ← hpendingExchange event]
  have hlocal :
      ∀ node ∈ image.nodes,
        scalarFinish.localState node = finish.machine.localState node := by
    intro node hnode
    rcases hexchange with
      ⟨_, _, _, _, _, hfinishFields, _, _, _, _, _, _, _, _, _, _⟩
    exact (hprogress.2.2.1 node hnode).symm.trans
      (hfinishFields node hnode).1.symm
  have hstores :
      PerLPOwnedStoresEquivalent image scalarFinish finish.machine :=
    perLPOwnedStoresEquivalent_of_wellFormed image
      scalarFinish finish.machine hprogress.1 hfinish.2.1
      horacle hpending hlocal
  exact ⟨scalarFinish, hexecution,
    roundScalarProgress_finish image afterDrain finish scalarFinish
      hprogress hexchange hfinish hstores⟩

end DaysExecutor
