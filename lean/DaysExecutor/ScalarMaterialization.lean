import DaysExecutor.RoundScalarExecution

namespace DaysExecutor

/-- The role-state owner projection is a duplicate-free suffix of a well-formed machine owner bag. -/
theorem ownedRoleStateReferences_nodup_of_machineWellFormed
    (image : SimulationImage State)
    (horacle : DescriptorOracleWellFormed image)
    (machine : MachineState State)
    (hwellFormed : MachineWellFormed image machine)
    (node : NodeDescriptor)
    (hnode : node ∈ image.nodes) :
    (ownedRoleStateReferences image node
      (machine.localState node)).Nodup := by
  have hmachine :=
    machineOwnedReferencesFor_nodup image machine hwellFormed
      node hnode horacle
  unfold machineOwnedReferencesFor at hmachine
  exact (List.sublist_append_right _ _).nodup hmachine

/-- Canonical scalar machine obtained by applying one already-known transition result. -/
def materializeScalarResult
    (image : SimulationImage State)
    (node : NodeDescriptor)
    (event : Event)
    (result : TransitionResult State node.kind)
    (before : MachineState State) : MachineState State :=
  { localState := fun target =>
      if h : target = node then
        h ▸ result.nextState
      else
        before.localState target
    packetStore := fun target =>
      installChildDescriptorsFor image target.id result.children
        (if target.id = node.id then
          applyPacketEffects result (before.packetStore target)
        else
          before.packetStore target)
    pending := insertEvents result.children (before.pending.erase event)
    summary := RunSummary.add before.summary result.summaryDelta
    observedPackets :=
      result.observedPackets.foldl
        (fun current descriptor => installDescriptor descriptor current)
        before.observedPackets
    departures :=
      result.departures.foldl
        (fun current record => insertDeparture record current)
        before.departures
    arrivals :=
      result.arrivals.foldl
        (fun current record => insertArrival record current)
        before.arrivals
    nextOriginSeq := fun origin =>
      if origin = node.id then
        before.nextOriginSeq node.id + result.children.length
      else
        before.nextOriginSeq origin
    allocatedKeys := before.allocatedKeys ++ result.children.map Event.key
    emissions :=
      before.emissions ++
        result.children.map fun child => (event, child) }

/--
A successful transition result can be materialized at any well-formed machine with the same
source state, allocation cursor, and structural owner effects.
-/
theorem materializeScalarResult_available
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (node : NodeDescriptor)
    (hnode : node ∈ image.nodes)
    (event : Event)
    (result : TransitionResult State node.kind)
    (before : MachineState State)
    (hbefore : MachineWellFormed image before)
    (hevent : event ∈ before.pending)
    (htarget : event.target = node.id)
    (htransition :
      transition node event (before.localState node) result)
    (horigin :
      ChildrenUseOriginSequence node.id
        (before.nextOriginSeq node.id) result.children)
    (hchildKeys : (result.children.map Event.key).Nodup)
    (hchildrenFresh :
      ∀ child ∈ result.children,
        child.key ∉ before.allocatedKeys)
    (hconsumptions :
      result.packetReferenceConsumptions.Perm
        (ownedEventReference image event ::
          stateReferenceConsumptions image node
            (before.localState node) result.nextState))
    (hincrements :
      ReferenceIncrementsValid image node
        (before.localState node) result)
    (hnewStateNodup :
      (ownedRoleStateReferences image node result.nextState).Nodup) :
    let after :=
      materializeScalarResult image node event result before
    AvailableEventStep image transition event before after ∧
      MachineWellFormed image after := by
  let after :=
    materializeScalarResult image node event result before
  have hfreshPending :
      FreshEventKeys result.children (before.pending.erase event) := by
    intro child hchild pending hpending heq
    apply hchildrenFresh child hchild
    have hpendingBefore := List.mem_of_mem_erase hpending
    have hpendingAllocated := (hbefore.2.2.2.1 pending hpendingBefore).1
    rw [heq]
    exact hpendingAllocated
  have hallocates :
      AllocatesChildrenInOrder node result.children before after := by
    refine ⟨horigin, hchildKeys, hchildrenFresh, rfl, ?_, ?_⟩
    · simp [after, materializeScalarResult]
    · intro origin hne
      simp [after, materializeScalarResult, hne]
  have hvalidConsumptions :
      ReferenceConsumptionsValid image node event
        (before.localState node) result (before.packetStore node) :=
    ⟨hconsumptions,
      packetReferencesHeld_perm hconsumptions
        (structuralConsumptionsHeld_of_machineWellFormed
          image before hbefore node hnode event hevent htarget
          result.nextState)⟩
  have hafterCanonical :
      CanonicalPending after.pending := by
    unfold after materializeScalarResult
    exact canonicalPending_insertEvents result.children
      (before.pending.erase event)
      (canonicalPending_erase event before.pending hbefore.1)
      hchildKeys hfreshPending
  have hdescriptorResult :=
    hdescriptors node event (before.localState node) result htransition
  have hsourceCoherent :
      DescriptorStoreCoherent image (after.packetStore node) := by
    unfold after materializeScalarResult
    simp only
    simp
    exact scalar_source_store_coherent image horacle node event result
      before hbefore hnode hevent htarget hvalidConsumptions
      hincrements hdescriptorResult.1 hdescriptorResult.2.1
      hchildKeys hchildrenFresh hafterCanonical hnewStateNodup
  have happlies :
      AppliesScalarTransitionResult image node event result
        before after := by
    refine ⟨?_, ?_, ?_, ⟨rfl, rfl, rfl, rfl⟩,
      hvalidConsumptions, hincrements⟩
    · simp [after, materializeScalarResult]
    · unfold after materializeScalarResult
      simp only
      simp
    · intro other hother hid
      have hne : other ≠ node := by
        intro heq
        subst other
        exact hid rfl
      constructor
      · simp [after, materializeScalarResult, hne]
      · unfold after materializeScalarResult
        simp only
        rw [if_neg hid]
  have hcore :
      AvailableEventStepCore image transition event before after :=
    ⟨hevent, node, hnode, result, htarget, htransition,
      hfreshPending, hallocates, happlies, hsourceCoherent, rfl, rfl⟩
  have hafterWellFormed :=
    availableEventStepCore_preserves_machineWellFormed
      image transition hunique horacle hgenerated hdescriptors
      event before after hbefore hcore
  have hchildrenAvailable :
      ChildReferencesAvailableAtTargets image after result.children := by
    apply childReferencesAvailable_of_wellFormed image horacle
      after result.children hafterWellFormed
    intro child hchild
    unfold after materializeScalarResult
    rw [mem_insertEvents_iff]
    exact Or.inl hchild
  exact
    ⟨⟨hevent, node, hnode, result, htarget, htransition,
      hfreshPending, hallocates, happlies, hsourceCoherent,
      hchildrenAvailable, rfl, rfl⟩,
    hafterWellFormed⟩

end DaysExecutor
