import DaysExecutor.MachinePreservation

namespace DaysExecutor

/-- Exact structural matching against a coherent store forces duplicate-free owners. -/
theorem references_nodup_of_match_coherent
    (image : SimulationImage State)
    (references : List OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hcoherent : DescriptorStoreCoherent image store)
    (hmatch : OwnedReferencesMatchStore references store) :
    references.Nodup := by
  rw [List.nodup_iff_count]
  intro reference
  by_cases hreference : reference ∈ references
  · rw [hmatch.1 reference hreference]
    exact ownedReferenceCount_le_one_of_coherent image
      reference store hcoherent
  · rw [List.count_eq_zero.mpr hreference]
    exact Nat.zero_le _

/-- A canonical pending list is disjoint from every queue and in-service owner list. -/
theorem machine_owner_shape_nodup
    (image : SimulationImage State)
    (node : NodeDescriptor)
    (pending : List Event)
    (state : RoleState State node.kind)
    (hcanonical : CanonicalPending pending)
    (hstate : (ownedRoleStateReferences image node state).Nodup) :
    (pendingOwnedReferencesFor image node.id pending ++
      ownedRoleStateReferences image node state).Nodup := by
  apply List.nodup_append.mpr
  refine ⟨?_, hstate, ?_⟩
  · unfold pendingOwnedReferencesFor
    exact List.pairwise_map.mpr
      ((pending_ownedEventReference_pairwise image pending hcanonical).filter
        (fun event => event.target = node.id))
  · intro pendingReference hpending stateReference hstateReference hrefEq
    unfold pendingOwnedReferencesFor at hpending
    rcases List.mem_map.mp hpending with ⟨event, _, rfl⟩
    unfold ownedRoleStateReferences at hstateReference
    rcases List.mem_append.mp hstateReference with hqueue | hservice
    · rcases List.mem_map.mp hqueue with ⟨payload, _, hqueueEq⟩
      have howner :=
        congrArg OwnedPacketReference.owner (hrefEq.trans hqueueEq.symm)
      simp [ownedEventReference, ownedQueueReference] at howner
    · rcases List.mem_map.mp hservice with ⟨payload, _, hserviceEq⟩
      have howner :=
        congrArg OwnedPacketReference.owner (hrefEq.trans hserviceEq.symm)
      simp [ownedEventReference, ownedInServiceReference] at howner

/--
Structural releases followed by the exact role-state acquisitions and local child owners preserve
source-store coherence whenever the resulting structural owner shape is duplicate-free.
-/
theorem scalar_source_store_coherent
    (image : SimulationImage State)
    (horacle : DescriptorOracleWellFormed image)
    (node : NodeDescriptor)
    (event : Event)
    (result : TransitionResult State node.kind)
    (before : MachineState State)
    (hbefore : MachineWellFormed image before)
    (hnode : node ∈ image.nodes)
    (hevent : event ∈ before.pending)
    (htarget : event.target = node.id)
    (hconsumptions :
      ReferenceConsumptionsValid image node event
        (before.localState node) result (before.packetStore node))
    (hincrements :
      ReferenceIncrementsValid image node
        (before.localState node) result)
    (hdescriptorIncrements :
      ∀ reference ∈ result.packetReferenceIncrements,
        reference.descriptor =
          image.packetDescriptor reference.descriptor.id)
    (hdescriptorConsumptions :
      ∀ reference ∈ result.packetReferenceConsumptions,
        reference.descriptor =
          image.packetDescriptor reference.descriptor.id)
    (hchildrenKeys : (result.children.map Event.key).Nodup)
    (hchildrenFresh :
      ∀ child ∈ result.children,
        child.key ∉ before.allocatedKeys)
    (hafterCanonical :
      CanonicalPending
        (insertEvents result.children (before.pending.erase event)))
    (hnewStateNodup :
      (ownedRoleStateReferences image node result.nextState).Nodup) :
    DescriptorStoreCoherent image
      (installChildDescriptorsFor image node.id result.children
        (applyPacketEffects result (before.packetStore node))) := by
  let oldPending :=
    pendingOwnedReferencesFor image node.id before.pending
  let oldState :=
    ownedRoleStateReferences image node (before.localState node)
  let newState :=
    ownedRoleStateReferences image node result.nextState
  let childReferences :=
    pendingOwnedReferencesFor image node.id result.children
  let structuralConsumptions :=
    ownedEventReference image event ::
      stateReferenceConsumptions image node
        (before.localState node) result.nextState
  let structuralIncrements :=
    stateReferenceIncrements image node
      (before.localState node) result.nextState
  let releasedStore :=
    result.packetReferenceConsumptions.foldl
      (fun current reference => releaseOwnedReference reference current)
      (before.packetStore node)
  let additions :=
    result.packetReferenceIncrements ++ childReferences
  have hbeforeReferences :
      machineOwnedReferencesFor image before node =
        oldPending ++ oldState := rfl
  have hbeforePerm :
      (oldPending ++ oldState).Perm
        (storeOwnedReferences (before.packetStore node)) := by
    rw [← hbeforeReferences]
    exact
      (ownedReferencesMatchStore_iff_perm image
        (machineOwnedReferencesFor image before node)
        (before.packetStore node)
        (hbefore.2.2.2.2.1 node hnode)
        (machineOwnedReferencesFor_oracle image horacle before node)).mp
        (hbefore.2.2.2.2.2.1 node hnode)
  have hrelease :=
    releaseOwnedReferences_preserves image
      result.packetReferenceConsumptions (before.packetStore node)
      (hbefore.2.2.2.2.1 node hnode)
      hdescriptorConsumptions hconsumptions.2
  have hremovedPerm :
      (listBagDifference
        (storeOwnedReferences (before.packetStore node))
        result.packetReferenceConsumptions).Perm
        (listBagDifference (oldPending ++ oldState)
          structuralConsumptions) := by
    exact listBagDifference_perm hbeforePerm.symm
      (by
        unfold structuralConsumptions
        exact hconsumptions.1)
  have hadditionsPerm :
      additions.Perm (structuralIncrements ++ childReferences) := by
    unfold additions structuralIncrements
    exact hincrements.append_right childReferences
  have hprocessedPending :
      ownedEventReference image event ∈ oldPending := by
    unfold oldPending pendingOwnedReferencesFor
    apply List.mem_map.mpr
    exact ⟨event,
      List.mem_filter.mpr
        ⟨hevent, by simpa only [decide_eq_true_eq] using htarget⟩,
      rfl⟩
  have holdNodup :
      (oldPending ++ oldState).Nodup := by
    rw [← hbeforeReferences]
    exact machineOwnedReferencesFor_nodup image before hbefore
      node hnode horacle
  have hprocessedNew :
      ownedEventReference image event ∉ newState := by
    unfold newState ownedRoleStateReferences
    intro hmem
    rcases List.mem_append.mp hmem with hqueue | hservice
    · rcases List.mem_map.mp hqueue with ⟨payload, _, heq⟩
      have howner := congrArg OwnedPacketReference.owner heq
      simp [ownedEventReference, ownedQueueReference] at howner
    · rcases List.mem_map.mp hservice with ⟨payload, _, heq⟩
      have howner := congrArg OwnedPacketReference.owner heq
      simp [ownedEventReference, ownedInServiceReference] at howner
  have hstructuralNormal :=
    processing_owner_bag_reconcile oldPending oldState newState
      childReferences (ownedEventReference image event)
      holdNodup hprocessedPending hprocessedNew
  have hafterPending :
      (pendingOwnedReferencesFor image node.id
        (insertEvents result.children (before.pending.erase event))).Perm
        (childReferences ++
          oldPending.erase (ownedEventReference image event)) := by
    simpa [childReferences, oldPending, htarget] using
      (pendingOwnedReferencesFor_insertEvents_erase image node.id
        before.pending result.children event hbefore.1 hevent)
  have hshapeNodup :
      (childReferences ++
        oldPending.erase (ownedEventReference image event) ++
          newState).Nodup := by
    have hmachineShape :=
      machine_owner_shape_nodup image node
        (insertEvents result.children (before.pending.erase event))
        result.nextState hafterCanonical hnewStateNodup
    exact
      (hafterPending.append_right newState).nodup_iff.mp hmachineShape
  have hactualNormal :
      (additions ++
        storeOwnedReferences releasedStore).Perm
        (childReferences ++
          oldPending.erase (ownedEventReference image event) ++
            newState) := by
    have hnormal :
        (additions ++
          listBagDifference
            (storeOwnedReferences (before.packetStore node))
            result.packetReferenceConsumptions).Perm
          ((structuralIncrements ++ childReferences) ++
            listBagDifference (oldPending ++ oldState)
              structuralConsumptions) :=
      hadditionsPerm.append hremovedPerm
    exact ((List.Perm.of_eq hrelease.2).append_left additions).trans
      (hnormal.trans hstructuralNormal.symm)
  have hconcatNodup :
      (additions ++ storeOwnedReferences releasedStore).Nodup :=
    hactualNormal.nodup_iff.mpr hshapeNodup
  have hadditionsNodup := (List.nodup_append.mp hconcatNodup).1
  have hadditionsDisjoint := (List.nodup_append.mp hconcatNodup).2.2
  have hadditionsOracle :
      ∀ reference ∈ additions,
        reference.descriptor =
          image.packetDescriptor reference.descriptor.id := by
    intro reference hreference
    rcases List.mem_append.mp hreference with hincrement | hchild
    · exact hdescriptorIncrements reference hincrement
    · unfold childReferences pendingOwnedReferencesFor at hchild
      rcases List.mem_map.mp hchild with ⟨child, _, rfl⟩
      unfold ownedEventReference
      rw [(horacle _).1]
  have hadditionsAbsent :
      ∀ reference ∈ additions,
        ownedReferenceCount reference releasedStore = 0 := by
    intro reference hreference
    rw [← storeOwnedReferences_count image releasedStore
      reference hrelease.1 (hadditionsOracle reference hreference)]
    apply List.count_eq_zero.mpr
    intro hstore
    exact hadditionsDisjoint reference hreference reference hstore rfl
  have hfinal :=
    acquireOwnedReferences_preserves image additions releasedStore
      hrelease.1 hadditionsNodup hadditionsOracle hadditionsAbsent
  simpa [installChildDescriptorsFor_eq_acquire_fold,
    applyPacketEffects, additions, releasedStore, childReferences,
    List.foldl_append] using hfinal.1

/-- Round ownership plus a coherent LP store makes that LP's role-state owner projection unique. -/
theorem ownedRoleStateReferences_nodup_of_round
    (image : SimulationImage State)
    (state : RoundState State)
    (node : NodeDescriptor)
    (hnode : node ∈ image.nodes)
    (hcoherent :
      DescriptorStoreCoherent image (state.machine.packetStore node))
    (hheld : RoundReferencesHeld image state) :
    (ownedRoleStateReferences image node
      (state.machine.localState node)).Nodup := by
  have hfull :=
    references_nodup_of_match_coherent image
      (machineOwnedReferencesFor image state.machine node ++
        (state.outboxes node.id).map ownedEnvelopeReference)
      (state.machine.packetStore node) hcoherent (hheld node hnode)
  unfold machineOwnedReferencesFor at hfull
  exact
    ((List.sublist_append_right
      ((state.machine.pending.filter
        fun event => event.target = node.id).map
          (ownedEventReference image))
      (ownedRoleStateReferences image node
        (state.machine.localState node))).trans
      (List.sublist_append_left
        (((state.machine.pending.filter
          fun event => event.target = node.id).map
            (ownedEventReference image)) ++
          ownedRoleStateReferences image node
            (state.machine.localState node))
        ((state.outboxes node.id).map ownedEnvelopeReference))).nodup hfull

/-- Concrete scalar machine produced alongside one CPU-local transition. -/
def materializeLocalScalarStep
    (image : SimulationImage State)
    (node : NodeDescriptor)
    (event : Event)
    (result : TransitionResult State node.kind)
    (scalarBefore : MachineState State)
    (cpuAfter : RoundState State) : MachineState State :=
  { cpuAfter.machine with
    packetStore := fun target =>
      installChildDescriptorsFor image target.id result.children
        (if target.id = node.id then
          applyPacketEffects result (scalarBefore.packetStore target)
        else
          scalarBefore.packetStore target)
    pending :=
      insertEvents result.children (scalarBefore.pending.erase event) }

/--
The scalar-step fields needed for invariant preservation. Child target availability is a
consequence of these fields and exact owner/store correspondence, so it is omitted to avoid a
circular construction proof.
-/
def AvailableEventStepCore
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (event : Event)
    (before after : MachineState State) : Prop :=
  event ∈ before.pending ∧
    ∃ node ∈ image.nodes, ∃ result,
      event.target = node.id ∧
      transition node event (before.localState node) result ∧
      FreshEventKeys result.children (before.pending.erase event) ∧
      AllocatesChildrenInOrder node result.children before after ∧
      AppliesScalarTransitionResult image node event result before after ∧
      DescriptorStoreCoherent image (after.packetStore node) ∧
      after.pending = insertEvents result.children (before.pending.erase event) ∧
      after.emissions =
        before.emissions ++ (result.children.map fun child => (event, child))

/--
The owned scalar update of one available event preserves canonical per-LP stores and their exact
structural owner correspondence.
-/
theorem availableEventStepCore_preserves_owned_stores
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (event : Event)
    (before after : MachineState State)
    (hbefore : MachineWellFormed image before)
    (hstep : AvailableEventStepCore image transition event before after) :
    (∀ target ∈ image.nodes,
      DescriptorStoreCoherent image (after.packetStore target)) ∧
    ∀ target ∈ image.nodes,
      OwnedReferencesMatchStore
        (machineOwnedReferencesFor image after target)
        (after.packetStore target) := by
  rcases hstep with
    ⟨hevent, node, hnode, result, htarget, htransition, hfresh,
      hallocates, happlies, hsourceCoherent, hpending, _⟩
  rcases hallocates with
    ⟨_, hchildKeys, hchildAllocatedFresh, _, _, _⟩
  rcases happlies with
    ⟨hstate, hsourceStore, hother, _, hconsumptions, hincrements⟩
  have hdescriptorResult := hdescriptors node event
    (before.localState node) result htransition
  have hchildFresh :
      ∀ child ∈ result.children,
        child.key ∉ before.allocatedKeys :=
    hchildAllocatedFresh
  have hstores :
      ∀ target ∈ image.nodes,
        DescriptorStoreCoherent image (after.packetStore target) := by
    intro target htargetMember
    by_cases heq : target = node
    · subst target
      exact hsourceCoherent
    · have hid : target.id ≠ node.id := by
        intro hid
        exact heq (node_eq_of_unique_ids hunique htargetMember hnode hid)
      have hbaseCoherent :=
        hbefore.2.2.2.2.1 target htargetMember
      have hchildrenFresh :=
        pendingChildReferences_fresh_for_target image before hbefore
          horacle target htargetMember result.children hchildKeys hchildFresh
      have hinstalled :=
        acquireOwnedReferences_preserves image
          (pendingOwnedReferencesFor image target.id result.children)
          (before.packetStore target)
          hbaseCoherent hchildrenFresh.1 hchildrenFresh.2.1
          hchildrenFresh.2.2
      rw [(hother target htargetMember hid).2,
        installChildDescriptorsFor_eq_acquire_fold]
      exact hinstalled.1
  refine ⟨hstores, ?_⟩
  intro target htargetMember
  apply
    (ownedReferencesMatchStore_iff_perm image
      (machineOwnedReferencesFor image after target)
      (after.packetStore target)
      (hstores target htargetMember)
      (machineOwnedReferencesFor_oracle image horacle after target)).mpr
  by_cases heq : target = node
  · subst target
    let oldPending :=
      pendingOwnedReferencesFor image node.id before.pending
    let oldState :=
      ownedRoleStateReferences image node (before.localState node)
    let newState :=
      ownedRoleStateReferences image node result.nextState
    let childReferences :=
      pendingOwnedReferencesFor image node.id result.children
    let structuralConsumptions :=
      ownedEventReference image event ::
        stateReferenceConsumptions image node
          (before.localState node) result.nextState
    let structuralIncrements :=
      stateReferenceIncrements image node
        (before.localState node) result.nextState
    let releasedStore :=
      result.packetReferenceConsumptions.foldl
        (fun current reference => releaseOwnedReference reference current)
        (before.packetStore node)
    let additions :=
      result.packetReferenceIncrements ++ childReferences
    have hbeforeReferences :
        machineOwnedReferencesFor image before node =
          oldPending ++ oldState := rfl
    have hbeforePerm :
        (oldPending ++ oldState).Perm
          (storeOwnedReferences (before.packetStore node)) := by
      rw [← hbeforeReferences]
      exact
        (ownedReferencesMatchStore_iff_perm image
          (machineOwnedReferencesFor image before node)
          (before.packetStore node)
          (hbefore.2.2.2.2.1 node hnode)
          (machineOwnedReferencesFor_oracle image horacle before node)).mp
          (hbefore.2.2.2.2.2.1 node hnode)
    have hrelease :=
      releaseOwnedReferences_preserves image
        result.packetReferenceConsumptions (before.packetStore node)
        (hbefore.2.2.2.2.1 node hnode)
        hdescriptorResult.2.1 hconsumptions.2
    have hadditionsOracle :
        ∀ reference ∈ additions,
          reference.descriptor =
            image.packetDescriptor reference.descriptor.id := by
      intro reference hreference
      rcases List.mem_append.mp hreference with hincrement | hchild
      · exact hdescriptorResult.1 reference hincrement
      · unfold childReferences pendingOwnedReferencesFor at hchild
        rcases List.mem_map.mp hchild with ⟨child, _, rfl⟩
        unfold ownedEventReference
        rw [(horacle _).1]
    have hsourceStoreEq :
        after.packetStore node =
          additions.foldl
            (fun current reference => acquireOwnedReference reference current)
            releasedStore := by
      rw [hsourceStore,
        installChildDescriptorsFor_eq_acquire_fold]
      simp only [applyPacketEffects, additions, releasedStore,
        childReferences, List.foldl_append]
    have hfinalExact :
        (storeOwnedReferences (after.packetStore node)).Perm
          (additions ++ storeOwnedReferences releasedStore) := by
      rw [hsourceStoreEq]
      exact acquireOwnedReferences_exact_of_final_coherent image
        additions releasedStore hrelease.1 hadditionsOracle
        (by rw [← hsourceStoreEq]; exact hsourceCoherent)
    have hremovedPerm :
        (listBagDifference
          (storeOwnedReferences (before.packetStore node))
          result.packetReferenceConsumptions).Perm
          (listBagDifference (oldPending ++ oldState)
            structuralConsumptions) := by
      exact listBagDifference_perm hbeforePerm.symm
        (by
          unfold structuralConsumptions
          exact hconsumptions.1)
    have hadditionsPerm :
        additions.Perm (structuralIncrements ++ childReferences) := by
      unfold additions structuralIncrements
      exact hincrements.append_right childReferences
    have hactualNormal :
        (additions ++
          listBagDifference
            (storeOwnedReferences (before.packetStore node))
            result.packetReferenceConsumptions).Perm
          ((structuralIncrements ++ childReferences) ++
            listBagDifference (oldPending ++ oldState)
              structuralConsumptions) :=
      hadditionsPerm.append hremovedPerm
    have hprocessedPending :
        ownedEventReference image event ∈ oldPending := by
      unfold oldPending pendingOwnedReferencesFor
      apply List.mem_map.mpr
      exact ⟨event,
        List.mem_filter.mpr
          ⟨hevent, by simpa only [decide_eq_true_eq] using htarget⟩,
        rfl⟩
    have holdNodup :
        (oldPending ++ oldState).Nodup := by
      rw [← hbeforeReferences]
      exact machineOwnedReferencesFor_nodup image before hbefore
        node hnode horacle
    have hprocessedNew :
        ownedEventReference image event ∉ newState := by
      unfold newState ownedRoleStateReferences
      intro hmem
      rcases List.mem_append.mp hmem with hqueue | hservice
      · rcases List.mem_map.mp hqueue with ⟨payload, _, heq⟩
        have howner := congrArg OwnedPacketReference.owner heq
        simp [ownedEventReference, ownedQueueReference] at howner
      · rcases List.mem_map.mp hservice with ⟨payload, _, heq⟩
        have howner := congrArg OwnedPacketReference.owner heq
        simp [ownedEventReference, ownedInServiceReference] at howner
    have hstructuralNormal :=
      processing_owner_bag_reconcile oldPending oldState newState
        childReferences (ownedEventReference image event)
        holdNodup hprocessedPending hprocessedNew
    have hafterPending :
        (pendingOwnedReferencesFor image node.id after.pending).Perm
          (childReferences ++
            oldPending.erase (ownedEventReference image event)) := by
      rw [hpending]
      simpa [childReferences, oldPending, htarget] using
        (pendingOwnedReferencesFor_insertEvents_erase image node.id
          before.pending result.children event hbefore.1 hevent)
    have hafterStructural :
        (machineOwnedReferencesFor image after node).Perm
          (childReferences ++
            oldPending.erase (ownedEventReference image event) ++
              newState) := by
      unfold machineOwnedReferencesFor
      change
        (pendingOwnedReferencesFor image node.id after.pending ++
          ownedRoleStateReferences image node (after.localState node)).Perm _
      rw [hstate]
      exact hafterPending.append_right newState
    exact hafterStructural.trans
      (hstructuralNormal.trans
        (hactualNormal.symm.trans
          ((List.Perm.of_eq hrelease.2.symm).append_left additions
            |>.trans hfinalExact.symm)))
  · have hid : target.id ≠ node.id := by
      intro hid
      exact heq (node_eq_of_unique_ids hunique htargetMember hnode hid)
    let oldPending :=
      pendingOwnedReferencesFor image target.id before.pending
    let childReferences :=
      pendingOwnedReferencesFor image target.id result.children
    let roleReferences :=
      ownedRoleStateReferences image target (before.localState target)
    have hbeforeReferences :
        machineOwnedReferencesFor image before target =
          oldPending ++ roleReferences := rfl
    have hbeforePerm :
        (oldPending ++ roleReferences).Perm
          (storeOwnedReferences (before.packetStore target)) := by
      rw [← hbeforeReferences]
      exact
        (ownedReferencesMatchStore_iff_perm image
          (machineOwnedReferencesFor image before target)
          (before.packetStore target)
          (hbefore.2.2.2.2.1 target htargetMember)
          (machineOwnedReferencesFor_oracle image horacle before target)).mp
          (hbefore.2.2.2.2.2.1 target htargetMember)
    have hchildrenFresh :=
      pendingChildReferences_fresh_for_target image before hbefore
        horacle target htargetMember result.children hchildKeys hchildFresh
    have hinstalled :=
      acquireOwnedReferences_preserves image childReferences
        (before.packetStore target)
        (hbefore.2.2.2.2.1 target htargetMember)
        (by simpa [childReferences] using hchildrenFresh.1)
        (by simpa [childReferences] using hchildrenFresh.2.1)
        (by simpa [childReferences] using hchildrenFresh.2.2)
    have hafterPending :
        (pendingOwnedReferencesFor image target.id after.pending).Perm
          (childReferences ++ oldPending) := by
      rw [hpending]
      simpa [childReferences, oldPending, htarget, hid, Ne.symm hid] using
        (pendingOwnedReferencesFor_insertEvents_erase image target.id
          before.pending result.children event hbefore.1 hevent)
    have hafterStructural :
        (machineOwnedReferencesFor image after target).Perm
          (childReferences ++ (oldPending ++ roleReferences)) := by
      unfold machineOwnedReferencesFor
      change
        (pendingOwnedReferencesFor image target.id after.pending ++
          ownedRoleStateReferences image target (after.localState target)).Perm _
      rw [(hother target htargetMember hid).1]
      simpa only [List.append_assoc] using
        hafterPending.append_right roleReferences
    have hstoreEq :
        after.packetStore target =
          childReferences.foldl
            (fun current reference => acquireOwnedReference reference current)
            (before.packetStore target) := by
      rw [(hother target htargetMember hid).2,
        installChildDescriptorsFor_eq_acquire_fold]
    rw [hstoreEq]
    exact hafterStructural.trans
      ((hbeforePerm.append_left childReferences).trans hinstalled.2.symm)

/-- Public available steps preserve exact owned stores. -/
theorem availableEventStep_preserves_owned_stores
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (event : Event)
    (before after : MachineState State)
    (hbefore : MachineWellFormed image before)
    (hstep : AvailableEventStep image transition event before after) :
    (∀ target ∈ image.nodes,
      DescriptorStoreCoherent image (after.packetStore target)) ∧
    ∀ target ∈ image.nodes,
      OwnedReferencesMatchStore
        (machineOwnedReferencesFor image after target)
        (after.packetStore target) := by
  rcases hstep with
    ⟨hevent, node, hnode, result, htarget, htransition, hfresh,
      hallocates, happlies, hsourceCoherent, _, hpending, hemissions⟩
  exact availableEventStepCore_preserves_owned_stores image transition
    hunique horacle hdescriptors event before after hbefore
    ⟨hevent, node, hnode, result, htarget, htransition, hfresh,
      hallocates, happlies, hsourceCoherent, hpending, hemissions⟩

/--
Every scalar-style event step preserves the full machine invariant.  This is the induction
closure needed when a CPU round is replayed with immediate child insertion.
-/
theorem availableEventStepCore_preserves_machineWellFormed
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (event : Event)
    (before after : MachineState State)
    (hbefore : MachineWellFormed image before)
    (hstep : AvailableEventStepCore image transition event before after) :
    MachineWellFormed image after := by
  rcases hstep with
    ⟨hevent, node, hnode, result, htarget, htransition, hfresh,
      hallocates, happlies, hsourceCoherent, hpending, hemissions⟩
  have hstores :=
    availableEventStepCore_preserves_owned_stores image transition
      hunique horacle hdescriptors event before after hbefore
      ⟨hevent, node, hnode, result, htarget, htransition, hfresh,
        hallocates, happlies, hsourceCoherent, hpending, hemissions⟩
  rcases happlies with
    ⟨_, _, _, houtput, _, _⟩
  have hcanonical :
      CanonicalPending after.pending := by
    rw [hpending]
    exact canonicalPending_insertEvents result.children
      (before.pending.erase event)
      (canonicalPending_erase event before.pending hbefore.1)
      hallocates.2.1 hfresh
  have hallocatedNodup :
      after.allocatedKeys.Nodup :=
    allocatedKeys_nodup_after result.children before after
      hbefore.2.1 hallocates
  have hallocatedCursor :=
    allocatedKeys_below_cursor_after image node result.children
      before after hnode hbefore.2.2.1 hallocates
  have hpendingValid :
      ∀ candidate ∈ after.pending,
        candidate.key ∈ after.allocatedKeys ∧
          ∃ target ∈ image.nodes,
            candidate.target = target.id ∧
              roleSupports target.kind candidate.kind := by
    intro candidate hcandidate
    rw [hpending, mem_insertEvents_iff] at hcandidate
    rcases hcandidate with hchild | hold
    · constructor
      · rw [hallocates.2.2.2.1]
        exact List.mem_append_right _
          (List.mem_map.mpr ⟨candidate, hchild, rfl⟩)
      · exact hgenerated node event (before.localState node)
          result htransition candidate hchild
    · have holdBefore := List.mem_of_mem_erase hold
      rcases hbefore.2.2.2.1 candidate holdBefore with
        ⟨hkey, target, htargetMember, htargetCandidate, hrole⟩
      constructor
      · rw [hallocates.2.2.2.1]
        exact List.mem_append_left _ hkey
      · exact ⟨target, htargetMember, htargetCandidate, hrole⟩
  have hobserved :
      DescriptorListCoherent image after.observedPackets := by
    rw [houtput.2.1]
    exact descriptorListCoherent_install_many image
      result.observedPackets before.observedPackets
      hbefore.2.2.2.2.2.2
      ((hdescriptors node event (before.localState node)
        result htransition).2.2)
  exact ⟨hcanonical, hallocatedNodup, hallocatedCursor,
    hpendingValid, hstores.1, hstores.2, hobserved⟩

/-- Public available steps preserve the full machine invariant. -/
theorem availableEventStep_preserves_machineWellFormed
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (event : Event)
    (before after : MachineState State)
    (hbefore : MachineWellFormed image before)
    (hstep : AvailableEventStep image transition event before after) :
    MachineWellFormed image after := by
  rcases hstep with
    ⟨hevent, node, hnode, result, htarget, htransition, hfresh,
      hallocates, happlies, hsourceCoherent, _, hpending, hemissions⟩
  exact availableEventStepCore_preserves_machineWellFormed
    image transition hunique horacle hgenerated hdescriptors
    event before after hbefore
    ⟨hevent, node, hnode, result, htarget, htransition, hfresh,
      hallocates, happlies, hsourceCoherent, hpending, hemissions⟩

end DaysExecutor
