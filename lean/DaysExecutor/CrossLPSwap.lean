import DaysExecutor.ScalarMaterialization

namespace DaysExecutor

/-- A generated key is allocated by the materialized step that emitted it. -/
theorem child_key_mem_allocated_after
    (node : NodeDescriptor)
    (children : List Event)
    (before after : MachineState State)
    (hallocates :
      AllocatesChildrenInOrder node children before after)
    {child : Event}
    (hchild : child ∈ children) :
    child.key ∈ after.allocatedKeys := by
  rw [hallocates.2.2.2.1]
  exact List.mem_append_right _
    (List.mem_map.mpr ⟨child, hchild, rfl⟩)

/--
An inverted adjacent pair on distinct LPs can be replayed in the opposite order. Endpoint
equivalence is established separately after both materialized steps are available.
-/
theorem inverted_cross_lp_steps_replay
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (hadvance : ChildrenAdvanceParent transition)
    (before afterLeft afterLeftRight : MachineState State)
    (left right : Event)
    (hbefore : MachineWellFormed image before)
    (hleft :
      AvailableEventStep image transition left before afterLeft)
    (hright :
      AvailableEventStep image transition right afterLeft afterLeftRight)
    (htarget : left.target ≠ right.target)
    (hkey : right.key < left.key) :
    ∃ afterRight afterRightLeft,
      AvailableEventStep image transition right before afterRight ∧
      AvailableEventStep image transition left afterRight afterRightLeft ∧
      MachineWellFormed image afterRightLeft := by
  have hleftSaved := hleft
  have hrightSaved := hright
  have hafterLeft :=
    availableEventStep_preserves_machineWellFormed image transition
      hunique horacle hgenerated hdescriptors left before afterLeft
      hbefore hleft
  have hafterLeftRight :=
    availableEventStep_preserves_machineWellFormed image transition
      hunique horacle hgenerated hdescriptors right afterLeft
      afterLeftRight hafterLeft hright
  rcases hleft with
    ⟨hleftMem, leftNode, hleftNode, leftResult, hleftTarget,
      hleftTransition, _, hleftAllocates, hleftApplies, _, _,
      hleftPending, _⟩
  rcases hright with
    ⟨_, rightNode, hrightNode, rightResult, hrightTarget,
      hrightTransition, _, hrightAllocates, hrightApplies, _, _,
      hrightPending, _⟩
  rcases hleftAllocates with
    ⟨hleftOrigin, hleftKeys, hleftFresh, hleftAllocated,
      hleftCursor, hleftOtherCursor⟩
  rcases hrightAllocates with
    ⟨hrightOrigin, hrightKeys, hrightFresh, hrightAllocated,
      hrightCursor, hrightOtherCursor⟩
  rcases hleftApplies with
    ⟨hleftState, _, hleftOther, _, hleftConsumptions,
      hleftIncrements⟩
  rcases hrightApplies with
    ⟨hrightState, _, hrightOther, _, hrightConsumptions,
      hrightIncrements⟩
  have hnodeIds : leftNode.id ≠ rightNode.id := by
    intro heq
    apply htarget
    rw [hleftTarget, hrightTarget, heq]
  have hrightStateBefore :
      afterLeft.localState rightNode = before.localState rightNode :=
    (hleftOther rightNode hrightNode (Ne.symm hnodeIds)).1
  have hrightMemBefore :
      right ∈ before.pending :=
    right_mem_before_of_inverted_steps image transition hadvance
      left right before afterLeft afterLeftRight
      hleftSaved hrightSaved hkey
  have hrightTransitionBefore :
      transition rightNode right (before.localState rightNode)
        rightResult := by
    rw [← hrightStateBefore]
    exact hrightTransition
  have hrightOriginBefore :
      ChildrenUseOriginSequence rightNode.id
        (before.nextOriginSeq rightNode.id) rightResult.children := by
    rw [← hleftOtherCursor rightNode.id (Ne.symm hnodeIds)]
    exact hrightOrigin
  have hrightFreshBefore :
      ∀ child ∈ rightResult.children,
        child.key ∉ before.allocatedKeys := by
    intro child hchild hmem
    apply hrightFresh child hchild
    rw [hleftAllocated]
    exact List.mem_append_left _ hmem
  have hrightConsumptionsBefore :
      rightResult.packetReferenceConsumptions.Perm
        (ownedEventReference image right ::
          stateReferenceConsumptions image rightNode
            (before.localState rightNode) rightResult.nextState) := by
    simpa [← hrightStateBefore] using hrightConsumptions.1
  have hrightIncrementsBefore :
      ReferenceIncrementsValid image rightNode
        (before.localState rightNode) rightResult := by
    simpa [← hrightStateBefore] using hrightIncrements
  have hrightNewStateNodup :
      (ownedRoleStateReferences image rightNode
        rightResult.nextState).Nodup := by
    rw [← hrightState]
    exact ownedRoleStateReferences_nodup_of_machineWellFormed
      image horacle afterLeftRight hafterLeftRight rightNode hrightNode
  let afterRight :=
    materializeScalarResult image rightNode right rightResult before
  have hrightMaterialized :=
    materializeScalarResult_available image transition hunique horacle
      hgenerated hdescriptors rightNode hrightNode right rightResult
      before hbefore hrightMemBefore hrightTarget
      hrightTransitionBefore hrightOriginBefore hrightKeys
      hrightFreshBefore hrightConsumptionsBefore
      hrightIncrementsBefore hrightNewStateNodup
  have hrightStep :
      AvailableEventStep image transition right before afterRight :=
    hrightMaterialized.1
  have hafterRight : MachineWellFormed image afterRight :=
    hrightMaterialized.2
  have hleftRightNe : left ≠ right := by
    intro heq
    apply htarget
    rw [heq]
  have hleftMemAfterRight : left ∈ afterRight.pending := by
    unfold afterRight materializeScalarResult
    rw [mem_insertEvents_iff]
    exact Or.inr ((List.mem_erase_of_ne hleftRightNe).mpr hleftMem)
  have hleftTransitionAfterRight :
      transition leftNode left (afterRight.localState leftNode)
        leftResult := by
    have hnodeNe : leftNode ≠ rightNode := by
      intro heq
      exact hnodeIds (congrArg NodeDescriptor.id heq)
    simpa [afterRight, materializeScalarResult, hnodeNe] using
      hleftTransition
  have hleftOriginAfterRight :
      ChildrenUseOriginSequence leftNode.id
        (afterRight.nextOriginSeq leftNode.id) leftResult.children := by
    simpa [afterRight, materializeScalarResult, hnodeIds] using
      hleftOrigin
  have hleftFreshAfterRight :
      ∀ child ∈ leftResult.children,
        child.key ∉ afterRight.allocatedKeys := by
    intro leftChild hleftChild hmem
    unfold afterRight materializeScalarResult at hmem
    rcases List.mem_append.mp hmem with hold | hrightChildKey
    · exact hleftFresh leftChild hleftChild hold
    · rcases List.mem_map.mp hrightChildKey with
        ⟨rightChild, hrightChild, hkeyEq⟩
      apply hrightFresh rightChild hrightChild
      rw [hleftAllocated]
      exact List.mem_append_right _
        (List.mem_map.mpr
          ⟨leftChild, hleftChild, hkeyEq.symm⟩)
  have hleftConsumptionsAfterRight :
      leftResult.packetReferenceConsumptions.Perm
        (ownedEventReference image left ::
          stateReferenceConsumptions image leftNode
            (afterRight.localState leftNode) leftResult.nextState) := by
    have hnodeNe : leftNode ≠ rightNode := by
      intro heq
      exact hnodeIds (congrArg NodeDescriptor.id heq)
    simpa [afterRight, materializeScalarResult, hnodeNe] using
      hleftConsumptions.1
  have hleftIncrementsAfterRight :
      ReferenceIncrementsValid image leftNode
        (afterRight.localState leftNode) leftResult := by
    have hnodeNe : leftNode ≠ rightNode := by
      intro heq
      exact hnodeIds (congrArg NodeDescriptor.id heq)
    simpa [afterRight, materializeScalarResult, hnodeNe] using
      hleftIncrements
  have hleftNewStateNodup :
      (ownedRoleStateReferences image leftNode
        leftResult.nextState).Nodup := by
    rw [← hleftState]
    exact ownedRoleStateReferences_nodup_of_machineWellFormed
      image horacle afterLeft hafterLeft leftNode hleftNode
  let afterRightLeft :=
    materializeScalarResult image leftNode left leftResult afterRight
  have hleftMaterialized :=
    materializeScalarResult_available image transition hunique horacle
      hgenerated hdescriptors leftNode hleftNode left leftResult
      afterRight hafterRight hleftMemAfterRight hleftTarget
      hleftTransitionAfterRight hleftOriginAfterRight hleftKeys
      hleftFreshAfterRight hleftConsumptionsAfterRight
      hleftIncrementsAfterRight hleftNewStateNodup
  exact ⟨afterRight, afterRightLeft, hrightStep,
    hleftMaterialized.1, hleftMaterialized.2⟩

/--
The inverted cross-LP replay has the same complete owner-preserving result.  Public equality is a
corollary of this stronger relation.
-/
theorem inverted_cross_lp_steps_commute
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (hdeterministic : TransitionDeterministic transition)
    (hadvance : ChildrenAdvanceParent transition)
    (hobservations : TransitionObservationsUseEventKey transition)
    (before afterLeft afterLeftRight : MachineState State)
    (left right : Event)
    (hbefore : MachineWellFormed image before)
    (hleft :
      AvailableEventStep image transition left before afterLeft)
    (hright :
      AvailableEventStep image transition right afterLeft afterLeftRight)
    (htarget : left.target ≠ right.target)
    (hkey : right.key < left.key) :
    ∃ afterRight afterRightLeft,
      AvailableEventStep image transition right before afterRight ∧
      AvailableEventStep image transition left afterRight afterRightLeft ∧
      StrongMachineReplay image afterLeftRight afterRightLeft := by
  have hleftSaved := hleft
  have hrightSaved := hright
  have hafterLeft :=
    availableEventStep_preserves_machineWellFormed image transition
      hunique horacle hgenerated hdescriptors left before afterLeft
      hbefore hleftSaved
  have hafterLeftRight :=
    availableEventStep_preserves_machineWellFormed image transition
      hunique horacle hgenerated hdescriptors right afterLeft
      afterLeftRight hafterLeft hrightSaved
  obtain ⟨afterRight, afterRightLeft, hrightFirst, hleftSecond,
      hafterRightLeft⟩ :=
    inverted_cross_lp_steps_replay image transition hunique horacle
      hgenerated hdescriptors hadvance before afterLeft afterLeftRight
      left right hbefore hleftSaved hrightSaved htarget hkey
  have hrightFirstSaved := hrightFirst
  have hleftSecondSaved := hleftSecond
  have hafterRight :=
    availableEventStep_preserves_machineWellFormed image transition
      hunique horacle hgenerated hdescriptors right before afterRight
      hbefore hrightFirstSaved
  rcases hleft with
    ⟨_, leftNode, hleftNode, leftResult, hleftTarget,
      hleftTransition, _, hleftAllocates, hleftApplies, _, _,
      hleftPending, hleftEmissions⟩
  rcases hright with
    ⟨_, rightNode, hrightNode, rightResult, hrightTarget,
      hrightTransition, _, hrightAllocates, hrightApplies, _, _,
      hrightPending, hrightEmissions⟩
  rcases hrightFirst with
    ⟨_, swappedRightNode, hswappedRightNode, swappedRightResult,
      hswappedRightTarget, hswappedRightTransition, _,
      hswappedRightAllocates, hswappedRightApplies, _, _,
      hswappedRightPending, hswappedRightEmissions⟩
  rcases hleftSecond with
    ⟨_, swappedLeftNode, hswappedLeftNode, swappedLeftResult,
      hswappedLeftTarget, hswappedLeftTransition, _,
      hswappedLeftAllocates, hswappedLeftApplies, _, _,
      hswappedLeftPending, hswappedLeftEmissions⟩
  have hrightNodes : swappedRightNode = rightNode := by
    apply node_eq_of_unique_ids hunique hswappedRightNode hrightNode
    exact hswappedRightTarget.symm.trans hrightTarget
  subst swappedRightNode
  have hleftNodes : swappedLeftNode = leftNode := by
    apply node_eq_of_unique_ids hunique hswappedLeftNode hleftNode
    exact hswappedLeftTarget.symm.trans hleftTarget
  subst swappedLeftNode
  rcases hleftApplies with
    ⟨hleftState, _, hleftOther, hleftOutput, _, _⟩
  rcases hrightApplies with
    ⟨hrightState, _, hrightOther, hrightOutput, _, _⟩
  rcases hswappedRightApplies with
    ⟨hswappedRightState, _, hswappedRightOther, hswappedRightOutput,
      _, _⟩
  rcases hswappedLeftApplies with
    ⟨hswappedLeftState, _, hswappedLeftOther, hswappedLeftOutput,
      _, _⟩
  have hnodeIds : leftNode.id ≠ rightNode.id := by
    intro heq
    apply htarget
    rw [hleftTarget, hrightTarget, heq]
  have hrightStateBefore :
      afterLeft.localState rightNode = before.localState rightNode :=
    (hleftOther rightNode hrightNode (Ne.symm hnodeIds)).1
  have hrightResults : swappedRightResult = rightResult := by
    exact hdeterministic rightNode right (before.localState rightNode)
      swappedRightResult rightResult hswappedRightTransition
      (by rw [← hrightStateBefore]; exact hrightTransition)
  subst swappedRightResult
  have hleftStateAfterRight :
      afterRight.localState leftNode = before.localState leftNode :=
    (hswappedRightOther leftNode hleftNode hnodeIds).1
  have hleftResults : swappedLeftResult = leftResult := by
    exact hdeterministic leftNode left (before.localState leftNode)
      swappedLeftResult leftResult
      (by rw [← hleftStateAfterRight]; exact hswappedLeftTransition)
      hleftTransition
  subst swappedLeftResult
  have hlocal :
      ∀ node ∈ image.nodes,
        afterLeftRight.localState node =
          afterRightLeft.localState node := by
    intro node hnode
    by_cases hleftEq : node = leftNode
    · subst node
      rw [(hrightOther leftNode hleftNode hnodeIds).1,
        hleftState, hswappedLeftState]
    · by_cases hrightEq : node = rightNode
      · subst node
        rw [hrightState,
          (hswappedLeftOther rightNode hrightNode
            (Ne.symm hnodeIds)).1,
          hswappedRightState]
      · have hnodeLeft : node.id ≠ leftNode.id := by
          intro hid
          exact hleftEq
            (node_eq_of_unique_ids hunique hnode hleftNode hid)
        have hnodeRight : node.id ≠ rightNode.id := by
          intro hid
          exact hrightEq
            (node_eq_of_unique_ids hunique hnode hrightNode hid)
        rw [(hrightOther node hnode hnodeRight).1,
          (hleftOther node hnode hnodeLeft).1,
          (hswappedLeftOther node hnode hnodeLeft).1,
          (hswappedRightOther node hnode hnodeRight).1]
  have hleftRightNe : left ≠ right := by
    intro heq
    exact htarget (congrArg Event.target heq)
  have hrightNotLeftChild : right ∉ leftResult.children := by
    intro hchild
    have hforward :=
      hadvance leftNode left (before.localState leftNode)
        leftResult hleftTransition right hchild
    exact (EventKey.lt_irrefl right.key)
      (EventKey.lt_trans hkey hforward)
  have hleftNotRightChild : left ∉ rightResult.children := by
    intro hchild
    have hrightFresh := hrightAllocates.2.2.1 left hchild
    apply hrightFresh
    rw [hleftAllocates.2.2.2.1]
    exact List.mem_append_left _
      ((hbefore.2.2.2.1 left hleftSaved.1).1)
  have hpending :
      afterLeftRight.pending = afterRightLeft.pending := by
    apply canonicalPending_eq_of_mem_iff
      hafterLeftRight.1 hafterRightLeft.1
    intro candidate
    rw [hrightPending, hswappedLeftPending,
      mem_insertEvents_iff, mem_insertEvents_iff]
    rw [
      (canonicalPending_nodup hafterLeft.1).mem_erase_iff,
      (canonicalPending_nodup hafterRight.1).mem_erase_iff]
    rw [hleftPending, hswappedRightPending,
      mem_insertEvents_iff, mem_insertEvents_iff]
    rw [
      (canonicalPending_nodup hbefore.1).mem_erase_iff,
      (canonicalPending_nodup hbefore.1).mem_erase_iff]
    constructor <;> intro hmem
    · rcases hmem with hrightChild |
          ⟨hcandidateRight, hleftChild |
            ⟨hcandidateLeft, hbeforeMem⟩⟩
      · exact Or.inr
          ⟨by
            intro heq
            subst candidate
            exact hleftNotRightChild hrightChild,
          Or.inl hrightChild⟩
      · exact Or.inl hleftChild
      · exact Or.inr
          ⟨hcandidateLeft, Or.inr ⟨hcandidateRight, hbeforeMem⟩⟩
    · rcases hmem with hleftChild |
          ⟨hcandidateLeft, hrightChild |
            ⟨hcandidateRight, hbeforeMem⟩⟩
      · exact Or.inr
          ⟨by
            intro heq
            subst candidate
            exact hrightNotLeftChild hleftChild,
          Or.inl hleftChild⟩
      · exact Or.inl hrightChild
      · exact Or.inr
          ⟨hcandidateRight, Or.inr ⟨hcandidateLeft, hbeforeMem⟩⟩
  have hstores :
      PerLPOwnedStoresEquivalent image afterLeftRight afterRightLeft :=
    perLPOwnedStoresEquivalent_of_wellFormed image
      afterLeftRight afterRightLeft hafterLeftRight hafterRightLeft
      horacle hpending hlocal
  have hsummary :
      afterLeftRight.summary = afterRightLeft.summary := by
    rw [hrightOutput.1, hleftOutput.1,
      hswappedLeftOutput.1, hswappedRightOutput.1]
    exact RunSummary.add_left_comm before.summary
      leftResult.summaryDelta rightResult.summaryDelta
  have hobserved :
      afterLeftRight.observedPackets =
        afterRightLeft.observedPackets := by
    rw [hrightOutput.2.1, hleftOutput.2.1,
      hswappedLeftOutput.2.1, hswappedRightOutput.2.1]
    exact foldl_installDescriptor_batches_commute
      leftResult.observedPackets rightResult.observedPackets
      (by
        have hleftDescriptors :=
          (hdescriptors leftNode left (before.localState leftNode)
            leftResult hleftTransition).2.2
        have hrightDescriptors :=
          (hdescriptors rightNode right
            (afterLeft.localState rightNode)
            rightResult hrightTransition).2.2
        intro first hfirst second hsecond hid
        rcases List.mem_append.mp hfirst with hfirst | hfirst <;>
          rcases List.mem_append.mp hsecond with hsecond | hsecond
        · rw [hleftDescriptors first hfirst,
            hleftDescriptors second hsecond, hid]
        · rw [hleftDescriptors first hfirst,
            hrightDescriptors second hsecond, hid]
        · rw [hrightDescriptors first hfirst,
            hleftDescriptors second hsecond, hid]
        · rw [hrightDescriptors first hfirst,
            hrightDescriptors second hsecond, hid])
      before.observedPackets
  have hkeyNe : left.key ≠ right.key :=
    fun heq => by
      rw [← heq] at hkey
      exact EventKey.lt_irrefl _ hkey
  have hleftObservation :=
    hobservations leftNode left (before.localState leftNode)
      leftResult hleftTransition
  have hrightObservation :=
    hobservations rightNode right (afterLeft.localState rightNode)
      rightResult hrightTransition
  have hdepartures :
      afterLeftRight.departures = afterRightLeft.departures := by
    rw [hrightOutput.2.2.1, hleftOutput.2.2.1,
      hswappedLeftOutput.2.2.1, hswappedRightOutput.2.2.1]
    exact foldl_insertDeparture_batches_commute
      leftResult.departures rightResult.departures
      (by
        intro first hfirst second hsecond
        rw [hleftObservation.1 first hfirst,
          hrightObservation.1 second hsecond]
        exact hkeyNe)
      before.departures
  have harrivals :
      afterLeftRight.arrivals = afterRightLeft.arrivals := by
    rw [hrightOutput.2.2.2, hleftOutput.2.2.2,
      hswappedLeftOutput.2.2.2, hswappedRightOutput.2.2.2]
    exact foldl_insertArrival_batches_commute
      leftResult.arrivals rightResult.arrivals
      (by
        intro first hfirst second hsecond
        rw [hleftObservation.2 first hfirst,
          hrightObservation.2 second hsecond]
        exact hkeyNe)
      before.arrivals
  have hcursors :
      afterLeftRight.nextOriginSeq =
        afterRightLeft.nextOriginSeq := by
    funext origin
    by_cases hleftOriginEq : origin = leftNode.id
    · subst origin
      rw [hrightAllocates.2.2.2.2.2 leftNode.id hnodeIds,
        hleftAllocates.2.2.2.2.1,
        hswappedLeftAllocates.2.2.2.2.1,
        hswappedRightAllocates.2.2.2.2.2 leftNode.id
          hnodeIds]
    · by_cases hrightOriginEq : origin = rightNode.id
      · subst origin
        rw [hrightAllocates.2.2.2.2.1,
          hleftAllocates.2.2.2.2.2 rightNode.id
            (Ne.symm hnodeIds),
          hswappedLeftAllocates.2.2.2.2.2 rightNode.id
            (Ne.symm hnodeIds),
          hswappedRightAllocates.2.2.2.2.1]
      · rw [hrightAllocates.2.2.2.2.2 origin hrightOriginEq,
          hleftAllocates.2.2.2.2.2 origin hleftOriginEq,
          hswappedLeftAllocates.2.2.2.2.2 origin hleftOriginEq,
          hswappedRightAllocates.2.2.2.2.2 origin hrightOriginEq]
  have hallocated :
      afterLeftRight.allocatedKeys.Perm
        afterRightLeft.allocatedKeys := by
    rw [hrightAllocates.2.2.2.1, hleftAllocates.2.2.2.1,
      hswappedLeftAllocates.2.2.2.1,
      hswappedRightAllocates.2.2.2.1]
    simpa only [List.append_assoc] using
      (List.Perm.append_left before.allocatedKeys
        (List.perm_append_comm :
          (leftResult.children.map Event.key ++
            rightResult.children.map Event.key).Perm
          (rightResult.children.map Event.key ++
            leftResult.children.map Event.key)))
  have hemissions :
      afterLeftRight.emissions.Perm afterRightLeft.emissions := by
    rw [hrightEmissions, hleftEmissions,
      hswappedLeftEmissions, hswappedRightEmissions]
    simpa only [List.append_assoc] using
      (List.Perm.append_left before.emissions
        (List.perm_append_comm :
          ((leftResult.children.map fun child => (left, child)) ++
            (rightResult.children.map fun child => (right, child))).Perm
          ((rightResult.children.map fun child => (right, child)) ++
            (leftResult.children.map fun child => (left, child)))))
  exact ⟨afterRight, afterRightLeft,
    hrightFirstSaved, hleftSecondSaved,
    ⟨hlocal, hstores, hsummary, hobserved, hdepartures, harrivals,
      hpending, hcursors, hallocated, hemissions⟩⟩

end DaysExecutor
