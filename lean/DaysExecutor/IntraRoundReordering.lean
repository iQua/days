import DaysExecutor.ExecutionCanonical
import DaysExecutor.LinearExtensionSwaps
import DaysExecutor.WitnessChecks

namespace DaysExecutor

/-!
F5 replay support.  The public commutation premise makes a reversed adjacent pair executable;
the owned cross-LP diamond upgrades that pair to suffix-stable replay equality.
-/

theorem canonicalSerialExecution_to_executionInOrder
    (hexecution :
      CanonicalSerialExecution image transition eligible before events after) :
    ExecutionInOrder image transition before events after := by
  induction hexecution with
  | refl =>
      exact .refl _
  | step first rest ih =>
      exact .step first.2 ih

theorem consecutive_canonical_steps_key_lt
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hadvance : ChildrenAdvanceParent transition)
    (eligible : Event → Prop)
    (before middle after : MachineState State)
    (left right : Event)
    (hbefore : MachineWellFormed image before)
    (hleft :
      CanonicalSerialStep image transition eligible
        before left middle)
    (hright :
      CanonicalSerialStep image transition eligible
        middle right after) :
    left.key < right.key := by
  have hleftStep := hleft.2
  have hrightStep := hright.2
  have hkeyNe :=
    adjacent_available_steps_key_ne image transition hadvance
      before middle after left right hbefore hleftStep hrightStep
  rcases hleftStep with
    ⟨_, node, _, result, _, htransition, _, _, _, _, _,
      hpending, _⟩
  have hrightMiddle := hright.1.1
  rw [hpending, mem_insertEvents_iff] at hrightMiddle
  rcases hrightMiddle with hchild | hold
  · exact hadvance node left (before.localState node)
      result htransition right hchild
  · exact EventKey.lt_of_le_of_ne
      (hleft.1.2.2 right (List.mem_of_mem_erase hold) hright.1.2.1)
      hkeyNe

theorem canonicalSerialExecution_keyOrdered
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (hadvance : ChildrenAdvanceParent transition)
    (eligible : Event → Prop)
    (before after : MachineState State)
    (events : List Event)
    (hbefore : MachineWellFormed image before)
    (hexecution :
      CanonicalSerialExecution image transition eligible
        before events after) :
    KeyOrdered events := by
  induction hexecution with
  | refl =>
      exact List.Pairwise.nil
  | step first rest ih =>
      have hmiddle :=
        availableEventStep_preserves_machineWellFormed image transition
          hunique horacle hgenerated hdescriptors _ _ _
          hbefore first.2
      have htail := ih hmiddle
      apply List.pairwise_cons.mpr
      constructor
      · cases rest with
        | refl =>
            simp
        | step second laterExecution =>
            have hfirstNext :=
              consecutive_canonical_steps_key_lt image transition
                hadvance eligible _ _ _ _ _ hbefore first second
            intro candidate hcandidate
            rcases List.mem_cons.mp hcandidate with rfl | hremaining
            · exact hfirstNext
            · exact EventKey.lt_trans hfirstNext
                ((List.pairwise_cons.mp htail).1 candidate hremaining)
      · exact htail

theorem executionInOrder_emission_suffix_advances
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hadvance : ChildrenAdvanceParent transition)
    (before after : MachineState State)
    (events : List Event)
    (hexecution :
      ExecutionInOrder image transition before events after) :
    ∃ delta,
      after.emissions = before.emissions ++ delta ∧
        ∀ parent child,
          (parent, child) ∈ delta →
            parent.key < child.key := by
  induction hexecution with
  | refl =>
      exact ⟨[], by simp, by simp⟩
  | @step event before middle events after first rest ih =>
      rcases first with
        ⟨_, node, _, result, _, htransition, _, _, _, _, _,
          _, hemissions⟩
      obtain ⟨later, hlater, hlaterAdvances⟩ := ih
      refine
        ⟨result.children.map (fun child => (event, child)) ++ later,
          ?_, ?_⟩
      · rw [hlater, hemissions, List.append_assoc]
      · intro parent child hmember
        rw [List.mem_append] at hmember
        rcases hmember with hcurrent | hfuture
        · rcases List.mem_map.mp hcurrent with
            ⟨emitted, hemitted, hpair⟩
          injection hpair with hparent hchild
          subst parent
          subst child
          exact hadvance node event (before.localState node)
            result htransition emitted hemitted
        · exact hlaterAdvances parent child hfuture

theorem recordedCausalBefore_key_lt
    (hedges :
      ∀ parent child,
        RecordedEmissionEdge emissions parent child →
          parent.key < child.key)
    (hcausal : RecordedCausalBefore emissions parent child) :
    parent.key < child.key := by
  induction hcausal with
  | direct edge =>
      exact hedges _ _ edge
  | tail edge rest ih =>
      exact EventKey.lt_trans (hedges _ _ edge) ih

theorem roundEmissionDelta_required_edges_advance
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hadvance : ChildrenAdvanceParent transition)
    (start finish : MachineState State)
    (events : List Event)
    (roundEmissions : List (Event × Event))
    (hexecution :
      ExecutionInOrder image transition start events finish)
    (hdelta : RoundEmissionDelta start finish roundEmissions) :
    ∀ left right,
      RequiredIntraRoundBefore roundEmissions left right →
        left.key < right.key := by
  obtain ⟨actualDelta, hemissions, hadvances⟩ :=
    executionInOrder_emission_suffix_advances image transition
      hadvance start finish events hexecution
  have hdeltaEq : roundEmissions = actualDelta := by
    exact List.append_cancel_left (hdelta.symm.trans hemissions)
  subst actualDelta
  intro left right hrequired
  rcases hrequired with hpacket | hqueue
  · exact recordedCausalBefore_key_lt
      (fun parent child hedge =>
        hadvances parent child hedge)
      hpacket.2
  · exact hqueue.2.2

theorem availableEventStep_same_before_strong
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (hdeterministic : TransitionDeterministic transition)
    (event : Event)
    (before leftAfter rightAfter : MachineState State)
    (hleft :
      AvailableEventStep image transition event before leftAfter)
    (hright :
      AvailableEventStep image transition event before rightAfter) :
    StrongMachineReplay image leftAfter rightAfter := by
  rcases hleft with
    ⟨_, leftNode, hleftNode, leftResult, hleftTarget,
      hleftTransition, _, hleftAllocates, hleftApplies, _, _,
      hleftPending, hleftEmissions⟩
  rcases hright with
    ⟨_, rightNode, hrightNode, rightResult, hrightTarget,
      hrightTransition, _, hrightAllocates, hrightApplies, _, _,
      hrightPending, hrightEmissions⟩
  have hnode : leftNode = rightNode :=
    node_eq_of_unique_ids hunique hleftNode hrightNode
      (hleftTarget.symm.trans hrightTarget)
  subst rightNode
  have hresult : leftResult = rightResult :=
    hdeterministic leftNode event (before.localState leftNode)
      leftResult rightResult hleftTransition hrightTransition
  subst rightResult
  rcases hleftAllocates with
    ⟨_, _, _, hleftAllocated, hleftCursor, hleftOtherCursor⟩
  rcases hrightAllocates with
    ⟨_, _, _, hrightAllocated, hrightCursor, hrightOtherCursor⟩
  rcases hleftApplies with
    ⟨hleftState, hleftStore, hleftOther, hleftOutput, _, _⟩
  rcases hrightApplies with
    ⟨hrightState, hrightStore, hrightOther, hrightOutput, _, _⟩
  rcases hleftOutput with
    ⟨hleftSummary, hleftObserved, hleftDepartures, hleftArrivals⟩
  rcases hrightOutput with
    ⟨hrightSummary, hrightObserved, hrightDepartures, hrightArrivals⟩
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro node hnode
    by_cases heq : node = leftNode
    · subst node
      exact hleftState.trans hrightState.symm
    · have hid : node.id ≠ leftNode.id := by
        intro hid
        exact heq (node_eq_of_unique_ids hunique hnode hleftNode hid)
      exact (hleftOther node hnode hid).1.trans
        (hrightOther node hnode hid).1.symm
  · intro node hnode
    have hstoreEq :
        leftAfter.packetStore node = rightAfter.packetStore node := by
      by_cases heq : node = leftNode
      · subst node
        exact hleftStore.trans hrightStore.symm
      · have hid : node.id ≠ leftNode.id := by
          intro hid
          exact heq (node_eq_of_unique_ids hunique hnode hleftNode hid)
        exact (hleftOther node hnode hid).2.trans
          (hrightOther node hnode hid).2.symm
    rw [hstoreEq]
    exact ownedStoresEquivalent_refl _
  · exact hleftSummary.trans hrightSummary.symm
  · exact hleftObserved.trans hrightObserved.symm
  · exact hleftDepartures.trans hrightDepartures.symm
  · exact hleftArrivals.trans hrightArrivals.symm
  · exact hleftPending.trans hrightPending.symm
  · funext origin
    by_cases heq : origin = leftNode.id
    · subst origin
      exact hleftCursor.trans hrightCursor.symm
    · exact (hleftOtherCursor origin heq).trans
        (hrightOtherCursor origin heq).symm
  · rw [hleftAllocated, hrightAllocated]
  · rw [hleftEmissions, hrightEmissions]

theorem availableEventStep_strong_congr
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (hdeterministic : TransitionDeterministic transition)
    (event : Event)
    (leftBefore leftAfter rightBefore rightAfter : MachineState State)
    (hbefore : StrongMachineReplay image leftBefore rightBefore)
    (hleft :
      AvailableEventStep image transition event leftBefore leftAfter)
    (hright :
      AvailableEventStep image transition event rightBefore rightAfter) :
    StrongMachineReplay image leftAfter rightAfter := by
  obtain ⟨transported, htransported, hreplay⟩ :=
    availableEventStep_of_strongMachineReplay image transition
      hunique event leftBefore leftAfter rightBefore hbefore hleft
  exact strongMachineReplay_trans image hreplay
    (availableEventStep_same_before_strong image transition
      hunique hdeterministic event rightBefore transported rightAfter
      htransported hright)

theorem executionInOrder_split_append
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (first later : List Event)
    (before after : MachineState State)
    (hexecution :
      ExecutionInOrder image transition before (first ++ later) after) :
    ∃ middle,
      ExecutionInOrder image transition before first middle ∧
        ExecutionInOrder image transition middle later after := by
  induction first generalizing before with
  | nil =>
      exact ⟨before, .refl _, hexecution⟩
  | cons head tail ih =>
      cases hexecution with
      | step hhead hrest =>
          obtain ⟨middle, htail, hlater⟩ := ih _ hrest
          exact ⟨middle, .step hhead htail, hlater⟩

theorem executionInOrder_excludes_absent_allocated
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
    ghost ∉ events := by
  induction hexecution with
  | refl =>
      simp
  | step first rest ih =>
      intro hmember
      rcases List.mem_cons.mp hmember with heq | htail
      · apply habsent
        rw [heq]
        exact first.1
      · have hmiddle :=
          availableEventStep_preserves_machineWellFormed image transition
            hunique horacle hgenerated hdescriptors _ _ _
            hbefore first
        have hpreserved :=
          availableEventStep_preserves_absent_allocated image transition
            ghost _ _ hallocated habsent first
        exact ih hmiddle hpreserved.2 hpreserved.1 htail

theorem executionInOrder_events_nodup
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
    events.Nodup := by
  induction hexecution with
  | refl =>
      exact List.nodup_nil
  | step first rest ih =>
      have hmiddle :=
        availableEventStep_preserves_machineWellFormed image transition
          hunique horacle hgenerated hdescriptors _ _ _
          hbefore first
      have hremoved :=
        availableEventStep_removes_processed image transition
          _ _ _ hbefore first
      exact List.nodup_cons.mpr
        ⟨executionInOrder_excludes_absent_allocated image transition
            hunique horacle hgenerated hdescriptors _ _ _ _
            hmiddle hremoved.2 hremoved.1 rest,
          ih hmiddle⟩

theorem executionInOrder_preserves_machineWellFormed
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
    MachineWellFormed image after := by
  induction hexecution with
  | refl =>
      exact hbefore
  | step first rest ih =>
      exact ih
        (availableEventStep_preserves_machineWellFormed image transition
          hunique horacle hgenerated hdescriptors _ _ _
          hbefore first)

theorem independent_inverted_targets_ne
    (hindependent : IntraRoundIndependent emissions left right)
    (hinverted : right.key < left.key) :
    left.target ≠ right.target := by
  intro htarget
  apply hindependent.2
  exact Or.inr
    ⟨htarget.symm, by simp [fifoQueueConflictClass], hinverted⟩

theorem independentAdjacentSwapTrace_replay
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (hdeterministic : TransitionDeterministic transition)
    (hadvance : ChildrenAdvanceParent transition)
    (hobservations : TransitionObservationsUseEventKey transition)
    (emissions : List (Event × Event))
    (hcommute : IndependentStepsCommute image transition emissions)
    (before canonicalFinish : MachineState State)
    (candidate canonical : List Event)
    (hbefore : MachineWellFormed image before)
    (htrace :
      IndependentAdjacentSwapTrace emissions candidate canonical)
    (hcanonical :
      ExecutionInOrder image transition
        before canonical canonicalFinish) :
    ∃ candidateFinish,
      ExecutionInOrder image transition
        before candidate candidateFinish ∧
      StrongMachineReplay image canonicalFinish candidateFinish := by
  induction htrace with
  | refl =>
      exact ⟨canonicalFinish, hcanonical,
        strongMachineReplay_refl image canonicalFinish⟩
  | step beforePart afterPart left right hinverted hindependent rest ih =>
      obtain ⟨closerFinish, hcloserExecution, hcloserReplay⟩ :=
        ih hcanonical
      obtain ⟨pairBefore, hprefix, hpairAndSuffix⟩ :=
        executionInOrder_split_append image transition beforePart
          (right :: left :: afterPart) before closerFinish
          (by simpa using hcloserExecution)
      cases hpairAndSuffix with
      | step hright hleftAndSuffix =>
          cases hleftAndSuffix with
          | step hleft hsuffix =>
              have hpairBefore :
                  MachineWellFormed image pairBefore :=
                executionInOrder_preserves_machineWellFormed image transition
                  hunique horacle hgenerated hdescriptors before
                  pairBefore beforePart hbefore hprefix
              have hindependentReverse :
                  IntraRoundIndependent emissions right left :=
                ⟨hindependent.2, hindependent.1⟩
              obtain ⟨afterLeft, afterLeftRight, hleftFirst,
                  hrightSecond, _⟩ :=
                hcommute pairBefore right left _ _
                  hindependentReverse hright hleft
              obtain ⟨afterRightAgain, afterRightLeftAgain,
                  hrightAgain, hleftAgain, hinvertedReplay⟩ :=
                inverted_cross_lp_steps_commute image transition
                  hunique horacle hgenerated hdescriptors
                  hdeterministic hadvance hobservations
                  pairBefore afterLeft afterLeftRight left right
                  hpairBefore hleftFirst hrightSecond
                  (independent_inverted_targets_ne hindependent
                    hinverted)
                  hinverted
              have hrightMiddleReplay :
                  StrongMachineReplay image _ _ :=
                availableEventStep_same_before_strong image transition
                  hunique hdeterministic right pairBefore
                  _ _ hright hrightAgain
              have hsortedPairReplay :
                  StrongMachineReplay image _ _ :=
                availableEventStep_strong_congr image transition
                  hunique hdeterministic left _ _ _ _
                  hrightMiddleReplay hleft hleftAgain
              have horiginalToCandidate :
                  StrongMachineReplay image _ afterLeftRight :=
                strongMachineReplay_trans image hsortedPairReplay
                  (strongMachineReplay_symm image hinvertedReplay)
              obtain ⟨candidateFinish, hcandSuffix,
                  hsuffixReplay⟩ :=
                executionInOrder_of_strongMachineReplay
                  image transition hunique afterPart _ _
                  afterLeftRight horiginalToCandidate hsuffix
              have hcandExecution :
                  ExecutionInOrder image transition before
                    (beforePart ++ left :: right :: afterPart)
                    candidateFinish :=
                executionInOrder_append image transition hprefix
                  (.step hleftFirst (.step hrightSecond hcandSuffix))
              exact ⟨candidateFinish, hcandExecution,
                strongMachineReplay_trans image hcloserReplay
                  hsuffixReplay⟩

theorem crossLPSameKindClusteringLicensed_proved :
    CrossLPSameKindClusteringLicensed := by
  intro emissions left right _ htarget hleftPacket hrightPacket
  constructor
  · rintro (hpacket | hqueue)
    · exact hleftPacket hpacket
    · exact htarget hqueue.1
  · rintro (hpacket | hqueue)
    · exact hrightPacket hpacket
    · exact htarget hqueue.1.symm

theorem f5IntraRoundReordering_proved
    (image : SimulationImage State)
    (transition : TransitionRelation State) :
    F5IntraRoundReordering image transition := by
  refine ⟨?_, crossLPSameKindClusteringLicensed_proved,
    unsoundReorderingCounterexampleCheck_true⟩
  intro haccepted bounds cut start drainedEvents roundFinish
      canonicalOrder canonicalFinish candidateOrder roundEmissions
      hround hcanonical hdelta hcommute hmembership hpreserves
  rcases haccepted with
    ⟨⟨hunique, _, _, _, _, _, _, _, _, horacle, _, _, _, _, _, _, _⟩,
      _, haxioms, _⟩
  rcases haxioms with
    ⟨hdeterministic, _, hgenerated, hadvance, _, _, _, _, _,
      hdescriptors, hobservations⟩
  rcases hround with
    ⟨hstart, _, _, _, afterDrain, hdrain, _, _, _,
      hexchange, hfinish⟩
  have hstartWellFormed : MachineWellFormed image start.machine :=
    hstart.2.1
  obtain ⟨scalarDrainFinish, hscalarDrain, _⟩ :=
    sequentialRoundDrain_materializes_scalar image transition
      hunique horacle hgenerated hdescriptors hdeterministic
      bounds start afterDrain roundFinish drainedEvents
      hstart hdrain hexchange hfinish
  have hdrainedNodup : drainedEvents.Nodup :=
    executionInOrder_events_nodup image transition
      hunique horacle hgenerated hdescriptors
      start.machine scalarDrainFinish drainedEvents
      hstartWellFormed hscalarDrain
  have hcanonicalExecution :
      ExecutionInOrder image transition start.machine
        canonicalOrder canonicalFinish :=
    canonicalSerialExecution_to_executionInOrder hcanonical.1
  have hordered : KeyOrdered canonicalOrder :=
    canonicalSerialExecution_keyOrdered image transition
      hunique horacle hgenerated hdescriptors hadvance cut
      start.machine canonicalFinish canonicalOrder
      hstartWellFormed hcanonical.1
  have hrequiredKey :
      ∀ left ∈ canonicalOrder, ∀ right ∈ canonicalOrder,
        RequiredIntraRoundBefore roundEmissions left right →
          left.key < right.key := by
    intro left _ right _ hrequired
    exact roundEmissionDelta_required_edges_advance
      image transition hadvance start.machine canonicalFinish
      canonicalOrder roundEmissions hcanonicalExecution hdelta
      left right hrequired
  have hpreservesCanonical :
      PreservesRequiredIntraRoundOrder roundEmissions
        canonicalOrder candidateOrder := by
    constructor
    · have hcanonicalNodup : canonicalOrder.Nodup :=
        hordered.imp (fun {left right} hlt heq => by
          subst right
          exact EventKey.lt_irrefl left.key hlt)
      have hcandidateNodup : candidateOrder.Nodup :=
        hpreserves.1.symm.nodup hdrainedNodup
      exact perm_of_nodup_of_mem_iff
        hcandidateNodup hcanonicalNodup
        (fun event =>
          hpreserves.1.mem_iff.trans (hmembership event).symm)
    · intro left hleft right hright hrequired
      exact hpreserves.2 left ((hmembership left).mp hleft)
        right ((hmembership right).mp hright) hrequired
  have htrace :=
    linearExtension_to_keyOrder_by_independent_adjacent_swaps
      roundEmissions canonicalOrder candidateOrder hordered
      hpreservesCanonical hrequiredKey
  obtain ⟨candidateFinish, hcandExecution, hcandReplay⟩ :=
    independentAdjacentSwapTrace_replay image transition
      hunique horacle hgenerated hdescriptors hdeterministic
      hadvance hobservations roundEmissions hcommute
      start.machine canonicalFinish candidateOrder canonicalOrder
      hstartWellFormed htrace hcanonicalExecution
  exact ⟨candidateFinish, hcandExecution,
    strongMachineReplay_implies_result image
      canonicalFinish candidateFinish hcandReplay⟩

end DaysExecutor
