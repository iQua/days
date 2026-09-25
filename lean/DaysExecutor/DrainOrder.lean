import DaysExecutor.CrossLPSwap

namespace DaysExecutor

/-- Strict canonical pending order makes the event key injective on list members. -/
theorem event_eq_of_canonical_key_eq
    (pending : List Event)
    (hcanonical : CanonicalPending pending)
    {left right : Event}
    (hleft : left ∈ pending)
    (hright : right ∈ pending)
    (hkey : left.key = right.key) :
    left = right := by
  induction pending with
  | nil =>
      simp at hleft
  | cons head tail ih =>
      have hhead := (List.pairwise_cons.mp hcanonical).1
      have htail := (List.pairwise_cons.mp hcanonical).2
      rcases List.mem_cons.mp hleft with hleft | hleft
      · subst left
        rcases List.mem_cons.mp hright with hright | hright
        · exact hright.symm
        · have hlt := hhead right hright
          rw [← hkey] at hlt
          exact False.elim (EventKey.lt_irrefl _ hlt)
      · rcases List.mem_cons.mp hright with hright | hright
        · subst right
          have hlt := hhead left hleft
          rw [hkey] at hlt
          exact False.elim (EventKey.lt_irrefl _ hlt)
        · exact ih htail hleft hright

/-- Every recorded event in one LP drain targets that LP. -/
theorem sequentialDrainLP_event_target
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (node : NodeDescriptor)
    (before after : RoundState State)
    (events : List Event)
    (hdrain :
      SequentialDrainLP image transition bounds node before events after) :
    ∀ event ∈ events, event.target = node.id := by
  induction hdrain with
  | done =>
      simp
  | step first rest ih =>
      intro candidate hcandidate
      rcases List.mem_cons.mp hcandidate with rfl | htail
      · exact first.2.1.2.1.1
      · exact ih candidate htail

/-- Consecutive events in one LP drain strictly increase in canonical event-key order. -/
theorem consecutive_local_steps_key_lt
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hadvance : ChildrenAdvanceParent transition)
    (bounds : BoundFamily)
    (node : NodeDescriptor)
    (before middle after : RoundState State)
    (scalarBefore : MachineState State)
    (left right : Event)
    (hprogress : RoundScalarProgress image before scalarBefore)
    (hleft :
      LocalRoundStep image transition bounds node before left middle)
    (hright :
      LocalRoundStep image transition bounds node middle right after) :
    left.key < right.key := by
  rcases hprogress with
    ⟨hscalarWellFormed, huniverse, _, _, _, _, _, hpendingCover,
      _, _, _⟩
  rcases hleft with
    ⟨_, hleftLeast, result, htransition, _, _, _, _, _,
      hpending, _, _, _, _, _, _⟩
  have hrightEligible := hright.2.1.2.1
  have hrightMiddle := hright.2.1.1
  rw [hpending, mem_insertEvents_iff] at hrightMiddle
  rcases hrightMiddle with hchild | hold
  · exact hadvance node left (before.machine.localState node)
      result htransition right (List.mem_filter.mp hchild).1
  · have hrightBefore := List.mem_of_mem_erase hold
    have hle :=
      hleftLeast.2.2 right hrightBefore
        hrightEligible
    apply EventKey.lt_of_le_of_ne hle
    intro hkey
    have hbeforePendingNodup :=
      (List.nodup_append.mp huniverse).1
    have hrightNeLeft : right ≠ left :=
      (hbeforePendingNodup.mem_erase_iff.mp hold).1
    apply hrightNeLeft
    apply event_eq_of_canonical_key_eq scalarBefore.pending
      hscalarWellFormed.1
    · exact (hpendingCover right).mpr
        (by
          unfold roundEventUniverse
          exact List.mem_append_left _ hrightBefore)
    · exact (hpendingCover left).mpr
        (by
          unfold roundEventUniverse
          exact List.mem_append_left _ hleftLeast.1)
    · exact hkey.symm

/-- Events within a complete LP drain are pairwise key ordered. -/
theorem sequentialDrainLP_events_pairwise
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (hdeterministic : TransitionDeterministic transition)
    (hadvance : ChildrenAdvanceParent transition)
    (bounds : BoundFamily)
    (node : NodeDescriptor)
    (before after : RoundState State)
    (events : List Event)
    (scalarBefore : MachineState State)
    (hprogress : RoundScalarProgress image before scalarBefore)
    (hdrain :
      SequentialDrainLP image transition bounds node before events after) :
    events.Pairwise fun left right => left.key < right.key := by
  induction hdrain generalizing scalarBefore with
  | done =>
      exact List.Pairwise.nil
  | @step before event middle events after first rest ih =>
      obtain ⟨scalarMiddle, _, hmiddleProgress⟩ :=
        localRoundStep_materializes_scalar image transition
          hunique horacle hgenerated hdescriptors hdeterministic
          bounds node before middle scalarBefore event hprogress first
      have htail := ih scalarMiddle hmiddleProgress
      apply List.pairwise_cons.mpr
      constructor
      · intro candidate hcandidate
        cases rest with
        | done =>
            simp at hcandidate
        | step nextFirst nextRest =>
            have hfirstNext :=
              consecutive_local_steps_key_lt image transition hadvance
                bounds node _ _ _ scalarBefore
                event _ hprogress first nextFirst
            rcases List.mem_cons.mp hcandidate with rfl | hcandidate
            · exact hfirstNext
            · have hnextAll :=
                (List.pairwise_cons.mp htail).1 candidate hcandidate
              exact EventKey.lt_trans hfirstNext hnextAll
      · exact htail

/-- Per-target order retained by a candidate event list. -/
def TargetKeyOrdered (events : List Event) : Prop :=
  events.Pairwise fun left right =>
    left.target = right.target → left.key < right.key

/-- Every drained event targets one of the LP identifiers in the chosen drain order. -/
theorem drainLPsInOrder_event_target_mem
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (order : List NodeId)
    (before after : RoundState State)
    (events : List Event)
    (hdrain :
      DrainLPsInOrder image transition bounds order before events after) :
    ∀ event ∈ events, event.target ∈ order := by
  induction hdrain with
  | nil =>
      simp
  | @cons node before localEvents middle order later after
      hnode first rest ih =>
      intro event hmem
      rw [List.mem_append] at hmem
      rcases hmem with hlocal | hlater
      · exact List.mem_cons.mpr
          (Or.inl (sequentialDrainLP_event_target
            image transition bounds _ _ _ _ first event hlocal))
      · exact List.mem_cons.mpr (Or.inr (ih event hlater))

/-- Whole-LP drains preserve strict key order within each target LP. -/
theorem drainLPsInOrder_targetKeyOrdered
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (hdeterministic : TransitionDeterministic transition)
    (hadvance : ChildrenAdvanceParent transition)
    (bounds : BoundFamily)
    (order : List NodeId)
    (before after : RoundState State)
    (events : List Event)
    (scalarBefore : MachineState State)
    (hprogress : RoundScalarProgress image before scalarBefore)
    (horder : order.Nodup)
    (hdrain :
      DrainLPsInOrder image transition bounds order before events after) :
    TargetKeyOrdered events := by
  induction hdrain generalizing scalarBefore with
  | nil =>
      exact List.Pairwise.nil
  | cons hnode first rest ih =>
      have horderParts := List.nodup_cons.mp horder
      obtain ⟨scalarMiddle, _, hmiddleProgress⟩ :=
        sequentialDrainLP_materializes_scalar image transition
          hunique horacle hgenerated hdescriptors hdeterministic
          bounds _ _ _ _ scalarBefore
          hprogress first
      have hlocalOrdered :=
        sequentialDrainLP_events_pairwise image transition
          hunique horacle hgenerated hdescriptors hdeterministic
          hadvance bounds _ _ _ _ scalarBefore
          hprogress first
      have hlaterOrdered :=
        ih scalarMiddle hmiddleProgress horderParts.2
      unfold TargetKeyOrdered
      rw [List.pairwise_append]
      refine ⟨?_, hlaterOrdered, ?_⟩
      · exact hlocalOrdered.imp (by
          intro left right hlt _
          exact hlt)
      · intro left hleft right hright hsame
        have hleftTarget :=
          sequentialDrainLP_event_target image transition bounds _
            _ _ _ first left hleft
        have hrightTarget :=
          drainLPsInOrder_event_target_mem image transition bounds
            _ _ _ _ rest right hright
        apply False.elim
        apply horderParts.1
        rw [← hleftTarget, hsame]
        exact hrightTarget

/-- The recorded event list of a sequential round drain is key ordered within every LP. -/
theorem sequentialRoundDrain_targetKeyOrdered
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (hdeterministic : TransitionDeterministic transition)
    (hadvance : ChildrenAdvanceParent transition)
    (bounds : BoundFamily)
    (start after : RoundState State)
    (events : List Event)
    (hstart : PostExchangeStart image start)
    (hdrain :
      SequentialRoundDrain image transition bounds start events after) :
    TargetKeyOrdered events := by
  rcases hdrain with ⟨order, horder, horderedDrain⟩
  apply drainLPsInOrder_targetKeyOrdered image transition
    hunique horacle hgenerated hdescriptors hdeterministic hadvance
    bounds order start after events start.machine
    (roundScalarProgress_start image start hstart)
    (horder.nodup_iff.mpr hunique)
    horderedDrain

end DaysExecutor
