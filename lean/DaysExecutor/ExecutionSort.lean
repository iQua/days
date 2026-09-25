import DaysExecutor.DrainOrder

namespace DaysExecutor

/-- A caller-supplied execution transports across strong owner-preserving replay equality. -/
theorem executionInOrder_of_strongMachineReplay
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (events : List Event)
    (leftBefore leftAfter rightBefore : MachineState State)
    (hreplay : StrongMachineReplay image leftBefore rightBefore)
    (hexecution :
      ExecutionInOrder image transition leftBefore events leftAfter) :
    ∃ rightAfter,
      ExecutionInOrder image transition rightBefore events rightAfter ∧
      StrongMachineReplay image leftAfter rightAfter := by
  induction hexecution generalizing rightBefore with
  | refl =>
      exact ⟨rightBefore, .refl _, hreplay⟩
  | step first rest ih =>
      obtain ⟨rightMiddle, hfirst, hmiddleReplay⟩ :=
        availableEventStep_of_strongMachineReplay image transition
          hunique _ _ _ rightBefore hreplay first
      obtain ⟨rightAfter, hrest, hafterReplay⟩ :=
        ih rightMiddle hmiddleReplay
      exact ⟨rightAfter, .step hfirst hrest, hafterReplay⟩

/-- Consecutive successful scalar events have distinct lifetime keys. -/
theorem adjacent_available_steps_key_ne
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hadvance : ChildrenAdvanceParent transition)
    (before middle after : MachineState State)
    (left right : Event)
    (hbefore : MachineWellFormed image before)
    (hleft :
      AvailableEventStep image transition left before middle)
    (hright :
      AvailableEventStep image transition right middle after) :
    left.key ≠ right.key := by
  rcases hleft with
    ⟨hleftMem, node, _, result, _, htransition, _, _, _, _, _,
      hpending, _⟩
  have hrightMiddle := hright.1
  rw [hpending, mem_insertEvents_iff] at hrightMiddle
  rcases hrightMiddle with hchild | hold
  · have hlt :=
      hadvance node left (before.localState node)
        result htransition right hchild
    exact fun heq => EventKey.lt_irrefl _ (by simpa [heq] using hlt)
  · have hrightBefore := List.mem_of_mem_erase hold
    have hrightNeLeft :=
      ((canonicalPending_nodup hbefore.1).mem_erase_iff.mp hold).1
    intro hkey
    apply hrightNeLeft
    exact event_eq_of_canonical_key_eq before.pending hbefore.1
      hrightBefore hleftMem hkey.symm

/-- A strictly key-ordered list. -/
def KeyOrdered (events : List Event) : Prop :=
  events.Pairwise fun left right => left.key < right.key

/--
Insert the first event into an already key-ordered suffix by replaying adjacent inverted cross-LP
pairs. The endpoint retains strong owner provenance.
-/
theorem insertHeadExecution
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (hdeterministic : TransitionDeterministic transition)
    (hadvance : ChildrenAdvanceParent transition)
    (hobservations : TransitionObservationsUseEventKey transition)
    (head : Event)
    (events : List Event)
    (before after : MachineState State)
    (hbefore : MachineWellFormed image before)
    (hordered : KeyOrdered events)
    (htargetOrdered : TargetKeyOrdered (head :: events))
    (hexecution :
      ExecutionInOrder image transition before (head :: events) after) :
    ∃ inserted finish,
      ExecutionInOrder image transition before inserted finish ∧
      KeyOrdered inserted ∧
      inserted.Perm (head :: events) ∧
      StrongMachineReplay image after finish := by
  induction events generalizing before after with
  | nil =>
      exact ⟨[head], after, hexecution, by simp [KeyOrdered],
        List.Perm.refl _, strongMachineReplay_refl image after⟩
  | cons second rest ih =>
      cases hexecution with
      | step hhead htail =>
        cases htail with
        | step hsecond hrest =>
          have hkeyNe :=
            adjacent_available_steps_key_ne image transition hadvance
              _ _ _ head second hbefore hhead hsecond
          rcases EventKey.lt_trichotomy head.key second.key with
              hheadSecond | heq | hsecondHead
          · have htailOrdered := List.pairwise_cons.mp hordered
            have hall :
                ∀ candidate ∈ second :: rest,
                  head.key < candidate.key := by
              intro candidate hcandidate
              rcases List.mem_cons.mp hcandidate with rfl | hcandidate
              · exact hheadSecond
              · exact EventKey.lt_trans hheadSecond
                  (htailOrdered.1 candidate hcandidate)
            exact ⟨head :: second :: rest, after,
              .step hhead (.step hsecond hrest),
              List.pairwise_cons.mpr ⟨hall, hordered⟩,
              List.Perm.refl _,
              strongMachineReplay_refl image after⟩
          · exact False.elim (hkeyNe heq)
          · have htargets := (List.pairwise_cons.mp htargetOrdered).1
            have htargetNe : head.target ≠ second.target := by
              intro heqTarget
              have hforward :=
                htargets second List.mem_cons_self heqTarget
              exact EventKey.not_le_of_lt hsecondHead
                (EventKey.lt_implies_le hforward)
            obtain ⟨afterSecond, afterSecondHead, hsecondFirst,
                hheadSecondStep, hpairReplay⟩ :=
              inverted_cross_lp_steps_commute image transition
                hunique horacle hgenerated hdescriptors hdeterministic
                hadvance hobservations _ _ _ head second hbefore
                hhead hsecond htargetNe hsecondHead
            obtain ⟨transportedAfter, htransportedRest,
                htransportedReplay⟩ :=
              executionInOrder_of_strongMachineReplay
                image transition hunique rest _ _ afterSecondHead
                hpairReplay hrest
            have hafterSecond :
                MachineWellFormed image afterSecond :=
              availableEventStep_preserves_machineWellFormed
                image transition hunique horacle hgenerated hdescriptors
                second before afterSecond hbefore hsecondFirst
            have hheadRestTarget :
                TargetKeyOrdered (head :: rest) := by
              apply List.pairwise_cons.mpr
              constructor
              · intro candidate hcandidate
                exact (List.pairwise_cons.mp htargetOrdered).1 candidate
                  (List.mem_cons_of_mem second hcandidate)
              · exact (List.pairwise_cons.mp
                  (List.pairwise_cons.mp htargetOrdered).2).2
            have hrestOrdered :
                KeyOrdered rest :=
              (List.pairwise_cons.mp hordered).2
            obtain ⟨insertedTail, insertedFinish, hinsertedExecution,
                hinsertedOrdered, hinsertedPerm, hinsertedReplay⟩ :=
              ih afterSecond transportedAfter hafterSecond hrestOrdered
                hheadRestTarget
                (.step hheadSecondStep htransportedRest)
            have hsecondAll :
                ∀ candidate ∈ insertedTail,
                  second.key < candidate.key := by
              intro candidate hcandidate
              have horiginal :=
                hinsertedPerm.subset hcandidate
              rcases List.mem_cons.mp horiginal with rfl | hrestMem
              · exact hsecondHead
              · exact (List.pairwise_cons.mp hordered).1 candidate hrestMem
            exact ⟨second :: insertedTail, insertedFinish,
              .step hsecondFirst hinsertedExecution,
              List.pairwise_cons.mpr
                ⟨hsecondAll, hinsertedOrdered⟩,
              (hinsertedPerm.cons second).trans
                (List.Perm.swap head second rest),
              strongMachineReplay_trans image htransportedReplay
                hinsertedReplay⟩

/--
Any target-key-ordered execution can be normalized to global EventKey order using only adjacent
cross-LP swaps.
-/
theorem executionInOrder_sort
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (horacle : DescriptorOracleWellFormed image)
    (hgenerated : GeneratedEventsRoleCorrect image transition)
    (hdescriptors : TransitionDescriptorEffectsCoherent image transition)
    (hdeterministic : TransitionDeterministic transition)
    (hadvance : ChildrenAdvanceParent transition)
    (hobservations : TransitionObservationsUseEventKey transition)
    (before after : MachineState State)
    (events : List Event)
    (hbefore : MachineWellFormed image before)
    (htargetOrdered : TargetKeyOrdered events)
    (hexecution :
      ExecutionInOrder image transition before events after) :
    ∃ sorted finish,
      ExecutionInOrder image transition before sorted finish ∧
      KeyOrdered sorted ∧
      sorted.Perm events ∧
      StrongMachineReplay image after finish := by
  induction hexecution with
  | refl =>
      exact ⟨[], _, .refl _, List.Pairwise.nil,
        List.Perm.refl _, strongMachineReplay_refl image _⟩
  | @step event before middle events after first rest ih =>
      have hmiddle :=
        availableEventStep_preserves_machineWellFormed image transition
          hunique horacle hgenerated hdescriptors event before middle
          hbefore first
      have htailTarget := (List.pairwise_cons.mp htargetOrdered).2
      obtain ⟨sortedTail, tailFinish, htailExecution,
          htailOrdered, htailPerm, htailReplay⟩ :=
        ih hmiddle htailTarget
      have hheadSortedTarget :
          TargetKeyOrdered (event :: sortedTail) := by
        apply List.pairwise_cons.mpr
        constructor
        · intro candidate hcandidate
          have horiginal := htailPerm.subset hcandidate
          exact (List.pairwise_cons.mp htargetOrdered).1
            candidate horiginal
        · exact htailOrdered.imp (by
            intro left right hlt _
            exact hlt)
      obtain ⟨sorted, finish, hsortedExecution, hsortedOrdered,
          hsortedPerm, hsortedReplay⟩ :=
        insertHeadExecution image transition hunique horacle
          hgenerated hdescriptors hdeterministic hadvance
          hobservations event sortedTail before tailFinish hbefore
          htailOrdered hheadSortedTarget (.step first htailExecution)
      exact ⟨sorted, finish, hsortedExecution, hsortedOrdered,
        hsortedPerm.trans (htailPerm.cons event),
        strongMachineReplay_trans image htailReplay hsortedReplay⟩

end DaysExecutor
