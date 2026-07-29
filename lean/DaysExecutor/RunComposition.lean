import DaysExecutor.RoundBoundaryOrder

namespace DaysExecutor

theorem safeHorizonRounds_append
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (start middle finish : RoundState State)
    (leftBounds rightBounds : List BoundFamily)
    (hleft :
      SafeHorizonRounds image transition start leftBounds middle)
    (hright :
      SafeHorizonRounds image transition middle rightBounds finish) :
    SafeHorizonRounds image transition start
      (leftBounds ++ rightBounds) finish := by
  induction hleft with
  | refl =>
      exact hright
  | step first _ ih =>
      exact .step first (ih hright)

/-- Completing a valid round extends the actual post-exchange reachability prefix. -/
theorem reachablePostExchangeStartAfterRound_proved
    (image : SimulationImage State)
    (transition : TransitionRelation State) :
    ReachablePostExchangeStartAfterRound image transition := by
  intro bounds cut start drainedEvents finish hreachable hround
  rcases hreachable with
    ⟨initial, prefixBounds, hinitial, hinitialStart, hprefix⟩
  exact ⟨initial, prefixBounds ++ [bounds], hinitial, hinitialStart,
    safeHorizonRounds_append image transition
      initial start finish prefixBounds [bounds] hprefix
      (.step hround (.refl _))⟩

/-- Every event drained by a valid round lies through the configured inclusive stop. -/
theorem safeHorizonRound_drained_withinStop
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (cut : Event → Prop)
    (start finish : RoundState State)
    (drainedEvents : List Event)
    (hround :
      SafeHorizonRound image transition bounds cut
        start drainedEvents finish) :
    ∀ event ∈ drainedEvents,
      withinInclusiveStop image.stopTimeNs event := by
  rcases hround with
    ⟨_, _, _, hwithin, afterDrain, hdrain, _, _, hcut, _, _⟩
  rcases hdrain with ⟨order, horder, horderedDrain⟩
  intro event hevent
  have hbelow : belowBound bounds event :=
    (hcut.2.2 event).mp ((hcut.2.1 event).mp hevent) |>.2
  have htargetOrder :=
    drainLPsInOrder_event_target_mem image transition bounds order
      start afterDrain drainedEvents horderedDrain event hevent
  have htargetNodes : event.target ∈ image.nodes.map NodeDescriptor.id := by
    exact horder.subset htargetOrder
  rcases List.mem_map.mp htargetNodes with ⟨node, hnode, htarget⟩
  have hbound := hwithin node hnode
  unfold withinInclusiveStop
  unfold belowBound at hbelow
  rw [EventKey.lt_timeBoundary_iff] at hbelow
  rw [← htarget] at hbelow
  unfold stopExclusive at hbound
  omega

/--
All safe-horizon rounds materialize as one scalar trace. Exact owner provenance stitches round
endpoints, while validated bounds make the concatenated trace key ordered within every LP.
-/
theorem safeHorizonRounds_materializes_scalar
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (haccepted : AcceptedModel image transition)
    (start finish : RoundState State)
    (bounds : List BoundFamily)
    (hreachable : ReachablePostExchangeStart image transition start)
    (hrounds :
      SafeHorizonRounds image transition start bounds finish) :
    ∃ events scalarFinish,
      ExecutionInOrder image transition
        start.machine events scalarFinish ∧
      StrongMachineReplay image scalarFinish finish.machine ∧
      TargetKeyOrdered events ∧
      ∀ event ∈ events, withinInclusiveStop image.stopTimeNs event := by
  rcases haccepted with
    ⟨⟨hunique, _, _, _, _, _, _, _, _, horacle, _, _, _, _, _, _, _⟩,
      _, haxioms, _⟩
  rcases haxioms with
    ⟨hdeterministic, _, hgenerated, hadvance, _, _, _, _, _,
      hdescriptors, _⟩
  induction hrounds with
  | refl =>
      exact ⟨[], _, .refl _,
        strongMachineReplay_refl image _,
        List.Pairwise.nil, by simp⟩
  | @step bounds cut start drained middle boundsTail finish
      hround later ih =>
      have hroundCopy := hround
      rcases hround with
        ⟨hstart, _, _, _, afterDrain, hdrain, _, _, _,
          hexchange, hmiddleStart⟩
      obtain ⟨firstFinish, hfirstExecution, hfirstReplay⟩ :=
        sequentialRoundDrain_materializes_scalar image transition
          hunique horacle hgenerated hdescriptors hdeterministic
          bounds start afterDrain middle drained
          hstart hdrain hexchange hmiddleStart
      have hmiddleReachable :=
        reachablePostExchangeStartAfterRound_proved image transition
          bounds cut start drained middle hreachable hroundCopy
      obtain ⟨laterEvents, laterFinish, hlaterExecution,
          hlaterReplay, hlaterTargetOrdered, hlaterWithin⟩ :=
        ih hmiddleReachable
      obtain ⟨transportedFinish, htransportedExecution,
          htransportedReplay⟩ :=
        executionInOrder_of_strongMachineReplay image transition
          hunique laterEvents middle.machine laterFinish firstFinish
          (strongMachineReplay_symm image hfirstReplay) hlaterExecution
      have hfirstTargetOrdered :=
        sequentialRoundDrain_targetKeyOrdered image transition
          hunique horacle hgenerated hdescriptors hdeterministic hadvance
          bounds start afterDrain drained hstart hdrain
      have hlaterReachable :=
        (executionInOrder_potentiallyReachable image transition
          middle.machine.pending middle.machine laterFinish laterEvents
          (fun event hmem => PotentiallyReachableEvent.seed hmem)
          hlaterExecution).1
      have htargetOrdered :
          TargetKeyOrdered (drained ++ laterEvents) := by
        unfold TargetKeyOrdered
        rw [List.pairwise_append]
        refine ⟨hfirstTargetOrdered, hlaterTargetOrdered, ?_⟩
        intro left hleft right hright hsame
        exact safeHorizonRound_drained_lt_future_same_target
          image transition hunique hadvance bounds cut start middle drained
          hroundCopy hleft (hlaterReachable right hright) hsame
      exact ⟨drained ++ laterEvents, transportedFinish,
        executionInOrder_append image transition
          hfirstExecution htransportedExecution,
        strongMachineReplay_trans image
          (strongMachineReplay_symm image htransportedReplay)
          hlaterReplay,
        htargetOrdered,
        by
          intro event hevent
          rw [List.mem_append] at hevent
          rcases hevent with hfirst | hlater
          · exact safeHorizonRound_drained_withinStop
              image transition bounds cut start middle drained
              hroundCopy event hfirst
          · exact hlaterWithin event hlater⟩

/-- Repeated safe-horizon rounds compose to the canonical scalar run through stop. -/
theorem f3RunComposition_proved
    (image : SimulationImage State)
    (transition : TransitionRelation State) :
    F3RunComposition image transition := by
  intro haccepted _ start bounds finish hinitial hstart hrounds hstopped
  have hacceptedCopy := haccepted
  rcases haccepted with
    ⟨⟨hunique, _, _, _, _, _, _, _, _, horacle, _, _, _, _, _, _, _⟩,
      _, haxioms, _⟩
  rcases haxioms with
    ⟨hdeterministic, _, hgenerated, hadvance, _, _, _, _, _,
      hdescriptors, hobservations⟩
  have hreachable :
      ReachablePostExchangeStart image transition start :=
    ⟨start, [], hinitial, hstart, .refl _⟩
  have hinitialWellFormed : MachineWellFormed image start.machine :=
    hinitial.2.2.2.2.2.2.2.2.2
  obtain ⟨events, scalarFinish, hexecution, hfinishReplay,
      htargetOrdered, hallWithin⟩ :=
    safeHorizonRounds_materializes_scalar image transition
      hacceptedCopy
      start finish bounds hreachable hrounds
  obtain ⟨serialOrder, serialFinish, hserialExecution,
      hserialOrdered, hserialPerm, hsortReplay⟩ :=
    executionInOrder_sort image transition
      hunique horacle hgenerated hdescriptors hdeterministic
      hadvance hobservations start.machine scalarFinish events
      hinitialWellFormed htargetOrdered hexecution
  have hpending :
      serialFinish.pending = finish.machine.pending :=
    hsortReplay.2.2.2.2.2.2.1.symm.trans
      hfinishReplay.2.2.2.2.2.2.1
  have hnoFinal :
      NoEligibleEvent (withinInclusiveStop image.stopTimeNs)
        serialFinish.pending := by
    rw [hpending]
    exact hstopped
  have hcanonical :=
    executionInOrder_to_canonicalRestricted image transition
      hunique horacle hgenerated hdescriptors
      (withinInclusiveStop image.stopTimeNs)
      start.machine serialFinish serialOrder hinitialWellFormed hserialOrdered
      (fun event hevent =>
        hallWithin event (hserialPerm.mem_iff.mp hevent))
      hnoFinal hserialExecution
  exact ⟨serialOrder, serialFinish, hcanonical,
    strongMachineReplay_implies_result image serialFinish finish.machine
      (strongMachineReplay_trans image
        (strongMachineReplay_symm image hsortReplay)
        hfinishReplay)⟩

end DaysExecutor
