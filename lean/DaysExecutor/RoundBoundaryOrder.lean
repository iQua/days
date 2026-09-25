import DaysExecutor.RoundSerializability

namespace DaysExecutor

/-- Reachability of pending work and buffered remote envelopes from one round's start roots. -/
def RoundFutureReachability
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (startPending : List Event)
    (state : RoundState State) : Prop :=
  (∀ event ∈ state.machine.pending,
      PotentiallyReachableEvent transition startPending event) ∧
    ∀ owner ∈ image.nodes, ∀ envelope ∈ state.outboxes owner.id,
      UnseenRemoteAt transition startPending envelope.event.target envelope.event

theorem remoteEnvelopesFromStore_event_mem_public
    (image : SimulationImage State)
    (source : NodeId)
    (store : List PacketStoreEntry)
    {events : List Event}
    {envelopes : List RemoteEnvelope}
    (hfrom : RemoteEnvelopesFromStore image source store events envelopes)
    {envelope : RemoteEnvelope}
    (hmem : envelope ∈ envelopes) :
    envelope.event ∈ events := by
  induction events generalizing envelopes with
  | nil =>
      cases envelopes <;> simp [RemoteEnvelopesFromStore] at hfrom hmem
  | cons event events ih =>
      cases envelopes with
      | nil =>
          simp [RemoteEnvelopesFromStore] at hfrom
      | cons head tail =>
          simp only [RemoteEnvelopesFromStore] at hfrom
          rcases hfrom with ⟨_, hevent, _, _, _, htail⟩
          simp only [List.mem_cons] at hmem ⊢
          rcases hmem with rfl | hmem
          · exact Or.inl hevent
          · exact Or.inr (ih htail hmem)

theorem roundFutureReachability_start
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (start : RoundState State)
    (hstart : PostExchangeStart image start) :
    RoundFutureReachability image transition start.machine.pending start := by
  constructor
  · intro event hmem
    exact PotentiallyReachableEvent.seed hmem
  · intro owner howner envelope hmem
    rw [hstart.1 owner howner] at hmem
    simp at hmem

theorem roundFutureReachability_step
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (startPending : List Event)
    (node : NodeDescriptor)
    (before after : RoundState State)
    (event : Event)
    (hinvariant :
      RoundFutureReachability image transition startPending before)
    (hstep :
      LocalRoundStep image transition bounds node before event after) :
    RoundFutureReachability image transition startPending after := by
  rcases hstep with
    ⟨hnode, hleast, result, htransition, _, _, _, _, _, hpending,
      _, _, emittedRemote, hremoteEnvelopes, houtbox, hotherOutboxes⟩
  have heventReachable :=
    hinvariant.1 event hleast.1
  have heventTarget : event.target = node.id := hleast.2.1.1
  constructor
  · intro candidate hcandidate
    rw [hpending, mem_insertEvents_iff] at hcandidate
    rcases hcandidate with hlocal | hremaining
    · have hchild : candidate ∈ result.children :=
        (List.mem_filter.mp hlocal).1
      exact PotentiallyReachableEvent.child heventReachable
        ⟨node, before.machine.localState node, result, htransition, hchild⟩
    · exact hinvariant.1 candidate (List.mem_of_mem_erase hremaining)
  · intro owner howner envelope henvelope
    by_cases hid : owner.id = node.id
    · rw [hid, houtbox] at henvelope
      rcases List.mem_append.mp henvelope with hold | hnew
      · exact hinvariant.2 owner howner envelope (by simpa [hid] using hold)
      · have hremoteEvent :=
          remoteEnvelopesFromStore_event_mem_public image
            node.id (after.machine.packetStore node) hremoteEnvelopes hnew
        have hchild : envelope.event ∈ result.children :=
          (List.mem_filter.mp hremoteEvent).1
        have htargetNe : envelope.event.target ≠ node.id :=
          of_decide_eq_true (List.mem_filter.mp hremoteEvent).2
        exact ⟨rfl, event, heventReachable,
          ⟨node, before.machine.localState node, result, htransition, hchild⟩,
          fun hequal => htargetNe (hequal.symm.trans heventTarget)⟩
    · rw [hotherOutboxes owner howner hid] at henvelope
      exact hinvariant.2 owner howner envelope henvelope

theorem roundFutureReachability_drainLP
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (startPending : List Event)
    (node : NodeDescriptor)
    (before : RoundState State)
    (drained : List Event)
    (after : RoundState State)
    (hinvariant :
      RoundFutureReachability image transition startPending before)
    (hdrain :
      SequentialDrainLP image transition bounds node before drained after) :
    RoundFutureReachability image transition startPending after := by
  induction hdrain with
  | done =>
      exact hinvariant
  | step first rest ih =>
      exact ih (roundFutureReachability_step image transition bounds
        startPending node _ _ _ hinvariant first)

theorem roundFutureReachability_drainLPs
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (startPending : List Event)
    (order : List NodeId)
    (before : RoundState State)
    (drained : List Event)
    (after : RoundState State)
    (hinvariant :
      RoundFutureReachability image transition startPending before)
    (hdrain :
      DrainLPsInOrder image transition bounds order before drained after) :
    RoundFutureReachability image transition startPending after := by
  induction hdrain with
  | nil =>
      exact hinvariant
  | cons nodeMember first rest ih =>
      exact ih (roundFutureReachability_drainLP image transition bounds
        startPending _ _ _ _ hinvariant first)

theorem sequentialRoundDrain_futureReachability
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (start afterDrain : RoundState State)
    (drainedEvents : List Event)
    (hstart : PostExchangeStart image start)
    (hdrain :
      SequentialRoundDrain image transition bounds
        start drainedEvents afterDrain) :
    RoundFutureReachability image transition
      start.machine.pending afterDrain := by
  rcases hdrain with ⟨order, _, horderedDrain⟩
  exact roundFutureReachability_drainLPs image transition bounds
    start.machine.pending order start drainedEvents afterDrain
    (roundFutureReachability_start image transition start hstart)
    horderedDrain

/-- A local step on another LP preserves exhaustion of a fixed target LP. -/
theorem localRoundStep_preserves_noEligible_other
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (node : NodeDescriptor)
    (target : NodeId)
    (before after : RoundState State)
    (event : Event)
    (hne : node.id ≠ target)
    (hno :
      NoEligibleEvent
        (fun candidate =>
          candidate.target = target ∧ belowBound bounds candidate)
        before.machine.pending)
    (hstep :
      LocalRoundStep image transition bounds node before event after) :
    NoEligibleEvent
      (fun candidate =>
        candidate.target = target ∧ belowBound bounds candidate)
      after.machine.pending := by
  rcases hstep with
    ⟨_, _, result, _, _, _, _, _, _, hpending, _, _, _⟩
  intro candidate hcandidate heligible
  rw [hpending, mem_insertEvents_iff] at hcandidate
  rcases hcandidate with hlocal | hremaining
  · have htargetNode : candidate.target = node.id :=
      of_decide_eq_true (List.mem_filter.mp hlocal).2
    exact hne (htargetNode.symm.trans heligible.1)
  · exact hno candidate (List.mem_of_mem_erase hremaining) heligible

theorem sequentialDrainLP_preserves_noEligible_other
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (node : NodeDescriptor)
    (target : NodeId)
    (before after : RoundState State)
    (events : List Event)
    (hne : node.id ≠ target)
    (hno :
      NoEligibleEvent
        (fun candidate =>
          candidate.target = target ∧ belowBound bounds candidate)
        before.machine.pending)
    (hdrain :
      SequentialDrainLP image transition bounds node before events after) :
    NoEligibleEvent
      (fun candidate =>
        candidate.target = target ∧ belowBound bounds candidate)
      after.machine.pending := by
  induction hdrain with
  | done =>
      exact hno
  | step first rest ih =>
      exact ih (localRoundStep_preserves_noEligible_other
        image transition bounds node target _ _ _ hne hno first)

theorem sequentialDrainLP_noEligible
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (node : NodeDescriptor)
    (before after : RoundState State)
    (events : List Event)
    (hdrain :
      SequentialDrainLP image transition bounds node before events after) :
    NoEligibleEvent
      (fun candidate =>
        candidate.target = node.id ∧ belowBound bounds candidate)
      after.machine.pending := by
  induction hdrain with
  | done empty =>
      exact empty
  | step _ _ ih =>
      exact ih

theorem drainLPsInOrder_preserves_noEligible_absent
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (order : List NodeId)
    (target : NodeId)
    (before after : RoundState State)
    (events : List Event)
    (htarget : target ∉ order)
    (hno :
      NoEligibleEvent
        (fun candidate =>
          candidate.target = target ∧ belowBound bounds candidate)
        before.machine.pending)
    (hdrain :
      DrainLPsInOrder image transition bounds order before events after) :
    NoEligibleEvent
      (fun candidate =>
        candidate.target = target ∧ belowBound bounds candidate)
      after.machine.pending := by
  induction hdrain with
  | nil =>
      exact hno
  | @cons node before localEvents middle order later after
      hnode first rest ih =>
      have hparts : target ≠ node.id ∧ target ∉ order := by
        simpa using htarget
      exact ih hparts.2
        (sequentialDrainLP_preserves_noEligible_other
          image transition bounds node target before middle localEvents
          hparts.1.symm hno first)

theorem drainLPsInOrder_noEligible_mem
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (order : List NodeId)
    (before after : RoundState State)
    (events : List Event)
    (horder : order.Nodup)
    (hdrain :
      DrainLPsInOrder image transition bounds order before events after) :
    ∀ target ∈ order,
      NoEligibleEvent
        (fun candidate =>
          candidate.target = target ∧ belowBound bounds candidate)
        after.machine.pending := by
  induction hdrain with
  | nil =>
      simp
  | @cons node before localEvents middle order later after
      hnode first rest ih =>
      have hparts := List.nodup_cons.mp horder
      intro target htarget
      rcases List.mem_cons.mp htarget with rfl | htail
      · exact drainLPsInOrder_preserves_noEligible_absent
          image transition bounds order node.id middle after later
          hparts.1
          (sequentialDrainLP_noEligible image transition bounds node
            before middle localEvents first)
          rest
      · exact ih hparts.2 target htail

/-- A complete round drain leaves no pending event below its target LP bound. -/
theorem sequentialRoundDrain_noEligible
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (bounds : BoundFamily)
    (start afterDrain : RoundState State)
    (events : List Event)
    (htargets :
      ∀ event ∈ afterDrain.machine.pending,
        ∃ node ∈ image.nodes, event.target = node.id)
    (hdrain :
      SequentialRoundDrain image transition bounds start events afterDrain) :
    NoEligibleEvent (belowBound bounds) afterDrain.machine.pending := by
  rcases hdrain with ⟨order, horder, horderedDrain⟩
  have horderNodup : order.Nodup :=
    horder.nodup_iff.mpr hunique
  intro event hevent hbelow
  have htargetInOrder : event.target ∈ order := by
    rw [horder.mem_iff]
    rcases htargets event hevent with ⟨node, hnode, htarget⟩
    exact List.mem_map.mpr ⟨node, hnode, htarget.symm⟩
  exact (drainLPsInOrder_noEligible_mem image transition bounds order
    start afterDrain events horderNodup horderedDrain
    event.target htargetInOrder) event hevent ⟨rfl, hbelow⟩

/-- Potential reachability is monotone when every new root is reachable from the old roots. -/
theorem potentiallyReachable_mono
    (transition : TransitionRelation State)
    (oldRoots newRoots : List Event)
    (hroots :
      ∀ event ∈ newRoots,
        PotentiallyReachableEvent transition oldRoots event)
    {event : Event}
    (hreachable :
      PotentiallyReachableEvent transition newRoots event) :
    PotentiallyReachableEvent transition oldRoots event := by
  induction hreachable with
  | seed member =>
      exact hroots _ member
  | child _ edge ih =>
      exact PotentiallyReachableEvent.child ih edge

/-- Scalar execution preserves reachability of pending work and records reachable executed work. -/
theorem executionInOrder_potentiallyReachable
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (roots : List Event)
    (before after : MachineState State)
    (events : List Event)
    (hbefore :
      ∀ event ∈ before.pending,
        PotentiallyReachableEvent transition roots event)
    (hexecution :
      ExecutionInOrder image transition before events after) :
    (∀ event ∈ events,
      PotentiallyReachableEvent transition roots event) ∧
    (∀ event ∈ after.pending,
      PotentiallyReachableEvent transition roots event) := by
  induction hexecution with
  | refl =>
      exact ⟨by simp, hbefore⟩
  | @step event before middle events after first rest ih =>
      have heventReachable := hbefore event first.1
      rcases first with
        ⟨_, node, _, result, _, htransition, _, _, _, _, _, hpending, _⟩
      have hmiddle :
          ∀ candidate ∈ middle.pending,
            PotentiallyReachableEvent transition roots candidate := by
        intro candidate hcandidate
        rw [hpending, mem_insertEvents_iff] at hcandidate
        rcases hcandidate with hchild | hremaining
        · exact PotentiallyReachableEvent.child heventReachable
            ⟨node, before.localState node, result, htransition,
              hchild⟩
        · exact hbefore candidate (List.mem_of_mem_erase hremaining)
      have htail := ih hmiddle
      constructor
      · intro candidate hcandidate
        rcases List.mem_cons.mp hcandidate with rfl | hmem
        · exact heventReachable
        · exact htail.1 candidate hmem
      · exact htail.2

/-- The post-exchange pending queue remains reachable from the round-start roots. -/
theorem safeHorizonRound_finish_potentiallyReachable
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (cut : Event → Prop)
    (start finish : RoundState State)
    (drainedEvents : List Event)
    (hround :
      SafeHorizonRound image transition bounds cut
        start drainedEvents finish) :
    ∀ event ∈ finish.machine.pending,
      PotentiallyReachableEvent transition start.machine.pending event := by
  rcases hround with
    ⟨hstart, _, _, _, afterDrain, hdrain, _, _, _, hexchange, _⟩
  have hinvariant :=
    sequentialRoundDrain_futureReachability image transition bounds
      start afterDrain drainedEvents hstart hdrain
  rcases hexchange with
    ⟨ordered, hperm, _, _, _, _, hpending, _, _, _, _, _, _, _, _, _⟩
  intro event hevent
  rw [hpending, mem_insertEvents_iff] at hevent
  rcases hevent with hremote | hremaining
  · rcases List.mem_map.mp hremote with ⟨envelope, henvelope, rfl⟩
    have hflattened : envelope ∈ flattenedOutboxes image afterDrain :=
      hperm.symm.subset henvelope
    simp only [flattenedOutboxes, List.mem_flatMap] at hflattened
    rcases hflattened with ⟨owner, howner, houtbox⟩
    rcases hinvariant.2 owner howner envelope houtbox with
      ⟨_, parent, hparent, hedge, _⟩
    exact PotentiallyReachableEvent.child hparent hedge
  · exact hinvariant.1 event hremaining

/-- A complete exchange does not reintroduce any event below the just-drained bounds. -/
theorem safeHorizonRound_finish_noBelowBound
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (bounds : BoundFamily)
    (cut : Event → Prop)
    (start finish : RoundState State)
    (drainedEvents : List Event)
    (hround :
      SafeHorizonRound image transition bounds cut
        start drainedEvents finish) :
    NoEligibleEvent (belowBound bounds) finish.machine.pending := by
  rcases hround with
    ⟨hstart, hbounds, _, _, afterDrain, hdrain, _, _, _,
      hexchange, hfinish⟩
  rcases hexchange with
    ⟨ordered, hperm, _, _, _, _, hpending, _, _, _, _, _, _, _, _, _⟩
  have hinvariant :=
    sequentialRoundDrain_futureReachability image transition bounds
      start afterDrain drainedEvents hstart hdrain
  have htargets :
      ∀ event ∈ afterDrain.machine.pending,
        ∃ node ∈ image.nodes, event.target = node.id := by
    intro event hevent
    have hfinishMem : event ∈ finish.machine.pending := by
      rw [hpending, mem_insertEvents_iff]
      exact Or.inr hevent
    rcases (hfinish.2.1.2.2.2.1 event hfinishMem).2 with
      ⟨node, hnode, htarget, _⟩
    exact ⟨node, hnode, htarget⟩
  have hafterNo :=
    sequentialRoundDrain_noEligible image transition hunique bounds
      start afterDrain drainedEvents htargets hdrain
  intro event hevent hbelow
  rw [hpending, mem_insertEvents_iff] at hevent
  rcases hevent with hremote | hremaining
  · rcases List.mem_map.mp hremote with ⟨envelope, henvelope, heq⟩
    subst event
    have hflattened : envelope ∈ flattenedOutboxes image afterDrain :=
      hperm.symm.subset henvelope
    simp only [flattenedOutboxes, List.mem_flatMap] at hflattened
    rcases hflattened with ⟨owner, howner, houtbox⟩
    exact hbounds envelope.event.target envelope.event
      (hinvariant.2 owner howner envelope houtbox) hbelow
  · exact hafterNo event hremaining hbelow

theorem EventKey.lt_of_below_of_not_below
    (bounds : BoundFamily)
    {left right : Event}
    (htarget : left.target = right.target)
    (hleft : belowBound bounds left)
    (hright : ¬ belowBound bounds right) :
    left.key < right.key := by
  unfold belowBound at hleft hright
  rw [htarget] at hleft
  rcases EventKey.lt_trichotomy
      (EventKey.timeBoundary (bounds right.target)) right.key with
      hboundaryRight | heq | hrightBoundary
  · exact EventKey.lt_trans hleft hboundaryRight
  · simpa [heq] using hleft
  · exact False.elim (hright hrightBoundary)

/--
Every event drained in one round precedes same-target work reachable after its exchange. Local
descendants advance their parent; a cross-LP descendant is an unseen remote event and therefore
lies beyond the validated target bound.
-/
theorem safeHorizonRound_drained_lt_future_same_target
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (hadvance : ChildrenAdvanceParent transition)
    (bounds : BoundFamily)
    (cut : Event → Prop)
    (start finish : RoundState State)
    (drainedEvents : List Event)
    (hround :
      SafeHorizonRound image transition bounds cut
        start drainedEvents finish)
    {left right : Event}
    (hleft : left ∈ drainedEvents)
    (hright :
      PotentiallyReachableEvent transition finish.machine.pending right)
    (htarget : left.target = right.target) :
    left.key < right.key := by
  have hroundCopy := hround
  rcases hround with
    ⟨_, hbounds, _, _, _, _, _, _, hcut, _, _⟩
  have hleftBelow : belowBound bounds left :=
    (hcut.2.2 left).mp ((hcut.2.1 left).mp hleft) |>.2
  have hfinishNo :=
    safeHorizonRound_finish_noBelowBound image transition hunique
      bounds cut start finish drainedEvents hroundCopy
  have hfinishReach :=
    safeHorizonRound_finish_potentiallyReachable image transition
      bounds cut start finish drainedEvents hroundCopy
  induction hright with
  | seed hmem =>
      exact EventKey.lt_of_below_of_not_below bounds htarget hleftBelow
        (hfinishNo _ hmem)
  | @child parent child hparent hedge ih =>
      rcases hedge with ⟨node, state, result, htransition, hchild⟩
      by_cases hlocal : parent.target = child.target
      · exact EventKey.lt_trans
          (ih (htarget.trans hlocal.symm))
          (hadvance node parent state result htransition child hchild)
      · have hparentStart :
            PotentiallyReachableEvent transition start.machine.pending parent :=
          potentiallyReachable_mono transition start.machine.pending
            finish.machine.pending hfinishReach hparent
        have hunseen :
            UnseenRemoteAt transition start.machine.pending
              child.target child :=
          ⟨rfl, parent, hparentStart,
            ⟨node, state, result, htransition, hchild⟩, hlocal⟩
        exact EventKey.lt_of_below_of_not_below bounds htarget hleftBelow
          (hbounds child.target child hunseen)

end DaysExecutor
