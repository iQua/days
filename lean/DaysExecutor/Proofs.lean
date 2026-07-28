import DaysExecutor.Statements

namespace DaysExecutor

private theorem foldlExtendedMin_preservesUpper
    (items : List α)
    (measure : α → ExtendedTime)
    (upper : Nat) :
    ∃ minimum,
      items.foldl
          (fun current item => extendedMin current (measure item))
          (some upper) =
        some minimum ∧
      minimum ≤ upper := by
  induction items generalizing upper with
  | nil =>
      exact ⟨upper, rfl, Nat.le_refl upper⟩
  | cons head tail ih =>
      cases hmeasure : measure head with
      | none =>
          simpa [extendedMin, hmeasure] using ih upper
      | some value =>
          obtain ⟨minimum, hminimum, hle⟩ := ih (Nat.min upper value)
          exact ⟨minimum, by simpa [extendedMin, hmeasure] using hminimum,
            Nat.le_trans hle (Nat.min_le_left upper value)⟩

private theorem foldlExtendedMin_le_of_mem
    (items : List α)
    (measure : α → ExtendedTime)
    (initial : ExtendedTime)
    {item : α}
    (hitem : item ∈ items)
    {time : Nat}
    (hmeasure : measure item = some time) :
    ∃ minimum,
      items.foldl
          (fun current entry => extendedMin current (measure entry))
          initial =
        some minimum ∧
      minimum ≤ time := by
  induction items generalizing initial with
  | nil =>
      simp at hitem
  | cons head tail ih =>
      simp only [List.mem_cons] at hitem
      rcases hitem with rfl | htail
      · cases initial with
        | none =>
            simpa [extendedMin, hmeasure] using
              foldlExtendedMin_preservesUpper tail measure time
        | some upper =>
            obtain ⟨minimum, hminimum, hle⟩ :=
              foldlExtendedMin_preservesUpper tail measure (Nat.min upper time)
            exact ⟨minimum, by simpa [extendedMin, hmeasure] using hminimum,
              Nat.le_trans hle (Nat.min_le_right upper time)⟩
      · exact ih (extendedMin initial (measure head)) htail

theorem minimumEventTime_le_of_mem
    {event : Event}
    {events : List Event}
    (hmem : event ∈ events) :
    ∃ minimum,
      minimumEventTime events = some minimum ∧
      minimum ≤ event.key.timeNs := by
  exact
    foldlExtendedMin_le_of_mem events
      (fun candidate => some candidate.key.timeNs)
      none hmem rfl

theorem leastPendingTimeFor_le_of_mem
    {event : Event}
    {pending : List Event}
    (hmem : event ∈ pending) :
    ∃ minimum,
      leastPendingTimeFor pending event.target = some minimum ∧
      minimum ≤ event.key.timeNs := by
  apply minimumEventTime_le_of_mem
  simpa using hmem

theorem minimumLPFrontier_le_of_pending
    (image : SimulationImage State)
    (machine : MachineState State)
    (hwellFormed : MachineWellFormed image machine)
    {event : Event}
    (hmem : event ∈ machine.pending) :
    ∃ frontier,
      minimumLPFrontier image machine = some frontier ∧
      frontier ≤ event.key.timeNs := by
  obtain ⟨minimum, hminimum, hminimumLe⟩ :=
    leastPendingTimeFor_le_of_mem hmem
  obtain ⟨node, hnode, htarget, _, _⟩ :=
    (hwellFormed.2.2.2.1 event hmem).2
  rw [htarget] at hminimum
  obtain ⟨frontier, hfrontier, hfrontierLe⟩ :=
    foldlExtendedMin_le_of_mem image.nodes
      (fun owner => leastPendingTimeFor machine.pending owner.id)
      none hnode hminimum
  exact ⟨frontier, hfrontier, Nat.le_trans hfrontierLe hminimumLe⟩

theorem minimumChannelDelay_le_of_mem
    (image : SimulationImage State)
    {channel : RemoteChannel}
    (hmem : channel ∈ image.channels) :
    ∃ lookahead,
      minimumChannelDelay image = some lookahead ∧
      lookahead ≤ channel.minDelayNs := by
  exact
    foldlExtendedMin_le_of_mem image.channels
      (fun candidate => some candidate.minDelayNs)
      none hmem rfl

theorem emissionEdge_time_mono
    (hadvance : ChildrenAdvanceParent transition)
    {parent child : Event}
    (hedge : PotentialEmissionEdge transition parent child) :
    parent.key.timeNs ≤ child.key.timeNs := by
  obtain ⟨node, state, result, htransition, hchild⟩ := hedge
  have hkey := hadvance node parent state result htransition child hchild
  change EventKey.lexLT parent.key child.key at hkey
  unfold EventKey.lexLT at hkey
  omega

theorem potentiallyReachable_seed_time_le
    (hadvance : ChildrenAdvanceParent transition)
    {startPending : List Event}
    {event : Event}
    (hreachable : PotentiallyReachableEvent transition startPending event) :
    ∃ seed ∈ startPending, seed.key.timeNs ≤ event.key.timeNs := by
  induction hreachable with
  | seed hmember =>
      exact ⟨_, hmember, Nat.le_refl _⟩
  | child parentReachable edge ih =>
      obtain ⟨seed, hseed, hle⟩ := ih
      exact ⟨seed, hseed,
        Nat.le_trans hle (emissionEdge_time_mono hadvance edge)⟩

theorem constantGlobalBoundsValid
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (haccepted : AcceptedModel image transition)
    (start : RoundState State)
    (hstart : PostExchangeStart image start) :
    ConstantGlobalBoundsValid image transition start.machine := by
  intro target child hunseen
  rcases hunseen with ⟨htarget, parent, hparentReachable, hedge, hremote⟩
  subst target
  obtain ⟨node, state, result, htransition, hchild⟩ := hedge
  rcases haccepted.2.2.1 with
    ⟨_, hroleCorrectAxiom, _, hadvance, _, _, _, hcoverage,
      hsoundness, _, _⟩
  have hroleCorrect :=
    hroleCorrectAxiom node parent state result htransition
  have hnodeTarget : parent.target = node.id := hroleCorrect.1
  have hchildRemote : child.target ≠ node.id := by
    intro heq
    apply hremote
    exact hnodeTarget.trans heq.symm
  obtain ⟨channel, hchannel, hsource, hchannelTarget, hkind⟩ :=
    hcoverage
      node parent state result htransition child hchild hchildRemote
  obtain ⟨_, _, _, _, hdelay⟩ :=
    hsoundness
      node parent state result htransition child hchild hchildRemote
      channel hchannel hsource hchannelTarget hkind
  obtain ⟨seed, hseed, hseedLeParent⟩ :=
    potentiallyReachable_seed_time_le
      hadvance hparentReachable
  obtain ⟨frontier, hfrontier, hfrontierLe⟩ :=
    minimumLPFrontier_le_of_pending image start.machine hstart.2 hseed
  obtain ⟨lookahead, hlookahead, hlookaheadLe⟩ :=
    minimumChannelDelay_le_of_mem image hchannel
  have hboundLe : globalHorizon image start.machine ≤ child.key.timeNs := by
    have hhorizon :
        globalHorizon image start.machine ≤ frontier + lookahead := by
      simp only [globalHorizon, frontierPlusLookahead, hfrontier, hlookahead]
      exact Nat.min_le_right _ _
    have hfrontierLeParent :
        frontier ≤ parent.key.timeNs :=
      Nat.le_trans hfrontierLe hseedLeParent
    have hsum :
        frontier + lookahead ≤
          parent.key.timeNs + channel.minDelayNs :=
      Nat.add_le_add hfrontierLeParent hlookaheadLe
    exact Nat.le_trans hhorizon (Nat.le_trans hsum hdelay.2)
  intro hbelow
  have htimeLt :
      child.key.timeNs < globalHorizon image start.machine :=
    (belowConstantTimeBound_iff (globalHorizon image start.machine) child).mp hbelow
  omega

private theorem mem_insertEvent
    (candidate inserted : Event)
    (pending : List Event) :
    candidate ∈ insertEvent inserted pending ↔
      candidate = inserted ∨ candidate ∈ pending := by
  induction pending with
  | nil =>
      simp [insertEvent]
  | cons head tail ih =>
      simp only [insertEvent]
      split
      · simp
      · simp only [List.mem_cons, ih]
        constructor
        · rintro (rfl | rfl | htail)
          · exact Or.inr (Or.inl rfl)
          · exact Or.inl rfl
          · exact Or.inr (Or.inr htail)
        · rintro (rfl | rfl | htail)
          · exact Or.inr (Or.inl rfl)
          · exact Or.inl rfl
          · exact Or.inr (Or.inr htail)

private theorem mem_insertEvents
    (candidate : Event)
    (children pending : List Event) :
    candidate ∈ insertEvents children pending ↔
      candidate ∈ children ∨ candidate ∈ pending := by
  induction children generalizing pending with
  | nil =>
      simp [insertEvents]
  | cons child tail ih =>
      simp only [insertEvents, List.foldl_cons]
      change
        candidate ∈ insertEvents tail (insertEvent child pending) ↔
          candidate ∈ child :: tail ∨ candidate ∈ pending
      rw [ih, mem_insertEvent]
      simp [or_assoc, or_left_comm]

private theorem remoteEnvelopesFromStore_event_mem
    (image : SimulationImage State)
    (store : List PacketDescriptor)
    {events : List Event}
    {envelopes : List RemoteEnvelope}
    (hfrom : RemoteEnvelopesFromStore image store events envelopes)
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
          rcases hfrom with ⟨hevent, _, _, _, htail⟩
          simp only [List.mem_cons] at hmem ⊢
          rcases hmem with rfl | hmem
          · exact Or.inl hevent
          · exact Or.inr (ih htail hmem)

private def RoundReachabilityInvariant
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (startPending : List Event)
    (state : RoundState State) : Prop :=
  (∀ event ∈ state.machine.pending,
      PotentiallyReachableEvent transition startPending event) ∧
    ∀ owner ∈ image.nodes, ∀ envelope ∈ state.outboxes owner.id,
      UnseenRemoteAt transition startPending envelope.event.target envelope.event

private theorem roundReachabilityInvariant_start
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (start : RoundState State)
    (hstart : PostExchangeStart image start) :
    RoundReachabilityInvariant image transition start.machine.pending start := by
  constructor
  · intro event hmem
    exact PotentiallyReachableEvent.seed hmem
  · intro owner howner envelope hmem
    rw [hstart.1 owner howner] at hmem
    simp at hmem

private theorem roundReachabilityInvariant_step
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (startPending : List Event)
    (node : NodeDescriptor)
    (before after : RoundState State)
    (event : Event)
    (hinvariant :
      RoundReachabilityInvariant image transition startPending before)
    (hstep :
      LocalRoundStep image transition bounds node before event after) :
    RoundReachabilityInvariant image transition startPending after := by
  rcases hstep with
    ⟨hnode, hleast, result, htransition, _, _, _, _, _, hpending,
      _, emittedRemote, hremoteEnvelopes, houtbox, hotherOutboxes⟩
  have heventReachable :=
    hinvariant.1 event hleast.1
  have heventTarget : event.target = node.id := hleast.2.1.1
  constructor
  · intro candidate hcandidate
    rw [hpending, mem_insertEvents] at hcandidate
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
          remoteEnvelopesFromStore_event_mem image
            (after.machine.packetStore node) hremoteEnvelopes hnew
        have hchild : envelope.event ∈ result.children := by
          exact (List.mem_filter.mp hremoteEvent).1
        have htargetNe : envelope.event.target ≠ node.id := by
          exact of_decide_eq_true (List.mem_filter.mp hremoteEvent).2
        refine ⟨rfl, event, heventReachable, ?_, ?_⟩
        · exact
            ⟨node, before.machine.localState node, result,
              htransition, hchild⟩
        · intro hequal
          apply htargetNe
          rw [← heventTarget, hequal]
    · rw [hotherOutboxes owner howner hid] at henvelope
      exact hinvariant.2 owner howner envelope henvelope

private theorem roundReachabilityInvariant_drainLP
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (startPending : List Event)
    (node : NodeDescriptor)
    (before : RoundState State)
    (drained : List Event)
    (after : RoundState State)
    (hinvariant :
      RoundReachabilityInvariant image transition startPending before)
    (hdrain :
      SequentialDrainLP image transition bounds node before drained after) :
    RoundReachabilityInvariant image transition startPending after := by
  induction hdrain with
  | done =>
      exact hinvariant
  | step first rest ih =>
      exact ih (roundReachabilityInvariant_step
        image transition bounds startPending node _ _ _ hinvariant first)

private theorem roundReachabilityInvariant_drainLPs
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (startPending : List Event)
    (order : List NodeId)
    (before : RoundState State)
    (drained : List Event)
    (after : RoundState State)
    (hinvariant :
      RoundReachabilityInvariant image transition startPending before)
    (hdrain :
      DrainLPsInOrder image transition bounds order before drained after) :
    RoundReachabilityInvariant image transition startPending after := by
  induction hdrain with
  | nil =>
      exact hinvariant
  | cons nodeMember first rest ih =>
      exact ih (roundReachabilityInvariant_drainLP
        image transition bounds startPending _ _ _ _ hinvariant first)

private theorem flattenedOutboxes_unseen
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (startPending : List Event)
    (state : RoundState State)
    (hinvariant :
      RoundReachabilityInvariant image transition startPending state)
    {envelope : RemoteEnvelope}
    (hmem : envelope ∈ flattenedOutboxes image state) :
    UnseenRemoteAt transition startPending envelope.event.target envelope.event := by
  simp only [flattenedOutboxes, List.mem_flatMap] at hmem
  obtain ⟨owner, howner, henvelope⟩ := hmem
  exact hinvariant.2 owner howner envelope henvelope

theorem sequentialRoundDrain_buffered_unseen
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (start afterDrain : RoundState State)
    (drainedEvents : List Event)
    (hstart : PostExchangeStart image start)
    (hdrain :
      SequentialRoundDrain image transition bounds
        start drainedEvents afterDrain) :
    ∀ envelope ∈ flattenedOutboxes image afterDrain,
      UnseenRemoteAt transition start.machine.pending
        envelope.event.target envelope.event := by
  obtain ⟨order, _, horderedDrain⟩ := hdrain
  have hinvariant :=
    roundReachabilityInvariant_drainLPs
      image transition bounds start.machine.pending order
      start drainedEvents afterDrain
      (roundReachabilityInvariant_start image transition start hstart)
      horderedDrain
  intro envelope hmem
  exact flattenedOutboxes_unseen image transition
    start.machine.pending afterDrain hinvariant hmem

theorem f1RemoteLowerBound_proved
    (image : SimulationImage State)
    (transition : TransitionRelation State) :
    F1RemoteLowerBound image transition := by
  constructor
  · intro haccepted start hstart
    exact constantGlobalBoundsValid image transition haccepted start hstart
  · intro haccepted bounds start drainedEvents afterDrain
      hstart hbounds hdrain
    constructor
    · intro target event hunseen
      have htarget := hunseen.1
      subst target
      have hnotBelow := hbounds event.target event hunseen
      unfold belowBound at hnotBelow
      rw [EventKey.lt_timeBoundary_iff] at hnotBelow
      omega
    · intro envelope henvelope
      exact hbounds envelope.event.target envelope.event
        (sequentialRoundDrain_buffered_unseen
          image transition bounds
          start afterDrain drainedEvents hstart hdrain
          envelope henvelope)

theorem f2GlobalTimePrefixCorollary_proved :
    F2GlobalTimePrefixCorollary := by
  intro emissions startPending drainedEvents bounds horizon cut
      hconstant hdrained event
  subst bounds
  rw [hdrained.2.2 event]
  unfold TimePrefix
  rw [belowConstantTimeBound_iff]

end DaysExecutor
