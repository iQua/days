import DaysExecutor.ConcreteFIFOReordering

namespace DaysExecutor

/-!
Concrete SP and WFQ transition instances for the frozen executor model.

Both instances expose the queue in exact service order through `RoleState.serviceQueue`.  An arrival
may change that order; `TxReady` consults only the current public list and commits at most its head.
Discipline metadata is immutable configuration captured by the transition, never private mutable
state consulted at service start.
-/

/-- Stable insertion into a queue ordered by a strict discipline comparator. -/
def insertByDiscipline
    (before : PayloadId → PayloadId → Bool)
    (packet : PayloadId) : List PayloadId → List PayloadId
  | [] => [packet]
  | head :: tail =>
      if before packet head then
        packet :: head :: tail
      else
        head :: insertByDiscipline before packet tail

/-- Immutable SP priorities for the concrete transition instance; larger values run first. -/
def spPriority : PayloadId → Nat
  | 21 => 2
  | 12 => 1
  | _ => 0

/-- Strict-priority order with stable FIFO order for equal priorities. -/
def spBefore (left right : PayloadId) : Bool :=
  spPriority right < spPriority left

/-- Positive immutable WFQ weights for the concrete transition instance. -/
def wfqWeight : PayloadId → Nat
  | 21 => 2
  | _ => 1

/--
Exact integer WFQ finish tags for this equal-size, zero-virtual-start instance: eight scaled service
units divided by a positive class weight.  The resulting tags are represented by the public queue
order; no floating-point value participates in the executor semantics.
-/
def wfqFinishTag (packet : PayloadId) : Nat :=
  8 / wfqWeight packet

/-- WFQ serves the least exact integer finish tag, stably breaking equal tags by arrival order. -/
def wfqBefore (left right : PayloadId) : Bool :=
  wfqFinishTag left < wfqFinishTag right

/-- Mutable bookkeeping excluded from service selection by the frozen F4 contract. -/
structure OrderedQueuePrivateState where
  capacity : Nat
  accepted : List PayloadId
  dropped : List PayloadId
  deriving DecidableEq, Repr

/-- Role-indexed state family shared by the two concrete scheduler instances. -/
abbrev OrderedQueueStateFamily : StateFamily :=
  fun _ => OrderedQueuePrivateState

/-- The single switch LP used to discharge the concrete transition obligations. -/
def orderedQueueNode : NodeDescriptor :=
  { id := 0, kind := .switch, stateSlot := 0 }

/-- Initial arrival that must be visible to the later service-start selection. -/
def orderedQueueArrival : Event :=
  { key :=
      { timeNs := 5
        phase := eventPhase .remoteArrival
        originNode := 0
        originSeq := 0 }
    target := 0
    kind := .remoteArrival
    payload := 21 }

/-- The actual service-start decision after `orderedQueueArrival`. -/
def orderedQueueReady : Event :=
  { key :=
      { timeNs := 10
        phase := eventPhase .txReady
        originNode := 0
        originSeq := 1 }
    target := 0
    kind := .txReady
    payload := 21 }

/-- Immutable packet descriptors used by the concrete scheduler image. -/
def orderedQueueDescriptor (payload : PayloadId) : PacketDescriptor :=
  { id := payload, flow := payload, sizeBytes := 1, kind := .data }

/-- One descriptor-store entry with explicit structural owners. -/
def orderedQueueEntry
    (payload : PayloadId)
    (owners : List ReferenceOwner) : PacketStoreEntry :=
  { descriptor := orderedQueueDescriptor payload, owners }

/-- Initial public queue and private bookkeeping. -/
def orderedQueueInitialState : RoleState OrderedQueueStateFamily .switch :=
  { privateState := { capacity := 0, accepted := [], dropped := [] }
    serviceQueue := [12]
    committedService := [] }

/--
Small accepted image used for both transition instances.  Service children remain local, so the
instance exercises queue semantics without adding a remote-channel premise unrelated to the
discipline.
-/
def orderedQueueImage : SimulationImage OrderedQueueStateFamily :=
  { stopTimeNs := 12
    nodes := [orderedQueueNode]
    stateArena := fun kind =>
      match kind with
      | .host => []
      | .switch => [orderedQueueInitialState]
    links := []
    channels := []
    initialEvents := [orderedQueueArrival, orderedQueueReady]
    packetDescriptor := orderedQueueDescriptor
    initialPacketStore := fun node =>
      if node = 0 then
        [ orderedQueueEntry 12
            [.queueEntry 0 12],
          orderedQueueEntry 21
            [.pendingEvent orderedQueueArrival.key,
              .pendingEvent orderedQueueReady.key] ]
      else
        []
    initialNextOriginSeq := fun _ => 2
    payloadBytes := fun _ => 1 }

/-- Arrival admission and discipline ordering of the authoritative public queue. -/
def orderedQueueArrivalState
    (before : PayloadId → PayloadId → Bool)
    (state : RoleState OrderedQueueStateFamily kind)
    (packet : PayloadId) : RoleState OrderedQueueStateFamily kind :=
  if state.privateState.capacity ≠ 0 ∧
      state.privateState.capacity ≤ state.serviceQueue.length then
    { state with
      privateState :=
        { state.privateState with
          dropped := state.privateState.dropped ++ [packet] } }
  else
    { privateState :=
        { state.privateState with
          accepted := state.privateState.accepted ++ [packet] }
      serviceQueue := insertByDiscipline before packet state.serviceQueue
      committedService := state.committedService }

/-- Service selection reads only the public queue and committed-service ledger. -/
def orderedQueueSelectedPacket
    (state : RoleState OrderedQueueStateFamily kind) : Option PayloadId :=
  match state.committedService with
  | _ :: _ => none
  | [] => state.serviceQueue.head?

/-- One exact non-preemptive decision, if the current public queue has a selectable head. -/
def orderedQueueDecisions
    (node : NodeDescriptor)
    (event : Event) : Option PayloadId → List ServiceDecision
  | none => []
  | some packet =>
      [{ node := node.id
         decisionKey := event.key
         packet
         committedNonPreemptively := true }]

/-- Completion and local arrival emitted for exactly the selected packet. -/
def orderedQueueServiceChildren
    (node : NodeDescriptor)
    (event : Event) : Option PayloadId → List Event
  | none => []
  | some packet =>
      [ { key :=
            { timeNs := event.key.timeNs + 1
              phase := eventPhase .txComplete
              originNode := node.id
              originSeq := 2 }
          target := node.id
          kind := .txComplete
          payload := packet },
        { key :=
            { timeNs := event.key.timeNs + 1
              phase := eventPhase .remoteArrival
              originNode := node.id
              originSeq := 3 }
          target := node.id
          kind := .remoteArrival
          payload := packet } ]

/-- Public queue removal and exact committed-service append at service start. -/
def orderedQueueReadyState
    (selected : Option PayloadId)
    (state : RoleState OrderedQueueStateFamily kind) :
    RoleState OrderedQueueStateFamily kind :=
  match selected with
  | none => state
  | some packet =>
      { state with
        serviceQueue := state.serviceQueue.erase packet
        committedService := state.committedService ++ [packet] }

/-- One complete ordered-queue handler result. -/
def orderedQueueTransitionResult
    (before : PayloadId → PayloadId → Bool)
    (node : NodeDescriptor)
    (event : Event)
    (state : RoleState OrderedQueueStateFamily node.kind) :
    TransitionResult OrderedQueueStateFamily node.kind :=
  let selected :=
    if event.kind = .txReady then orderedQueueSelectedPacket state else none
  let nextState :=
    match event.kind with
    | .remoteArrival => orderedQueueArrivalState before state event.payload
    | .txReady => orderedQueueReadyState selected state
    | .txComplete =>
        { state with
          committedService := state.committedService.erase event.payload }
    | .packetArrival => state
  { nextState
    children := orderedQueueServiceChildren node event selected
    packetReferenceIncrements :=
      stateReferenceIncrements orderedQueueImage node state nextState
    packetReferenceConsumptions :=
      ownedEventReference orderedQueueImage event ::
        stateReferenceConsumptions orderedQueueImage node state nextState
    summaryDelta := RunSummary.zero
    observedPackets := []
    departures := []
    arrivals := []
    decisions := orderedQueueDecisions node event selected }

/-- Successful transition relation for one immutable ordered-queue policy. -/
def orderedQueueTransition
    (before : PayloadId → PayloadId → Bool) :
    TransitionRelation OrderedQueueStateFamily :=
  fun node event state result =>
    node ∈ orderedQueueImage.nodes ∧
      event.target = node.id ∧
      roleSupports node.kind event.kind ∧
      (event.kind = .txComplete → event.payload ∈ state.committedService) ∧
      result = orderedQueueTransitionResult before node event state

/-- Static-priority transition instance. -/
def spTransition : TransitionRelation OrderedQueueStateFamily :=
  orderedQueueTransition spBefore

/-- Exact-integer WFQ transition instance. -/
def wfqTransition : TransitionRelation OrderedQueueStateFamily :=
  orderedQueueTransition wfqBefore

private theorem orderedQueue_static :
    StaticImageWellFormed orderedQueueImage := by
  unfold StaticImageWellFormed
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · simp [UniqueNodeIds, orderedQueueImage, orderedQueueNode]
  · constructor
    · intro node hnode
      simp [orderedQueueImage, orderedQueueNode] at hnode
      subst node
      decide
    · intro kind slot
      cases kind <;>
        simp [orderedQueueImage, orderedQueueNode] <;>
        omega
  · intro kind state hstate
    cases kind <;>
      simp [orderedQueueImage, orderedQueueInitialState] at hstate
    simp_all
  · unfold UniqueEventKeys
    decide
  · unfold InitialEventsOrdered
    decide
  · unfold UniqueLinkIds
    decide
  · unfold UniqueChannelRoutes
    decide
  · intro event hevent
    simp [orderedQueueImage] at hevent
    rcases hevent with rfl | rfl
    all_goals
      exact ⟨orderedQueueNode, by simp [orderedQueueImage], rfl⟩
  · intro event hevent
    simp [orderedQueueImage] at hevent
    rcases hevent with rfl | rfl
    all_goals
      exact ⟨rfl, orderedQueueNode, by simp [orderedQueueImage], rfl⟩
  · intro payload
    exact ⟨rfl, rfl⟩
  · constructor
    · intro node hnode
      simp [orderedQueueImage, orderedQueueNode] at hnode
      subst node
      simp [orderedQueueImage, DescriptorStoreSorted, orderedQueueEntry,
        orderedQueueDescriptor]
      decide
    · intro node hnode state hstate
      simp [orderedQueueImage, orderedQueueNode] at hnode
      subst node
      simp [orderedQueueImage, stateAt?, listGet?] at hstate
      subst state
      unfold OwnedReferencesMatchStore
      constructor
      · intro reference hreference
        simp [initialOwnedReferencesFor, ownedRoleStateReferences,
          orderedQueueImage, orderedQueueInitialState, orderedQueueArrival,
          orderedQueueReady, ownedEventReference, ownedQueueReference,
          orderedQueueEntry, orderedQueueDescriptor] at hreference
        rcases hreference with rfl | rfl | rfl <;> decide
      · intro entry hentry owner howner
        simp [orderedQueueImage, orderedQueueEntry] at hentry
        rcases hentry with rfl | rfl
        · simp at howner
          rcases howner with rfl | rfl <;> decide
        · simp at howner
          rcases howner with rfl | rfl <;> decide
  · intro node hnode event hevent horigin
    simp [orderedQueueImage, orderedQueueNode] at hnode
    subst node
    simp [orderedQueueImage] at hevent
    rcases hevent with rfl | rfl <;>
      simp [orderedQueueArrival, orderedQueueReady, orderedQueueImage] at horigin ⊢ <;>
      omega
  · unfold PositiveLinkRates
    simp [orderedQueueImage]
  · intro link hlink
    simp [orderedQueueImage] at hlink
  · unfold PositiveChannelBounds
    simp [orderedQueueImage]
  · intro channel hchannel
    simp [orderedQueueImage] at hchannel
  · intro channel hchannel
    simp [orderedQueueImage] at hchannel

private theorem orderedQueue_initial_roles :
    InitialEventsRoleCorrect orderedQueueImage := by
  simp [InitialEventsRoleCorrect, orderedQueueImage, orderedQueueNode,
    orderedQueueArrival, orderedQueueReady, roleSupports]

private theorem orderedQueueTransition_deterministic (before) :
    TransitionDeterministic (orderedQueueTransition before) := by
  intro node event state left right hleft hright
  exact hleft.2.2.2.2.trans hright.2.2.2.2.symm

private theorem orderedQueueTransition_role_correct (before) :
    TransitionRoleCorrect (orderedQueueTransition before) := by
  intro node event state result htransition
  exact ⟨htransition.2.1, htransition.2.2.1⟩

private theorem orderedQueueTransition_generated_roles (before) :
    GeneratedEventsRoleCorrect orderedQueueImage
      (orderedQueueTransition before) := by
  intro node event state result htransition child hchild
  rcases htransition with ⟨hnode, _, _, _, rfl⟩
  simp [orderedQueueImage, orderedQueueNode] at hnode
  subst node
  by_cases hkind : event.kind = .txReady
  · simp only [orderedQueueTransitionResult, hkind, ↓reduceIte] at hchild
    cases hselected : orderedQueueSelectedPacket state with
    | none =>
        simp [orderedQueueServiceChildren, hselected] at hchild
    | some packet =>
        simp [orderedQueueServiceChildren, hselected] at hchild
        rcases hchild with rfl | rfl <;>
          exact ⟨orderedQueueNode, by simp [orderedQueueImage], rfl,
            by simp [orderedQueueNode, roleSupports]⟩
  · simp [orderedQueueTransitionResult, hkind,
      orderedQueueServiceChildren] at hchild

private theorem orderedQueueTransition_children_advance (before) :
    ChildrenAdvanceParent (orderedQueueTransition before) := by
  intro node event state result htransition child hchild
  rcases htransition with ⟨_, _, _, _, rfl⟩
  by_cases hkind : event.kind = .txReady
  · simp only [orderedQueueTransitionResult, hkind, ↓reduceIte] at hchild
    cases hselected : orderedQueueSelectedPacket state with
    | none =>
        simp [orderedQueueServiceChildren, hselected] at hchild
    | some packet =>
        simp [orderedQueueServiceChildren, hselected] at hchild
        rcases hchild with rfl | rfl
        all_goals
          change EventKey.lexLT event.key _
          unfold EventKey.lexLT
          exact Or.inl (Nat.lt_succ_self _)
  · simp [orderedQueueTransitionResult, hkind,
      orderedQueueServiceChildren] at hchild

private theorem orderedQueueTransition_children_origin (before) :
    ChildrenUseOwnerOrigin (orderedQueueTransition before) := by
  intro node event state result htransition child hchild
  rcases htransition with ⟨_, _, _, _, rfl⟩
  by_cases hkind : event.kind = .txReady
  · simp only [orderedQueueTransitionResult, hkind, ↓reduceIte] at hchild
    cases hselected : orderedQueueSelectedPacket state with
    | none =>
        simp [orderedQueueServiceChildren, hselected] at hchild
    | some packet =>
        simp [orderedQueueServiceChildren, hselected] at hchild
        rcases hchild with rfl | rfl <;> rfl
  · simp [orderedQueueTransitionResult, hkind,
      orderedQueueServiceChildren] at hchild

private theorem orderedQueueTransition_children_phase (before) :
    ChildrenUseCanonicalPhase (orderedQueueTransition before) := by
  intro node event state result htransition child hchild
  rcases htransition with ⟨_, _, _, _, rfl⟩
  by_cases hkind : event.kind = .txReady
  · simp only [orderedQueueTransitionResult, hkind, ↓reduceIte] at hchild
    cases hselected : orderedQueueSelectedPacket state with
    | none =>
        simp [orderedQueueServiceChildren, hselected] at hchild
    | some packet =>
        simp [orderedQueueServiceChildren, hselected] at hchild
        rcases hchild with rfl | rfl <;> rfl
  · simp [orderedQueueTransitionResult, hkind,
      orderedQueueServiceChildren] at hchild

private theorem orderedQueueTransition_children_unique (before) :
    TransitionChildrenHaveUniqueKeys (orderedQueueTransition before) := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, _, rfl⟩
  unfold UniqueEventKeys
  by_cases hkind : event.kind = .txReady
  · simp only [orderedQueueTransitionResult, hkind, ↓reduceIte]
    cases hselected : orderedQueueSelectedPacket state with
    | none => simp [orderedQueueServiceChildren]
    | some packet => simp [orderedQueueServiceChildren]
  · simp [orderedQueueTransitionResult, hkind,
      orderedQueueServiceChildren]

private theorem orderedQueueTransition_remote_coverage (before) :
    RemoteEmissionCoverage orderedQueueImage
      (orderedQueueTransition before) := by
  intro node event state result htransition child hchild hremote
  rcases htransition with ⟨_, _, _, _, rfl⟩
  by_cases hkind : event.kind = .txReady
  · simp only [orderedQueueTransitionResult, hkind, ↓reduceIte] at hchild
    cases hselected : orderedQueueSelectedPacket state with
    | none => simp [orderedQueueServiceChildren, hselected] at hchild
    | some packet =>
        simp [orderedQueueServiceChildren, hselected] at hchild
        rcases hchild with rfl | rfl <;> exact False.elim (hremote rfl)
  · simp [orderedQueueTransitionResult, hkind,
      orderedQueueServiceChildren] at hchild

private theorem orderedQueueTransition_bound_sound (before) :
    CertifiedBoundSoundness orderedQueueImage
      (orderedQueueTransition before) := by
  intro node event state result htransition child hchild hremote
  rcases htransition with ⟨_, _, _, _, rfl⟩
  by_cases hkind : event.kind = .txReady
  · simp only [orderedQueueTransitionResult, hkind, ↓reduceIte] at hchild
    cases hselected : orderedQueueSelectedPacket state with
    | none => simp [orderedQueueServiceChildren, hselected] at hchild
    | some packet =>
        simp [orderedQueueServiceChildren, hselected] at hchild
        rcases hchild with rfl | rfl <;> exact False.elim (hremote rfl)
  · simp [orderedQueueTransitionResult, hkind,
      orderedQueueServiceChildren] at hchild

private theorem listBagDifference_member
    [BEq α] [LawfulBEq α]
    (source removed : List α)
    {item : α}
    (hitem : item ∈ listBagDifference source removed) :
    item ∈ source := by
  induction removed generalizing source with
  | nil => exact hitem
  | cons head tail ih =>
      simp only [listBagDifference, List.foldl_cons] at hitem
      exact List.mem_of_mem_erase (ih (source.erase head) hitem)

private theorem orderedRoleStateReference_oracle
    (node : NodeDescriptor)
    (state : RoleState OrderedQueueStateFamily node.kind)
    (reference : OwnedPacketReference)
    (hreference :
      reference ∈ ownedRoleStateReferences orderedQueueImage node state) :
    reference.descriptor =
      orderedQueueImage.packetDescriptor reference.descriptor.id := by
  unfold ownedRoleStateReferences at hreference
  rw [List.mem_append] at hreference
  rcases hreference with hqueue | hservice
  · rcases List.mem_map.mp hqueue with ⟨payload, _, rfl⟩
    rfl
  · rcases List.mem_map.mp hservice with ⟨payload, _, rfl⟩
    rfl

private theorem orderedQueueTransition_descriptors (before) :
    TransitionDescriptorEffectsCoherent orderedQueueImage
      (orderedQueueTransition before) := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, _, rfl⟩
  constructor
  · intro reference hreference
    unfold orderedQueueTransitionResult at hreference
    exact orderedRoleStateReference_oracle node _ reference
      (listBagDifference_member _ _ hreference)
  constructor
  · intro reference hreference
    unfold orderedQueueTransitionResult at hreference
    rcases List.mem_cons.mp hreference with rfl | hreference
    · rfl
    · exact orderedRoleStateReference_oracle node state reference
        (listBagDifference_member _ _ hreference)
  · intro descriptor hdescriptor
    simp [orderedQueueTransitionResult] at hdescriptor

private theorem orderedQueueTransition_observation_keys (before) :
    TransitionObservationsUseEventKey (orderedQueueTransition before) := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, _, rfl⟩
  simp [ObservationRecordsUseEventKey, orderedQueueTransitionResult]

private theorem orderedQueueTransition_axioms (before) :
    TransitionAxioms orderedQueueImage (orderedQueueTransition before) :=
  ⟨orderedQueueTransition_deterministic before,
    orderedQueueTransition_role_correct before,
    orderedQueueTransition_generated_roles before,
    orderedQueueTransition_children_advance before,
    orderedQueueTransition_children_origin before,
    orderedQueueTransition_children_phase before,
    orderedQueueTransition_children_unique before,
    orderedQueueTransition_remote_coverage before,
    orderedQueueTransition_bound_sound before,
    orderedQueueTransition_descriptors before,
    orderedQueueTransition_observation_keys before⟩

private theorem orderedQueue_actual_service_start (before) :
    ActualServiceStartDiscipline (orderedQueueTransition before) := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, _, rfl⟩
  by_cases hkind : event.kind = .txReady
  · simp only [orderedQueueTransitionResult, hkind, ↓reduceIte]
    cases hselected : orderedQueueSelectedPacket state with
    | none => simp [orderedQueueDecisions]
    | some packet => simp [orderedQueueDecisions]
  · simp [orderedQueueTransitionResult, hkind, orderedQueueDecisions]

private theorem orderedQueue_decision_emissions (before) :
    ServiceDecisionEmissionsMatch (orderedQueueTransition before) := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, _, rfl⟩
  unfold ServiceDecisionChildrenMatch
  by_cases hkind : event.kind = .txReady
  · simp only [orderedQueueTransitionResult, hkind, ↓reduceIte]
    cases hselected : orderedQueueSelectedPacket state with
    | none =>
        simp [orderedQueueServiceChildren, orderedQueueDecisions]
    | some packet =>
        simp [orderedQueueServiceChildren, orderedQueueDecisions]
  · simp [orderedQueueTransitionResult, hkind,
      orderedQueueServiceChildren, orderedQueueDecisions]

private theorem ordered_list_ne_append_singleton
    (items : List α) (item : α) :
    items ≠ items ++ [item] := by
  intro heq
  have hlength := congrArg List.length heq
  simp at hlength

private theorem ordered_erase_ne_append_singleton_of_mem
    [BEq α] [LawfulBEq α]
    (items : List α)
    (removed item : α)
    (hmem : removed ∈ items) :
    items.erase removed ≠ items ++ [item] := by
  intro heq
  have hlength := congrArg List.length heq
  rw [List.length_erase_of_mem hmem] at hlength
  simp at hlength

private theorem orderedQueue_decision_trace (before) :
    ServiceDecisionTraceComplete (orderedQueueTransition before) := by
  intro node event state result htransition packet
  rcases htransition with ⟨_, _, _, hcompletion, rfl⟩
  cases hkind : event.kind with
  | packetArrival =>
      simp [SelectionIntroduced, orderedQueueTransitionResult, hkind,
        orderedQueueDecisions]
  | remoteArrival =>
      simp [SelectionIntroduced, orderedQueueTransitionResult, hkind,
        orderedQueueDecisions, orderedQueueArrivalState]
      split <;> exact ordered_list_ne_append_singleton _ _
  | txReady =>
      cases hcommitted : state.committedService with
      | nil =>
          cases hqueue : state.serviceQueue with
          | nil =>
              simp [SelectionIntroduced, orderedQueueTransitionResult,
                orderedQueueSelectedPacket, orderedQueueReadyState,
                orderedQueueDecisions, hkind, hcommitted, hqueue]
          | cons head tail =>
              simp [SelectionIntroduced, orderedQueueTransitionResult,
                orderedQueueSelectedPacket, orderedQueueReadyState,
                orderedQueueDecisions, hkind, hcommitted, hqueue]
      | cons head tail =>
          simp [SelectionIntroduced, orderedQueueTransitionResult,
            orderedQueueSelectedPacket, orderedQueueReadyState,
            orderedQueueDecisions, hkind, hcommitted]
  | txComplete =>
      have hpresent : event.payload ∈ state.committedService :=
        hcompletion (by simpa using hkind)
      simp [SelectionIntroduced, orderedQueueTransitionResult, hkind,
        orderedQueueDecisions,
        ordered_erase_ne_append_singleton_of_mem _ _ _ hpresent]

private theorem orderedQueue_committed_nonpreemptive (before) :
    CommittedServiceNonPreemptive (orderedQueueTransition before) := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, hcompletion, rfl⟩
  unfold CommittedServiceTransitionValid
  cases hkind : event.kind with
  | packetArrival =>
      simp [orderedQueueTransitionResult, hkind, orderedQueueDecisions]
  | remoteArrival =>
      simp [orderedQueueTransitionResult, hkind, orderedQueueDecisions,
        orderedQueueArrivalState]
      split <;> exact ⟨fun h => h, rfl⟩
  | txReady =>
      cases hcommitted : state.committedService with
      | nil =>
          cases hqueue : state.serviceQueue with
          | nil =>
              simp [orderedQueueTransitionResult, orderedQueueSelectedPacket,
                orderedQueueReadyState, orderedQueueDecisions, hkind,
                hcommitted, hqueue]
          | cons head tail =>
              simp [orderedQueueTransitionResult, orderedQueueSelectedPacket,
                orderedQueueReadyState, orderedQueueDecisions, hkind,
                hcommitted, hqueue]
      | cons head tail =>
          simp [orderedQueueTransitionResult, orderedQueueSelectedPacket,
            orderedQueueReadyState, orderedQueueDecisions, hkind, hcommitted]
  | txComplete =>
      have hpresent : event.payload ∈ state.committedService :=
        hcompletion (by simpa using hkind)
      simp only [orderedQueueTransitionResult, hkind, orderedQueueDecisions,
        ↓reduceIte, hpresent, true_and]
      exact ⟨fun hnodup => hnodup.erase _, trivial⟩

private theorem orderedQueue_private_irrelevant (before) :
    TxReadySelectionPrivateIrrelevant (orderedQueueTransition before) := by
  intro node event state result htransition hkind privateState
  rcases htransition with ⟨hnode, htarget, hsupport, _, rfl⟩
  let alternateState : RoleState OrderedQueueStateFamily node.kind :=
    { privateState
      serviceQueue := state.serviceQueue
      committedService := state.committedService }
  let alternateResult :=
    orderedQueueTransitionResult before node event alternateState
  refine ⟨alternateResult, ?_, ?_⟩
  · exact ⟨hnode, htarget, hsupport,
      fun hcomplete => False.elim (by simp [hkind] at hcomplete), rfl⟩
  · unfold SameServiceSelectionResult alternateResult alternateState
    cases hcommitted : state.committedService with
    | nil =>
        cases hqueue : state.serviceQueue with
        | nil =>
            simp [orderedQueueTransitionResult, orderedQueueSelectedPacket,
              orderedQueueReadyState, hkind, hcommitted, hqueue,
              stateReferenceIncrements, stateReferenceConsumptions,
              ownedRoleStateReferences]
        | cons head tail =>
            simp [orderedQueueTransitionResult, orderedQueueSelectedPacket,
              orderedQueueReadyState, hkind, hcommitted, hqueue,
              stateReferenceIncrements, stateReferenceConsumptions,
              ownedRoleStateReferences]
    | cons head tail =>
        simp [orderedQueueTransitionResult, orderedQueueSelectedPacket,
          orderedQueueReadyState, hkind, hcommitted,
          stateReferenceIncrements, stateReferenceConsumptions,
          ownedRoleStateReferences]

private theorem orderedQueue_complete_service_start (before) :
    CompleteActualServiceStartDiscipline (orderedQueueTransition before) :=
  ⟨orderedQueue_actual_service_start before,
    orderedQueue_decision_trace before,
    orderedQueue_private_irrelevant before,
    orderedQueue_decision_emissions before,
    orderedQueue_committed_nonpreemptive before⟩

/-- The adversarial arrival becomes the public head under SP. -/
theorem sp_arrival_precedes_selection :
    insertByDiscipline spBefore 21 [12] = [21, 12] := by
  decide

/-- The same arrival becomes the least exact-finish-tag public head under WFQ. -/
theorem wfq_arrival_precedes_selection :
    insertByDiscipline wfqBefore 21 [12] = [21, 12] := by
  decide

/-- Completion child emitted by the concrete arrival-before-selection trajectory. -/
def orderedQueueCompletion : Event :=
  { key :=
      { timeNs := 11
        phase := eventPhase .txComplete
        originNode := 0
        originSeq := 2 }
    target := 0
    kind := .txComplete
    payload := 21 }

/-- Local arrival child emitted for the selected packet. -/
def orderedQueueRemote : Event :=
  { key :=
      { timeNs := 11
        phase := eventPhase .remoteArrival
        originNode := 0
        originSeq := 3 }
    target := 0
    kind := .remoteArrival
    payload := 21 }

private def orderedQueueAfterArrival
    (before : PayloadId → PayloadId → Bool) :
    RoleState OrderedQueueStateFamily .switch :=
  (orderedQueueTransitionResult before orderedQueueNode orderedQueueArrival
    orderedQueueInitialState).nextState

private def orderedQueueAfterReady
    (before : PayloadId → PayloadId → Bool) :
    RoleState OrderedQueueStateFamily .switch :=
  (orderedQueueTransitionResult before orderedQueueNode orderedQueueReady
    (orderedQueueAfterArrival before)).nextState

private def orderedQueueAfterRemote
    (before : PayloadId → PayloadId → Bool) :
    RoleState OrderedQueueStateFamily .switch :=
  (orderedQueueTransitionResult before orderedQueueNode orderedQueueRemote
    (orderedQueueAfterReady before)).nextState

private def orderedQueueAfterCompletion
    (before : PayloadId → PayloadId → Bool) :
    RoleState OrderedQueueStateFamily .switch :=
  (orderedQueueTransitionResult before orderedQueueNode orderedQueueCompletion
    (orderedQueueAfterRemote before)).nextState

private inductive OrderedQueueReachableShape
    (before : PayloadId → PayloadId → Bool) :
    MachineState OrderedQueueStateFamily → Prop
  | initial
      (hpending : machine.pending = [orderedQueueArrival, orderedQueueReady])
      (hstate : machine.localState orderedQueueNode = orderedQueueInitialState)
      (hcursor : machine.nextOriginSeq 0 = 2) :
      OrderedQueueReachableShape before machine
  | afterArrival
      (hpending : machine.pending = [orderedQueueReady])
      (hstate :
        machine.localState orderedQueueNode = orderedQueueAfterArrival before)
      (hcursor : machine.nextOriginSeq 0 = 2) :
      OrderedQueueReachableShape before machine
  | afterReady
      (hpending : machine.pending = [orderedQueueRemote, orderedQueueCompletion])
      (hstate : machine.localState orderedQueueNode = orderedQueueAfterReady before) :
      OrderedQueueReachableShape before machine
  | afterRemote
      (hpending : machine.pending = [orderedQueueCompletion])
      (hstate : machine.localState orderedQueueNode = orderedQueueAfterRemote before) :
      OrderedQueueReachableShape before machine
  | finished
      (hpending : machine.pending = [])
      (hstate :
        machine.localState orderedQueueNode = orderedQueueAfterCompletion before) :
      OrderedQueueReachableShape before machine

private theorem orderedQueue_unique_nodes :
    UniqueNodeIds orderedQueueImage := by
  simp [UniqueNodeIds, orderedQueueImage, orderedQueueNode]

private theorem orderedQueue_descriptor_oracle :
    DescriptorOracleWellFormed orderedQueueImage := by
  intro payload
  simp [orderedQueueImage, orderedQueueDescriptor]

private theorem orderedQueueNode_mem :
    orderedQueueNode ∈ orderedQueueImage.nodes := by
  simp [orderedQueueImage]

private theorem orderedQueueReachableShape_initial
    (machine : MachineState OrderedQueueStateFamily)
    (hinitial : InitialMachine orderedQueueImage machine) :
    OrderedQueueReachableShape before machine := by
  rcases hinitial with
    ⟨hpending, _, _, _, _, hcursor, _, _, hnodes, _⟩
  apply OrderedQueueReachableShape.initial
  · simpa [orderedQueueImage] using hpending
  · have hstate := (hnodes orderedQueueNode orderedQueueNode_mem).1
    simpa [stateAt?, listGet?, orderedQueueImage, orderedQueueNode,
      orderedQueueInitialState] using (Option.some.inj hstate).symm
  · have := congrFun hcursor 0
    simpa [orderedQueueImage] using this

private theorem orderedQueueReachableShape_step
    (hordered : insertByDiscipline before 21 [12] = [21, 12])
    (hshape : OrderedQueueReachableShape before machine)
    (hstep :
      CanonicalSerialStep orderedQueueImage
        (orderedQueueTransition before) (fun _ => True)
        machine event after) :
    OrderedQueueReachableShape before after := by
  rcases hstep with ⟨hleast, havailable⟩
  rcases havailable with
    ⟨hevent, node, hnode, result, htarget, htransition, _,
      hallocates, happlies, _, _, hpending, _⟩
  cases hshape with
  | initial hbeforePending hbeforeState hbeforeCursor =>
      have heventEq : event = orderedQueueArrival := by
        rw [hbeforePending] at hleast
        rcases List.mem_cons.mp hleast.1 with heq | htail
        · exact heq
        · have heq := List.mem_singleton.mp htail
          subst event
          have horder := hleast.2.2 orderedQueueArrival (by simp) trivial
          have himpossible :
              ¬ orderedQueueReady.key ≤ orderedQueueArrival.key := by
            decide
          exact (himpossible horder).elim
      subst event
      have hnodeEq : node = orderedQueueNode := by
        apply node_eq_of_unique_ids orderedQueue_unique_nodes
          hnode orderedQueueNode_mem
        simpa [orderedQueueNode, orderedQueueArrival] using htarget.symm
      subst node
      have hresultEq :
          result = orderedQueueTransitionResult before orderedQueueNode
            orderedQueueArrival (machine.localState orderedQueueNode) :=
        htransition.2.2.2.2
      subst result
      apply OrderedQueueReachableShape.afterArrival
      · rw [hpending, hbeforePending]
        simp [insertEvents, orderedQueueTransitionResult,
          orderedQueueServiceChildren, orderedQueueArrival]
      · rw [happlies.1, hbeforeState]
        rfl
      · have hcursorAtNode := hallocates.2.2.2.2.1
        simpa [orderedQueueTransitionResult, orderedQueueServiceChildren,
          orderedQueueArrival, orderedQueueNode, hbeforeCursor] using hcursorAtNode
  | afterArrival hbeforePending hbeforeState hbeforeCursor =>
      have heventEq : event = orderedQueueReady := by
        simpa [hbeforePending] using hevent
      subst event
      have hnodeEq : node = orderedQueueNode := by
        apply node_eq_of_unique_ids orderedQueue_unique_nodes
          hnode orderedQueueNode_mem
        simpa [orderedQueueNode, orderedQueueReady] using htarget.symm
      subst node
      have hresultEq :
          result = orderedQueueTransitionResult before orderedQueueNode
            orderedQueueReady (machine.localState orderedQueueNode) :=
        htransition.2.2.2.2
      subst result
      apply OrderedQueueReachableShape.afterReady
      · rw [hpending, hbeforePending, hbeforeState]
        simp [orderedQueueAfterArrival, orderedQueueTransitionResult,
          orderedQueueArrivalState, orderedQueueInitialState,
          orderedQueueArrival, orderedQueueReady,
          orderedQueueSelectedPacket, orderedQueueReadyState,
          orderedQueueServiceChildren, orderedQueueRemote,
          orderedQueueCompletion, orderedQueueNode, insertEvents, hordered]
        decide
      · rw [happlies.1, hbeforeState]
        rfl
  | afterReady hbeforePending hbeforeState =>
      have heventEq : event = orderedQueueRemote := by
        rw [hbeforePending] at hleast
        rcases List.mem_cons.mp hleast.1 with heq | htail
        · exact heq
        · have heq := List.mem_singleton.mp htail
          subst event
          have horder := hleast.2.2 orderedQueueRemote (by simp) trivial
          have himpossible :
              ¬ orderedQueueCompletion.key ≤ orderedQueueRemote.key := by
            decide
          exact (himpossible horder).elim
      subst event
      have hnodeEq : node = orderedQueueNode := by
        apply node_eq_of_unique_ids orderedQueue_unique_nodes
          hnode orderedQueueNode_mem
        simpa [orderedQueueNode, orderedQueueRemote] using htarget.symm
      subst node
      have hresultEq :
          result = orderedQueueTransitionResult before orderedQueueNode
            orderedQueueRemote (machine.localState orderedQueueNode) :=
        htransition.2.2.2.2
      subst result
      apply OrderedQueueReachableShape.afterRemote
      · rw [hpending, hbeforePending]
        simp [insertEvents, orderedQueueTransitionResult,
          orderedQueueServiceChildren, orderedQueueRemote]
      · rw [happlies.1, hbeforeState]
        rfl
  | afterRemote hbeforePending hbeforeState =>
      have heventEq : event = orderedQueueCompletion := by
        simpa [hbeforePending] using hevent
      subst event
      have hnodeEq : node = orderedQueueNode := by
        apply node_eq_of_unique_ids orderedQueue_unique_nodes
          hnode orderedQueueNode_mem
        simpa [orderedQueueNode, orderedQueueCompletion] using htarget.symm
      subst node
      have hresultEq :
          result = orderedQueueTransitionResult before orderedQueueNode
            orderedQueueCompletion (machine.localState orderedQueueNode) :=
        htransition.2.2.2.2
      subst result
      apply OrderedQueueReachableShape.finished
      · rw [hpending, hbeforePending]
        simp [insertEvents, orderedQueueTransitionResult,
          orderedQueueServiceChildren, orderedQueueCompletion]
      · rw [happlies.1, hbeforeState]
        rfl
  | finished hbeforePending _ =>
      rw [hbeforePending] at hevent
      simp at hevent

private theorem orderedQueueExecution_preserves_shape
    (hordered : insertByDiscipline before 21 [12] = [21, 12])
    (hbeforeWellFormed : MachineWellFormed orderedQueueImage start)
    (hbeforeShape : OrderedQueueReachableShape before start)
    (hexecution :
      CanonicalSerialExecution orderedQueueImage
        (orderedQueueTransition before) (fun _ => True)
        start events finish) :
    MachineWellFormed orderedQueueImage finish ∧
      OrderedQueueReachableShape before finish := by
  induction hexecution with
  | refl => exact ⟨hbeforeWellFormed, hbeforeShape⟩
  | step first rest ih =>
      have hmiddleWellFormed :=
        availableEventStep_preserves_machineWellFormed
          orderedQueueImage (orderedQueueTransition before)
          orderedQueue_unique_nodes orderedQueue_descriptor_oracle
          (orderedQueueTransition_generated_roles before)
          (orderedQueueTransition_descriptors before)
          _ _ _ hbeforeWellFormed first.2
      have hmiddleShape :=
        orderedQueueReachableShape_step hordered hbeforeShape first
      exact ih hmiddleWellFormed hmiddleShape

private theorem orderedQueueReachable_wellFormed_shape
    (hordered : insertByDiscipline before 21 [12] = [21, 12])
    (hreachable :
      CanonicallyReachableMachine orderedQueueImage
        (orderedQueueTransition before) machine) :
    MachineWellFormed orderedQueueImage machine ∧
      OrderedQueueReachableShape before machine := by
  rcases hreachable with ⟨initial, executed, hinitial, hexecution⟩
  exact orderedQueueExecution_preserves_shape hordered
    hinitial.2.2.2.2.2.2.2.2.2
    (orderedQueueReachableShape_initial initial hinitial)
    hexecution

private theorem orderedQueue_materialized_available
    (before : PayloadId → PayloadId → Bool)
    (machine : MachineState OrderedQueueStateFamily)
    (hwellFormed : MachineWellFormed orderedQueueImage machine)
    (event : Event)
    (hevent : event ∈ machine.pending)
    (htarget : event.target = orderedQueueNode.id)
    (hsupport : roleSupports orderedQueueNode.kind event.kind)
    (hguard :
      event.kind = .txComplete →
        event.payload ∈
          (machine.localState orderedQueueNode).committedService)
    (horigin :
      ChildrenUseOriginSequence orderedQueueNode.id
        (machine.nextOriginSeq orderedQueueNode.id)
        (orderedQueueTransitionResult before orderedQueueNode event
          (machine.localState orderedQueueNode)).children)
    (hchildrenFresh :
      ∀ child ∈
          (orderedQueueTransitionResult before orderedQueueNode event
            (machine.localState orderedQueueNode)).children,
        child.key ∉ machine.allocatedKeys)
    (hnewStateNodup :
      (ownedRoleStateReferences orderedQueueImage orderedQueueNode
        (orderedQueueTransitionResult before orderedQueueNode event
          (machine.localState orderedQueueNode)).nextState).Nodup) :
    ∃ after,
      AvailableEventStep orderedQueueImage
        (orderedQueueTransition before) event machine after := by
  let result :=
    orderedQueueTransitionResult before orderedQueueNode event
      (machine.localState orderedQueueNode)
  have htransition :
      orderedQueueTransition before orderedQueueNode event
        (machine.localState orderedQueueNode) result :=
    ⟨orderedQueueNode_mem, htarget, hsupport, hguard, rfl⟩
  have hchildKeys : (result.children.map Event.key).Nodup := by
    unfold result
    by_cases hkind : event.kind = .txReady
    · simp only [orderedQueueTransitionResult, hkind, ↓reduceIte]
      cases hselected :
          orderedQueueSelectedPacket (machine.localState orderedQueueNode) with
      | none => simp [orderedQueueServiceChildren]
      | some packet => simp [orderedQueueServiceChildren]
    · simp [orderedQueueTransitionResult, hkind,
        orderedQueueServiceChildren]
  have hconsumptions :
      result.packetReferenceConsumptions.Perm
        (ownedEventReference orderedQueueImage event ::
          stateReferenceConsumptions orderedQueueImage orderedQueueNode
            (machine.localState orderedQueueNode) result.nextState) := by
    rfl
  have hincrements :
      ReferenceIncrementsValid orderedQueueImage orderedQueueNode
        (machine.localState orderedQueueNode) result := by
    unfold ReferenceIncrementsValid result
    rfl
  have havailable :=
    materializeScalarResult_available
      orderedQueueImage (orderedQueueTransition before)
      orderedQueue_unique_nodes orderedQueue_descriptor_oracle
      (orderedQueueTransition_generated_roles before)
      (orderedQueueTransition_descriptors before)
      orderedQueueNode orderedQueueNode_mem event result machine
      hwellFormed hevent htarget htransition horigin hchildKeys
      hchildrenFresh hconsumptions hincrements hnewStateNodup
  exact ⟨materializeScalarResult orderedQueueImage orderedQueueNode event
      result machine, havailable.1⟩

private theorem orderedQueueTransition_enabledOnReachable
    (before : PayloadId → PayloadId → Bool)
    (hordered : insertByDiscipline before 21 [12] = [21, 12]) :
    TransitionEnabledOnReachable orderedQueueImage
      (orderedQueueTransition before) := by
  intro machine hreachable node hnode event hleast htarget hsupport
  rcases orderedQueueReachable_wellFormed_shape hordered hreachable with
    ⟨hwellFormed, hshape⟩
  have hevent := hleast.1
  simp [orderedQueueImage] at hnode
  subst node
  cases hshape with
  | initial hpending hstate hcursor =>
      have heventEq : event = orderedQueueArrival := by
        rw [hpending] at hleast
        rcases List.mem_cons.mp hleast.1 with heq | htail
        · exact heq
        · have heq := List.mem_singleton.mp htail
          subst event
          have horder := hleast.2.2 orderedQueueArrival (by simp)
            (by simp [orderedQueueArrival, orderedQueueNode])
          have himpossible :
              ¬ orderedQueueReady.key ≤ orderedQueueArrival.key := by
            decide
          exact (himpossible horder).elim
      subst event
      refine orderedQueue_materialized_available before machine hwellFormed
        orderedQueueArrival hevent htarget hsupport ?_ ?_ ?_ ?_
      · simp [orderedQueueArrival]
      · simp [ChildrenUseOriginSequence, orderedQueueTransitionResult,
          orderedQueueServiceChildren, orderedQueueArrival]
      · simp [orderedQueueTransitionResult, orderedQueueServiceChildren,
          orderedQueueArrival]
      · rw [hstate]
        simp [orderedQueueTransitionResult, orderedQueueArrival,
          orderedQueueArrivalState, orderedQueueInitialState,
          ownedRoleStateReferences, hordered]
        decide
  | afterArrival hpending hstate hcursor =>
      have heventEq : event = orderedQueueReady := by
        simpa [hpending] using hevent
      subst event
      refine orderedQueue_materialized_available before machine hwellFormed
        orderedQueueReady hevent htarget hsupport ?_ ?_ ?_ ?_
      · simp [orderedQueueReady]
      · rw [hstate]
        have hcursorAtNode : machine.nextOriginSeq orderedQueueNode.id = 2 := by
          simpa [orderedQueueNode] using hcursor
        rw [hcursorAtNode]
        simp [orderedQueueAfterArrival, orderedQueueTransitionResult,
          orderedQueueArrivalState, orderedQueueInitialState,
          orderedQueueArrival, orderedQueueReady, orderedQueueSelectedPacket,
          orderedQueueServiceChildren, orderedQueueNode, hordered,
          ChildrenUseOriginSequence]
      · intro child hchild hallocated
        rw [hstate] at hchild
        have hchildren :
            (orderedQueueTransitionResult before orderedQueueNode
              orderedQueueReady (orderedQueueAfterArrival before)).children =
              [orderedQueueCompletion, orderedQueueRemote] := by
          simp [orderedQueueAfterArrival, orderedQueueTransitionResult,
            orderedQueueArrivalState, orderedQueueInitialState,
            orderedQueueArrival, orderedQueueReady, orderedQueueSelectedPacket,
            orderedQueueServiceChildren, orderedQueueCompletion,
            orderedQueueRemote, orderedQueueNode, hordered]
        rw [hchildren] at hchild
        rcases List.mem_cons.mp hchild with heq | htail
        · subst child
          have hlt :=
            (hwellFormed.2.2.1 orderedQueueCompletion.key hallocated).1
          simp [orderedQueueCompletion, hcursor] at hlt
        · have heq := List.mem_singleton.mp htail
          subst child
          have hlt :=
            (hwellFormed.2.2.1 orderedQueueRemote.key hallocated).1
          simp [orderedQueueRemote, hcursor] at hlt
      · rw [hstate]
        simp [orderedQueueAfterArrival, orderedQueueTransitionResult,
          orderedQueueArrivalState, orderedQueueInitialState,
          orderedQueueArrival, orderedQueueReady, orderedQueueSelectedPacket,
          orderedQueueReadyState, ownedRoleStateReferences, hordered]
        decide
  | afterReady hpending hstate =>
      have heventEq : event = orderedQueueRemote := by
        rw [hpending] at hleast
        rcases List.mem_cons.mp hleast.1 with heq | htail
        · exact heq
        · have heq := List.mem_singleton.mp htail
          subst event
          have horder := hleast.2.2 orderedQueueRemote (by simp)
            (by simp [orderedQueueRemote, orderedQueueNode])
          have himpossible :
              ¬ orderedQueueCompletion.key ≤ orderedQueueRemote.key := by
            decide
          exact (himpossible horder).elim
      subst event
      refine orderedQueue_materialized_available before machine hwellFormed
        orderedQueueRemote hevent htarget hsupport ?_ ?_ ?_ ?_
      · simp [orderedQueueRemote]
      · simp [ChildrenUseOriginSequence, orderedQueueTransitionResult,
          orderedQueueServiceChildren, orderedQueueRemote]
      · simp [orderedQueueTransitionResult, orderedQueueServiceChildren,
          orderedQueueRemote]
      · rw [hstate]
        simp [orderedQueueAfterReady, orderedQueueAfterArrival,
          orderedQueueTransitionResult, orderedQueueArrivalState,
          orderedQueueInitialState, orderedQueueArrival, orderedQueueReady,
          orderedQueueRemote, orderedQueueSelectedPacket,
          orderedQueueReadyState, ownedRoleStateReferences, hordered]
        decide
  | afterRemote hpending hstate =>
      have heventEq : event = orderedQueueCompletion := by
        simpa [hpending] using hevent
      subst event
      refine orderedQueue_materialized_available before machine hwellFormed
        orderedQueueCompletion hevent htarget hsupport ?_ ?_ ?_ ?_
      · intro _
        rw [hstate]
        simp [orderedQueueAfterRemote, orderedQueueAfterReady,
          orderedQueueAfterArrival, orderedQueueTransitionResult,
          orderedQueueArrivalState, orderedQueueInitialState,
          orderedQueueArrival, orderedQueueReady, orderedQueueRemote,
          orderedQueueCompletion, orderedQueueSelectedPacket,
          orderedQueueReadyState, hordered]
      · simp [ChildrenUseOriginSequence, orderedQueueTransitionResult,
          orderedQueueServiceChildren, orderedQueueCompletion]
      · simp [orderedQueueTransitionResult, orderedQueueServiceChildren,
          orderedQueueCompletion]
      · rw [hstate]
        simp [orderedQueueAfterRemote, orderedQueueAfterReady,
          orderedQueueAfterArrival, orderedQueueTransitionResult,
          orderedQueueArrivalState, orderedQueueInitialState,
          orderedQueueArrival, orderedQueueReady, orderedQueueRemote,
          orderedQueueCompletion, orderedQueueSelectedPacket,
          orderedQueueReadyState, ownedRoleStateReferences, hordered]
        decide
  | finished hpending _ =>
      rw [hpending] at hevent
      simp at hevent

/-- SP satisfies every frozen global transition axiom. -/
theorem sp_transitionAxioms :
    TransitionAxioms orderedQueueImage spTransition := by
  simpa [spTransition] using orderedQueueTransition_axioms spBefore

/-- WFQ satisfies every frozen global transition axiom. -/
theorem wfq_transitionAxioms :
    TransitionAxioms orderedQueueImage wfqTransition := by
  simpa [wfqTransition] using orderedQueueTransition_axioms wfqBefore

/-- SP selects only at the current service start and commits exactly one public head. -/
theorem sp_completeActualServiceStart :
    CompleteActualServiceStartDiscipline spTransition := by
  simpa [spTransition] using orderedQueue_complete_service_start spBefore

/-- WFQ selects only at the current service start and commits exactly one public head. -/
theorem wfq_completeActualServiceStart :
    CompleteActualServiceStartDiscipline wfqTransition := by
  simpa [wfqTransition] using orderedQueue_complete_service_start wfqBefore

/-- Reachable least-event progress for the SP transition instance. -/
theorem sp_transitionEnabledOnReachable :
    TransitionEnabledOnReachable orderedQueueImage spTransition := by
  simpa [spTransition] using
    orderedQueueTransition_enabledOnReachable spBefore
      sp_arrival_precedes_selection

/-- Reachable least-event progress for the exact-integer WFQ transition instance. -/
theorem wfq_transitionEnabledOnReachable :
    TransitionEnabledOnReachable orderedQueueImage wfqTransition := by
  simpa [wfqTransition] using
    orderedQueueTransition_enabledOnReachable wfqBefore
      wfq_arrival_precedes_selection

/-- Complete accepted-model instance for SP. -/
theorem sp_acceptedModel : AcceptedModel orderedQueueImage spTransition :=
  ⟨orderedQueue_static, orderedQueue_initial_roles,
    sp_transitionAxioms, sp_transitionEnabledOnReachable⟩

/-- Complete accepted-model instance for exact-integer WFQ. -/
theorem wfq_acceptedModel : AcceptedModel orderedQueueImage wfqTransition :=
  ⟨orderedQueue_static, orderedQueue_initial_roles,
    wfq_transitionAxioms, wfq_transitionEnabledOnReachable⟩

/-- The intervening arrival is visible to the SP decision made at `TxReady`. -/
theorem sp_arrival_visible_at_service_start :
    let afterArrival :=
      (orderedQueueTransitionResult spBefore orderedQueueNode
        orderedQueueArrival orderedQueueInitialState).nextState
    (orderedQueueTransitionResult spBefore orderedQueueNode
      orderedQueueReady afterArrival).decisions.map ServiceDecision.packet = [21] := by
  decide

/-- The intervening arrival is visible to the WFQ decision made at `TxReady`. -/
theorem wfq_arrival_visible_at_service_start :
    let afterArrival :=
      (orderedQueueTransitionResult wfqBefore orderedQueueNode
        orderedQueueArrival orderedQueueInitialState).nextState
    (orderedQueueTransitionResult wfqBefore orderedQueueNode
      orderedQueueReady afterArrival).decisions.map ServiceDecision.packet = [21] := by
  decide

/--
SP arrival, selection, and completion all occupy F5's conservative same-LP queue conflict class.
-/
theorem sp_queueMutationConflictCoverage :
    fifoQueueConflictClass .remoteArrival = fifoQueueConflictClass .txReady ∧
      fifoQueueConflictClass .txReady = fifoQueueConflictClass .txComplete := by
  exact ⟨rfl, rfl⟩

/--
WFQ arrival, selection, and completion all occupy F5's conservative same-LP queue conflict class.
-/
theorem wfq_queueMutationConflictCoverage :
    fifoQueueConflictClass .remoteArrival = fifoQueueConflictClass .txReady ∧
      fifoQueueConflictClass .txReady = fifoQueueConflictClass .txComplete := by
  exact ⟨rfl, rfl⟩

/-- Actual emission delta of the concrete arrival-before-selection service start. -/
def orderedQueueRoundEmissions : List (Event × Event) :=
  [(orderedQueueReady, orderedQueueCompletion),
    (orderedQueueReady, orderedQueueRemote)]

private theorem orderedQueueRoundEmissions_payload :
    ∀ parent child,
      RecordedEmissionEdge orderedQueueRoundEmissions parent child →
        parent.payload = child.payload := by
  intro parent child hedge
  simp [RecordedEmissionEdge, orderedQueueRoundEmissions] at hedge
  rcases hedge with ⟨rfl, rfl⟩ | ⟨rfl, rfl⟩ <;> rfl

/-- SP discharges the scoped F5 commutation premise for its actual service emissions. -/
theorem sp_independentStepsCommute :
    IndependentStepsCommute orderedQueueImage spTransition
      orderedQueueRoundEmissions :=
  acceptedModel_independentStepsCommute orderedQueueImage spTransition
    orderedQueueRoundEmissions sp_acceptedModel
    orderedQueueRoundEmissions_payload

/-- WFQ discharges the scoped F5 commutation premise for its actual service emissions. -/
theorem wfq_independentStepsCommute :
    IndependentStepsCommute orderedQueueImage wfqTransition
      orderedQueueRoundEmissions :=
  acceptedModel_independentStepsCommute orderedQueueImage wfqTransition
    orderedQueueRoundEmissions wfq_acceptedModel
    orderedQueueRoundEmissions_payload

/-- License in hand for every conflict-respecting SP permutation of the concrete round. -/
theorem sp_conflictRespectingReorderings_preserve_results
    (bounds : BoundFamily)
    (cut : Event → Prop)
    (start : RoundState OrderedQueueStateFamily)
    (drainedEvents : List Event)
    (roundFinish : RoundState OrderedQueueStateFamily)
    (canonicalOrder : List Event)
    (canonicalFinish : MachineState OrderedQueueStateFamily)
    (candidateOrder : List Event)
    (hround :
      SafeHorizonRound orderedQueueImage spTransition bounds cut
        start drainedEvents roundFinish)
    (hcanonical :
      CanonicalSerialRestricted orderedQueueImage spTransition cut
        start.machine canonicalOrder canonicalFinish)
    (hdelta :
      RoundEmissionDelta start.machine canonicalFinish
        orderedQueueRoundEmissions)
    (hmembership :
      ∀ event, event ∈ canonicalOrder ↔ event ∈ drainedEvents)
    (hpreserves :
      PreservesRequiredIntraRoundOrder orderedQueueRoundEmissions
        drainedEvents candidateOrder) :
    ∃ candidateFinish,
      ExecutionInOrder orderedQueueImage spTransition start.machine
          candidateOrder candidateFinish ∧
        SameMachineResult orderedQueueImage canonicalFinish candidateFinish := by
  exact
    (f5IntraRoundReordering_proved orderedQueueImage spTransition).1
      sp_acceptedModel bounds cut start drainedEvents roundFinish
      canonicalOrder canonicalFinish candidateOrder
      orderedQueueRoundEmissions hround hcanonical hdelta
      sp_independentStepsCommute hmembership hpreserves

/-- License in hand for every conflict-respecting WFQ permutation of the concrete round. -/
theorem wfq_conflictRespectingReorderings_preserve_results
    (bounds : BoundFamily)
    (cut : Event → Prop)
    (start : RoundState OrderedQueueStateFamily)
    (drainedEvents : List Event)
    (roundFinish : RoundState OrderedQueueStateFamily)
    (canonicalOrder : List Event)
    (canonicalFinish : MachineState OrderedQueueStateFamily)
    (candidateOrder : List Event)
    (hround :
      SafeHorizonRound orderedQueueImage wfqTransition bounds cut
        start drainedEvents roundFinish)
    (hcanonical :
      CanonicalSerialRestricted orderedQueueImage wfqTransition cut
        start.machine canonicalOrder canonicalFinish)
    (hdelta :
      RoundEmissionDelta start.machine canonicalFinish
        orderedQueueRoundEmissions)
    (hmembership :
      ∀ event, event ∈ canonicalOrder ↔ event ∈ drainedEvents)
    (hpreserves :
      PreservesRequiredIntraRoundOrder orderedQueueRoundEmissions
        drainedEvents candidateOrder) :
    ∃ candidateFinish,
      ExecutionInOrder orderedQueueImage wfqTransition start.machine
          candidateOrder candidateFinish ∧
        SameMachineResult orderedQueueImage canonicalFinish candidateFinish := by
  exact
    (f5IntraRoundReordering_proved orderedQueueImage wfqTransition).1
      wfq_acceptedModel bounds cut start drainedEvents roundFinish
      canonicalOrder canonicalFinish candidateOrder
      orderedQueueRoundEmissions hround hcanonical hdelta
      wfq_independentStepsCommute hmembership hpreserves

end DaysExecutor
