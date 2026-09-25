import DaysExecutor.ReplayInvariant
import DaysExecutor.RoundReplay

namespace DaysExecutor

/-- Every structurally derived machine owner carries its oracle descriptor. -/
theorem machineOwnedReferencesFor_descriptor_coherent
    (image : SimulationImage State)
    (machine : MachineState State)
    (node : NodeDescriptor)
    (horacle : DescriptorOracleWellFormed image) :
    ∀ reference ∈ machineOwnedReferencesFor image machine node,
      reference.descriptor =
        image.packetDescriptor reference.descriptor.id := by
  intro reference hreference
  unfold machineOwnedReferencesFor at hreference
  rcases List.mem_append.mp hreference with hpending | hstate
  · rcases List.mem_map.mp hpending with ⟨event, _, rfl⟩
    unfold ownedEventReference
    rw [(horacle _).1]
  · unfold ownedRoleStateReferences at hstate
    rcases List.mem_append.mp hstate with hqueue | hservice
    · rcases List.mem_map.mp hqueue with ⟨payload, _, rfl⟩
      unfold ownedQueueReference
      rw [(horacle _).1]
    · rcases List.mem_map.mp hservice with ⟨payload, _, rfl⟩
      unfold ownedInServiceReference
      rw [(horacle _).1]

/--
Two well-formed machines with equal pending work and declared local state have owner-equivalent
stores at every LP.
-/
theorem perLPOwnedStoresEquivalent_of_wellFormed
    (image : SimulationImage State)
    (left right : MachineState State)
    (hleft : MachineWellFormed image left)
    (hright : MachineWellFormed image right)
    (horacle : DescriptorOracleWellFormed image)
    (hpending : left.pending = right.pending)
    (hlocal :
      ∀ node ∈ image.nodes,
        left.localState node = right.localState node) :
    PerLPOwnedStoresEquivalent image left right := by
  intro node hnode
  have hreferences :
      machineOwnedReferencesFor image left node =
        machineOwnedReferencesFor image right node := by
    unfold machineOwnedReferencesFor
    rw [hpending, hlocal node hnode]
  apply ownedStoresEquivalent_of_matching_references image
    (machineOwnedReferencesFor image left node)
  · exact hleft.2.2.2.2.1 node hnode
  · exact hright.2.2.2.2.1 node hnode
  · exact machineOwnedReferencesFor_descriptor_coherent image left node horacle
  · exact hleft.2.2.2.2.2.1 node hnode
  · rw [hreferences]
    exact hright.2.2.2.2.2.1 node hnode

/-- Descriptor-carrying envelope construction preserves the exact emitted-event list. -/
theorem remoteEnvelopesFromStore_map_event
    (image : SimulationImage State)
    (source : NodeId)
    (store : List PacketStoreEntry)
    {events : List Event}
    {envelopes : List RemoteEnvelope}
    (hfrom :
      RemoteEnvelopesFromStore image source store events envelopes) :
    envelopes.map RemoteEnvelope.event = events := by
  induction events generalizing envelopes with
  | nil =>
      cases envelopes <;> simp_all [RemoteEnvelopesFromStore]
  | cons event events ih =>
      cases envelopes with
      | nil =>
          simp [RemoteEnvelopesFromStore] at hfrom
      | cons envelope envelopes =>
          simp only [RemoteEnvelopesFromStore] at hfrom
          simp only [List.map_cons]
          rw [hfrom.2.1, ih hfrom.2.2.2.2.2]

/-- Every constructed envelope carries the exact source-side child owner. -/
theorem ownedEnvelopeReference_eq_ownedChildReference
    (image : SimulationImage State)
    (source : NodeId)
    (store : List PacketStoreEntry)
    {events : List Event}
    {envelopes : List RemoteEnvelope}
    (hfrom :
      RemoteEnvelopesFromStore image source store events envelopes)
    {envelope : RemoteEnvelope}
    (henvelope : envelope ∈ envelopes) :
    ownedEnvelopeReference envelope =
      ownedEnvelopeEventReference image source envelope.event := by
  induction events generalizing envelopes with
  | nil =>
      cases envelopes <;> simp_all [RemoteEnvelopesFromStore]
  | cons event events ih =>
      cases envelopes with
      | nil =>
          simp [RemoteEnvelopesFromStore] at hfrom
      | cons head tail =>
          simp only [RemoteEnvelopesFromStore] at hfrom
          simp only [List.mem_cons] at henvelope
          rcases henvelope with rfl | htail
          · unfold ownedEnvelopeReference ownedEnvelopeEventReference
            rw [hfrom.1, hfrom.2.2.2.2.1]
          · exact ih hfrom.2.2.2.2.2 htail

/-- Pointwise equality on a node list lifts through outbox flattening. -/
private theorem flatMap_outboxes_eq
    (nodes : List NodeDescriptor)
    (left right : NodeId → List RemoteEnvelope)
    (heq : ∀ node ∈ nodes, left node.id = right node.id) :
    (nodes.flatMap fun node => left node.id) =
      nodes.flatMap fun node => right node.id := by
  induction nodes with
  | nil =>
      rfl
  | cons head tail ih =>
      simp only [List.flatMap_cons]
      rw [heq head List.mem_cons_self]
      apply congrArg
      apply ih
      intro node hnode
      exact heq node (List.mem_cons_of_mem _ hnode)

/--
Updating one declared source outbox by appending a batch adds exactly that batch to the flattened
outbox multiset.
-/
theorem flatMap_outbox_update_perm
    (nodes : List NodeDescriptor)
    (source : NodeDescriptor)
    (before after : NodeId → List RemoteEnvelope)
    (emitted : List RemoteEnvelope)
    (hunique : (nodes.map NodeDescriptor.id).Nodup)
    (hsource : source ∈ nodes)
    (hupdated : after source.id = before source.id ++ emitted)
    (hother :
      ∀ other ∈ nodes, other.id ≠ source.id →
        after other.id = before other.id) :
    ((nodes.flatMap fun node => before node.id) ++ emitted).Perm
      (nodes.flatMap fun node => after node.id) := by
  induction nodes with
  | nil =>
      simp at hsource
  | cons head tail ih =>
      have hheadFresh := (List.nodup_cons.mp hunique).1
      have htailUnique := (List.nodup_cons.mp hunique).2
      simp only [List.mem_cons] at hsource
      rcases hsource with rfl | hsource
      · simp only [List.flatMap_cons]
        rw [hupdated]
        have htailEq :
            (tail.flatMap fun node => after node.id) =
              tail.flatMap fun node => before node.id := by
          apply flatMap_outboxes_eq
          intro other hmem
          apply hother other (List.mem_cons_of_mem _ hmem)
          intro heq
          apply hheadFresh
          rw [← heq]
          exact List.mem_map.mpr ⟨other, hmem, rfl⟩
        rw [htailEq]
        simpa only [List.append_assoc] using
          (List.Perm.append_left (before source.id)
            (List.perm_append_comm :
              ((tail.flatMap fun node => before node.id) ++ emitted).Perm
                (emitted ++
                  (tail.flatMap fun node => before node.id))))
      · have hheadNe : head.id ≠ source.id := by
          intro heq
          apply hheadFresh
          rw [heq]
          exact List.mem_map.mpr ⟨source, hsource, rfl⟩
        have hheadEq := hother head List.mem_cons_self hheadNe
        simp only [List.flatMap_cons]
        rw [hheadEq]
        simpa only [List.append_assoc] using
          (List.Perm.append_left (before head.id)
            (ih htailUnique hsource
              (fun other hmem =>
                hother other (List.mem_cons_of_mem _ hmem))))

/-- One local step adds precisely its newly constructed envelopes to flattened outboxes. -/
theorem localRoundStep_flattenedOutboxes_perm
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (node : NodeDescriptor)
    (before after : RoundState State)
    (event : Event)
    (hunique : UniqueNodeIds image)
    (hstep :
      LocalRoundStep image transition bounds node before event after) :
    ∃ result emittedRemote,
      transition node event (before.machine.localState node) result ∧
      RemoteEnvelopesFromStore image node.id
        (after.machine.packetStore node)
        (remoteChildren node.id result.children)
        emittedRemote ∧
      ((flattenedOutboxes image before) ++ emittedRemote).Perm
        (flattenedOutboxes image after) := by
  rcases hstep with
    ⟨hnode, _, result, htransition, _, _, _, _, _, _, _, _,
      emittedRemote, hfrom, hsource, hother⟩
  exact ⟨result, emittedRemote, htransition, hfrom,
    flatMap_outbox_update_perm image.nodes node before.outboxes after.outboxes
      emittedRemote hunique hnode hsource hother⟩

/--
Pure machine obtained by virtually exchanging a chosen ordering of all currently buffered
envelopes.
-/
def materializeRoundMachine
    (state : RoundState State)
    (ordered : List RemoteEnvelope) : MachineState State :=
  { state.machine with
    packetStore := fun node =>
      installRemoteEnvelopesFor node.id ordered
        (consumeRemoteEnvelopesFor node.id ordered
          (state.machine.packetStore node))
    pending :=
      insertEvents (ordered.map RemoteEnvelope.event)
        state.machine.pending }

/-- A complete exchange is exactly the corresponding virtual materialization. -/
theorem completeCanonicalExchange_materializes
    (image : SimulationImage State)
    (drained next : RoundState State)
    (hexchange : CompleteCanonicalExchange image drained next) :
    ∃ ordered,
      (flattenedOutboxes image drained).Perm ordered ∧
      ordered.Pairwise exchangeLT ∧
      (∀ envelope ∈ ordered, RemoteEnvelope.Coherent image envelope) ∧
      StrongMachineReplay image next.machine
        (materializeRoundMachine drained ordered) := by
  rcases hexchange with
    ⟨ordered, hperm, hsorted, hcoherent, _, hstores, hpending,
      hsummary, hobserved, hdepartures, harrivals, hcursors,
      hallocated, hemissions, _, _⟩
  refine ⟨ordered, hperm, hsorted, hcoherent, ?_⟩
  refine ⟨?_, ?_, hsummary, hobserved, hdepartures, harrivals,
    hpending, hcursors, ?_, ?_⟩
  · intro node hnode
    exact (hstores node hnode).1
  · intro node hnode
    rw [(hstores node hnode).2]
    exact ownedStoresEquivalent_refl _
  · rw [hallocated]
    exact List.Perm.refl _
  · rw [hemissions]
    exact List.Perm.refl _

/-- Pending events plus not-yet-exchanged envelope events at one CPU round state. -/
def roundEventUniverse
    (image : SimulationImage State)
    (state : RoundState State) : List Event :=
  state.machine.pending ++
    (flattenedOutboxes image state).map RemoteEnvelope.event

/--
Induction relation between an in-round CPU state and scalar immediate-child execution.  Stores are
compared structurally only at post-exchange boundaries; during the drain, well-formed scalar
ownership and a duplicate-free CPU event universe are the stable facts needed by the next step.
-/
def RoundScalarProgress
    (image : SimulationImage State)
    (cpu : RoundState State)
    (scalar : MachineState State) : Prop :=
  MachineWellFormed image scalar ∧
    (roundEventUniverse image cpu).Nodup ∧
    (∀ node ∈ image.nodes,
      cpu.machine.localState node = scalar.localState node) ∧
    cpu.machine.summary = scalar.summary ∧
    cpu.machine.observedPackets = scalar.observedPackets ∧
    cpu.machine.departures = scalar.departures ∧
    cpu.machine.arrivals = scalar.arrivals ∧
    (∀ event,
      event ∈ scalar.pending ↔ event ∈ roundEventUniverse image cpu) ∧
    cpu.machine.nextOriginSeq = scalar.nextOriginSeq ∧
    cpu.machine.allocatedKeys = scalar.allocatedKeys ∧
    cpu.machine.emissions = scalar.emissions

/-- Empty declared outboxes flatten to the empty list. -/
theorem flattenedOutboxes_eq_nil_of_allEmpty
    (image : SimulationImage State)
    (state : RoundState State)
    (hempty : AllOutboxesEmpty image state) :
    flattenedOutboxes image state = [] := by
  unfold flattenedOutboxes
  rw [flatMap_outboxes_eq
    (nodes := image.nodes)
    (left := state.outboxes)
    (right := fun _ => [])
    (heq := by
      intro node hnode
      exact hempty node hnode)]
  simp

/-- A post-exchange boundary initially agrees with its own scalar projection. -/
theorem roundScalarProgress_start
    (image : SimulationImage State)
    (start : RoundState State)
    (hstart : PostExchangeStart image start) :
    RoundScalarProgress image start start.machine := by
  have houtboxes :=
    flattenedOutboxes_eq_nil_of_allEmpty image start hstart.1
  refine ⟨hstart.2.1, ?_, fun _ _ => rfl, rfl, rfl, rfl, rfl,
    ?_, rfl, rfl, rfl⟩
  · unfold roundEventUniverse
    rw [houtboxes]
    simp only [List.map_nil, List.append_nil]
    exact canonicalPending_nodup hstart.2.1.1
  · intro event
    unfold roundEventUniverse
    rw [houtboxes]
    simp

/-- Complete exchange pending membership is exactly the pre-exchange round event universe. -/
theorem completeCanonicalExchange_pending_mem_iff
    (image : SimulationImage State)
    (drained next : RoundState State)
    (hexchange : CompleteCanonicalExchange image drained next)
    (event : Event) :
    event ∈ next.machine.pending ↔
      event ∈ roundEventUniverse image drained := by
  rcases hexchange with
    ⟨ordered, hperm, _, _, _, _, hpending, _, _, _, _, _, _, _, _, _⟩
  rw [hpending, mem_insertEvents_iff]
  unfold roundEventUniverse
  rw [List.mem_append, or_comm]
  apply or_congr Iff.rfl
  constructor
  · intro hmem
    rcases List.mem_map.mp hmem with ⟨envelope, henvelope, heq⟩
    apply List.mem_map.mpr
    exact ⟨envelope, hperm.symm.subset henvelope, heq⟩
  · intro hmem
    rcases List.mem_map.mp hmem with ⟨envelope, henvelope, heq⟩
    apply List.mem_map.mpr
    exact ⟨envelope, hperm.subset henvelope, heq⟩

/--
At exchange completion, a CPU/scalar progress relation plus endpoint store provenance collapses to
the strong machine replay relation.
-/
theorem roundScalarProgress_finish
    (image : SimulationImage State)
    (drained next : RoundState State)
    (scalar : MachineState State)
    (hprogress : RoundScalarProgress image drained scalar)
    (hexchange : CompleteCanonicalExchange image drained next)
    (hnext : PostExchangeStart image next)
    (hstores :
      PerLPOwnedStoresEquivalent image scalar next.machine) :
    StrongMachineReplay image scalar next.machine := by
  have hpendingExchange :=
    completeCanonicalExchange_pending_mem_iff image drained next hexchange
  rcases hprogress with
    ⟨hscalarWellFormed, _, hlocal, hsummary, hobserved, hdepartures,
      harrivals, hpendingCover, hcursors, hallocated, hemissions⟩
  rcases hexchange with
    ⟨_, _, _, _, _, hnextFields, _, hnextSummary, hnextObserved,
      hnextDepartures, hnextArrivals, hnextCursors, hnextAllocated,
      hnextEmissions, _, _⟩
  have hpending :
      scalar.pending = next.machine.pending := by
    apply canonicalPending_eq_of_mem_iff
      hscalarWellFormed.1 hnext.2.1.1
    intro event
    rw [hpendingCover, ← hpendingExchange event]
  refine ⟨?_, hstores, ?_, ?_, ?_, ?_, hpending, ?_, ?_, ?_⟩
  · intro node hnode
    exact (hlocal node hnode).symm.trans (hnextFields node hnode).1.symm
  · exact hsummary.symm.trans hnextSummary.symm
  · exact hobserved.symm.trans hnextObserved.symm
  · exact hdepartures.symm.trans hnextDepartures.symm
  · exact harrivals.symm.trans hnextArrivals.symm
  · exact hcursors.symm.trans hnextCursors.symm
  · rw [← hallocated, hnextAllocated]
  · rw [← hemissions, hnextEmissions]

/-- Local and remote child partitions contain exactly the original child list. -/
theorem local_remote_children_perm
    (source : NodeId)
    (children : List Event) :
    (localChildren source children ++ remoteChildren source children).Perm
      children := by
  simpa [localChildren, remoteChildren] using
    (List.filter_append_perm
      (fun child : Event => decide (child.target = source)) children)

/--
One already-constructed scalar step advances the CPU/scalar progress invariant alongside its
matching local CPU step.
-/
theorem roundScalarProgress_step
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hdeterministic : TransitionDeterministic transition)
    (hunique : UniqueNodeIds image)
    (bounds : BoundFamily)
    (node : NodeDescriptor)
    (cpuBefore cpuAfter : RoundState State)
    (scalarBefore scalarAfter : MachineState State)
    (event : Event)
    (hprogress :
      RoundScalarProgress image cpuBefore scalarBefore)
    (hcpu :
      LocalRoundStep image transition bounds node
        cpuBefore event cpuAfter)
    (hscalar :
      AvailableEventStep image transition event
        scalarBefore scalarAfter)
    (hscalarWellFormed : MachineWellFormed image scalarAfter) :
    RoundScalarProgress image cpuAfter scalarAfter := by
  rcases hprogress with
    ⟨hbeforeWellFormed, huniverse, hlocal, hsummary, hobserved,
      hdepartures, harrivals, hpendingCover, hcursors,
      hallocated, hemissionsEq⟩
  rcases hcpu with
    ⟨hnode, hleast, cpuResult, hcpuTransition, hcpuFresh,
      hcpuAllocates, hcpuApplies, _, _, hcpuPending, hcpuEmissions,
      _, emittedRemote, hremoteFrom, hsourceOutbox, hotherOutboxes⟩
  rcases hscalar with
    ⟨_, scalarNode, hscalarNode, scalarResult, hscalarTarget,
      hscalarTransition, _, hscalarAllocates, hscalarApplies, _,
      _, hscalarPending, hscalarEmissions⟩
  have hcpuTarget : event.target = node.id := hleast.2.1.1
  have hnodesEqual : scalarNode = node := by
    apply node_eq_of_unique_ids hunique hscalarNode hnode
    exact hscalarTarget.symm.trans hcpuTarget
  subst scalarNode
  have hresults : scalarResult = cpuResult := by
    exact hdeterministic node event (scalarBefore.localState node)
      scalarResult cpuResult hscalarTransition
      (by
        rw [← hlocal node hnode]
        exact hcpuTransition)
  subst scalarResult
  rcases hcpuAllocates with
    ⟨_, hchildKeys, _, hcpuAllocatedAfter, hcpuCursorNode,
      hcpuCursorOther⟩
  rcases hscalarAllocates with
    ⟨_, _, _, hscalarAllocatedAfter, hscalarCursorNode,
      hscalarCursorOther⟩
  rcases hcpuApplies with
    ⟨hcpuState, _, hcpuOtherStates, hcpuOutput, _, _⟩
  rcases hscalarApplies with
    ⟨hscalarState, _, hscalarOtherStates, hscalarOutput, _, _⟩
  have hflattened :
      ((flattenedOutboxes image cpuBefore) ++ emittedRemote).Perm
        (flattenedOutboxes image cpuAfter) :=
    flatMap_outbox_update_perm image.nodes node
      cpuBefore.outboxes cpuAfter.outboxes emittedRemote hunique hnode
      hsourceOutbox hotherOutboxes
  have hemittedEvents :
      emittedRemote.map RemoteEnvelope.event =
        remoteChildren node.id cpuResult.children :=
    remoteEnvelopesFromStore_map_event image node.id
      (cpuAfter.machine.packetStore node) hremoteFrom
  have hcpuPendingPerm :
      cpuAfter.machine.pending.Perm
        (localChildren node.id cpuResult.children ++
          cpuBefore.machine.pending.erase event) := by
    rw [hcpuPending]
    exact insertEvents_perm_append _ _
  have houtboxEventsPerm :
      ((flattenedOutboxes image cpuAfter).map RemoteEnvelope.event).Perm
        (((flattenedOutboxes image cpuBefore).map RemoteEnvelope.event) ++
          remoteChildren node.id cpuResult.children) := by
    have hmapped := hflattened.map RemoteEnvelope.event
    rw [List.map_append, hemittedEvents] at hmapped
    exact hmapped.symm
  have heventCpu : event ∈ cpuBefore.machine.pending := hleast.1
  have hchildrenNodup : cpuResult.children.Nodup := by
    have hpairwise :=
      List.pairwise_map.mp hchildKeys
    exact hpairwise.imp fun hne heq =>
      hne (congrArg Event.key heq)
  have huniverseErased :
      (roundEventUniverse image cpuBefore).erase event =
        cpuBefore.machine.pending.erase event ++
          (flattenedOutboxes image cpuBefore).map RemoteEnvelope.event := by
    unfold roundEventUniverse
    exact List.erase_append_left _ heventCpu
  have hafterUniversePerm :
      (roundEventUniverse image cpuAfter).Perm
        (cpuResult.children ++
          (roundEventUniverse image cpuBefore).erase event) := by
    unfold roundEventUniverse
    have hcombined := hcpuPendingPerm.append houtboxEventsPerm
    have hmoveRemote :
        ((localChildren node.id cpuResult.children ++
            cpuBefore.machine.pending.erase event) ++
          ((flattenedOutboxes image cpuBefore).map RemoteEnvelope.event ++
            remoteChildren node.id cpuResult.children)).Perm
          ((localChildren node.id cpuResult.children ++
              remoteChildren node.id cpuResult.children) ++
            (cpuBefore.machine.pending.erase event ++
              (flattenedOutboxes image cpuBefore).map
                RemoteEnvelope.event)) := by
      simpa only [List.append_assoc] using
        (List.Perm.append_left
          (localChildren node.id cpuResult.children)
          (List.perm_append_comm :
            ((cpuBefore.machine.pending.erase event ++
                (flattenedOutboxes image cpuBefore).map
                  RemoteEnvelope.event) ++
              remoteChildren node.id cpuResult.children).Perm
            (remoteChildren node.id cpuResult.children ++
              (cpuBefore.machine.pending.erase event ++
                (flattenedOutboxes image cpuBefore).map
                  RemoteEnvelope.event))))
    exact hcombined.trans
      (hmoveRemote.trans
        ((local_remote_children_perm node.id cpuResult.children).append_right _
          |>.trans
            ((List.Perm.of_eq huniverseErased.symm).append_left
              cpuResult.children)))
  have htargetNodup :
      (cpuResult.children ++
        (roundEventUniverse image cpuBefore).erase event).Nodup := by
    rw [List.nodup_append]
    refine ⟨hchildrenNodup, huniverse.erase event, ?_⟩
    intro child hchild old hold heq
    subst old
    have holdUniverse := List.mem_of_mem_erase hold
    exact hcpuFresh child hchild child holdUniverse rfl
  have hafterUniverseNodup :
      (roundEventUniverse image cpuAfter).Nodup :=
    hafterUniversePerm.nodup_iff.mpr htargetNodup
  have hbeforePendingNodup :=
    canonicalPending_nodup hbeforeWellFormed.1
  have hbeforePendingPerm :
      scalarBefore.pending.Perm
        (roundEventUniverse image cpuBefore) :=
    perm_of_nodup_of_mem_iff hbeforePendingNodup huniverse hpendingCover
  have hscalarAfterPerm :
      scalarAfter.pending.Perm
        (roundEventUniverse image cpuAfter) := by
    rw [hscalarPending]
    exact (insertEvents_perm_append _ _).trans
      (((hbeforePendingPerm.erase event).append_left cpuResult.children).trans
        hafterUniversePerm.symm)
  refine ⟨hscalarWellFormed, hafterUniverseNodup, ?_, ?_, ?_, ?_, ?_,
    ?_, ?_, ?_, ?_⟩
  · intro other hother
    by_cases hid : other.id = node.id
    next =>
      have heq :=
        node_eq_of_unique_ids hunique hother hnode hid
      subst other
      exact hcpuState.trans hscalarState.symm
    next =>
      exact (hcpuOtherStates other hother hid).1.trans
        ((hlocal other hother).trans
          (hscalarOtherStates other hother hid).1.symm)
  · rw [hcpuOutput.1, hscalarOutput.1, hsummary]
  · rw [hcpuOutput.2.1, hscalarOutput.2.1, hobserved]
  · rw [hcpuOutput.2.2.1, hscalarOutput.2.2.1, hdepartures]
  · rw [hcpuOutput.2.2.2, hscalarOutput.2.2.2, harrivals]
  · intro candidate
    exact hscalarAfterPerm.mem_iff
  · funext origin
    by_cases heq : origin = node.id
    · subst origin
      rw [hcpuCursorNode, hscalarCursorNode,
        congrArg (fun cursor => cursor node.id) hcursors]
    · rw [hcpuCursorOther origin heq, hscalarCursorOther origin heq,
        congrArg (fun cursor => cursor origin) hcursors]
  · rw [hcpuAllocatedAfter, hscalarAllocatedAfter, hallocated]
  · rw [hcpuEmissions, hscalarEmissions, hemissionsEq]

end DaysExecutor
