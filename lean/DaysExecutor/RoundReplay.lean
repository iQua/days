import DaysExecutor.ReplayAlgebra
import DaysExecutor.SafeHorizon

namespace DaysExecutor

/--
Replay equality for owned stores is symmetric.  The descriptor order stays exact; only the
non-semantic order of owners inside an entry may change.
-/
theorem ownedStoresEquivalent_symm
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right) :
    OwnedStoresEquivalent right left := by
  induction hequivalent with
  | nil =>
      exact .nil
  | cons head tail ih =>
      exact .cons ⟨head.1.symm, head.2.symm⟩ ih

/-- Owned-store replay equality is transitive. -/
theorem ownedStoresEquivalent_trans
    {left middle right : List PacketStoreEntry}
    (hleft : OwnedStoresEquivalent left middle)
    (hright : OwnedStoresEquivalent middle right) :
    OwnedStoresEquivalent left right := by
  induction hleft generalizing right with
  | nil =>
      cases hright
      exact .nil
  | @cons leftHead middleHead leftTail middleTail head tail ih =>
      cases hright with
      | cons nextHead nextTail =>
          exact .cons
            ⟨head.1.trans nextHead.1, head.2.trans nextHead.2⟩
            (ih nextTail)

/-- Equivalent owner multisets give the same exact-owner count. -/
theorem ownedReferenceCount_eq_of_ownedStoresEquivalent
    (reference : OwnedPacketReference)
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right) :
    ownedReferenceCount reference left =
      ownedReferenceCount reference right := by
  induction hequivalent with
  | nil =>
      rfl
  | @cons leftHead rightHead leftTail rightTail head tail ih =>
      rcases head with ⟨hdescriptor, howners⟩
      simp only [ownedReferenceCount]
      by_cases hid : leftHead.descriptor.id = reference.descriptor.id
      · have hid' : rightHead.descriptor.id = reference.descriptor.id := by
          rw [← hdescriptor]
          exact hid
        simp only [hid, hid', ↓reduceIte]
        exact howners.count_eq reference.owner
      · have hid' : rightHead.descriptor.id ≠ reference.descriptor.id := by
          rw [← hdescriptor]
          exact hid
        simp only [hid, hid', ↓reduceIte]
        exact ih

/-- Equivalent owner stores give the same aggregate payload count. -/
theorem descriptorReferenceCount_eq_of_ownedStoresEquivalent
    (payload : PayloadId)
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right) :
    descriptorReferenceCount payload left =
      descriptorReferenceCount payload right := by
  induction hequivalent with
  | nil =>
      rfl
  | @cons leftHead rightHead leftTail rightTail head tail ih =>
      rcases head with ⟨hdescriptor, howners⟩
      simp only [descriptorReferenceCount]
      by_cases hid : leftHead.descriptor.id = payload
      · have hid' : rightHead.descriptor.id = payload := by
          rw [← hdescriptor]
          exact hid
        simp only [hid, hid', ↓reduceIte]
        exact howners.length_eq
      · have hid' : rightHead.descriptor.id ≠ payload := by
          rw [← hdescriptor]
          exact hid
        simp only [hid, hid', ↓reduceIte]
        exact ih

/-- Acquisition respects owner-store replay equality. -/
theorem acquireOwnedReference_congr
    (reference : OwnedPacketReference)
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right) :
    OwnedStoresEquivalent
      (acquireOwnedReference reference left)
      (acquireOwnedReference reference right) := by
  induction hequivalent with
  | nil =>
      exact ownedStoresEquivalent_refl _
  | @cons leftHead rightHead leftTail rightTail head tail ih =>
      rcases head with ⟨hdescriptor, howners⟩
      have hid :
          reference.descriptor.id = leftHead.descriptor.id ↔
            reference.descriptor.id = rightHead.descriptor.id := by
        rw [hdescriptor]
      have hle :
          descriptorLE reference.descriptor leftHead.descriptor ↔
            descriptorLE reference.descriptor rightHead.descriptor := by
        rw [hdescriptor]
      simp only [acquireOwnedReference]
      split
      next heq =>
        have heq' := hid.mp heq
        simp only [heq', ↓reduceIte]
        exact .cons
          ⟨by simp [hdescriptor],
            List.Perm.cons reference.owner howners⟩
          tail
      next hne =>
        have hne' : reference.descriptor.id ≠ rightHead.descriptor.id := by
          intro heq
          exact hne (hid.mpr heq)
        simp only [hne', ↓reduceIte]
        split
        next hbefore =>
          have hbefore' := hle.mp hbefore
          simp only [hbefore', ↓reduceIte]
          exact .cons ⟨rfl, List.Perm.refl _⟩
            (.cons ⟨hdescriptor, howners⟩ tail)
        next hafter =>
          have hafter' : ¬ descriptorLE reference.descriptor rightHead.descriptor := by
            intro hright
            exact hafter (hle.mpr hright)
          simp only [hafter', ↓reduceIte]
          exact .cons ⟨hdescriptor, howners⟩ ih

/-- Release respects owner-store replay equality. -/
theorem releaseOwnedReference_congr
    (reference : OwnedPacketReference)
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right) :
    OwnedStoresEquivalent
      (releaseOwnedReference reference left)
      (releaseOwnedReference reference right) := by
  induction hequivalent with
  | nil =>
      exact .nil
  | @cons leftHead rightHead leftTail rightTail head tail ih =>
      rcases head with ⟨hdescriptor, howners⟩
      have hid :
          leftHead.descriptor.id = reference.descriptor.id ↔
            rightHead.descriptor.id = reference.descriptor.id := by
        rw [← hdescriptor]
      simp only [releaseOwnedReference]
      split
      next heq =>
        have heq' := hid.mp heq
        simp only [heq', ↓reduceIte]
        have herase :
            (leftHead.owners.erase reference.owner).Perm
              (rightHead.owners.erase reference.owner) :=
          howners.erase reference.owner
        have hnil :
            leftHead.owners.erase reference.owner = [] ↔
              rightHead.owners.erase reference.owner = [] := by
          constructor
          · intro hleft
            have hlength := herase.length_eq
            rw [hleft] at hlength
            exact List.eq_nil_of_length_eq_zero (by simpa using hlength.symm)
          · intro hright
            have hlength := herase.length_eq
            rw [hright] at hlength
            exact List.eq_nil_of_length_eq_zero (by simpa using hlength)
        by_cases hempty : leftHead.owners.erase reference.owner = []
        · have hempty' := hnil.mp hempty
          simp only [hempty, hempty', List.isEmpty_nil, ↓reduceIte]
          exact tail
        · have hempty' : rightHead.owners.erase reference.owner ≠ [] := by
            intro hright
            exact hempty (hnil.mpr hright)
          have hleftNotEmpty :
              (leftHead.owners.erase reference.owner).isEmpty = false := by
            rw [List.isEmpty_eq_false_iff]
            exact hempty
          have hrightNotEmpty :
              (rightHead.owners.erase reference.owner).isEmpty = false := by
            rw [List.isEmpty_eq_false_iff]
            exact hempty'
          simp only [hleftNotEmpty, hrightNotEmpty, Bool.false_eq_true,
            ↓reduceIte]
          exact .cons ⟨hdescriptor, herase⟩ tail
      next hne =>
        have hne' : rightHead.descriptor.id ≠ reference.descriptor.id := by
          intro heq
          exact hne (hid.mpr heq)
        simp only [hne', ↓reduceIte]
        exact .cons ⟨hdescriptor, howners⟩ ih

/-- A fold of acquisitions respects owner-store replay equality. -/
theorem acquireOwnedReferences_congr
    (references : List OwnedPacketReference)
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right) :
    OwnedStoresEquivalent
      (references.foldl
        (fun current reference => acquireOwnedReference reference current)
        left)
      (references.foldl
        (fun current reference => acquireOwnedReference reference current)
        right) := by
  induction references generalizing left right with
  | nil =>
      exact hequivalent
  | cons head tail ih =>
      exact ih (acquireOwnedReference_congr head hequivalent)

/-- A fold of releases respects owner-store replay equality. -/
theorem releaseOwnedReferences_congr
    (references : List OwnedPacketReference)
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right) :
    OwnedStoresEquivalent
      (references.foldl
        (fun current reference => releaseOwnedReference reference current)
        left)
      (references.foldl
        (fun current reference => releaseOwnedReference reference current)
        right) := by
  induction references generalizing left right with
  | nil =>
      exact hequivalent
  | cons head tail ih =>
      exact ih (releaseOwnedReference_congr head hequivalent)

/-- Structural packet effects respect owner-store replay equality. -/
theorem applyPacketEffects_congr
    (result : TransitionResult State kind)
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right) :
    OwnedStoresEquivalent
      (applyPacketEffects result left)
      (applyPacketEffects result right) := by
  unfold applyPacketEffects
  apply acquireOwnedReferences_congr
  exact releaseOwnedReferences_congr _ hequivalent

/-- Target-side child installation respects owner-store replay equality. -/
theorem installChildDescriptorsFor_congr
    (image : SimulationImage State)
    (target : NodeId)
    (children : List Event)
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right) :
    OwnedStoresEquivalent
      (installChildDescriptorsFor image target children left)
      (installChildDescriptorsFor image target children right) := by
  induction children generalizing left right with
  | nil =>
      exact hequivalent
  | cons child tail ih =>
      simp only [installChildDescriptorsFor, List.foldl_cons]
      split
      · exact ih (acquireOwnedReference_congr _ hequivalent)
      · exact ih hequivalent

/-- Source-side child holding respects owner-store replay equality. -/
theorem holdEmittedChildReferences_congr
    (image : SimulationImage State)
    (source : NodeId)
    (children : List Event)
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right) :
    OwnedStoresEquivalent
      (holdEmittedChildReferences image source children left)
      (holdEmittedChildReferences image source children right) := by
  induction children generalizing left right with
  | nil =>
      exact hequivalent
  | cons child tail ih =>
      simp only [holdEmittedChildReferences, List.foldl_cons]
      exact ih (acquireOwnedReference_congr _ hequivalent)

/-- Equivalent stores have the same descriptor spine. -/
theorem ownedStoresEquivalent_map_descriptor
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right) :
    left.map PacketStoreEntry.descriptor =
      right.map PacketStoreEntry.descriptor := by
  induction hequivalent with
  | nil =>
      rfl
  | cons head tail ih =>
      simp only [List.map_cons]
      rw [head.1, ih]

/-- Strict payload ordering is invariant under owner-list permutation. -/
theorem descriptorStoreSorted_of_ownedStoresEquivalent
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right)
    (hsorted : DescriptorStoreSorted left) :
    DescriptorStoreSorted right := by
  have hdescriptorsRaw :=
    congrArg
      (List.map PacketDescriptor.id)
      (ownedStoresEquivalent_map_descriptor hequivalent)
  have hdescriptors :
      (left.map fun entry => entry.descriptor.id) =
        right.map fun entry => entry.descriptor.id := by
    simpa only [List.map_map, Function.comp_apply] using hdescriptorsRaw
  have hleft :
      (left.map fun entry => entry.descriptor.id).Pairwise (· < ·) := by
    exact List.pairwise_map.mpr hsorted
  rw [hdescriptors] at hleft
  exact List.pairwise_map.mp hleft

/--
Every entry on the right of an owned-store equivalence has a matching left entry with the same
descriptor and a permutation-equivalent owner multiset.
-/
theorem exists_left_entry_of_ownedStoresEquivalent
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right)
    {rightEntry : PacketStoreEntry}
    (hmem : rightEntry ∈ right) :
    ∃ leftEntry ∈ left,
      leftEntry.descriptor = rightEntry.descriptor ∧
        leftEntry.owners.Perm rightEntry.owners := by
  induction hequivalent with
  | nil =>
      simp at hmem
  | @cons leftHead rightHead leftTail rightTail head tail ih =>
      simp only [List.mem_cons] at hmem
      rcases hmem with rfl | htail
      · exact ⟨leftHead, List.mem_cons_self, head.1, head.2⟩
      · rcases ih htail with ⟨leftEntry, hleft, hdescriptor, howners⟩
        exact ⟨leftEntry, List.mem_cons_of_mem _ hleft, hdescriptor, howners⟩

/-- Descriptor-store coherence is invariant under owner-list permutation. -/
theorem descriptorStoreCoherent_of_ownedStoresEquivalent
    (image : SimulationImage State)
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right)
    (hcoherent : DescriptorStoreCoherent image left) :
    DescriptorStoreCoherent image right := by
  refine ⟨descriptorStoreSorted_of_ownedStoresEquivalent hequivalent hcoherent.1,
    ?_, ?_⟩
  · have hidsRaw :=
      congrArg
        (List.map PacketDescriptor.id)
        (ownedStoresEquivalent_map_descriptor hequivalent)
    have hids :
        (left.map fun entry => entry.descriptor.id) =
          right.map fun entry => entry.descriptor.id := by
      simpa only [List.map_map, Function.comp_apply] using hidsRaw
    rw [← hids]
    exact hcoherent.2.1
  · intro rightEntry hright
    rcases exists_left_entry_of_ownedStoresEquivalent hequivalent hright with
      ⟨leftEntry, hleft, hdescriptor, howners⟩
    have hleftCoherent := hcoherent.2.2 leftEntry hleft
    constructor
    · intro hempty
      have hlength := howners.length_eq
      rw [hempty] at hlength
      have : leftEntry.owners = [] :=
        List.eq_nil_of_length_eq_zero (by simpa using hlength)
      exact hleftCoherent.1 this
    constructor
    · exact howners.nodup_iff.mp hleftCoherent.2.1
    · rw [← hdescriptor]
      exact hleftCoherent.2.2

/-- Exact held-reference validity transports across owner-store replay equality. -/
theorem packetReferencesHeld_of_ownedStoresEquivalent
    (references : List OwnedPacketReference)
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right)
    (hheld : PacketReferencesHeld references left) :
    PacketReferencesHeld references right := by
  intro reference hreference
  rw [← ownedReferenceCount_eq_of_ownedStoresEquivalent reference hequivalent]
  exact hheld reference hreference

/-- Scalar child availability transports across owner-store replay equality at every LP. -/
theorem childReferencesAvailable_of_perLPOwnedStoresEquivalent
    (image : SimulationImage State)
    (left right : MachineState State)
    (children : List Event)
    (hequivalent : PerLPOwnedStoresEquivalent image left right)
    (havailable : ChildReferencesAvailableAtTargets image left children) :
    ChildReferencesAvailableAtTargets image right children := by
  intro child hchild
  rcases havailable child hchild with ⟨target, htarget, hid, hcount⟩
  exact ⟨target, htarget, hid,
    by
      rw [← ownedReferenceCount_eq_of_ownedStoresEquivalent
        (ownedEventReference image child) (hequivalent target htarget)]
      exact hcount⟩

/--
Suffix-stable scalar replay equality.  In addition to owner provenance it retains the raw keyed
observation lists and the allocation ghosts that are intentionally absent from the public result.
-/
def StrongMachineReplay
    (image : SimulationImage State)
    (left right : MachineState State) : Prop :=
  (∀ node ∈ image.nodes, left.localState node = right.localState node) ∧
    PerLPOwnedStoresEquivalent image left right ∧
    left.summary = right.summary ∧
    left.observedPackets = right.observedPackets ∧
    left.departures = right.departures ∧
    left.arrivals = right.arrivals ∧
    left.pending = right.pending ∧
    left.nextOriginSeq = right.nextOriginSeq ∧
    left.allocatedKeys.Perm right.allocatedKeys ∧
    left.emissions.Perm right.emissions

/-- Strong replay equality is reflexive. -/
theorem strongMachineReplay_refl
    (image : SimulationImage State)
    (machine : MachineState State) :
    StrongMachineReplay image machine machine := by
  exact ⟨fun _ _ => rfl, fun node hnode => ownedStoresEquivalent_refl _,
    rfl, rfl, rfl, rfl, rfl, rfl, List.Perm.refl _, List.Perm.refl _⟩

/-- Strong replay equality is symmetric. -/
theorem strongMachineReplay_symm
    (image : SimulationImage State)
    {left right : MachineState State}
    (hreplay : StrongMachineReplay image left right) :
    StrongMachineReplay image right left := by
  exact ⟨fun node hnode => (hreplay.1 node hnode).symm,
    fun node hnode => ownedStoresEquivalent_symm (hreplay.2.1 node hnode),
    hreplay.2.2.1.symm,
    hreplay.2.2.2.1.symm,
    hreplay.2.2.2.2.1.symm,
    hreplay.2.2.2.2.2.1.symm,
    hreplay.2.2.2.2.2.2.1.symm,
    hreplay.2.2.2.2.2.2.2.1.symm,
    hreplay.2.2.2.2.2.2.2.2.1.symm,
    hreplay.2.2.2.2.2.2.2.2.2.symm⟩

/-- Strong replay equality is transitive. -/
theorem strongMachineReplay_trans
    (image : SimulationImage State)
    {left middle right : MachineState State}
    (hleft : StrongMachineReplay image left middle)
    (hright : StrongMachineReplay image middle right) :
    StrongMachineReplay image left right := by
  exact ⟨fun node hnode =>
      (hleft.1 node hnode).trans (hright.1 node hnode),
    fun node hnode =>
      ownedStoresEquivalent_trans
        (hleft.2.1 node hnode) (hright.2.1 node hnode),
    hleft.2.2.1.trans hright.2.2.1,
    hleft.2.2.2.1.trans hright.2.2.2.1,
    hleft.2.2.2.2.1.trans hright.2.2.2.2.1,
    hleft.2.2.2.2.2.1.trans hright.2.2.2.2.2.1,
    hleft.2.2.2.2.2.2.1.trans hright.2.2.2.2.2.2.1,
    hleft.2.2.2.2.2.2.2.1.trans hright.2.2.2.2.2.2.2.1,
    hleft.2.2.2.2.2.2.2.2.1.trans hright.2.2.2.2.2.2.2.2.1,
    hleft.2.2.2.2.2.2.2.2.2.trans hright.2.2.2.2.2.2.2.2.2⟩

/-- Descriptor spines agree after flattening a list of replay-equivalent LP stores. -/
private theorem flattenedStoreDescriptors_eq_for
    (nodes : List NodeDescriptor)
    (left right : MachineState State)
    (hequivalent : ∀ node ∈ nodes,
      OwnedStoresEquivalent
        (left.packetStore node) (right.packetStore node)) :
    ((nodes.flatMap left.packetStore).map PacketStoreEntry.descriptor) =
      ((nodes.flatMap right.packetStore).map PacketStoreEntry.descriptor) := by
  induction nodes with
  | nil =>
      rfl
  | cons node nodes ih =>
      simp only [List.flatMap_cons, List.map_append]
      rw [ownedStoresEquivalent_map_descriptor
        (hequivalent node List.mem_cons_self)]
      apply congrArg
      apply ih
      intro other hother
      exact hequivalent other (List.mem_cons_of_mem _ hother)

/-- Descriptor spines agree after flattening all declared LP stores. -/
private theorem flattenedStoreDescriptors_eq
    (image : SimulationImage State)
    (left right : MachineState State)
    (hequivalent : PerLPOwnedStoresEquivalent image left right) :
    ((image.nodes.flatMap left.packetStore).map PacketStoreEntry.descriptor) =
      ((image.nodes.flatMap right.packetStore).map PacketStoreEntry.descriptor) :=
  flattenedStoreDescriptors_eq_for image.nodes left right hequivalent

/-- Strong internal replay equality implies owner-preserving public replay equality. -/
theorem strongMachineReplay_implies_provenance
    (image : SimulationImage State)
    (left right : MachineState State)
    (hreplay : StrongMachineReplay image left right) :
    SameMachineProvenance image left right := by
  refine ⟨hreplay.1, hreplay.2.1, ?_⟩
  unfold projectRunResult
  rw [flattenedStoreDescriptors_eq image left right hreplay.2.1,
    hreplay.2.2.1,
    hreplay.2.2.2.1,
    hreplay.2.2.2.2.1,
    hreplay.2.2.2.2.2.1,
    hreplay.2.2.2.2.2.2.1]

/-- Strong internal replay equality implies the frozen public result comparison. -/
theorem strongMachineReplay_implies_result
    (image : SimulationImage State)
    (left right : MachineState State)
    (hreplay : StrongMachineReplay image left right) :
    SameMachineResult image left right :=
  sameMachineProvenance_implies_result image left right
    (strongMachineReplay_implies_provenance image left right hreplay)

/-- Unique declared LP identifiers identify the whole node descriptor. -/
theorem node_eq_of_unique_ids
    {nodes : List NodeDescriptor}
    (hunique : (nodes.map NodeDescriptor.id).Nodup)
    {left right : NodeDescriptor}
    (hleft : left ∈ nodes)
    (hright : right ∈ nodes)
    (hid : left.id = right.id) :
    left = right := by
  induction nodes with
  | nil =>
      simp at hleft
  | cons head tail ih =>
      have hheadFresh : head.id ∉ tail.map NodeDescriptor.id :=
        (List.nodup_cons.mp hunique).1
      have htailUnique := (List.nodup_cons.mp hunique).2
      simp only [List.mem_cons] at hleft hright
      rcases hleft with rfl | hleft
      · rcases hright with rfl | hright
        · rfl
        · exfalso
          apply hheadFresh
          rw [hid]
          exact List.mem_map.mpr ⟨right, hright, rfl⟩
      · rcases hright with rfl | hright
        · exfalso
          apply hheadFresh
          rw [← hid]
          exact List.mem_map.mpr ⟨left, hleft, rfl⟩
        · exact ih htailUnique hleft hright

/--
An arbitrary scalar step transports across strong replay equality.  This is the suffix-congruence
lemma used after every adjacent cross-LP swap.
-/
theorem availableEventStep_of_strongMachineReplay
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hunique : UniqueNodeIds image)
    (event : Event)
    (leftBefore leftAfter rightBefore : MachineState State)
    (hreplay : StrongMachineReplay image leftBefore rightBefore)
    (hstep :
      AvailableEventStep image transition event leftBefore leftAfter) :
    ∃ rightAfter,
      AvailableEventStep image transition event rightBefore rightAfter ∧
        StrongMachineReplay image leftAfter rightAfter := by
  rcases hreplay with
    ⟨hlocal, hstores, hsummary, hobserved, hdepartures, harrivals,
      hpendingEq, hcursors, hallocated, hemissionsEq⟩
  rcases hstep with
    ⟨hevent, node, hnode, result, htarget, htransition, hfresh,
      hallocates, happlies, hcoherent, hchildren, hpending, hemissions⟩
  rcases hallocates with
    ⟨horigin, hchildKeys, hchildrenFresh, hallocatedAfter,
      hcursorNode, hcursorOther⟩
  rcases happlies with
    ⟨hstate, hstore, hotherStates, houtput, hconsumptions, hincrements⟩
  let rightAfter : MachineState State :=
    { localState := leftAfter.localState
      packetStore := fun other =>
        if other = node then
          installChildDescriptorsFor image node.id result.children
            (applyPacketEffects result (rightBefore.packetStore node))
        else
          installChildDescriptorsFor image other.id result.children
            (rightBefore.packetStore other)
      pending :=
        insertEvents result.children (rightBefore.pending.erase event)
      summary := RunSummary.add rightBefore.summary result.summaryDelta
      observedPackets :=
        result.observedPackets.foldl
          (fun current descriptor => installDescriptor descriptor current)
          rightBefore.observedPackets
      departures :=
        result.departures.foldl
          (fun current record => insertDeparture record current)
          rightBefore.departures
      arrivals :=
        result.arrivals.foldl
          (fun current record => insertArrival record current)
          rightBefore.arrivals
      nextOriginSeq := fun origin =>
        if origin = node.id then
          rightBefore.nextOriginSeq node.id + result.children.length
        else
          rightBefore.nextOriginSeq origin
      allocatedKeys :=
        rightBefore.allocatedKeys ++ result.children.map Event.key
      emissions :=
        rightBefore.emissions ++
          result.children.map fun child => (event, child) }
  have hafterStores :
      PerLPOwnedStoresEquivalent image leftAfter rightAfter := by
    intro other hother
    by_cases heq : other = node
    · subst other
      rw [hstore]
      simp only [rightAfter, ↓reduceIte]
      exact installChildDescriptorsFor_congr image node.id result.children
        (applyPacketEffects_congr result (hstores node hnode))
    · have hid : other.id ≠ node.id := by
        intro hid
        exact heq
          (node_eq_of_unique_ids hunique hother hnode hid)
      have hotherStore := (hotherStates other hother hid).2
      rw [hotherStore]
      simp only [rightAfter, heq, ↓reduceIte]
      exact installChildDescriptorsFor_congr image other.id result.children
        (hstores other hother)
  have hrightConsumptions :
      ReferenceConsumptionsValid image node event
        (rightBefore.localState node) result
        (rightBefore.packetStore node) := by
    unfold ReferenceConsumptionsValid at hconsumptions ⊢
    rw [← hlocal node hnode]
    exact ⟨hconsumptions.1,
      packetReferencesHeld_of_ownedStoresEquivalent
        result.packetReferenceConsumptions
        (hstores node hnode) hconsumptions.2⟩
  have hrightIncrements :
      ReferenceIncrementsValid image node
        (rightBefore.localState node) result := by
    unfold ReferenceIncrementsValid at hincrements ⊢
    rw [← hlocal node hnode]
    exact hincrements
  have hrightAllocation :
      AllocatesChildrenInOrder node result.children rightBefore rightAfter := by
    refine ⟨?_, hchildKeys, ?_, rfl, ?_, ?_⟩
    · rw [← congrFun hcursors node.id]
      exact horigin
    · intro child hchild hmem
      exact hchildrenFresh child hchild
        (hallocated.symm.mem_iff.mp hmem)
    · simp [rightAfter]
    · intro other hne
      simp [rightAfter, hne]
  have hrightApplies :
      AppliesScalarTransitionResult image node event result
        rightBefore rightAfter := by
    refine ⟨hstate, ?_, ?_, ?_, hrightConsumptions, hrightIncrements⟩
    · simp [rightAfter]
    · intro other hother hid
      have heq : other ≠ node := by
        intro heq
        subst other
        exact hid rfl
      constructor
      · exact (hotherStates other hother hid).1 |>.trans
          (hlocal other hother)
      · simp [rightAfter, heq]
    · exact ⟨rfl, rfl, rfl, rfl⟩
  refine ⟨rightAfter, ?_, ?_⟩
  · refine ⟨by simpa [← hpendingEq] using hevent,
      node, hnode, result, htarget, ?_, ?_, hrightAllocation,
      hrightApplies, ?_, ?_, rfl, rfl⟩
    · rw [← hlocal node hnode]
      exact htransition
    · simpa [← hpendingEq] using hfresh
    · exact descriptorStoreCoherent_of_ownedStoresEquivalent image
        (hafterStores node hnode) hcoherent
    · exact childReferencesAvailable_of_perLPOwnedStoresEquivalent
        image leftAfter rightAfter result.children hafterStores hchildren
  · refine ⟨fun _ _ => rfl, hafterStores, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
    · rw [houtput.1, hsummary]
    · rw [houtput.2.1, hobserved]
    · rw [houtput.2.2.1, hdepartures]
    · rw [houtput.2.2.2, harrivals]
    · rw [hpending, hpendingEq]
    · funext origin
      by_cases heq : origin = node.id
      · subst origin
        simp [rightAfter, hcursorNode, congrFun hcursors node.id]
      · simp [rightAfter, heq, hcursorOther origin heq,
          congrFun hcursors origin]
    · rw [hallocatedAfter]
      exact hallocated.append_right _
    · rw [hemissions]
      exact hemissionsEq.append_right _

end DaysExecutor
