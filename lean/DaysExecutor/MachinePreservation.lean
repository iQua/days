import DaysExecutor.ReplayDiamond
import DaysExecutor.ReplayInvariant

namespace DaysExecutor

/-- Releasing an exact owner preserves canonical descriptor-store structure. -/
theorem descriptorStoreCoherent_release
    (image : SimulationImage State)
    (reference : OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hcoherent : DescriptorStoreCoherent image store) :
    DescriptorStoreCoherent image (releaseOwnedReference reference store) := by
  induction store with
  | nil =>
      exact hcoherent
  | cons head tail ih =>
      have hheadCoherent :=
        hcoherent.2.2 head List.mem_cons_self
      have htailCoherent : DescriptorStoreCoherent image tail :=
        ⟨(List.pairwise_cons.mp hcoherent.1).2,
          (List.nodup_cons.mp hcoherent.2.1).2,
          fun entry hentry =>
            hcoherent.2.2 entry (List.mem_cons_of_mem head hentry)⟩
      by_cases hid : head.descriptor.id = reference.descriptor.id
      · let remaining := head.owners.erase reference.owner
        by_cases hempty : remaining.isEmpty
        · simpa [releaseOwnedReference, hid, remaining, hempty] using
            htailCoherent
        · have hremaining : remaining ≠ [] := by
            exact fun heq => by
              rw [heq] at hempty
              simp at hempty
          simp only [releaseOwnedReference, hid, remaining, hempty,
            Bool.false_eq_true, ↓reduceIte]
          refine ⟨by
              simpa [releaseOwnedReference, hid, remaining, hempty] using
                (releaseOwnedReference_preserves_sorted reference
                  (head :: tail) hcoherent.1),
            ?_, ?_⟩
          · simpa only [List.map_cons] using hcoherent.2.1
          · intro entry hentry
            rcases List.mem_cons.mp hentry with rfl | htail
            · exact ⟨hremaining,
                (List.Nodup.erase _ (hcoherent.2.2 head
                  List.mem_cons_self).2.1),
                (hcoherent.2.2 head List.mem_cons_self).2.2⟩
            · exact htailCoherent.2.2 entry htail
      · simp only [releaseOwnedReference, hid, ↓reduceIte]
        have htailAfter := ih htailCoherent
        refine ⟨by
            simpa [releaseOwnedReference, hid] using
              (releaseOwnedReference_preserves_sorted reference
                (head :: tail) hcoherent.1),
          ?_, ?_⟩
        · apply List.nodup_cons.mpr
          constructor
          · intro hmem
            rcases List.mem_map.mp hmem with ⟨entry, hentry, heq⟩
            have ⟨existing, hexisting, hdescriptor⟩ :=
              mem_releaseOwnedReference_cases entry reference tail hentry
            apply (List.nodup_cons.mp hcoherent.2.1).1
            exact List.mem_map.mpr
              ⟨existing, hexisting, by simpa [hdescriptor] using heq⟩
          · exact htailAfter.2.1
        · intro entry hentry
          rcases List.mem_cons.mp hentry with rfl | htail
          · exact hheadCoherent
          · exact htailAfter.2.2 entry htail

/--
Acquiring a previously absent exact owner with its oracle descriptor preserves canonical store
structure.
-/
theorem descriptorStoreCoherent_acquire
    (image : SimulationImage State)
    (reference : OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hcoherent : DescriptorStoreCoherent image store)
    (horacle :
      reference.descriptor =
        image.packetDescriptor reference.descriptor.id)
    (habsent : ownedReferenceCount reference store = 0) :
    DescriptorStoreCoherent image (acquireOwnedReference reference store) := by
  induction store with
  | nil =>
      refine ⟨?_, ?_, ?_⟩
      · simp [DescriptorStoreSorted, acquireOwnedReference]
      · simp [acquireOwnedReference]
      · intro entry hentry
        simp [acquireOwnedReference] at hentry
        subst entry
        exact ⟨by simp, by simp, horacle⟩
  | cons head tail ih =>
      have hhead := hcoherent.2.2 head List.mem_cons_self
      have htailCoherent : DescriptorStoreCoherent image tail :=
        ⟨(List.pairwise_cons.mp hcoherent.1).2,
          (List.nodup_cons.mp hcoherent.2.1).2,
          fun entry hentry =>
            hcoherent.2.2 entry (List.mem_cons_of_mem head hentry)⟩
      by_cases hid : reference.descriptor.id = head.descriptor.id
      · have hdescriptor : reference.descriptor = head.descriptor := by
          rw [horacle, hhead.2.2, hid]
        have hownerAbsent : reference.owner ∉ head.owners := by
          apply List.count_eq_zero.mp
          simpa [ownedReferenceCount, hid.symm] using habsent
        simp only [acquireOwnedReference, hid, ↓reduceIte]
        refine ⟨by
            simpa [acquireOwnedReference, hid] using
              (acquireOwnedReference_preserves_sorted reference
                (head :: tail) hcoherent.1),
          ?_, ?_⟩
        · simpa only [List.map_cons] using hcoherent.2.1
        · intro entry hentry
          rcases List.mem_cons.mp hentry with rfl | htail
          · exact ⟨by simp,
              List.nodup_cons.mpr ⟨hownerAbsent, hhead.2.1⟩,
              hhead.2.2⟩
          · exact htailCoherent.2.2 entry htail
      · simp only [acquireOwnedReference, hid, ↓reduceIte]
        by_cases hle : descriptorLE reference.descriptor head.descriptor
        · simp only [hle, ↓reduceIte]
          refine ⟨by
              simpa [acquireOwnedReference, hid, hle] using
                (acquireOwnedReference_preserves_sorted reference
                  (head :: tail) hcoherent.1),
            ?_, ?_⟩
          · apply List.nodup_cons.mpr
            constructor
            · intro hmem
              rcases List.mem_map.mp hmem with ⟨entry, hentry, heq⟩
              rcases List.mem_cons.mp hentry with rfl | htail
              · exact hid heq.symm
              · have hheadLtEntry :=
                  (List.pairwise_cons.mp hcoherent.1).1 entry htail
                have hrefLtEntry :
                    reference.descriptor.id < entry.descriptor.id :=
                  Nat.lt_of_le_of_lt hle hheadLtEntry
                exact (Nat.ne_of_lt hrefLtEntry) heq.symm
            · exact hcoherent.2.1
          · intro entry hentry
            rcases List.mem_cons.mp hentry with rfl | htail
            · exact ⟨by simp, by simp, horacle⟩
            · exact hcoherent.2.2 entry htail
        · simp only [hle, ↓reduceIte]
          have htailAbsent :
              ownedReferenceCount reference tail = 0 := by
            simpa [ownedReferenceCount, Ne.symm hid] using habsent
          have htailAfter := ih htailCoherent htailAbsent
          refine ⟨by
              simpa [acquireOwnedReference, hid, hle] using
                (acquireOwnedReference_preserves_sorted reference
                  (head :: tail) hcoherent.1),
            ?_, ?_⟩
          · apply List.nodup_cons.mpr
            constructor
            · intro hmem
              rcases List.mem_map.mp hmem with ⟨entry, hentry, heq⟩
              rcases mem_acquireOwnedReference_cases entry reference tail hentry with
                hnew | ⟨existing, hexisting, hold⟩
              · exact hid (by simpa [hnew] using heq)
              · apply (List.nodup_cons.mp hcoherent.2.1).1
                exact List.mem_map.mpr
                  ⟨existing, hexisting, by simpa [hold] using heq⟩
            · exact htailAfter.2.1
          · intro entry hentry
            rcases List.mem_cons.mp hentry with rfl | htail
            · exact hhead
            · exact htailAfter.2.2 entry htail

/-- Flattening a coherent store into exact owners produces no duplicate owner reference. -/
theorem storeOwnedReferences_nodup
    (image : SimulationImage State)
    (store : List PacketStoreEntry)
    (hcoherent : DescriptorStoreCoherent image store) :
    (storeOwnedReferences store).Nodup := by
  induction store with
  | nil =>
      exact List.nodup_nil
  | cons head tail ih =>
      have htailCoherent : DescriptorStoreCoherent image tail :=
        ⟨(List.pairwise_cons.mp hcoherent.1).2,
          (List.nodup_cons.mp hcoherent.2.1).2,
          fun entry hentry =>
            hcoherent.2.2 entry (List.mem_cons_of_mem head hentry)⟩
      unfold storeOwnedReferences
      rw [List.nodup_append]
      refine ⟨?_, ih htailCoherent, ?_⟩
      · exact (hcoherent.2.2 head List.mem_cons_self).2.1.map
          (fun owner => ({ descriptor := head.descriptor, owner } :
            OwnedPacketReference))
          (by
            intro left right hne heq
            exact hne (congrArg OwnedPacketReference.owner heq))
      · intro left hleft right hright heq
        subst right
        rcases List.mem_map.mp hleft with ⟨owner, _, rfl⟩
        rcases mem_storeOwnedReferences.mp hright with
          ⟨entry, hentry, otherOwner, _, href⟩
        have hid :=
          congrArg (fun candidate : OwnedPacketReference =>
            candidate.descriptor.id) href
        exact (List.nodup_cons.mp hcoherent.2.1).1
          (List.mem_map.mpr ⟨entry, hentry, by simpa using hid.symm⟩)

/-- Exact structural owners of a well-formed machine are duplicate-free at each LP. -/
theorem machineOwnedReferencesFor_nodup
    (image : SimulationImage State)
    (machine : MachineState State)
    (hwellFormed : MachineWellFormed image machine)
    (node : NodeDescriptor)
    (hnode : node ∈ image.nodes)
    (horacle : DescriptorOracleWellFormed image) :
    (machineOwnedReferencesFor image machine node).Nodup := by
  have hperm :=
    (ownedReferencesMatchStore_iff_perm image
      (machineOwnedReferencesFor image machine node)
      (machine.packetStore node)
      (hwellFormed.2.2.2.2.1 node hnode)
      (machineOwnedReferencesFor_oracle image horacle machine node)).mp
      (hwellFormed.2.2.2.2.2.1 node hnode)
  exact hperm.nodup_iff.mpr
    (storeOwnedReferences_nodup image (machine.packetStore node)
      (hwellFormed.2.2.2.2.1 node hnode))

/-- Bag subtraction removes the requested multiplicity, capped at the source count. -/
theorem count_listBagDifference
    [BEq α] [LawfulBEq α]
    (source removed : List α)
    (item : α) :
    (listBagDifference source removed).count item =
      source.count item - removed.count item := by
  induction removed generalizing source with
  | nil =>
      simp [listBagDifference]
  | cons head tail ih =>
      simp only [listBagDifference, List.foldl_cons]
      change
        (listBagDifference (source.erase head) tail).count item =
          source.count item - (head :: tail).count item
      rw [ih]
      by_cases heq : head = item
      · subst head
        simp
        omega
      · have hcountErase :
            (source.erase head).count item = source.count item := by
          exact List.count_erase_of_ne (Ne.symm heq)
        rw [hcountErase]
        simp [heq]

/-- Bag subtraction depends only on the source and removed multisets. -/
theorem listBagDifference_perm
    [BEq α] [LawfulBEq α]
    {leftSource rightSource leftRemoved rightRemoved : List α}
    (hsource : leftSource.Perm rightSource)
    (hremoved : leftRemoved.Perm rightRemoved) :
    (listBagDifference leftSource leftRemoved).Perm
      (listBagDifference rightSource rightRemoved) := by
  rw [List.perm_iff_count]
  intro item
  rw [count_listBagDifference, count_listBagDifference,
    hsource.count_eq item, hremoved.count_eq item]

/-- Removing the old-only bag and adding the new-only bag reconstructs the new multiset. -/
theorem bag_reconcile_perm
    [BEq α] [LawfulBEq α]
    (old new : List α) :
    (listBagDifference old (listBagDifference old new) ++
      listBagDifference new old).Perm new := by
  rw [List.perm_iff_count]
  intro item
  rw [List.count_append, count_listBagDifference,
    count_listBagDifference, count_listBagDifference]
  omega

/-- A structurally held release batch preserves coherence and removes exactly that owner bag. -/
theorem releaseOwnedReferences_preserves
    (image : SimulationImage State)
    (references : List OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hcoherent : DescriptorStoreCoherent image store)
    (horacle :
      ∀ reference ∈ references,
        reference.descriptor =
          image.packetDescriptor reference.descriptor.id)
    (hheld : PacketReferencesHeld references store) :
    let after :=
      references.foldl
        (fun current reference => releaseOwnedReference reference current)
        store
    DescriptorStoreCoherent image after ∧
      storeOwnedReferences after =
        listBagDifference (storeOwnedReferences store) references := by
  induction references generalizing store with
  | nil =>
      exact ⟨hcoherent, rfl⟩
  | cons head tail ih =>
      have hheadOracle := horacle head List.mem_cons_self
      have hheadPositive :
          0 < ownedReferenceCount head store :=
        ownedReferenceCount_positive_of_held
          (head :: tail) store head hheld List.mem_cons_self
      have hheadStoreMem :
          head ∈ storeOwnedReferences store := by
        apply List.count_pos_iff.mp
        rw [storeOwnedReferences_count image store head hcoherent hheadOracle]
        exact hheadPositive
      have hrelease :=
        storeOwnedReferences_release image head store hcoherent
          hheadOracle hheadStoreMem
      have hafterCoherent :=
        descriptorStoreCoherent_release image head store hcoherent
      have htailHeld :
          PacketReferencesHeld tail (releaseOwnedReference head store) := by
        intro reference hreference
        have hreferenceOracle :=
          horacle reference (List.mem_cons_of_mem head hreference)
        have hall := hheld reference (List.mem_cons_of_mem head hreference)
        rw [← storeOwnedReferences_count image
          (releaseOwnedReference head store) reference
          hafterCoherent hreferenceOracle]
        rw [hrelease]
        rw [← storeOwnedReferences_count image store reference
          hcoherent hreferenceOracle] at hall
        by_cases heq : head = reference
        · subst head
          simp at hall ⊢
          omega
        · rw [List.count_erase_of_ne (Ne.symm heq)]
          simpa [heq] using hall
      have htailOracle :
          ∀ reference ∈ tail,
            reference.descriptor =
              image.packetDescriptor reference.descriptor.id := by
        intro reference hreference
        exact horacle reference (List.mem_cons_of_mem head hreference)
      have hrest :=
        ih (releaseOwnedReference head store)
          hafterCoherent htailOracle htailHeld
      simp only [List.foldl_cons]
      refine ⟨hrest.1, ?_⟩
      rw [hrest.2, hrelease]
      rfl

/--
A duplicate-free batch of previously absent oracle owners preserves coherence and adds exactly
that owner bag.
-/
theorem acquireOwnedReferences_preserves
    (image : SimulationImage State)
    (references : List OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hcoherent : DescriptorStoreCoherent image store)
    (hnodup : references.Nodup)
    (horacle :
      ∀ reference ∈ references,
        reference.descriptor =
          image.packetDescriptor reference.descriptor.id)
    (habsent :
      ∀ reference ∈ references,
        ownedReferenceCount reference store = 0) :
    let after :=
      references.foldl
        (fun current reference => acquireOwnedReference reference current)
        store
    DescriptorStoreCoherent image after ∧
      (storeOwnedReferences after).Perm
        (references ++ storeOwnedReferences store) := by
  induction references generalizing store with
  | nil =>
      exact ⟨hcoherent, List.Perm.refl _⟩
  | cons head tail ih =>
      have hheadOracle := horacle head List.mem_cons_self
      have hheadAbsent := habsent head List.mem_cons_self
      have hafterCoherent :=
        descriptorStoreCoherent_acquire image head store
          hcoherent hheadOracle hheadAbsent
      have hheadPerm :=
        storeOwnedReferences_acquire image head store hcoherent hheadOracle
      have htailNodup := (List.nodup_cons.mp hnodup).2
      have htailOracle :
          ∀ reference ∈ tail,
            reference.descriptor =
              image.packetDescriptor reference.descriptor.id := by
        intro reference hreference
        exact horacle reference (List.mem_cons_of_mem head hreference)
      have htailAbsent :
          ∀ reference ∈ tail,
            ownedReferenceCount reference
              (acquireOwnedReference head store) = 0 := by
        intro reference hreference
        have hreferenceOracle := htailOracle reference hreference
        rw [← storeOwnedReferences_count image
          (acquireOwnedReference head store) reference
          hafterCoherent hreferenceOracle]
        rw [hheadPerm.count_eq reference]
        have hne : head ≠ reference := by
          intro heq
          subst reference
          exact (List.nodup_cons.mp hnodup).1 hreference
        have holdZero := habsent reference
          (List.mem_cons_of_mem head hreference)
        rw [← storeOwnedReferences_count image store reference
          hcoherent hreferenceOracle] at holdZero
        simp [hne, holdZero]
      have hrest :=
        ih (acquireOwnedReference head store)
          hafterCoherent htailNodup htailOracle htailAbsent
      simp only [List.foldl_cons]
      refine ⟨hrest.1, ?_⟩
      exact hrest.2.trans
        ((hheadPerm.append_left tail).trans List.perm_middle)

/-- Mapping commutes with erasing an item when its image is unique in the list. -/
theorem map_erase_of_pairwise_ne
    [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    (f : α → β)
    (item : α)
    (items : List α)
    (hpairwise : items.Pairwise fun left right => f left ≠ f right)
    (hmem : item ∈ items) :
    (items.erase item).map f =
      (items.map f).erase (f item) := by
  induction items with
  | nil =>
      simp at hmem
  | cons head tail ih =>
      have htailPairwise := (List.pairwise_cons.mp hpairwise).2
      by_cases heq : item = head
      · subst item
        simp
      · have htailMem : item ∈ tail := by
          simpa [heq] using hmem
        have hfne : f item ≠ f head := by
          exact Ne.symm ((List.pairwise_cons.mp hpairwise).1 item htailMem)
        simp [List.erase_cons, Ne.symm heq, Ne.symm hfne,
          ih htailPairwise htailMem]

/-- Canonical pending events map injectively to their pending-event owner references. -/
theorem pending_ownedEventReference_pairwise
    (image : SimulationImage State)
    (pending : List Event)
    (hcanonical : CanonicalPending pending) :
    pending.Pairwise fun left right =>
      ownedEventReference image left ≠ ownedEventReference image right := by
  apply hcanonical.imp
  intro left right hlt heq
  show False
  ·
    have howner :=
      congrArg OwnedPacketReference.owner heq
    have hkey : left.key = right.key := by
      simpa [ownedEventReference] using howner
    exact (EventKey.lt_irrefl left.key) (by simpa [hkey] using hlt)

/-- Pending owners assigned to one LP. -/
def pendingOwnedReferencesFor
    (image : SimulationImage State)
    (target : NodeId)
    (pending : List Event) : List OwnedPacketReference :=
  (pending.filter fun event => event.target = target).map
    (ownedEventReference image)

/--
Immediate scalar child insertion adds the target's child owners and removes the processed owner
exactly at its processing LP.
-/
theorem pendingOwnedReferencesFor_insertEvents_erase
    (image : SimulationImage State)
    (target : NodeId)
    (before children : List Event)
    (event : Event)
    (hcanonical : CanonicalPending before)
    (hevent : event ∈ before) :
    (pendingOwnedReferencesFor image target
      (insertEvents children (before.erase event))).Perm
      ((pendingOwnedReferencesFor image target children) ++
        if event.target = target then
          (pendingOwnedReferencesFor image target before).erase
            (ownedEventReference image event)
        else
          pendingOwnedReferencesFor image target before) := by
  have hinsert :=
    (insertEvents_perm_append children (before.erase event)).filter
      (fun candidate => candidate.target = target)
  have hmapped := hinsert.map (ownedEventReference image)
  unfold pendingOwnedReferencesFor
  rw [List.filter_append, List.map_append] at hmapped
  by_cases htarget : event.target = target
  · simp only [htarget, ↓reduceIte]
    have hfilteredPairwise :
        (before.filter fun candidate => candidate.target = target).Pairwise
          fun left right =>
            ownedEventReference image left ≠
              ownedEventReference image right :=
      (pending_ownedEventReference_pairwise image before hcanonical).filter _
    have hfilteredMem :
        event ∈ before.filter fun candidate => candidate.target = target :=
      List.mem_filter.mpr
        ⟨hevent, by simpa only [decide_eq_true_eq] using htarget⟩
    have hmapErase :=
      map_erase_of_pairwise_ne (ownedEventReference image) event
        (before.filter fun candidate => candidate.target = target)
        hfilteredPairwise hfilteredMem
    rw [← List.erase_filter, hmapErase] at hmapped
    exact hmapped
  · simp only [htarget, ↓reduceIte]
    have hnotFiltered :
        event ∉ before.filter fun candidate => candidate.target = target := by
      intro hmem
      exact htarget
        (of_decide_eq_true (List.mem_filter.mp hmem).2)
    have hfilterErase :
        (before.erase event).filter
            (fun candidate => candidate.target = target) =
          before.filter fun candidate => candidate.target = target := by
      rw [← List.erase_filter,
        List.erase_eq_self_iff.mpr hnotFiltered]
    rw [hfilterErase] at hmapped
    exact hmapped

/-- Acquiring an owner never decreases any exact-owner multiplicity in a sorted store. -/
theorem ownedReferenceCount_le_acquire
    (inserted query : OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hsorted : DescriptorStoreSorted store) :
    ownedReferenceCount query store ≤
      ownedReferenceCount query (acquireOwnedReference inserted store) := by
  induction store with
  | nil =>
      simp [ownedReferenceCount, acquireOwnedReference]
  | cons head tail ih =>
      have htailSorted := (List.pairwise_cons.mp hsorted).2
      simp only [acquireOwnedReference]
      by_cases hid : inserted.descriptor.id = head.descriptor.id
      · simp only [hid, ↓reduceIte, ownedReferenceCount]
        by_cases hquery : head.descriptor.id = query.descriptor.id
        · simp only [hquery, ↓reduceIte, List.count_cons]
          split <;> omega
        · simp [hquery]
      · simp only [hid, ↓reduceIte]
        by_cases hle : descriptorLE inserted.descriptor head.descriptor
        · simp only [hle, ↓reduceIte, ownedReferenceCount]
          by_cases hqueryInserted :
              inserted.descriptor.id = query.descriptor.id
          · have hlt :
                inserted.descriptor.id < head.descriptor.id :=
              Std.lt_of_le_of_ne hle hid
            have hheadNe :
                head.descriptor.id ≠ query.descriptor.id := by
              intro heq
              apply hid
              exact hqueryInserted.trans heq.symm
            have hzero :
                ownedReferenceCount query (head :: tail) = 0 := by
              simp only [ownedReferenceCount, hheadNe, ↓reduceIte]
              apply Nat.eq_zero_of_not_pos
              intro hpositive
              rcases exists_payload_of_ownedReferenceCount_positive
                  query tail hpositive with
                ⟨entry, hentry, hentryId⟩
              have hheadLt := (List.pairwise_cons.mp hsorted).1 entry hentry
              rw [hentryId, ← hqueryInserted] at hheadLt
              exact (Nat.not_lt_of_ge (Nat.le_of_lt hlt)) hheadLt
            have htailZero :
                ownedReferenceCount query tail = 0 := by
              simpa only [ownedReferenceCount, hheadNe, ↓reduceIte] using
                hzero
            simp [hqueryInserted, hheadNe, htailZero]
          · simp only [hqueryInserted, ↓reduceIte]
            by_cases hqueryHead :
                head.descriptor.id = query.descriptor.id
            · simp [hqueryHead]
            · simp only [hqueryHead, ↓reduceIte]
              exact Nat.le_refl _
        · simp only [hle, ↓reduceIte, ownedReferenceCount]
          by_cases hqueryHead :
              head.descriptor.id = query.descriptor.id
          · simp [hqueryHead]
          · simp only [hqueryHead, ↓reduceIte]
            exact ih htailSorted

/-- Acquiring an exact owner increases its own multiplicity by one in a sorted store. -/
theorem ownedReferenceCount_acquire_self
    (reference : OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hsorted : DescriptorStoreSorted store) :
    ownedReferenceCount reference
        (acquireOwnedReference reference store) =
      ownedReferenceCount reference store + 1 := by
  induction store with
  | nil =>
      simp [ownedReferenceCount, acquireOwnedReference]
  | cons head tail ih =>
      have hhead := (List.pairwise_cons.mp hsorted).1
      have htailSorted := (List.pairwise_cons.mp hsorted).2
      simp only [acquireOwnedReference]
      by_cases hid : reference.descriptor.id = head.descriptor.id
      · simp only [ownedReferenceCount, hid.symm, ↓reduceIte,
          List.count_cons_self]
      · simp only [hid, ↓reduceIte]
        by_cases hle : descriptorLE reference.descriptor head.descriptor
        · have hlt :
              reference.descriptor.id < head.descriptor.id :=
            Std.lt_of_le_of_ne hle hid
          have hbefore :
              ownedReferenceCount reference (head :: tail) = 0 := by
            simp only [ownedReferenceCount, Ne.symm hid, ↓reduceIte]
            apply Nat.eq_zero_of_not_pos
            intro hpositive
            rcases exists_payload_of_ownedReferenceCount_positive
                reference tail hpositive with
              ⟨entry, hentry, hentryId⟩
            have hheadLt := hhead entry hentry
            rw [hentryId] at hheadLt
            exact (Nat.not_lt_of_ge (Nat.le_of_lt hlt)) hheadLt
          have htailZero :
              ownedReferenceCount reference tail = 0 := by
            simpa only [ownedReferenceCount, Ne.symm hid, ↓reduceIte] using
              hbefore
          simp [hle, ownedReferenceCount, Ne.symm hid, htailZero]
        · simp only [hle, ↓reduceIte, ownedReferenceCount, Ne.symm hid,
            ↓reduceIte]
          exact ih htailSorted

/-- Every exact owner in a coherent store has multiplicity at most one. -/
theorem ownedReferenceCount_le_one_of_coherent
    (image : SimulationImage State)
    (reference : OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hcoherent : DescriptorStoreCoherent image store) :
    ownedReferenceCount reference store ≤ 1 := by
  induction store with
  | nil =>
      simp [ownedReferenceCount]
  | cons head tail ih =>
      have htailCoherent : DescriptorStoreCoherent image tail :=
        ⟨(List.pairwise_cons.mp hcoherent.1).2,
          (List.nodup_cons.mp hcoherent.2.1).2,
          fun entry hentry =>
            hcoherent.2.2 entry (List.mem_cons_of_mem head hentry)⟩
      simp only [ownedReferenceCount]
      by_cases hid : head.descriptor.id = reference.descriptor.id
      · simp only [hid, ↓reduceIte]
        exact (List.nodup_iff_count.mp
          (hcoherent.2.2 head List.mem_cons_self).2.1) reference.owner
      · simp only [hid, ↓reduceIte]
        exact ih htailCoherent

/-- A fold of acquisitions is monotone for every exact-owner multiplicity. -/
theorem ownedReferenceCount_le_acquire_fold
    (references : List OwnedPacketReference)
    (query : OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hsorted : DescriptorStoreSorted store) :
    ownedReferenceCount query store ≤
      ownedReferenceCount query
        (references.foldl
          (fun current reference => acquireOwnedReference reference current)
          store) := by
  induction references generalizing store with
  | nil =>
      exact Nat.le_refl _
  | cons head tail ih =>
      simp only [List.foldl_cons]
      exact Nat.le_trans
        (ownedReferenceCount_le_acquire head query store hsorted)
        (ih (acquireOwnedReference head store)
          (acquireOwnedReference_preserves_sorted head store hsorted))

/-- Acquiring a listed owner raises its final multiplicity by at least one. -/
theorem ownedReferenceCount_add_one_le_acquire_fold_of_mem
    (references : List OwnedPacketReference)
    (query : OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hsorted : DescriptorStoreSorted store)
    (hmem : query ∈ references) :
    ownedReferenceCount query store + 1 ≤
      ownedReferenceCount query
        (references.foldl
          (fun current reference => acquireOwnedReference reference current)
          store) := by
  induction references generalizing store with
  | nil =>
      simp at hmem
  | cons head tail ih =>
      simp only [List.mem_cons] at hmem
      rcases hmem with rfl | htail
      · simp only [List.foldl_cons]
        rw [← ownedReferenceCount_acquire_self query store hsorted]
        exact ownedReferenceCount_le_acquire_fold tail query
          (acquireOwnedReference query store)
          (acquireOwnedReference_preserves_sorted query store hsorted)
      · simp only [List.foldl_cons]
        have hone :=
          ih (acquireOwnedReference head store)
            (acquireOwnedReference_preserves_sorted head store hsorted)
            htail
        exact Nat.le_trans
          (Nat.add_le_add_right
            (ownedReferenceCount_le_acquire head query store hsorted) 1)
          hone

/--
If a complete acquisition fold is coherent, its acquired owner list was duplicate-free and every
acquired owner was absent initially.
-/
theorem acquisition_batch_fresh_of_final_coherent
    (image : SimulationImage State)
    (references : List OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hcoherent : DescriptorStoreCoherent image store)
    (horacle :
      ∀ reference ∈ references,
        reference.descriptor =
          image.packetDescriptor reference.descriptor.id)
    (hfinal :
      DescriptorStoreCoherent image
        (references.foldl
          (fun current reference => acquireOwnedReference reference current)
          store)) :
    references.Nodup ∧
      ∀ reference ∈ references,
        ownedReferenceCount reference store = 0 := by
  constructor
  · induction references generalizing store with
    | nil =>
        exact List.nodup_nil
    | cons head tail ih =>
        simp only [List.foldl_cons] at hfinal
        have hheadOracle := horacle head List.mem_cons_self
        have hraised :=
          ownedReferenceCount_add_one_le_acquire_fold_of_mem
            (head :: tail) head store hcoherent.1 List.mem_cons_self
        simp only [List.foldl_cons] at hraised
        have hleOne :=
          ownedReferenceCount_le_one_of_coherent image head _ hfinal
        have hheadAbsent :
            ownedReferenceCount head store = 0 := by
          omega
        have hafterCoherent :=
          descriptorStoreCoherent_acquire image head store
            hcoherent hheadOracle hheadAbsent
        have htailOracle :
            ∀ reference ∈ tail,
              reference.descriptor =
                image.packetDescriptor reference.descriptor.id := by
          intro reference hreference
          exact horacle reference (List.mem_cons_of_mem head hreference)
        apply List.nodup_cons.mpr
        constructor
        · intro hheadTail
          have hsecond :=
            ownedReferenceCount_add_one_le_acquire_fold_of_mem
              tail head (acquireOwnedReference head store)
              hafterCoherent.1 hheadTail
          have hfirst :=
            ownedReferenceCount_acquire_self head store hcoherent.1
          omega
        · exact ih (acquireOwnedReference head store)
            hafterCoherent htailOracle hfinal
  · intro reference hreference
    have hraised :=
      ownedReferenceCount_add_one_le_acquire_fold_of_mem
        references reference store hcoherent.1 hreference
    have hleOne :=
      ownedReferenceCount_le_one_of_coherent image reference _ hfinal
    omega

/-- A coherent completed acquisition fold has the exact acquired-plus-old owner multiset. -/
theorem acquireOwnedReferences_exact_of_final_coherent
    (image : SimulationImage State)
    (references : List OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hcoherent : DescriptorStoreCoherent image store)
    (horacle :
      ∀ reference ∈ references,
        reference.descriptor =
          image.packetDescriptor reference.descriptor.id)
    (hfinal :
      DescriptorStoreCoherent image
        (references.foldl
          (fun current reference => acquireOwnedReference reference current)
          store)) :
    (storeOwnedReferences
      (references.foldl
        (fun current reference => acquireOwnedReference reference current)
        store)).Perm
      (references ++ storeOwnedReferences store) := by
  have hfresh :=
    acquisition_batch_fresh_of_final_coherent image references store
      hcoherent horacle hfinal
  exact (acquireOwnedReferences_preserves image references store
    hcoherent hfresh.1 horacle hfresh.2).2

/-- Structural pending/state owner reconciliation for the processing LP. -/
theorem processing_owner_bag_reconcile
    (pending oldState newState children : List OwnedPacketReference)
    (processed : OwnedPacketReference)
    (hnodup : (pending ++ oldState).Nodup)
    (hprocessed : processed ∈ pending)
    (hprocessedNew : processed ∉ newState) :
    (children ++ pending.erase processed ++ newState).Perm
      ((listBagDifference newState oldState ++ children) ++
        listBagDifference (pending ++ oldState)
          (processed :: listBagDifference oldState newState)) := by
  rw [List.perm_iff_count]
  intro reference
  have hpendingNodup := (List.nodup_append.mp hnodup).1
  have holdNodup := (List.nodup_append.mp hnodup).2.1
  have hdisjoint := (List.nodup_append.mp hnodup).2.2
  have hside :
      pending.count reference = 0 ∨ oldState.count reference = 0 := by
    by_cases hpending : reference ∈ pending
    · right
      apply List.count_eq_zero.mpr
      intro hold
      exact hdisjoint reference hpending reference hold rfl
    · exact Or.inl (List.count_eq_zero.mpr hpending)
  have hprocessedPending :
      pending.count processed = 1 := by
    rw [hpendingNodup.count, if_pos hprocessed]
  have hprocessedOld :
      oldState.count processed = 0 := by
    apply List.count_eq_zero.mpr
    intro hold
    exact hdisjoint processed hprocessed processed hold rfl
  simp only [List.count_append, count_listBagDifference,
    List.count_cons]
  by_cases heq : reference = processed
  · subst reference
    have hnewZero :
        newState.count processed = 0 :=
      List.count_eq_zero.mpr hprocessedNew
    simp [hprocessedPending, hprocessedOld, hnewZero]
  · have herase :
        (pending.erase processed).count reference =
          pending.count reference :=
      List.count_erase_of_ne heq
    rw [herase]
    have hprocessedNe : processed ≠ reference := Ne.symm heq
    simp [hprocessedNe]
    rcases hside with hzero | hzero <;> omega

/-- Scalar target-child installation is exactly acquisition of the target-filtered owner list. -/
theorem installChildDescriptorsFor_eq_acquire_fold
    (image : SimulationImage State)
    (target : NodeId)
    (children : List Event)
    (store : List PacketStoreEntry) :
    installChildDescriptorsFor image target children store =
      (pendingOwnedReferencesFor image target children).foldl
        (fun current reference => acquireOwnedReference reference current)
        store := by
  induction children generalizing store with
  | nil =>
      rfl
  | cons child tail ih =>
      simp only [installChildDescriptorsFor, List.foldl_cons,
        pendingOwnedReferencesFor, List.filter_cons]
      by_cases htarget : child.target = target
      · simpa [htarget, installChildDescriptorsFor] using
          ih (acquireOwnedReference (ownedEventReference image child) store)
      · simpa [htarget, installChildDescriptorsFor] using ih store

/-- A projection with duplicate-free keys has duplicate-free values when values retain the key. -/
theorem map_nodup_of_key_nodup
    (items : List α)
    (key : α → κ)
    (value : α → β)
    (hkeys : (items.map key).Nodup)
    (hretains :
      ∀ left right, value left = value right → key left = key right) :
    (items.map value).Nodup := by
  induction items with
  | nil =>
      exact List.nodup_nil
  | cons head tail ih =>
      apply List.nodup_cons.mpr
      constructor
      · intro hmem
        rcases List.mem_map.mp hmem with ⟨item, hitem, heq⟩
        have hkeyEq := hretains head item heq.symm
        exact (List.nodup_cons.mp hkeys).1
          (List.mem_map.mpr ⟨item, hitem, hkeyEq.symm⟩)
      · exact ih (List.nodup_cons.mp hkeys).2

/-- Fresh child keys make every target-filtered pending owner absent from the old target store. -/
theorem pendingChildReferences_fresh_for_target
    (image : SimulationImage State)
    (machine : MachineState State)
    (hwellFormed : MachineWellFormed image machine)
    (horacle : DescriptorOracleWellFormed image)
    (target : NodeDescriptor)
    (htarget : target ∈ image.nodes)
    (children : List Event)
    (hkeys : (children.map Event.key).Nodup)
    (hfresh :
      ∀ child ∈ children, child.key ∉ machine.allocatedKeys) :
    let references :=
      pendingOwnedReferencesFor image target.id children
    references.Nodup ∧
      (∀ reference ∈ references,
        reference.descriptor =
          image.packetDescriptor reference.descriptor.id) ∧
      ∀ reference ∈ references,
        ownedReferenceCount reference (machine.packetStore target) = 0 := by
  let selected :=
    children.filter fun child => child.target = target.id
  have hselectedKeys : (selected.map Event.key).Nodup := by
    exact (List.filter_sublist.map Event.key).nodup hkeys
  have hreferencesNodup :
      (selected.map (ownedEventReference image)).Nodup := by
    apply map_nodup_of_key_nodup selected Event.key
      (ownedEventReference image) hselectedKeys
    intro left right heq
    have howner := congrArg OwnedPacketReference.owner heq
    simpa [ownedEventReference] using howner
  have hreferenceOracle :
      ∀ reference ∈ selected.map (ownedEventReference image),
        reference.descriptor =
          image.packetDescriptor reference.descriptor.id := by
    intro reference hreference
    rcases List.mem_map.mp hreference with ⟨child, _, rfl⟩
    unfold ownedEventReference
    rw [(horacle _).1]
  have habsent :
      ∀ reference ∈ selected.map (ownedEventReference image),
        ownedReferenceCount reference (machine.packetStore target) = 0 := by
    intro reference hreference
    rcases List.mem_map.mp hreference with
      ⟨child, hchildSelected, rfl⟩
    have hchild := (List.mem_filter.mp hchildSelected).1
    have hchildFresh := hfresh child hchild
    have hnotStructural :
        ownedEventReference image child ∉
          machineOwnedReferencesFor image machine target := by
      intro hmem
      unfold machineOwnedReferencesFor at hmem
      rcases List.mem_append.mp hmem with hpending | hstate
      · rcases List.mem_map.mp hpending with
          ⟨oldEvent, holdFiltered, heq⟩
        have howner := congrArg OwnedPacketReference.owner heq
        have hkey : oldEvent.key = child.key := by
          simpa [ownedEventReference] using howner
        apply hchildFresh
        rw [← hkey]
        exact (hwellFormed.2.2.2.1 oldEvent
          (List.mem_filter.mp holdFiltered).1).1
      · unfold ownedRoleStateReferences at hstate
        rcases List.mem_append.mp hstate with hqueue | hservice
        · rcases List.mem_map.mp hqueue with ⟨payload, _, heq⟩
          have howner := congrArg OwnedPacketReference.owner heq
          simp [ownedEventReference, ownedQueueReference] at howner
        · rcases List.mem_map.mp hservice with ⟨payload, _, heq⟩
          have howner := congrArg OwnedPacketReference.owner heq
          simp [ownedEventReference, ownedInServiceReference] at howner
    have hstructuralCount :
        (machineOwnedReferencesFor image machine target).count
          (ownedEventReference image child) = 0 :=
      List.count_eq_zero.mpr hnotStructural
    have hperm :=
      (ownedReferencesMatchStore_iff_perm image
        (machineOwnedReferencesFor image machine target)
        (machine.packetStore target)
        (hwellFormed.2.2.2.2.1 target htarget)
        (machineOwnedReferencesFor_oracle image horacle machine target)).mp
        (hwellFormed.2.2.2.2.2.1 target htarget)
    rw [hperm.count_eq (ownedEventReference image child)] at hstructuralCount
    rw [← storeOwnedReferences_count image
      (machine.packetStore target) (ownedEventReference image child)
      (hwellFormed.2.2.2.2.1 target htarget)]
    · exact hstructuralCount
    · unfold ownedEventReference
      rw [(horacle _).1]
  change
    (selected.map (ownedEventReference image)).Nodup ∧
      (∀ reference ∈ selected.map (ownedEventReference image),
        reference.descriptor =
          image.packetDescriptor reference.descriptor.id) ∧
      ∀ reference ∈ selected.map (ownedEventReference image),
        ownedReferenceCount reference (machine.packetStore target) = 0
  exact ⟨hreferencesNodup, hreferenceOracle, habsent⟩

end DaysExecutor
