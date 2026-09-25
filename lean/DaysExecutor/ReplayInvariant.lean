import DaysExecutor.ReplayAlgebra
import DaysExecutor.RoundReplay

namespace DaysExecutor

/-!
Preservation of the structural machine invariant by one scalar-style available event step.

The proof is intentionally independent of reachability.  `AvailableEventStep` already contains the
successful transition result and its structural reference validation; the global transition axioms
supply only role, child-key, and descriptor coherence.
-/

theorem canonicalPending_erase
    (event : Event)
    (pending : List Event)
    (hcanonical : CanonicalPending pending) :
    CanonicalPending (pending.erase event) := by
  exact hcanonical.erase event

private theorem mem_installDescriptor
    (candidate inserted : PacketDescriptor)
    (store : List PacketDescriptor) :
    candidate ∈ installDescriptor inserted store →
      candidate = inserted ∨ candidate ∈ store := by
  induction store with
  | nil =>
      simp [installDescriptor]
  | cons head tail ih =>
      simp only [installDescriptor]
      split
      · intro hmem
        exact Or.inr hmem
      · split
        · intro hmem
          rcases List.mem_cons.mp hmem with rfl | hmem
          · exact Or.inl rfl
          · exact Or.inr hmem
        · intro hmem
          rcases List.mem_cons.mp hmem with rfl | hmem
          · exact Or.inr List.mem_cons_self
          · rcases ih hmem with heq | htail
            · exact Or.inl heq
            · exact Or.inr (List.mem_cons_of_mem head htail)

private theorem descriptorListCoherent_install
    (image : SimulationImage State)
    (descriptor : PacketDescriptor)
    (store : List PacketDescriptor)
    (hstore : DescriptorListCoherent image store)
    (hdescriptor :
      descriptor = image.packetDescriptor descriptor.id) :
    DescriptorListCoherent image (installDescriptor descriptor store) := by
  induction store generalizing descriptor with
  | nil =>
      refine ⟨?_, ?_, ?_⟩
      · simp [DescriptorStoreSorted, installDescriptor]
      · simp [installDescriptor]
      · intro candidate hcandidate
        simp [installDescriptor] at hcandidate
        subst candidate
        exact hdescriptor
  | cons head tail ih =>
      rcases hstore with ⟨hsorted, hnodup, horacle⟩
      have hhead := (List.pairwise_cons.mp hsorted).1
      have htail := (List.pairwise_cons.mp hsorted).2
      have hheadOracle := horacle head List.mem_cons_self
      have htailOracle :
          ∀ candidate ∈ tail,
            candidate = image.packetDescriptor candidate.id := by
        intro candidate hcandidate
        exact horacle candidate (List.mem_cons_of_mem head hcandidate)
      simp only [installDescriptor]
      by_cases hid : descriptor.id = head.id
      · simp only [hid, ↓reduceIte]
        exact ⟨hsorted, hnodup, horacle⟩
      · simp only [hid, ↓reduceIte]
        by_cases hle : descriptorLE descriptor head
        · simp only [hle, ↓reduceIte]
          have hdescriptorHead :
              descriptor.id < head.id :=
            Std.lt_of_le_of_ne hle hid
          apply And.intro
          · apply List.pairwise_cons.mpr
            constructor
            · intro candidate hcandidate
              rcases List.mem_cons.mp hcandidate with rfl | hcandidate
              · exact hdescriptorHead
              · have hheadLt := hhead candidate hcandidate
                exact Nat.lt_trans hdescriptorHead hheadLt
            · exact hsorted
          apply And.intro
          · apply List.nodup_cons.mpr
            constructor
            · intro hmemId
              rcases List.mem_cons.mp hmemId with heq | htailMemId
              · exact hid heq
              · rcases List.mem_map.mp htailMemId with
                  ⟨candidate, hcandidate, hcandidateId⟩
                have hheadLtCandidate := hhead candidate hcandidate
                have hheadLtDescriptor : head.id < descriptor.id := by
                  simpa [hcandidateId] using hheadLtCandidate
                exact (Nat.not_lt_of_ge hle) hheadLtDescriptor
            · exact hnodup
          · intro candidate hcandidate
            rcases List.mem_cons.mp hcandidate with rfl | hcandidate
            · exact hdescriptor
            · exact horacle candidate hcandidate
        · simp only [hle, ↓reduceIte]
          have hheadDescriptor : head.id < descriptor.id :=
            Nat.lt_of_not_le hle
          have htailCoherent : DescriptorListCoherent image tail :=
            ⟨htail, (List.nodup_cons.mp hnodup).2, htailOracle⟩
          have hi := ih descriptor htailCoherent hdescriptor
          apply And.intro
          · apply List.pairwise_cons.mpr
            constructor
            · intro candidate hcandidate
              rcases mem_installDescriptor candidate descriptor tail hcandidate with
                rfl | hcandidate
              · exact hheadDescriptor
              · exact hhead candidate hcandidate
            · exact hi.1
          apply And.intro
          · apply List.nodup_cons.mpr
            constructor
            · intro hmem
              rcases List.mem_map.mp hmem with
                ⟨candidate, hcandidate, hcandidateId⟩
              rcases mem_installDescriptor candidate descriptor tail hcandidate with
                heq | hcandidate
              · subst candidate
                exact hid hcandidateId
              · exact (List.nodup_cons.mp hnodup).1
                  (List.mem_map.mpr ⟨candidate, hcandidate, hcandidateId⟩)
            · exact hi.2.1
          · intro candidate hcandidate
            rcases List.mem_cons.mp hcandidate with rfl | hcandidate
            · exact hheadOracle
            · exact hi.2.2 candidate hcandidate

theorem descriptorListCoherent_install_many
    (image : SimulationImage State)
    (descriptors : List PacketDescriptor)
    (store : List PacketDescriptor)
    (hstore : DescriptorListCoherent image store)
    (hdescriptors :
      ∀ descriptor ∈ descriptors,
        descriptor = image.packetDescriptor descriptor.id) :
    DescriptorListCoherent image
      (descriptors.foldl
        (fun current descriptor => installDescriptor descriptor current)
        store) := by
  induction descriptors generalizing store with
  | nil =>
      exact hstore
  | cons head tail ih =>
      simp only [List.foldl_cons]
      apply ih
      · exact descriptorListCoherent_install image head store hstore
          (hdescriptors head List.mem_cons_self)
      · intro descriptor hdescriptor
        exact hdescriptors descriptor (List.mem_cons_of_mem head hdescriptor)

private theorem childrenOriginSequence_member
    (origin : NodeId)
    (next : Nat)
    (children : List Event)
    (hsequence : ChildrenUseOriginSequence origin next children)
    {child : Event}
    (hchild : child ∈ children) :
    child.key.originNode = origin ∧
      child.key.originSeq < next + children.length := by
  induction children generalizing next with
  | nil =>
      simp at hchild
  | cons head tail ih =>
      rcases hsequence with ⟨hheadOrigin, hheadSeq, htail⟩
      simp only [List.mem_cons] at hchild
      rcases hchild with rfl | hchild
      · simp [hheadOrigin, hheadSeq]
      · rcases ih (next + 1) htail hchild with ⟨horigin, hseq⟩
        constructor
        · exact horigin
        · simp only [List.length_cons]
          omega

theorem allocatedKeys_nodup_after
    (children : List Event)
    (before after : MachineState State)
    (hbefore : before.allocatedKeys.Nodup)
    (hallocates : AllocatesChildrenInOrder node children before after) :
    after.allocatedKeys.Nodup := by
  rw [hallocates.2.2.2.1]
  apply List.nodup_append.mpr
  refine ⟨hbefore, hallocates.2.1, ?_⟩
  intro oldKey hold childKey hchildKey heq
  rcases List.mem_map.mp hchildKey with ⟨child, hchild, hkey⟩
  subst childKey
  apply hallocates.2.2.1 child hchild
  rw [hkey]
  exact hold

theorem allocatedKeys_below_cursor_after
    (image : SimulationImage State)
    (node : NodeDescriptor)
    (children : List Event)
    (before after : MachineState State)
    (hnode : node ∈ image.nodes)
    (hbefore :
      ∀ key ∈ before.allocatedKeys,
        key.originSeq < before.nextOriginSeq key.originNode ∧
          ∃ origin ∈ image.nodes, key.originNode = origin.id)
    (hallocates : AllocatesChildrenInOrder node children before after) :
    ∀ key ∈ after.allocatedKeys,
      key.originSeq < after.nextOriginSeq key.originNode ∧
        ∃ origin ∈ image.nodes, key.originNode = origin.id := by
  intro key hkey
  rw [hallocates.2.2.2.1] at hkey
  rcases List.mem_append.mp hkey with hkeyOld | hkeyNew
  · rcases hbefore key hkeyOld with ⟨hseq, origin, horigin, hkeyOrigin⟩
    constructor
    · by_cases heq : key.originNode = node.id
      · rw [heq] at hseq
        rw [heq, hallocates.2.2.2.2.1]
        exact Nat.lt_of_lt_of_le hseq
          (Nat.le_add_right (before.nextOriginSeq node.id) children.length)
      · rw [hallocates.2.2.2.2.2 key.originNode heq]
        exact hseq
    · exact ⟨origin, horigin, hkeyOrigin⟩
  · rcases List.mem_map.mp hkeyNew with ⟨child, hchild, rfl⟩
    rcases childrenOriginSequence_member node.id
        (before.nextOriginSeq node.id) children hallocates.1 hchild with
      ⟨horigin, hseq⟩
    constructor
    · rw [horigin, hallocates.2.2.2.2.1]
      exact hseq
    · exact ⟨node, hnode, horigin⟩

def storeOwnedReferences :
    List PacketStoreEntry → List OwnedPacketReference
  | [] => []
  | entry :: tail =>
      (entry.owners.map fun owner =>
        ({ descriptor := entry.descriptor, owner } :
          OwnedPacketReference)) ++
        storeOwnedReferences tail

theorem mem_storeOwnedReferences
    {reference : OwnedPacketReference}
    {store : List PacketStoreEntry} :
    reference ∈ storeOwnedReferences store ↔
      ∃ entry ∈ store, ∃ owner ∈ entry.owners,
        reference = { descriptor := entry.descriptor, owner } := by
  induction store with
  | nil =>
      simp [storeOwnedReferences]
  | cons head tail ih =>
      simp only [storeOwnedReferences, List.mem_append, List.mem_map, ih,
        List.mem_cons]
      constructor
      · rintro (⟨owner, howner, rfl⟩ | ⟨entry, hentry, owner, howner, heq⟩)
        · exact ⟨head, Or.inl rfl, owner, howner, rfl⟩
        · exact ⟨entry, Or.inr hentry, owner, howner, heq⟩
      · rintro ⟨entry, rfl | hentry, owner, howner, heq⟩
        · exact Or.inl ⟨owner, howner, heq.symm⟩
        · exact Or.inr ⟨entry, hentry, owner, howner, heq⟩

private theorem count_ownedReference_map
    (descriptor : PacketDescriptor)
    (owners : List ReferenceOwner)
    (owner : ReferenceOwner) :
    (owners.map fun candidate =>
      ({ descriptor, owner := candidate } :
        OwnedPacketReference)).count
        ({ descriptor, owner } : OwnedPacketReference) =
      owners.count owner := by
  induction owners with
  | nil =>
      rfl
  | cons head tail ih =>
      by_cases heq : head = owner
      · subst head
        simp only [List.map_cons, List.count_cons_self, ih]
      · have hrefNe :
            ({ descriptor, owner := head } : OwnedPacketReference) ≠
              { descriptor, owner } := by
          intro href
          exact heq (congrArg OwnedPacketReference.owner href)
        simp [List.count_cons, hrefNe, heq, ih]

theorem storeOwnedReferences_count
    (image : SimulationImage State)
    (store : List PacketStoreEntry)
    (reference : OwnedPacketReference)
    (hcoherent : DescriptorStoreCoherent image store)
    (hreference :
      reference.descriptor =
        image.packetDescriptor reference.descriptor.id) :
    (storeOwnedReferences store).count reference =
      ownedReferenceCount reference store := by
  induction store with
  | nil =>
      rfl
  | cons head tail ih =>
      rcases hcoherent with ⟨hsorted, hnodup, hentries⟩
      have hheadEntry := hentries head List.mem_cons_self
      have htailEntries :
          ∀ entry ∈ tail,
            entry.owners ≠ [] ∧
              entry.owners.Nodup ∧
              entry.descriptor =
                image.packetDescriptor entry.descriptor.id := by
        intro entry hentry
        exact hentries entry (List.mem_cons_of_mem head hentry)
      have htailCoherent : DescriptorStoreCoherent image tail :=
        ⟨(List.pairwise_cons.mp hsorted).2,
          (List.nodup_cons.mp hnodup).2, htailEntries⟩
      simp only [storeOwnedReferences, List.count_append,
        ownedReferenceCount]
      by_cases hid : head.descriptor.id = reference.descriptor.id
      · have hdescriptor : head.descriptor = reference.descriptor := by
          rw [hheadEntry.2.2, hreference, hid]
        have htailNotMem :
            reference ∉ storeOwnedReferences tail := by
          intro hmem
          rcases mem_storeOwnedReferences.mp hmem with
            ⟨entry, hentry, owner, howner, heq⟩
          have hentryId : entry.descriptor.id = reference.descriptor.id := by
            rw [heq]
          have hheadFresh :
              head.descriptor.id ∉
                tail.map fun candidate => candidate.descriptor.id :=
            (List.nodup_cons.mp hnodup).1
          apply hheadFresh
          exact List.mem_map.mpr
            ⟨entry, hentry, by simpa [hid] using hentryId⟩
        have htailCount :
            (storeOwnedReferences tail).count reference = 0 :=
          List.count_eq_zero.mpr htailNotMem
        have hmapCount :
            (head.owners.map fun owner =>
              ({ descriptor := head.descriptor, owner } :
                OwnedPacketReference)).count reference =
              head.owners.count reference.owner := by
          have hreferenceStruct :
              reference =
                { descriptor := head.descriptor
                  owner := reference.owner } := by
            rcases reference with ⟨refDescriptor, refOwner⟩
            simp only [OwnedPacketReference.descriptor,
              OwnedPacketReference.owner] at hdescriptor ⊢
            subst refDescriptor
            rfl
          rw [hreferenceStruct]
          exact count_ownedReference_map
            head.descriptor head.owners reference.owner
        simp only [hid, ↓reduceIte, htailCount, Nat.add_zero]
        exact hmapCount
      · have hmapNotMem :
            reference ∉
              head.owners.map fun owner =>
                ({ descriptor := head.descriptor, owner } :
                  OwnedPacketReference) := by
          intro hmem
          rcases List.mem_map.mp hmem with ⟨owner, _, heq⟩
          apply hid
          exact congrArg (fun ref => ref.descriptor.id) heq
        rw [List.count_eq_zero.mpr hmapNotMem, Nat.zero_add]
        simp only [hid, ↓reduceIte]
        exact ih htailCoherent

private theorem ownedReferenceCount_of_entry
    (store : List PacketStoreEntry)
    (hnodup : (store.map fun entry => entry.descriptor.id).Nodup)
    (entry : PacketStoreEntry)
    (hentry : entry ∈ store)
    (owner : ReferenceOwner) :
    ownedReferenceCount
        ({ descriptor := entry.descriptor, owner } :
          OwnedPacketReference)
        store =
      entry.owners.count owner := by
  induction store with
  | nil =>
      simp at hentry
  | cons head tail ih =>
      rcases List.mem_cons.mp hentry with rfl | htail
      · simp [ownedReferenceCount]
      · have hheadNe : head.descriptor.id ≠ entry.descriptor.id := by
          intro heq
          exact (List.nodup_cons.mp hnodup).1
            (List.mem_map.mpr ⟨entry, htail, heq.symm⟩)
        simp only [ownedReferenceCount, hheadNe, ↓reduceIte]
        exact ih (List.nodup_cons.mp hnodup).2 htail

theorem ownedReferencesMatchStore_iff_perm
    (image : SimulationImage State)
    (references : List OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hcoherent : DescriptorStoreCoherent image store)
    (hreferences :
      ∀ reference ∈ references,
        reference.descriptor =
          image.packetDescriptor reference.descriptor.id) :
    OwnedReferencesMatchStore references store ↔
      references.Perm (storeOwnedReferences store) := by
  constructor
  · intro hmatch
    rw [List.perm_iff_count]
    intro reference
    by_cases hmem : reference ∈ references
    · rw [hmatch.1 reference hmem,
        storeOwnedReferences_count image store reference hcoherent
          (hreferences reference hmem)]
    · have hreferenceCount : references.count reference = 0 :=
        List.count_eq_zero.mpr hmem
      have hstoreNotMem : reference ∉ storeOwnedReferences store := by
        intro hstoreMem
        rcases mem_storeOwnedReferences.mp hstoreMem with
          ⟨entry, hentry, owner, howner, heq⟩
        have hownerCount : 0 < entry.owners.count owner :=
          List.count_pos_iff.mpr howner
        have hpositive :
            0 < references.count
              ({ descriptor := entry.descriptor, owner } :
                OwnedPacketReference) := by
          rw [hmatch.2 entry hentry owner howner]
          exact hownerCount
        rw [← heq, hreferenceCount] at hpositive
        omega
      rw [hreferenceCount, List.count_eq_zero.mpr hstoreNotMem]
  · intro hperm
    constructor
    · intro reference hreferenceMem
      rw [hperm.count_eq reference,
        storeOwnedReferences_count image store reference hcoherent
          (hreferences reference hreferenceMem)]
    · intro entry hentry owner howner
      have hentryCoherent := hcoherent.2.2 entry hentry
      rw [hperm.count_eq
        ({ descriptor := entry.descriptor, owner } :
          OwnedPacketReference)]
      rw [storeOwnedReferences_count image store
        ({ descriptor := entry.descriptor, owner } :
          OwnedPacketReference) hcoherent]
      · exact ownedReferenceCount_of_entry store hcoherent.2.1
          entry hentry owner
      · exact hentryCoherent.2.2

private theorem head_descriptor_id_le_of_mem
    (head : PacketStoreEntry)
    (tail : List PacketStoreEntry)
    (hsorted : DescriptorStoreSorted (head :: tail))
    (entry : PacketStoreEntry)
    (hentry : entry ∈ head :: tail) :
    head.descriptor.id ≤ entry.descriptor.id := by
  rcases List.mem_cons.mp hentry with rfl | htail
  · exact Nat.le_refl _
  · exact Nat.le_of_lt ((List.pairwise_cons.mp hsorted).1 entry htail)

private theorem ownedStoresEquivalent_of_storeReferences_perm
    (image : SimulationImage State)
    (left right : List PacketStoreEntry)
    (hleft : DescriptorStoreCoherent image left)
    (hright : DescriptorStoreCoherent image right)
    (hperm :
      (storeOwnedReferences left).Perm
        (storeOwnedReferences right)) :
    OwnedStoresEquivalent left right := by
  induction left generalizing right with
  | nil =>
      cases right with
      | nil =>
          exact .nil
      | cons head tail =>
          have hnonempty := hright.2.2 head List.mem_cons_self |>.1
          obtain ⟨owner, howner⟩ : ∃ owner, owner ∈ head.owners := by
            cases howners : head.owners with
            | nil =>
                exact False.elim (hnonempty howners)
            | cons owner owners =>
                exact ⟨owner, by simp [howners]⟩
          have hmem :
              ({ descriptor := head.descriptor, owner } :
                OwnedPacketReference) ∈
                storeOwnedReferences (head :: tail) :=
            mem_storeOwnedReferences.mpr
              ⟨head, List.mem_cons_self, owner, howner, rfl⟩
          have himpossible := hperm.symm.subset hmem
          simpa [storeOwnedReferences] using himpossible
  | cons leftHead leftTail ih =>
      cases right with
      | nil =>
          have hnonempty := hleft.2.2 leftHead List.mem_cons_self |>.1
          obtain ⟨owner, howner⟩ : ∃ owner, owner ∈ leftHead.owners := by
            cases howners : leftHead.owners with
            | nil =>
                exact False.elim (hnonempty howners)
            | cons owner owners =>
                exact ⟨owner, by simp [howners]⟩
          have hmem :
              ({ descriptor := leftHead.descriptor, owner } :
                OwnedPacketReference) ∈
                storeOwnedReferences (leftHead :: leftTail) :=
            mem_storeOwnedReferences.mpr
              ⟨leftHead, List.mem_cons_self, owner, howner, rfl⟩
          have himpossible := hperm.subset hmem
          simpa [storeOwnedReferences] using himpossible
      | cons rightHead rightTail =>
          have hleftHeadNonempty :=
            (hleft.2.2 leftHead List.mem_cons_self).1
          have hrightHeadNonempty :=
            (hright.2.2 rightHead List.mem_cons_self).1
          obtain ⟨leftOwner, hleftOwner⟩ :
              ∃ owner, owner ∈ leftHead.owners := by
            cases howners : leftHead.owners with
            | nil =>
                exact False.elim (hleftHeadNonempty howners)
            | cons owner owners =>
                exact ⟨owner, by simp [howners]⟩
          obtain ⟨rightOwner, hrightOwner⟩ :
              ∃ owner, owner ∈ rightHead.owners := by
            cases howners : rightHead.owners with
            | nil =>
                exact False.elim (hrightHeadNonempty howners)
            | cons owner owners =>
                exact ⟨owner, by simp [howners]⟩
          have hleftReference :
              ({ descriptor := leftHead.descriptor, owner := leftOwner } :
                OwnedPacketReference) ∈
                storeOwnedReferences (leftHead :: leftTail) :=
            mem_storeOwnedReferences.mpr
              ⟨leftHead, List.mem_cons_self, leftOwner, hleftOwner, rfl⟩
          have hrightReference :
              ({ descriptor := rightHead.descriptor, owner := rightOwner } :
                OwnedPacketReference) ∈
                storeOwnedReferences (rightHead :: rightTail) :=
            mem_storeOwnedReferences.mpr
              ⟨rightHead, List.mem_cons_self, rightOwner, hrightOwner, rfl⟩
          rcases mem_storeOwnedReferences.mp
              (hperm.subset hleftReference) with
            ⟨rightEntry, hrightEntry, _, _, hrightEq⟩
          rcases mem_storeOwnedReferences.mp
              (hperm.symm.subset hrightReference) with
            ⟨leftEntry, hleftEntry, _, _, hleftEq⟩
          have hrightLeLeft :
              rightHead.descriptor.id ≤ leftHead.descriptor.id := by
            have hle := head_descriptor_id_le_of_mem
              rightHead rightTail hright.1 rightEntry hrightEntry
            have hid := congrArg
              (fun reference => reference.descriptor.id) hrightEq
            have hid' :
                leftHead.descriptor.id = rightEntry.descriptor.id := by
              simpa using hid
            rw [← hid'] at hle
            exact hle
          have hleftLeRight :
              leftHead.descriptor.id ≤ rightHead.descriptor.id := by
            have hle := head_descriptor_id_le_of_mem
              leftHead leftTail hleft.1 leftEntry hleftEntry
            have hid := congrArg
              (fun reference => reference.descriptor.id) hleftEq
            have hid' :
                rightHead.descriptor.id = leftEntry.descriptor.id := by
              simpa using hid
            rw [← hid'] at hle
            exact hle
          have hheadId :
              leftHead.descriptor.id = rightHead.descriptor.id :=
            Nat.le_antisymm hleftLeRight hrightLeLeft
          have hheadDescriptor :
              leftHead.descriptor = rightHead.descriptor := by
            rw [(hleft.2.2 leftHead List.mem_cons_self).2.2,
              (hright.2.2 rightHead List.mem_cons_self).2.2, hheadId]
          have howners :
              leftHead.owners.Perm rightHead.owners := by
            rw [List.perm_iff_count]
            intro owner
            let reference : OwnedPacketReference :=
              { descriptor := leftHead.descriptor, owner }
            have hcount := hperm.count_eq reference
            rw [storeOwnedReferences_count image
                (leftHead :: leftTail) reference hleft,
              storeOwnedReferences_count image
                (rightHead :: rightTail) reference hright] at hcount
            · rw [ownedReferenceCount_of_entry
                  (leftHead :: leftTail) hleft.2.1 leftHead
                  List.mem_cons_self owner] at hcount
              have hrightQuery :
                  reference =
                    { descriptor := rightHead.descriptor, owner } := by
                simp [reference, hheadDescriptor]
              rw [hrightQuery,
                ownedReferenceCount_of_entry
                  (rightHead :: rightTail) hright.2.1 rightHead
                  List.mem_cons_self owner] at hcount
              exact hcount
            · exact (hleft.2.2 leftHead List.mem_cons_self).2.2
            · dsimp [reference]
              rw [hheadDescriptor]
              exact (hright.2.2 rightHead List.mem_cons_self).2.2
          have hheadReferencePerm :
              (leftHead.owners.map fun owner =>
                ({ descriptor := leftHead.descriptor, owner } :
                  OwnedPacketReference)).Perm
                (rightHead.owners.map fun owner =>
                  ({ descriptor := rightHead.descriptor, owner } :
                    OwnedPacketReference)) := by
            rw [hheadDescriptor]
            exact howners.map _
          have htailPerm :
              (storeOwnedReferences leftTail).Perm
                (storeOwnedReferences rightTail) := by
            rw [List.perm_iff_count]
            intro reference
            have hall := hperm.count_eq reference
            have hheadCounts := hheadReferencePerm.count_eq reference
            simp only [storeOwnedReferences, List.count_append] at hall
            omega
          have hleftTail : DescriptorStoreCoherent image leftTail :=
            ⟨(List.pairwise_cons.mp hleft.1).2,
              (List.nodup_cons.mp hleft.2.1).2,
              fun entry hentry =>
                hleft.2.2 entry (List.mem_cons_of_mem leftHead hentry)⟩
          have hrightTail : DescriptorStoreCoherent image rightTail :=
            ⟨(List.pairwise_cons.mp hright.1).2,
              (List.nodup_cons.mp hright.2.1).2,
              fun entry hentry =>
                hright.2.2 entry (List.mem_cons_of_mem rightHead hentry)⟩
          exact .cons ⟨hheadDescriptor, howners⟩
            (ih rightTail hleftTail hrightTail htailPerm)

/--
Coherent owner stores are uniquely determined, modulo owner-list order, by the same exact
structural-reference multiset.
-/
theorem ownedStoresEquivalent_of_matching_references
    (image : SimulationImage State)
    (references : List OwnedPacketReference)
    (left right : List PacketStoreEntry)
    (hleftCoherent : DescriptorStoreCoherent image left)
    (hrightCoherent : DescriptorStoreCoherent image right)
    (hreferences :
      ∀ reference ∈ references,
        reference.descriptor =
          image.packetDescriptor reference.descriptor.id)
    (hleftMatch : OwnedReferencesMatchStore references left)
    (hrightMatch : OwnedReferencesMatchStore references right) :
    OwnedStoresEquivalent left right := by
  apply ownedStoresEquivalent_of_storeReferences_perm image
    left right hleftCoherent hrightCoherent
  exact
    ((ownedReferencesMatchStore_iff_perm image references left
      hleftCoherent hreferences).mp hleftMatch).symm.trans
      ((ownedReferencesMatchStore_iff_perm image references right
        hrightCoherent hreferences).mp hrightMatch)

theorem storeOwnedReferences_acquire
    (image : SimulationImage State)
    (reference : OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hcoherent : DescriptorStoreCoherent image store)
    (hreference :
      reference.descriptor =
        image.packetDescriptor reference.descriptor.id) :
    (storeOwnedReferences
        (acquireOwnedReference reference store)).Perm
      (reference :: storeOwnedReferences store) := by
  induction store with
  | nil =>
      exact .refl _
  | cons head tail ih =>
      have hheadCoherent := hcoherent.2.2 head List.mem_cons_self
      have htailCoherent : DescriptorStoreCoherent image tail :=
        ⟨(List.pairwise_cons.mp hcoherent.1).2,
          (List.nodup_cons.mp hcoherent.2.1).2,
          fun entry hentry =>
            hcoherent.2.2 entry (List.mem_cons_of_mem head hentry)⟩
      simp only [acquireOwnedReference]
      by_cases hid : reference.descriptor.id = head.descriptor.id
      · have hdescriptor : reference.descriptor = head.descriptor := by
          rw [hreference, hheadCoherent.2.2, hid]
        have hreferenceEq :
            OwnedPacketReference.mk head.descriptor reference.owner =
              reference := by
          rcases reference with ⟨descriptor, owner⟩
          simp only [OwnedPacketReference.descriptor,
            OwnedPacketReference.owner] at hdescriptor ⊢
          subst descriptor
          rfl
        simp only [hid, ↓reduceIte, storeOwnedReferences, List.map_cons]
        rw [hreferenceEq]
        exact .refl _
      · simp only [hid, ↓reduceIte]
        by_cases hle : descriptorLE reference.descriptor head.descriptor
        · have hreferenceEq :
              OwnedPacketReference.mk
                  reference.descriptor reference.owner =
                reference := by
            cases reference
            rfl
          simp only [hle, ↓reduceIte, storeOwnedReferences, List.map_cons,
            List.map_nil, List.nil_append, hreferenceEq]
          exact .refl _
        · simp only [hle, ↓reduceIte, storeOwnedReferences]
          exact
            ((ih htailCoherent).append_left
              (head.owners.map fun owner =>
                ({ descriptor := head.descriptor, owner } :
                  OwnedPacketReference))).trans
              List.perm_middle

private theorem map_ownedReference_erase
    (descriptor : PacketDescriptor)
    (owner : ReferenceOwner)
    (owners : List ReferenceOwner) :
    ((owners.erase owner).map fun candidate =>
        ({ descriptor, owner := candidate } :
          OwnedPacketReference)) =
      (owners.map fun candidate =>
        ({ descriptor, owner := candidate } :
          OwnedPacketReference)).erase
        ({ descriptor, owner } : OwnedPacketReference) := by
  induction owners with
  | nil =>
      rfl
  | cons head tail ih =>
      by_cases heq : head = owner
      · subst head
        simp
      · have hreferenceNe :
            ({ descriptor, owner := head } : OwnedPacketReference) ≠
              { descriptor, owner } := by
          intro href
          exact heq (congrArg OwnedPacketReference.owner href)
        simp [List.erase_cons, heq, hreferenceNe, ih]

theorem storeOwnedReferences_release
    (image : SimulationImage State)
    (reference : OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hcoherent : DescriptorStoreCoherent image store)
    (hreference :
      reference.descriptor =
        image.packetDescriptor reference.descriptor.id)
    (hheld : reference ∈ storeOwnedReferences store) :
    storeOwnedReferences (releaseOwnedReference reference store) =
      (storeOwnedReferences store).erase reference := by
  induction store with
  | nil =>
      simp [storeOwnedReferences] at hheld
  | cons head tail ih =>
      have hheadCoherent := hcoherent.2.2 head List.mem_cons_self
      have htailCoherent : DescriptorStoreCoherent image tail :=
        ⟨(List.pairwise_cons.mp hcoherent.1).2,
          (List.nodup_cons.mp hcoherent.2.1).2,
          fun entry hentry =>
            hcoherent.2.2 entry (List.mem_cons_of_mem head hentry)⟩
      by_cases hid : head.descriptor.id = reference.descriptor.id
      · have hdescriptor : head.descriptor = reference.descriptor := by
          rw [hheadCoherent.2.2, hreference, hid]
        have hownerMem : reference.owner ∈ head.owners := by
          rcases mem_storeOwnedReferences.mp hheld with
            ⟨entry, hentry, owner, howner, heq⟩
          have hentryId : entry.descriptor.id = head.descriptor.id := by
            have := congrArg
              (fun candidate => candidate.descriptor.id) heq
            simpa [hid] using this.symm
          rcases List.mem_cons.mp hentry with rfl | htailEntry
          · have hownerEq := congrArg OwnedPacketReference.owner heq
            simpa [hownerEq] using howner
          · have hheadFresh :=
              (List.nodup_cons.mp hcoherent.2.1).1
            exact False.elim (hheadFresh
              (List.mem_map.mpr ⟨entry, htailEntry, hentryId⟩))
        have hreferenceEq :
            OwnedPacketReference.mk head.descriptor reference.owner =
              reference := by
          rcases reference with ⟨descriptor, owner⟩
          simp only [OwnedPacketReference.descriptor,
            OwnedPacketReference.owner] at hdescriptor ⊢
          subst descriptor
          rfl
        rw [← hreferenceEq]
        have hmapErase :=
          map_ownedReference_erase
            head.descriptor reference.owner head.owners
        simp only [releaseOwnedReference, ↓reduceIte]
        by_cases hempty :
            (head.owners.erase reference.owner).isEmpty = true
        · have hremaining :
              head.owners.erase reference.owner = [] :=
            List.isEmpty_iff.mp hempty
          simp only [hempty, ↓reduceIte, storeOwnedReferences]
          rw [List.erase_append_left _]
          · rw [← hmapErase, hremaining]
            rfl
          · exact List.mem_map.mpr
              ⟨reference.owner, hownerMem, rfl⟩
        · simp only [hempty, Bool.false_eq_true, ↓reduceIte,
            storeOwnedReferences]
          rw [List.erase_append_left _]
          · rw [← hmapErase]
          · exact List.mem_map.mpr
              ⟨reference.owner, hownerMem, rfl⟩
      · have hheadNotMem :
            reference ∉
              head.owners.map fun owner =>
                ({ descriptor := head.descriptor, owner } :
                  OwnedPacketReference) := by
          intro hmem
          rcases List.mem_map.mp hmem with ⟨owner, _, heq⟩
          apply hid
          have := congrArg (fun candidate => candidate.descriptor.id) heq
          simpa using this
        have htailHeld : reference ∈ storeOwnedReferences tail := by
          rw [storeOwnedReferences, List.mem_append] at hheld
          exact hheld.resolve_left hheadNotMem
        simp only [releaseOwnedReference, hid, ↓reduceIte,
          storeOwnedReferences]
        rw [List.erase_append_right _ hheadNotMem]
        exact congrArg
          ((head.owners.map fun owner =>
            ({ descriptor := head.descriptor, owner } :
              OwnedPacketReference)) ++ ·)
          (ih htailCoherent htailHeld)

end DaysExecutor
