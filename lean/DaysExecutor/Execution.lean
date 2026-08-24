import DaysExecutor.Transition

namespace DaysExecutor

/--
Semantic machine configuration shared by serial and round execution, corresponding to the complete
state and pending-event result assembled at `executor/src/scalar.rs:575-602` and
`executor/src/cpu.rs:3372-3433`.

`nextOriginSeq`, `allocatedKeys`, and `emissions` are proof-only ghosts. The remaining fields
project every Rust `RunResult` component, including descriptor data that crosses LPs.
-/
structure MachineState (State : StateFamily) where
  localState : (node : NodeDescriptor) → RoleState State node.kind
  packetStore : NodeDescriptor → List PacketStoreEntry
  pending : List Event
  summary : RunSummary
  observedPackets : List PacketDescriptor
  departures : List RecordedDeparture
  arrivals : List RecordedArrival
  nextOriginSeq : NodeId → Nat
  allocatedKeys : List EventKey
  emissions : List (Event × Event)

/--
Canonical insertion into an `EventKey`-ordered future-event list, modeling Rust's `BTreeMap`
insertion at `executor/src/scalar.rs:352-355`.
-/
def insertEvent (event : Event) : List Event → List Event
  | [] => [event]
  | head :: tail =>
      if event.key ≤ head.key then
        event :: head :: tail
      else
        head :: insertEvent event tail

/--
Canonical insertion of deterministic child emissions, modeling the scalar child loop at
`executor/src/scalar.rs:351-356`.
-/
def insertEvents (children pending : List Event) : List Event :=
  children.foldl (fun queue child => insertEvent child queue) pending

/--
Canonical pending-event normalization used for initial images and comparison, corresponding to the
ordered scalar queue built at `executor/src/scalar.rs:1714-1723`.
-/
def canonicalizeEvents (events : List Event) : List Event :=
  insertEvents events []

/--
Generated child keys are fresh relative to the remaining future-event list, matching the duplicate
diagnostic at `executor/src/scalar.rs:352-355`.
-/
def FreshEventKeys (children existing : List Event) : Prop :=
  ∀ child ∈ children, ∀ pending ∈ existing, child.key ≠ pending.key

/--
Payload ordering used only to normalize descriptor-valued public result fields after Rust forgets
resident counts at `executor/src/scalar.rs:577-590`.
-/
def descriptorLE (left right : PacketDescriptor) : Prop :=
  left.id ≤ right.id

/-- Decidability for payload-keyed descriptor normalization. -/
instance (left right : PacketDescriptor) : Decidable (descriptorLE left right) := by
  unfold descriptorLE
  infer_instance

/--
Insert one immutable descriptor into a descriptor-only public result projection. Resident reference
acquisition is modeled separately below. Rust assembles descriptor-valued result maps at
`executor/src/scalar.rs:577-590` and `executor/src/cpu.rs:3383-3426`.
-/
def installDescriptor (descriptor : PacketDescriptor) : List PacketDescriptor → List PacketDescriptor
  | [] => [descriptor]
  | head :: tail =>
      if descriptor.id = head.id then
        head :: tail
      else if descriptorLE descriptor head then
        descriptor :: head :: tail
      else
        head :: installDescriptor descriptor tail

/--
An owned descriptor store is in canonical payload order, has one oracle descriptor per payload,
and records a nonempty duplicate-free owner multiset for every resident payload. The owner list
order is intentionally non-semantic; exact holder multiplicity is observed through
`ownedReferenceCount`.
-/
def DescriptorStoreCoherent
    (image : SimulationImage State)
    (store : List PacketStoreEntry) : Prop :=
  DescriptorStoreSorted store ∧
    (store.map fun entry => entry.descriptor.id).Nodup ∧
      ∀ entry ∈ store,
        entry.owners ≠ [] ∧
          entry.owners.Nodup ∧
          entry.descriptor = image.packetDescriptor entry.descriptor.id

/--
Acquire one exact owner, inserting its immutable descriptor entry when necessary. Rust creates
pending-event and envelope owners at `executor/src/cpu.rs:649-667`; queue and in-service owners
are established by the corresponding handler state transition in
`executor/src/scalar.rs:869-892,999-1015,1105-1138,1473-1511`.
-/
def acquireOwnedReference
    (reference : OwnedPacketReference) : List PacketStoreEntry → List PacketStoreEntry
  | [] => [{ descriptor := reference.descriptor, owners := [reference.owner] }]
  | head :: tail =>
      if reference.descriptor.id = head.descriptor.id then
        { head with owners := reference.owner :: head.owners } :: tail
      else if descriptorLE reference.descriptor head.descriptor then
        { descriptor := reference.descriptor, owners := [reference.owner] } :: head :: tail
      else
        head :: acquireOwnedReference reference tail

/--
Release one exact owner and drop the payload entry exactly when its final owner disappears.
Releasing an absent owner is a total no-op; structural effect validity makes that branch
unreachable in semantic steps.
-/
def releaseOwnedReference
    (reference : OwnedPacketReference) : List PacketStoreEntry → List PacketStoreEntry
  | [] => []
  | head :: tail =>
      if head.descriptor.id = reference.descriptor.id then
        let remaining := head.owners.erase reference.owner
        if remaining.isEmpty then tail else { head with owners := remaining } :: tail
      else
        head :: releaseOwnedReference reference tail

/-- Entry-wise equivalence that retains exact descriptors and owner multiplicities. -/
def PacketStoreEntry.OwnershipEquivalent
    (left right : PacketStoreEntry) : Prop :=
  left.descriptor = right.descriptor ∧ left.owners.Perm right.owners

/-- Store equivalence modulo non-semantic ordering of each payload's owners. -/
inductive OwnedStoresEquivalent :
    List PacketStoreEntry → List PacketStoreEntry → Prop
  | nil : OwnedStoresEquivalent [] []
  | cons
      (head : PacketStoreEntry.OwnershipEquivalent left right)
      (tail : OwnedStoresEquivalent lefts rights) :
      OwnedStoresEquivalent (left :: lefts) (right :: rights)

/-- Exact store equality implies owned-store equivalence. -/
theorem ownedStoresEquivalent_refl (store : List PacketStoreEntry) :
    OwnedStoresEquivalent store store := by
  induction store with
  | nil => exact .nil
  | cons head tail ih =>
      exact .cons ⟨rfl, List.Perm.refl _⟩ ih

/--
Two acquisitions for the same immutable descriptor commute modulo owner-list permutation. This is
the owned lift of the round-9 increment commutation lemma.
-/
theorem acquireOwnedReference_commutes
    (left right : OwnedPacketReference)
    (hdescriptor : left.descriptor = right.descriptor)
    (store : List PacketStoreEntry) :
    OwnedStoresEquivalent
      (acquireOwnedReference left (acquireOwnedReference right store))
      (acquireOwnedReference right (acquireOwnedReference left store)) := by
  rcases left with ⟨descriptor, leftOwner⟩
  rcases right with ⟨rightDescriptor, rightOwner⟩
  simp only at hdescriptor
  subst rightDescriptor
  induction store with
  | nil =>
      simp only [acquireOwnedReference]
      apply OwnedStoresEquivalent.cons
      · exact ⟨rfl, List.Perm.swap rightOwner leftOwner []⟩
      · exact .nil
  | cons head tail ih =>
      by_cases hhead : descriptor.id = head.descriptor.id
      · simp only [acquireOwnedReference, hhead, ↓reduceIte]
        apply OwnedStoresEquivalent.cons
        · exact
            ⟨rfl, List.Perm.swap rightOwner leftOwner head.owners⟩
        · exact ownedStoresEquivalent_refl tail
      · by_cases hle : descriptorLE descriptor head.descriptor
        · simp only [acquireOwnedReference, hhead, hle, ↓reduceIte]
          apply OwnedStoresEquivalent.cons
          · exact ⟨rfl, List.Perm.swap rightOwner leftOwner []⟩
          · exact ownedStoresEquivalent_refl (head :: tail)
        · simp only [acquireOwnedReference, hhead, hle, ↓reduceIte]
          apply OwnedStoresEquivalent.cons
          · exact ⟨rfl, List.Perm.refl _⟩
          · exact ih

/-- Acquisitions of distinct payloads commute as exact canonical-store updates. -/
theorem acquireOwnedReference_commutes_of_distinct_payload
    (left right : OwnedPacketReference)
    (hne : left.descriptor.id ≠ right.descriptor.id)
    (store : List PacketStoreEntry) :
    acquireOwnedReference left (acquireOwnedReference right store) =
      acquireOwnedReference right (acquireOwnedReference left store) := by
  rcases Nat.lt_trichotomy left.descriptor.id right.descriptor.id with hlt | heq | hgt
  · have hne' : right.descriptor.id ≠ left.descriptor.id := Nat.ne_of_gt hlt
    have hle : left.descriptor.id ≤ right.descriptor.id := Nat.le_of_lt hlt
    have hnle : ¬ right.descriptor.id ≤ left.descriptor.id := Nat.not_le_of_gt hlt
    induction store with
    | nil =>
        simp [acquireOwnedReference, descriptorLE, hne, hne', hle, hnle]
    | cons head tail ih =>
        rcases Nat.lt_trichotomy left.descriptor.id head.descriptor.id with hlh | hlh | hlh
        <;> rcases Nat.lt_trichotomy right.descriptor.id head.descriptor.id with hrh | hrh | hrh
        <;> simp_all! +arith [acquireOwnedReference, descriptorLE,
          Nat.ne_of_lt, Nat.ne_of_gt, Nat.le_of_lt, Nat.not_le_of_gt]
        <;> omega
  · exact False.elim (hne heq)
  · have hne' : right.descriptor.id ≠ left.descriptor.id := Nat.ne_of_lt hgt
    have hle : right.descriptor.id ≤ left.descriptor.id := Nat.le_of_lt hgt
    have hnle : ¬ left.descriptor.id ≤ right.descriptor.id := Nat.not_le_of_gt hgt
    induction store with
    | nil =>
        simp [acquireOwnedReference, descriptorLE, hne, hne', hle, hnle]
    | cons head tail ih =>
        rcases Nat.lt_trichotomy left.descriptor.id head.descriptor.id with hlh | hlh | hlh
        <;> rcases Nat.lt_trichotomy right.descriptor.id head.descriptor.id with hrh | hrh | hrh
        <;> simp_all! +arith [acquireOwnedReference, descriptorLE,
          Nat.ne_of_lt, Nat.ne_of_gt, Nat.le_of_lt, Nat.not_le_of_gt]
        <;> omega

/-- Two exact-owner releases commute within one payload's owner multiset. -/
theorem releaseOwnedOwners_commutes
    (left right : ReferenceOwner)
    (owners : List ReferenceOwner) :
    (owners.erase left).erase right =
      (owners.erase right).erase left := by
  exact List.erase_comm left right

/-- Releases of owners belonging to distinct payloads commute as exact store updates. -/
theorem releaseOwnedReference_commutes_of_distinct_payload
    (left right : OwnedPacketReference)
    (hne : left.descriptor.id ≠ right.descriptor.id)
    (store : List PacketStoreEntry) :
    releaseOwnedReference left (releaseOwnedReference right store) =
      releaseOwnedReference right (releaseOwnedReference left store) := by
  induction store with
  | nil => rfl
  | cons head tail ih =>
      by_cases hl : head.descriptor.id = left.descriptor.id
      <;> by_cases hr : head.descriptor.id = right.descriptor.id
      <;> simp_all only [releaseOwnedReference, ↓reduceIte]
      <;> split <;> simp_all [releaseOwnedReference]

/--
Acquisition commutes with release of a different owner. Equal owners are deliberately excluded:
their acquire/release order is sequenced by that owner's lifecycle.
-/
theorem acquire_releaseOwnedOwners_commutes_of_ne
    (acquired released : ReferenceOwner)
    (howner : acquired ≠ released)
    (owners : List ReferenceOwner) :
    (acquired :: owners).erase released =
      acquired :: owners.erase released := by
  simp [howner]

/--
The same owner is not commuted: a fresh acquisition followed by that owner's release is exactly
one lifecycle segment.
-/
theorem acquire_releaseOwnedOwner_lifecycle
    (owner : ReferenceOwner)
    (owners : List ReferenceOwner) :
    (owner :: owners).erase owner = owners := by
  exact List.erase_cons_head owner owners

/--
Releasing two distinct owners of the same payload commutes whenever both owners are present. The
presence premises rule out the malformed duplicate-payload case where dropping one entry could
expose a second entry with the same payload.
-/
theorem releaseOwnedReference_commutes_same_payload
    (left right : OwnedPacketReference)
    (hpayload : left.descriptor.id = right.descriptor.id)
    (howner : left.owner ≠ right.owner)
    (store : List PacketStoreEntry)
    (hleft : 0 < ownedReferenceCount left store)
    (hright : 0 < ownedReferenceCount right store) :
    releaseOwnedReference left (releaseOwnedReference right store) =
      releaseOwnedReference right (releaseOwnedReference left store) := by
  induction store with
  | nil =>
      simp [ownedReferenceCount] at hleft
  | cons head tail ih =>
      by_cases hl : head.descriptor.id = left.descriptor.id
      · have hr : head.descriptor.id = right.descriptor.id := hl.trans hpayload
        have hleftMem : left.owner ∈ head.owners := by
          apply List.count_pos_iff.mp
          simpa [ownedReferenceCount, hl] using hleft
        have hrightMem : right.owner ∈ head.owners := by
          apply List.count_pos_iff.mp
          simpa [ownedReferenceCount, hr] using hright
        have hafterLeft : (head.owners.erase left.owner).isEmpty = false := by
          apply List.isEmpty_eq_false_iff.mpr
          intro hempty
          have hmem : right.owner ∈ head.owners.erase left.owner :=
            (List.mem_erase_of_ne (Ne.symm howner)).mpr hrightMem
          rw [hempty] at hmem
          exact (List.not_mem_nil (a := right.owner)) hmem
        have hafterRight : (head.owners.erase right.owner).isEmpty = false := by
          apply List.isEmpty_eq_false_iff.mpr
          intro hempty
          have hmem : left.owner ∈ head.owners.erase right.owner :=
            (List.mem_erase_of_ne howner).mpr hleftMem
          rw [hempty] at hmem
          exact (List.not_mem_nil (a := left.owner)) hmem
        simp only [releaseOwnedReference, hl, hpayload, hafterLeft, hafterRight,
          Bool.false_eq_true, ↓reduceIte]
        rw [releaseOwnedOwners_commutes]
      · have hr : head.descriptor.id ≠ right.descriptor.id := by
          intro heq
          exact hl (heq.trans hpayload.symm)
        simp only [releaseOwnedReference, hl, hr, ↓reduceIte]
        apply congrArg (head :: ·)
        apply ih
        · simpa [ownedReferenceCount, hl] using hleft
        · simpa [ownedReferenceCount, hr] using hright

/-- Acquiring a payload before all current entries inserts its owner at the store head. -/
private theorem acquireOwnedReference_before
    (reference : OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hbefore : ∀ entry ∈ store,
      reference.descriptor.id < entry.descriptor.id) :
    acquireOwnedReference reference store =
      { descriptor := reference.descriptor, owners := [reference.owner] } :: store := by
  cases store with
  | nil => rfl
  | cons head tail =>
      have hlt := hbefore head List.mem_cons_self
      have hne : reference.descriptor.id ≠ head.descriptor.id := Nat.ne_of_lt hlt
      have hle : descriptorLE reference.descriptor head.descriptor := Nat.le_of_lt hlt
      simp [acquireOwnedReference, hne, hle]

/-- A positive exact-owner count identifies a payload entry in the store. -/
theorem exists_payload_of_ownedReferenceCount_positive
    (reference : OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hpositive : 0 < ownedReferenceCount reference store) :
    ∃ entry ∈ store, entry.descriptor.id = reference.descriptor.id := by
  induction store with
  | nil =>
      simp [ownedReferenceCount] at hpositive
  | cons head tail ih =>
      by_cases hid : head.descriptor.id = reference.descriptor.id
      · exact ⟨head, List.mem_cons_self, hid⟩
      · rcases ih (by simpa [ownedReferenceCount, hid] using hpositive) with
          ⟨entry, hentry, heq⟩
        exact ⟨entry, List.mem_cons_of_mem _ hentry, heq⟩

/--
Acquisition commutes with release of a different owner, including the final-owner case where
release deletes the payload entry and acquisition canonically recreates it.
-/
theorem acquire_releaseOwnedReference_commutes_of_distinct_owner
    (acquired released : OwnedPacketReference)
    (hdescriptor : acquired.descriptor = released.descriptor)
    (howner : acquired.owner ≠ released.owner)
    (store : List PacketStoreEntry)
    (hsorted : DescriptorStoreSorted store)
    (hdescriptors : ∀ entry ∈ store,
      entry.descriptor.id = acquired.descriptor.id →
        entry.descriptor = acquired.descriptor)
    (hheld : 0 < ownedReferenceCount released store) :
    releaseOwnedReference released (acquireOwnedReference acquired store) =
      acquireOwnedReference acquired (releaseOwnedReference released store) := by
  have hdescriptorId :
      acquired.descriptor.id = released.descriptor.id :=
    congrArg PacketDescriptor.id hdescriptor
  induction store with
  | nil =>
      simp [ownedReferenceCount] at hheld
  | cons head tail ih =>
      have hhead := (List.pairwise_cons.mp hsorted).1
      have htail := (List.pairwise_cons.mp hsorted).2
      by_cases hid : head.descriptor.id = acquired.descriptor.id
      · have hreleasedId : head.descriptor.id = released.descriptor.id :=
          hid.trans hdescriptorId
        have hheadDescriptor : head.descriptor = acquired.descriptor :=
          hdescriptors head List.mem_cons_self hid
        have hreleasedMem : released.owner ∈ head.owners := by
          apply List.count_pos_iff.mp
          simpa [ownedReferenceCount, hreleasedId] using hheld
        have hremaining :
            (acquired.owner :: head.owners).erase released.owner =
              acquired.owner :: head.owners.erase released.owner :=
          acquire_releaseOwnedOwners_commutes_of_ne _ _ howner _
        have hnotEmpty :
            (acquired.owner :: head.owners.erase released.owner).isEmpty = false := by
          simp
        have hbefore : ∀ entry ∈ tail,
            acquired.descriptor.id < entry.descriptor.id := by
          intro entry hentry
          simpa [hid] using hhead entry hentry
        by_cases hempty : (head.owners.erase released.owner).isEmpty = true
        · have heraseNil : head.owners.erase released.owner = [] :=
            List.isEmpty_iff.mp hempty
          have hreinsert := acquireOwnedReference_before acquired tail hbefore
          simp only [acquireOwnedReference, releaseOwnedReference, hdescriptorId,
            hheadDescriptor, hremaining, hnotEmpty, hempty,
            Bool.false_eq_true, ↓reduceIte]
          simpa [heraseNil] using hreinsert.symm
        · simp [acquireOwnedReference, releaseOwnedReference, hdescriptorId,
            hheadDescriptor, hremaining, hnotEmpty, hempty]
      · have hreleasedNe : head.descriptor.id ≠ released.descriptor.id := by
          intro heq
          exact hid (heq.trans hdescriptorId.symm)
        have htailHeld : 0 < ownedReferenceCount released tail := by
          simpa [ownedReferenceCount, hreleasedNe] using hheld
        rcases exists_payload_of_ownedReferenceCount_positive released tail htailHeld with
          ⟨entry, hentry, hentryId⟩
        have hlt : head.descriptor.id < acquired.descriptor.id := by
          simpa [hentryId, hdescriptorId] using hhead entry hentry
        have hacquiredNe : acquired.descriptor.id ≠ head.descriptor.id := Nat.ne_of_gt hlt
        have hnle : ¬ descriptorLE acquired.descriptor head.descriptor :=
          Nat.not_le_of_gt hlt
        simp only [acquireOwnedReference, releaseOwnedReference, hreleasedNe,
          hacquiredNe, hnle, ↓reduceIte]
        apply congrArg (head :: ·)
        apply ih
        · exact htail
        · intro candidate hcandidate
          exact hdescriptors candidate (List.mem_cons_of_mem _ hcandidate)
        · exact htailHeld

/-- Every acquired entry is either the new descriptor or preserves an existing descriptor. -/
theorem mem_acquireOwnedReference_cases
    (candidate : PacketStoreEntry)
    (reference : OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hmem : candidate ∈ acquireOwnedReference reference store) :
    candidate.descriptor = reference.descriptor ∨
      ∃ existing ∈ store, candidate.descriptor = existing.descriptor := by
  induction store with
  | nil =>
      simp [acquireOwnedReference] at hmem
      subst candidate
      exact Or.inl rfl
  | cons head tail ih =>
      simp only [acquireOwnedReference] at hmem
      split at hmem
      next =>
        rcases List.mem_cons.mp hmem with rfl | hmem
        · exact Or.inr ⟨head, List.mem_cons_self, rfl⟩
        · exact Or.inr ⟨_, List.mem_cons_of_mem _ hmem, rfl⟩
      next =>
        split at hmem
        next =>
          rcases List.mem_cons.mp hmem with rfl | hmem
          · exact Or.inl rfl
          · exact Or.inr ⟨_, hmem, rfl⟩
        next =>
          rcases List.mem_cons.mp hmem with rfl | hmem
          · exact Or.inr ⟨_, List.mem_cons_self, rfl⟩
          · rcases ih hmem with hnew | ⟨existing, hexisting, heq⟩
            · exact Or.inl hnew
            · exact Or.inr ⟨existing, List.mem_cons_of_mem _ hexisting, heq⟩

/-- Owner acquisition preserves the derived store's strict payload ordering. -/
theorem acquireOwnedReference_preserves_sorted
    (reference : OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hsorted : DescriptorStoreSorted store) :
    DescriptorStoreSorted (acquireOwnedReference reference store) := by
  induction store with
  | nil =>
      simp [DescriptorStoreSorted, acquireOwnedReference]
  | cons head tail ih =>
      have hhead := (List.pairwise_cons.mp hsorted).1
      have htail := (List.pairwise_cons.mp hsorted).2
      simp only [acquireOwnedReference]
      split
      next =>
        simpa [DescriptorStoreSorted] using hsorted
      next hne =>
        split
        next hle =>
          apply List.pairwise_cons.mpr
          constructor
          · intro current hmem
            rcases List.mem_cons.mp hmem with hcurrent | hmem
            · subst current
              change reference.descriptor.id < head.descriptor.id
              change reference.descriptor.id ≤ head.descriptor.id at hle
              exact Std.lt_of_le_of_ne hle hne
            · have hheadCurrent := hhead current hmem
              change reference.descriptor.id ≤ head.descriptor.id at hle
              change head.descriptor.id < current.descriptor.id at hheadCurrent
              exact Nat.lt_trans (Std.lt_of_le_of_ne hle hne) hheadCurrent
          · exact hsorted
        next hnle =>
          apply List.pairwise_cons.mpr
          constructor
          · intro current hmem
            rcases mem_acquireOwnedReference_cases current reference tail hmem with
              hcurrent | ⟨existing, hexisting, hcurrent⟩
            · rw [hcurrent]
              change head.descriptor.id < reference.descriptor.id
              exact Nat.lt_of_not_le hnle
            · rw [hcurrent]
              exact hhead existing hexisting
          · exact ih htail

/-- Every released entry preserves the descriptor of an existing entry. -/
theorem mem_releaseOwnedReference_cases
    (candidate : PacketStoreEntry)
    (reference : OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hmem : candidate ∈ releaseOwnedReference reference store) :
    ∃ existing ∈ store, candidate.descriptor = existing.descriptor := by
  induction store with
  | nil =>
      simp [releaseOwnedReference] at hmem
  | cons first rest ih =>
      by_cases hid : first.descriptor.id = reference.descriptor.id
      · let remaining := first.owners.erase reference.owner
        by_cases hempty : remaining.isEmpty
        · simp [releaseOwnedReference, hid, remaining, hempty] at hmem
          exact ⟨candidate, List.mem_cons_of_mem _ hmem, rfl⟩
        · simp [releaseOwnedReference, hid, remaining, hempty] at hmem
          rcases hmem with hfirst | hrest
          · subst candidate
            exact ⟨first, List.mem_cons_self, rfl⟩
          · exact ⟨candidate, List.mem_cons_of_mem _ hrest, rfl⟩
      · simp [releaseOwnedReference, hid] at hmem
        rcases hmem with hfirst | hrest
        · subst candidate
          exact ⟨first, List.mem_cons_self, rfl⟩
        · rcases ih hrest with ⟨existing, hexisting, heq⟩
          exact ⟨existing, List.mem_cons_of_mem _ hexisting, heq⟩

/-- Exact-owner release preserves the derived store's strict payload ordering. -/
theorem releaseOwnedReference_preserves_sorted
    (reference : OwnedPacketReference)
    (store : List PacketStoreEntry)
    (hsorted : DescriptorStoreSorted store) :
    DescriptorStoreSorted (releaseOwnedReference reference store) := by
  induction store with
  | nil =>
      simpa [releaseOwnedReference] using hsorted
  | cons first rest ih =>
      have hhead := (List.pairwise_cons.mp hsorted).1
      have htail := (List.pairwise_cons.mp hsorted).2
      by_cases hid : first.descriptor.id = reference.descriptor.id
      · let remaining := first.owners.erase reference.owner
        by_cases hempty : remaining.isEmpty
        · simpa [releaseOwnedReference, hid, remaining, hempty] using htail
        · simpa [releaseOwnedReference, hid, remaining, hempty,
            DescriptorStoreSorted] using hsorted
      · simp only [releaseOwnedReference, hid, ↓reduceIte]
        apply List.pairwise_cons.mpr
        constructor
        · intro current hmem
          rcases mem_releaseOwnedReference_cases current reference rest hmem with
            ⟨existing, hexisting, heq⟩
          rw [heq]
          exact hhead existing hexisting
        · exact ih htail

/--
A multiset of exact owned references is held when each `(descriptor,owner)` multiplicity is
available. Aggregate payload counts alone cannot establish this predicate.
-/
def PacketReferencesHeld
    (references : List OwnedPacketReference)
    (store : List PacketStoreEntry) : Prop :=
  ∀ reference ∈ references,
    references.count reference ≤ ownedReferenceCount reference store

/-- Exact held-reference validity is executable for finite owner stores. -/
instance (references : List OwnedPacketReference) (store : List PacketStoreEntry) :
    Decidable (PacketReferencesHeld references store) := by
  unfold PacketReferencesHeld
  infer_instance

/-- Every held exact owner has positive multiplicity in the resident store. -/
theorem ownedReferenceCount_positive_of_held
    (references : List OwnedPacketReference)
    (store : List PacketStoreEntry)
    (reference : OwnedPacketReference)
    (hheld : PacketReferencesHeld references store)
    (hmem : reference ∈ references) :
    0 < ownedReferenceCount reference store := by
  have hcountNe : references.count reference ≠ 0 := by
    intro hzero
    exact (List.count_eq_zero.mp hzero) hmem
  exact Nat.lt_of_lt_of_le (Nat.zero_lt_of_ne_zero hcountNe) (hheld reference hmem)

/-- Remove one occurrence of every item in `removed` from a list-valued multiset. -/
def listBagDifference [BEq α] (source removed : List α) : List α :=
  removed.foldl (fun current item => current.erase item) source

/-- Structural mutable-state releases performed by one handler. -/
def stateReferenceConsumptions
    (image : SimulationImage State)
    (node : NodeDescriptor)
    (before after : RoleState State node.kind) : List OwnedPacketReference :=
  listBagDifference
    (ownedRoleStateReferences image node before)
    (ownedRoleStateReferences image node after)

/-- Structural mutable-state acquisitions performed by one handler. -/
def stateReferenceIncrements
    (image : SimulationImage State)
    (node : NodeDescriptor)
    (before after : RoleState State node.kind) : List OwnedPacketReference :=
  listBagDifference
    (ownedRoleStateReferences image node after)
    (ownedRoleStateReferences image node before)

/--
The only releasable owners are the exact pending event being processed and queue/in-service
residencies removed by that handler's public state delta. This makes foreign consume-and-reacquire
laundering structurally invalid even when aggregate counts are sufficient.
-/
def ReferenceConsumptionsValid
    (image : SimulationImage State)
    (node : NodeDescriptor)
    (event : Event)
    (state : RoleState State node.kind)
    (result : TransitionResult State node.kind)
    (store : List PacketStoreEntry) : Prop :=
  result.packetReferenceConsumptions.Perm
      (ownedEventReference image event ::
        stateReferenceConsumptions image node state result.nextState) ∧
    PacketReferencesHeld result.packetReferenceConsumptions store

/-- Structural release validity is executable. -/
instance
    (image : SimulationImage State)
    (node : NodeDescriptor)
    (event : Event)
    (state : RoleState State node.kind)
    (result : TransitionResult State node.kind)
    (store : List PacketStoreEntry) :
    Decidable (ReferenceConsumptionsValid image node event state result store) := by
  unfold ReferenceConsumptionsValid
  infer_instance

/-- Explicit acquisitions must be exactly the queue/in-service owners introduced by the state. -/
def ReferenceIncrementsValid
    (image : SimulationImage State)
    (node : NodeDescriptor)
    (state : RoleState State node.kind)
    (result : TransitionResult State node.kind) : Prop :=
  result.packetReferenceIncrements.Perm
    (stateReferenceIncrements image node state result.nextState)

/-- Structural acquisition validity is executable. -/
instance
    (image : SimulationImage State)
    (node : NodeDescriptor)
    (state : RoleState State node.kind)
    (result : TransitionResult State node.kind) :
    Decidable (ReferenceIncrementsValid image node state result) := by
  unfold ReferenceIncrementsValid
  infer_instance
/-- Canonical global payload-ID projection used by `RunResult.resident_packets`. -/
def canonicalizeDescriptors (descriptors : List PacketDescriptor) : List PacketDescriptor :=
  descriptors.foldl
    (fun current descriptor => installDescriptor descriptor current)
    []

/--
Coherence for descriptor-only observation/result lists. These carry no lifetime count in Rust's
`observed_packets` map at `executor/src/scalar.rs:370,577-590`.
-/
def DescriptorListCoherent
    (image : SimulationImage State)
    (descriptors : List PacketDescriptor) : Prop :=
  descriptors.Pairwise (fun left right => left.id < right.id) ∧
    (descriptors.map PacketDescriptor.id).Nodup ∧
      ∀ descriptor ∈ descriptors,
        descriptor = image.packetDescriptor descriptor.id

/--
Apply structurally checked exact-owner releases followed by queue/in-service acquisitions.
-/
def applyPacketEffects
    (result : TransitionResult State kind)
    (store : List PacketStoreEntry) : List PacketStoreEntry :=
  result.packetReferenceIncrements.foldl
    (fun current reference => acquireOwnedReference reference current)
    (result.packetReferenceConsumptions.foldl
      (fun current reference => releaseOwnedReference reference current)
      store)

/-- The hold owned by a not-yet-exchanged remote child envelope. -/
def ownedEnvelopeEventReference
    (image : SimulationImage State)
    (source : NodeId)
    (event : Event) : OwnedPacketReference :=
  { descriptor := image.packetDescriptor event.payload
    owner := .envelope source event.key }

/-- CPU-side child owner: local future or source outbox envelope. -/
def ownedChildReferenceAtSource
    (image : SimulationImage State)
    (source : NodeId)
    (child : Event) : OwnedPacketReference :=
  if child.target = source then
    ownedEventReference image child
  else
    ownedEnvelopeEventReference image source child

/--
Every emitted child has its exact local-event or remote-envelope owner at the source LP.
-/
def ChildDescriptorsAvailable
    (image : SimulationImage State)
    (source : NodeId)
    (store : List PacketStoreEntry)
    (children : List Event) : Prop :=
  PacketReferencesHeld
    (children.map (ownedChildReferenceAtSource image source))
    store

/--
Every scalar child has its exact pending-event owner at the target LP.
-/
def ChildReferencesAvailableAtTargets
    (image : SimulationImage State)
    (machine : MachineState State)
    (children : List Event) : Prop :=
  ∀ child ∈ children,
    ∃ target ∈ image.nodes,
      target.id = child.target ∧
        ownedReferenceCount (ownedEventReference image child)
          (machine.packetStore target) = 1

/--
Acquire exact pending-event owners for children addressed to one scalar LP.
-/
def installChildDescriptorsFor
    (image : SimulationImage State)
    (target : NodeId)
    (children : List Event)
    (store : List PacketStoreEntry) : List PacketStoreEntry :=
  children.foldl
    (fun current child =>
      if child.target = target then
        acquireOwnedReference (ownedEventReference image child) current
      else
        current)
    store

/--
Hold every emitted child at its source as either a local pending event or an envelope.
-/
def holdEmittedChildReferences
    (image : SimulationImage State)
    (source : NodeId)
    (children : List Event)
    (store : List PacketStoreEntry) : List PacketStoreEntry :=
  children.foldl
    (fun current child =>
      acquireOwnedReference (ownedChildReferenceAtSource image source child) current)
    store

/-- Insert a keyed departure in canonical event-key order. -/
def insertDeparture (record : RecordedDeparture) : List RecordedDeparture → List RecordedDeparture
  | [] => [record]
  | head :: tail =>
      if record.eventKey ≤ head.eventKey then record :: head :: tail
      else head :: insertDeparture record tail

/-- Insert a keyed arrival in canonical event-key order. -/
def insertArrival (record : RecordedArrival) : List RecordedArrival → List RecordedArrival
  | [] => [record]
  | head :: tail =>
      if record.eventKey ≤ head.eventKey then record :: head :: tail
      else head :: insertArrival record tail

/-- Canonically merge transition output into the complete normalized result surface. -/
def applyRecordedOutput
    (result : TransitionResult State kind)
    (before after : MachineState State) : Prop :=
  after.summary = RunSummary.add before.summary result.summaryDelta ∧
    after.observedPackets =
      result.observedPackets.foldl
        (fun current descriptor => installDescriptor descriptor current)
        before.observedPackets ∧
    after.departures =
      result.departures.foldl
        (fun current record => insertDeparture record current)
        before.departures ∧
    after.arrivals =
      result.arrivals.foldl
        (fun current record => insertArrival record current)
        before.arrivals

/--
Consecutive lifetime origin-sequence allocation in child-emission order, matching cursor
consumption at `executor/src/scalar.rs:1236-1297`.
-/
def ChildrenUseOriginSequence
    (origin : NodeId) : Nat → List Event → Prop
  | _, [] => True
  | next, child :: tail =>
      child.key.originNode = origin ∧
        child.key.originSeq = next ∧
        ChildrenUseOriginSequence origin (next + 1) tail

/--
Atomic lifetime key allocation for one transition. Consumed keys remain in `allocatedKeys`, and
children consume consecutive sequence values in result-list order.
-/
def AllocatesChildrenInOrder
    (node : NodeDescriptor)
    (children : List Event)
    (before after : MachineState State) : Prop :=
  ChildrenUseOriginSequence node.id (before.nextOriginSeq node.id) children ∧
    (children.map Event.key).Nodup ∧
    (∀ child ∈ children, child.key ∉ before.allocatedKeys) ∧
    after.allocatedKeys = before.allocatedKeys ++ children.map Event.key ∧
    after.nextOriginSeq node.id =
      before.nextOriginSeq node.id + children.length ∧
    ∀ other,
      other ≠ node.id →
      after.nextOriginSeq other = before.nextOriginSeq other

/--
Strictly key-ordered pending queue expected by both Rust executors at
`executor/src/scalar.rs:341-350` and `executor/src/safe_horizon.rs:170`.
-/
def CanonicalPending (pending : List Event) : Prop :=
  pending.Pairwise (fun left right => left.key < right.key)

/--
One event is the least currently eligible event, matching the scalar `pop_first` choice at
`executor/src/scalar.rs:344-350`.
-/
def IsLeastEligible
    (eligible : Event → Prop)
    (event : Event)
    (pending : List Event) : Prop :=
  event ∈ pending ∧
    eligible event ∧
    ∀ other ∈ pending, eligible other → event.key ≤ other.key

/--
No pending event satisfies the current boundary/cut predicate, matching loop termination at
`executor/src/scalar.rs:344-347`.
-/
def NoEligibleEvent (eligible : Event → Prop) (pending : List Event) : Prop :=
  ∀ event ∈ pending, ¬ eligible event

/--
LP-local transition application: the processing LP consumes only held references, applies explicit
acquisitions, and holds every emitted child until local insertion or remote exchange. Other LPs are
unchanged. This mirrors exclusive CPU state-slot ownership and outbox creation at
`executor/src/cpu.rs:586-690`.
-/
def AppliesTransitionResult
    (image : SimulationImage State)
    (node : NodeDescriptor)
    (event : Event)
    (result : TransitionResult State node.kind)
    (before after : MachineState State) : Prop :=
  after.localState node = result.nextState ∧
    after.packetStore node =
      holdEmittedChildReferences image node.id result.children
        (applyPacketEffects result (before.packetStore node)) ∧
    (∀ other ∈ image.nodes,
      other.id ≠ node.id →
      after.localState other = before.localState other ∧
        after.packetStore other = before.packetStore other) ∧
    applyRecordedOutput result before after ∧
    ReferenceConsumptionsValid image node event
      (before.localState node) result (before.packetStore node) ∧
    ReferenceIncrementsValid image node (before.localState node) result

/--
Scalar transition application in the owned per-LP projection. A processed event releases its exact
pending owner, queue/service changes use their structural owners, and each child is acquired
immediately at its target. The CPU path temporarily uses an envelope owner and transfers it at
exchange. Rust sites: `executor/src/scalar.rs:344-372,869-956,999-1015,1105-1218` and
`executor/src/cpu.rs:649-667,4485-4500`.
-/
def AppliesScalarTransitionResult
    (image : SimulationImage State)
    (node : NodeDescriptor)
    (event : Event)
    (result : TransitionResult State node.kind)
    (before after : MachineState State) : Prop :=
  after.localState node = result.nextState ∧
    after.packetStore node =
      installChildDescriptorsFor image node.id result.children
        (applyPacketEffects result (before.packetStore node)) ∧
    (∀ other ∈ image.nodes,
      other.id ≠ node.id →
      after.localState other = before.localState other ∧
        after.packetStore other =
          installChildDescriptorsFor image other.id result.children
            (before.packetStore other)) ∧
    applyRecordedOutput result before after ∧
    ReferenceConsumptionsValid image node event
      (before.localState node) result (before.packetStore node) ∧
    ReferenceIncrementsValid image node (before.localState node) result

/--
One arbitrary available-event step used to define an explicit candidate reordering; unlike the
canonical scalar loop, it does not itself impose least-key choice. Child descriptors become
available in their target LP projections when their events enter the scalar future list
(`executor/src/scalar.rs:344-356,362-372`).
-/
def AvailableEventStep
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (event : Event)
    (before after : MachineState State) : Prop :=
  event ∈ before.pending ∧
    ∃ node ∈ image.nodes, ∃ result,
      event.target = node.id ∧
      transition node event (before.localState node) result ∧
      FreshEventKeys result.children (before.pending.erase event) ∧
      AllocatesChildrenInOrder node result.children before after ∧
      AppliesScalarTransitionResult image node event result before after ∧
      DescriptorStoreCoherent image (after.packetStore node) ∧
      ChildReferencesAvailableAtTargets image after result.children ∧
      after.pending = insertEvents result.children (before.pending.erase event) ∧
      after.emissions =
        before.emissions ++ (result.children.map fun child => (event, child))

/--
One canonical least-key-first serial step over the same semantic image, mirroring
`executor/src/scalar.rs:335-359`.
-/
def CanonicalSerialStep
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (eligible : Event → Prop)
    (before : MachineState State)
    (event : Event)
    (after : MachineState State) : Prop :=
  IsLeastEligible eligible event before.pending ∧
    AvailableEventStep image transition event before after

/--
Finite canonical least-key-first reference execution, mirroring repeated scalar dispatch at
`executor/src/scalar.rs:335-359`.
-/
inductive CanonicalSerialExecution
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (eligible : Event → Prop) :
    MachineState State → List Event → MachineState State → Prop
  | refl (state) :
      CanonicalSerialExecution image transition eligible state [] state
  | step
      (first :
        CanonicalSerialStep image transition eligible before event middle)
      (rest :
        CanonicalSerialExecution image transition eligible middle events after) :
      CanonicalSerialExecution image transition eligible before (event :: events) after

/--
Execution in a caller-supplied event order for F5, using the same transition and immediate child
insertion semantics as `executor/src/scalar.rs:351-356`.
-/
inductive ExecutionInOrder
    (image : SimulationImage State)
    (transition : TransitionRelation State) :
    MachineState State → List Event → MachineState State → Prop
  | refl (state) :
      ExecutionInOrder image transition state [] state
  | step
      (first : AvailableEventStep image transition event before middle)
      (rest : ExecutionInOrder image transition middle events after) :
      ExecutionInOrder image transition before (event :: events) after

/--
Canonical serial execution restricted to a consistent cut: non-cut events remain pending while the
least eligible cut event runs. This is the cut projection required when per-LP bounds differ; the
global scalar loop at `executor/src/scalar.rs:344-350` is the constant-bound specialization.
-/
def CanonicalSerialRestricted
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (cut : Event → Prop)
    (before : MachineState State)
    (executed : List Event)
    (after : MachineState State) : Prop :=
  CanonicalSerialExecution image transition cut before executed after ∧
    NoEligibleEvent cut after.pending

/--
Canonical scalar execution through Rust's inclusive configured stop, intentionally distinct from
the half-open round bound (`executor/src/scalar.rs:322-359`).
-/
def CanonicalSerialThroughStop
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (before : MachineState State)
    (executed : List Event)
    (after : MachineState State) : Prop :=
  CanonicalSerialExecution image transition (withinInclusiveStop image.stopTimeNs)
      before executed after ∧
    NoEligibleEvent (withinInclusiveStop image.stopTimeNs) after.pending

/--
Canonical initial owned store derived for each LP. Rust's CPU constructor derives the future,
queue, and in-service ownership partition at `executor/src/cpu.rs:3053-3166`.
-/
def initialPacketStore
    (image : SimulationImage State)
    (node : NodeDescriptor) : List PacketStoreEntry :=
  image.initialPacketStore node.id

/-- Exact pending, queue, and in-service owners currently assigned to one LP. -/
def machineOwnedReferencesFor
    (image : SimulationImage State)
    (machine : MachineState State)
    (node : NodeDescriptor) : List OwnedPacketReference :=
  ((machine.pending.filter fun event => event.target = node.id).map
      (ownedEventReference image)) ++
    ownedRoleStateReferences image node (machine.localState node)

/--
Structural machine invariant used at round boundaries: pending events are declared and supported,
their lifetime keys remain allocated below the corresponding cursor, and every LP holds the exact
pending, queue, and in-service owner multiset derived from its public state
(`executor/src/cpu.rs:563-569,3053-3166,3389-3447`).
-/
def MachineWellFormed
    (image : SimulationImage State)
    (machine : MachineState State) : Prop :=
  CanonicalPending machine.pending ∧
    machine.allocatedKeys.Nodup ∧
    (∀ key ∈ machine.allocatedKeys,
      key.originSeq < machine.nextOriginSeq key.originNode ∧
        ∃ origin ∈ image.nodes, key.originNode = origin.id) ∧
    (∀ event ∈ machine.pending,
      event.key ∈ machine.allocatedKeys ∧
        ∃ node ∈ image.nodes,
          event.target = node.id ∧
            roleSupports node.kind event.kind) ∧
    (∀ node ∈ image.nodes,
      DescriptorStoreCoherent image (machine.packetStore node)) ∧
    (∀ node ∈ image.nodes,
      OwnedReferencesMatchStore
        (machineOwnedReferencesFor image machine node)
        (machine.packetStore node)) ∧
    DescriptorListCoherent image machine.observedPackets

/--
Initial machine relation resolving Rust's role arena plus `state_slot` representation at
`executor/src/image.rs:247-260` into the semantic node-indexed view used by the proof, including
the validated initial origin-sequence cursors from `executor/src/validate.rs:1876-1953`.
-/
def InitialMachine
    (image : SimulationImage State)
    (machine : MachineState State) : Prop :=
  machine.pending = canonicalizeEvents image.initialEvents ∧
    machine.summary = RunSummary.zero ∧
    machine.observedPackets = [] ∧
    machine.departures = [] ∧
    machine.arrivals = [] ∧
    machine.nextOriginSeq = image.initialNextOriginSeq ∧
    machine.allocatedKeys = image.initialEvents.map Event.key ∧
    machine.emissions = [] ∧
    (∀ node ∈ image.nodes,
      stateAt? image node = some (machine.localState node) ∧
        machine.packetStore node = initialPacketStore image node) ∧
    MachineWellFormed image machine

/--
Canonical scalar reachability invariant used for handler progress. It contains exactly successful
prefixes from an initial accepted machine, rather than arbitrary role/state combinations that Rust
handlers reject with `ExecutionError` at `executor/src/scalar.rs:80-165,863-972,1091-1233`.
-/
def CanonicallyReachableMachine
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (machine : MachineState State) : Prop :=
  ∃ initial executed,
    InitialMachine image initial ∧
      CanonicalSerialExecution image transition (fun _ => True)
        initial executed machine

/--
Successful-handler progress only at a canonically reachable least-event configuration. This is the
reachable-state invariant relied on by the theorem statements; it does not claim Rust's checked
handlers succeed on inconsistent arbitrary states. Enabledness is required for the least pending
event of each LP, which covers canonical scalar execution and ownership-preserving LP drains
without admitting invalid same-LP reorderings.
-/
def TransitionEnabledOnReachable
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  ∀ machine,
    CanonicallyReachableMachine image transition machine →
    ∀ node ∈ image.nodes,
      ∀ event,
        IsLeastEligible
          (fun candidate => candidate.target = node.id)
          event
          machine.pending →
        event.target = node.id →
        roleSupports node.kind event.kind →
        ∃ after, AvailableEventStep image transition event machine after

/--
Accepted heterogeneous image plus its successful abstract transition semantics. Static checks and
transition safety hold globally; enabledness is required only for canonically reachable
configurations, matching the validator-established preconditions consumed by the partial Rust
handlers.
-/
def AcceptedModel
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  StaticImageWellFormed image ∧
    InitialEventsRoleCorrect image ∧
    TransitionAxioms image transition ∧
    TransitionEnabledOnReachable image transition

/--
Non-arena semantic projection of the `RunResult` fields at `executor/src/scalar.rs:64-78`.
`SameMachineResult` separately compares every node's local arena state.
-/
structure RunResultView where
  summary : RunSummary
  residentPackets : List PacketDescriptor
  observedPackets : List PacketDescriptor
  departures : List PacketDeparture
  arrivals : List PacketArrivalObservation
  pendingEvents : List Event

/--
Deterministic non-arena portion of the full result projection. Resident packet data forgets counts
and is globally normalized from every LP entry as in `executor/src/cpu.rs:3372-3433`; exact counts
remain visible to `SameMachineResult` through per-LP store equality.
-/
def projectRunResult
    (image : SimulationImage State)
    (machine : MachineState State) : RunResultView :=
  { summary := machine.summary
    residentPackets :=
      canonicalizeDescriptors
        ((image.nodes.flatMap machine.packetStore).map PacketStoreEntry.descriptor)
    observedPackets := canonicalizeDescriptors machine.observedPackets
    departures := machine.departures.map RecordedDeparture.departure
    arrivals := machine.arrivals.map RecordedArrival.arrival
    pendingEvents := machine.pending }

/-- Aggregate compatibility view derived from one owned descriptor entry. -/
structure CountedPacketStoreEntry where
  descriptor : PacketDescriptor
  references : Nat
  deriving DecidableEq, Repr

/-- Forget owner provenance while retaining the former counted-store meaning. -/
def deriveCountedPacketStore
    (store : List PacketStoreEntry) : List CountedPacketStoreEntry :=
  store.map fun entry =>
    { descriptor := entry.descriptor
      references := entry.references }

/-- Exact owner equivalence projects to equality of the former counted store. -/
theorem deriveCountedPacketStore_eq_of_ownedStoresEquivalent
    {left right : List PacketStoreEntry}
    (hequivalent : OwnedStoresEquivalent left right) :
    deriveCountedPacketStore left = deriveCountedPacketStore right := by
  induction hequivalent with
  | nil => rfl
  | @cons leftEntry rightEntry lefts rights head tail ih =>
      rcases leftEntry with ⟨leftDescriptor, leftOwners⟩
      rcases rightEntry with ⟨rightDescriptor, rightOwners⟩
      rcases head with ⟨hdescriptor, howners⟩
      simp_all [deriveCountedPacketStore, PacketStoreEntry.references]
      exact howners.length_eq

/--
Owner-preserving equality used by replay proofs. This relation is deliberately stronger than the
counted public result projection because it retains the exact pending, queue, service, and
envelope holders.
-/
def PerLPOwnedStoresEquivalent
    (image : SimulationImage State)
    (left right : MachineState State) : Prop :=
  ∀ node ∈ image.nodes,
    OwnedStoresEquivalent (left.packetStore node) (right.packetStore node)

/--
Exact derived `(descriptor, positive reference count)` holdings at every declared LP. Owner tags
remain internal proof state; this comparison has the same meaning as the pre-ownership counted
store relation.
-/
def PerLPDescriptorStoresEqual
    (image : SimulationImage State)
    (left right : MachineState State) : Prop :=
  ∀ node ∈ image.nodes,
    deriveCountedPacketStore (left.packetStore node) =
      deriveCountedPacketStore (right.packetStore node)

/-- Owner-preserving per-LP equality implies the unchanged counted-store comparison. -/
theorem perLPOwnedStoresEquivalent_implies_counted
    (image : SimulationImage State)
    (left right : MachineState State)
    (hequivalent : PerLPOwnedStoresEquivalent image left right) :
    PerLPDescriptorStoresEqual image left right := by
  intro node hnode
  exact deriveCountedPacketStore_eq_of_ownedStoresEquivalent
    (hequivalent node hnode)

/--
Strong proof-state congruence for CPU/scalar replay. Public observations agree and every LP has
the same exact owner multiset; `SameMachineResult` below intentionally forgets those tags.
-/
def SameMachineProvenance
    (image : SimulationImage State)
    (left right : MachineState State) : Prop :=
  (∀ node ∈ image.nodes, left.localState node = right.localState node) ∧
    PerLPOwnedStoresEquivalent image left right ∧
    projectRunResult image left = projectRunResult image right

/--
Complete normalized-result equality used by all cross-executor claims. Derived reference counts,
descriptor identity, summary counters, full observations, pending events, and both role arenas
must all agree; exact owner tags and other proof ghosts are excluded.

Per-LP equality is stronger than assembled Rust `RunResult` equality because result assembly
globally deduplicates descriptors at `executor/src/cpu.rs:3372-3433`. This intentionally strengthens
the concrete `IndependentStepsCommute` instance obligation: swapped prefixes must retain identical
LP holdings so equality remains valid under every dependent suffix. Because the statements reuse
this relation, F2 and F3 also receive the stronger internal congruence guarantee.
-/
def SameMachineResult
    (image : SimulationImage State)
    (left right : MachineState State) : Prop :=
  (∀ node ∈ image.nodes, left.localState node = right.localState node) ∧
    PerLPDescriptorStoresEqual image left right ∧
    projectRunResult image left = projectRunResult image right

/-- Provenance replay congruence safely forgets owner tags at the public result boundary. -/
theorem sameMachineProvenance_implies_result
    (image : SimulationImage State)
    (left right : MachineState State)
    (hprovenance : SameMachineProvenance image left right) :
    SameMachineResult image left right := by
  exact ⟨hprovenance.1,
    perLPOwnedStoresEquivalent_implies_counted image left right hprovenance.2.1,
    hprovenance.2.2⟩

/--
One direct parent/child emission edge produced by the abstract handler relation, corresponding to
child generation at `executor/src/scalar.rs:351-356`.
-/
def PotentialEmissionEdge
    (transition : TransitionRelation State)
    (parent child : Event) : Prop :=
  ∃ node state result,
    transition node parent state result ∧ child ∈ result.children

/--
Transitive causal/emission order generated by handler children, formalizing the dependency behind
immediate local insertion at `executor/src/safe_horizon.rs:422-435`.
-/
inductive PotentialCausalBefore
    (transition : TransitionRelation State) : Event → Event → Prop
  | direct (edge : PotentialEmissionEdge transition parent child) :
      PotentialCausalBefore transition parent child
  | tail
      (edge : PotentialEmissionEdge transition parent middle)
      (rest : PotentialCausalBefore transition middle child) :
      PotentialCausalBefore transition parent child

/--
Events reachable from round-start pending work through zero or more handler emissions, including
local children drained immediately at `executor/src/safe_horizon.rs:422-435`.
-/
inductive PotentiallyReachableEvent
    (transition : TransitionRelation State)
    (startPending : List Event) : Event → Prop
  | seed (member : event ∈ startPending) :
      PotentiallyReachableEvent transition startPending event
  | child
      (parentReachable : PotentiallyReachableEvent transition startPending parent)
      (edge : PotentialEmissionEdge transition parent child) :
      PotentiallyReachableEvent transition startPending child

/--
An unseen remote event for a target is any transitively reachable child crossing an LP boundary,
not merely an event already present in the current outbox. This is the semantic closure required
by relay chains beyond `executor/src/safe_horizon.rs:309-335`.
-/
def UnseenRemoteAt
    (transition : TransitionRelation State)
    (startPending : List Event)
    (target : NodeId)
    (event : Event) : Prop :=
      event.target = target ∧
    ∃ parent,
      PotentiallyReachableEvent transition startPending parent ∧
      PotentialEmissionEdge transition parent event ∧
      parent.target ≠ event.target

/--
One parent/child edge recorded by an actual successful execution step, corresponding to the child
loop at `executor/src/scalar.rs:351-356`.
-/
def RecordedEmissionEdge
    (emissions : List (Event × Event))
  (parent child : Event) : Prop :=
  (parent, child) ∈ emissions

/--
Exact ghost-emission suffix produced since a round start. Lifetime history remains on the machine,
but consistent-cut causality consumes only this suffix so round-start pending events are causal
roots.
-/
def RoundEmissionDelta
    (start finish : MachineState State)
    (delta : List (Event × Event)) : Prop :=
  finish.emissions = start.emissions ++ delta

/--
Trace-specific causal closure of actual recorded child emissions, avoiding hypothetical handler
branches while modeling immediate insertion at `executor/src/safe_horizon.rs:422-435`.
-/
inductive RecordedCausalBefore
    (emissions : List (Event × Event)) : Event → Event → Prop
  | direct (edge : RecordedEmissionEdge emissions parent child) :
      RecordedCausalBefore emissions parent child
  | tail
      (edge : RecordedEmissionEdge emissions parent middle)
      (rest : RecordedCausalBefore emissions middle child) :
      RecordedCausalBefore emissions parent child

/--
Actual events reachable from round-start pending work through the recorded emission trace,
including local children drained immediately at `executor/src/safe_horizon.rs:422-435`.
-/
inductive RecordedReachableEvent
    (emissions : List (Event × Event))
    (startPending : List Event) : Event → Prop
  | seed (member : event ∈ startPending) :
      RecordedReachableEvent emissions startPending event
  | child
      (parentReachable : RecordedReachableEvent emissions startPending parent)
      (edge : RecordedEmissionEdge emissions parent child) :
      RecordedReachableEvent emissions startPending child

/--
Per-LP key-prefix closure over the reachable serial event universe, formalizing the cut drained by
per-LP variants of `executor/src/safe_horizon.rs:381-448`.
-/
def PerLPPrefix
    (eventUniverse cut : Event → Prop) : Prop :=
  ∀ later, cut later →
    ∀ earlier, eventUniverse earlier →
      earlier.target = later.target →
      earlier.key < later.key →
      cut earlier

/--
Backward closure of a cut under the transition emission order, formalizing the causal constraint
implicit in immediate child insertion at `executor/src/safe_horizon.rs:422-435`.
-/
def CausallyClosed
    (emissions : List (Event × Event))
    (cut : Event → Prop) : Prop :=
  ∀ parent child,
    RecordedCausalBefore emissions parent child →
    cut child →
    cut parent

/--
Consistent cut: a reachable per-LP key prefix closed under the current round's causal/emission
order, specifying the semantic set drained by `executor/src/safe_horizon.rs:268-336`.
-/
def IsConsistentCut
    (emissions : List (Event × Event))
    (startPending : List Event)
    (cut : Event → Prop) : Prop :=
  (∀ event, cut event → RecordedReachableEvent emissions startPending event) ∧
    PerLPPrefix (RecordedReachableEvent emissions startPending) cut ∧
    CausallyClosed emissions cut

/--
Global time-prefix event set corresponding to Rust's constant horizon drain at
`executor/src/safe_horizon.rs:264-271,392-395`.
-/
def TimePrefix
    (emissions : List (Event × Event))
    (startPending : List Event)
    (horizon : Nat)
    (event : Event) : Prop :=
  RecordedReachableEvent emissions startPending event ∧ event.key.timeNs < horizon

/--
The concrete drained list represents exactly the events reachable from the round-start roots below
their target LP's bound and forms a cut closed under round-local emissions, generalizing
`executor/src/safe_horizon.rs:381-448`.
-/
def DrainedConsistentCut
    (emissions : List (Event × Event))
    (startPending drained : List Event)
    (bounds : NodeId → Nat)
    (cut : Event → Prop) : Prop :=
  IsConsistentCut emissions startPending cut ∧
    (∀ event, event ∈ drained ↔ cut event) ∧
    (∀ event, cut event ↔
      RecordedReachableEvent emissions startPending event ∧ belowBound bounds event)

end DaysExecutor
