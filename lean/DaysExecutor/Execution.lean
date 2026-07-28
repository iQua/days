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
A local counted store is in canonical `BTreeMap` payload order, contains only positive entries,
and contains exactly the oracle descriptors. This mirrors `ResidentPacket` storage at
`executor/src/scalar.rs:367-381`; zero-count terminal entries are dropped at lines 1679-1698.
-/
def DescriptorStoreCoherent
    (image : SimulationImage State)
    (store : List PacketStoreEntry) : Prop :=
  DescriptorStoreSorted store ∧
    (store.map fun entry => entry.descriptor.id).Nodup ∧
      ∀ entry ∈ store,
        0 < entry.references ∧
          entry.descriptor = image.packetDescriptor entry.descriptor.id

/-- Add a batch of live references (`executor/src/scalar.rs:1645-1658`). -/
def incrementReferenceCountBy (amount references : Nat) : Nat :=
  references + amount

/-- Consume a batch of held references (`executor/src/scalar.rs:1661-1683`). -/
def consumeReferenceCountBy (amount references : Nat) : Nat :=
  references - amount

/-- One checked-success-path reference increment (`executor/src/scalar.rs:1645-1658`). -/
def incrementReferenceCount (references : Nat) : Nat :=
  incrementReferenceCountBy 1 references

/-- One checked-success-path reference consumption (`executor/src/scalar.rs:1661-1683`). -/
def consumeReferenceCount (references : Nat) : Nat :=
  consumeReferenceCountBy 1 references

/--
Two acquisitions commute on one reference counter, matching repeated transmitter increments at
`executor/src/scalar.rs:1645-1658`.
-/
theorem incrementReferenceCountBy_commutes (left right references : Nat) :
    incrementReferenceCountBy left (incrementReferenceCountBy right references) =
      incrementReferenceCountBy right (incrementReferenceCountBy left references) := by
  simp [incrementReferenceCountBy, Nat.add_comm, Nat.add_left_comm]

/--
Two consumptions commute in the totalized counter algebra. Semantic steps separately require held
references, so only Rust's checked-success path at `executor/src/scalar.rs:1661-1683` is reachable.
-/
theorem consumeReferenceCountBy_commutes (left right references : Nat) :
    consumeReferenceCountBy left (consumeReferenceCountBy right references) =
      consumeReferenceCountBy right (consumeReferenceCountBy left references) := by
  simp [consumeReferenceCountBy, Nat.sub_sub, Nat.add_comm]

/--
Acquisition and consumption commute whenever the consumed reference is held. Positivity is exactly
the condition that excludes Rust's checked-sub underflow at `executor/src/scalar.rs:1672-1678`.
-/
theorem increment_consumeReferenceCountBy_commutes
    (increment consume references : Nat)
    (hheld : consume ≤ references) :
    incrementReferenceCountBy increment (consumeReferenceCountBy consume references) =
      consumeReferenceCountBy consume (incrementReferenceCountBy increment references) := by
  simp [incrementReferenceCountBy, consumeReferenceCountBy]
  omega

/--
Acquire one descriptor reference, inserting a positive entry or incrementing its existing count.
This abstracts transmitter acquisition at `executor/src/scalar.rs:1645-1658` together with CPU
event/envelope ownership at `executor/src/cpu.rs:627-667,4485-4500`.
-/
def incrementDescriptorReference
    (descriptor : PacketDescriptor) : List PacketStoreEntry → List PacketStoreEntry
  | [] => [{ descriptor, references := 1 }]
  | head :: tail =>
      if descriptor.id = head.descriptor.id then
        { head with references := incrementReferenceCount head.references } :: tail
      else if descriptorLE descriptor head.descriptor then
        { descriptor, references := 1 } :: head :: tail
      else
        head :: incrementDescriptorReference descriptor tail

/--
Consume one held reference and automatically drop the entry at zero. Missing references are a
total no-op here so the update remains executable; `PacketReferencesHeld` makes that case
impossible in semantic steps, matching Rust's checked decrement and zero drop at
`executor/src/scalar.rs:1661-1683`.
-/
def consumeDescriptorReference
    (payload : PayloadId) : List PacketStoreEntry → List PacketStoreEntry
  | [] => []
  | head :: tail =>
      if head.descriptor.id = payload then
        if head.references = 1 then
          tail
        else
          { head with references := consumeReferenceCount head.references } :: tail
      else
        head :: consumeDescriptorReference payload tail

/--
Canonical acquisitions commute as exact counted-store updates. Equal payloads must carry the same
immutable descriptor, as Rust's payload-keyed resident map requires at
`executor/src/scalar.rs:367-381,1645-1658`.
-/
theorem incrementDescriptorReference_commutes
    (left right : PacketDescriptor)
    (hcanonical : left.id = right.id → left = right)
    (store : List PacketStoreEntry) :
    incrementDescriptorReference left (incrementDescriptorReference right store) =
      incrementDescriptorReference right (incrementDescriptorReference left store) := by
  rcases Nat.lt_trichotomy left.id right.id with hlt | heq | hgt
  · have hne : left.id ≠ right.id := Nat.ne_of_lt hlt
    have hne' : right.id ≠ left.id := Nat.ne_of_gt hlt
    have hle : left.id ≤ right.id := Nat.le_of_lt hlt
    have hnle : ¬ right.id ≤ left.id := Nat.not_le_of_gt hlt
    induction store with
    | nil =>
        simp [incrementDescriptorReference, descriptorLE, hne, hne', hle, hnle]
    | cons head tail ih =>
        rcases Nat.lt_trichotomy left.id head.descriptor.id with hlh | hlh | hlh
        <;> rcases Nat.lt_trichotomy right.id head.descriptor.id with hrh | hrh | hrh
        <;> simp_all! +arith [incrementDescriptorReference, incrementReferenceCount,
          incrementReferenceCountBy, descriptorLE, Nat.ne_of_lt, Nat.ne_of_gt,
          Nat.le_of_lt, Nat.not_le_of_gt]
        <;> omega
  · have heq' := hcanonical heq
    subst right
    rfl
  · have hne : left.id ≠ right.id := Nat.ne_of_gt hgt
    have hne' : right.id ≠ left.id := Nat.ne_of_lt hgt
    have hle : right.id ≤ left.id := Nat.le_of_lt hgt
    have hnle : ¬ left.id ≤ right.id := Nat.not_le_of_gt hgt
    induction store with
    | nil =>
        simp [incrementDescriptorReference, descriptorLE, hne, hne', hle, hnle]
    | cons head tail ih =>
        rcases Nat.lt_trichotomy left.id head.descriptor.id with hlh | hlh | hlh
        <;> rcases Nat.lt_trichotomy right.id head.descriptor.id with hrh | hrh | hrh
        <;> simp_all! +arith [incrementDescriptorReference, incrementReferenceCount,
          incrementReferenceCountBy, descriptorLE, Nat.ne_of_lt, Nat.ne_of_gt,
          Nat.le_of_lt, Nat.not_le_of_gt]
        <;> omega

/--
Canonical consumptions commute as exact counted-store updates, including zero-count erasure. The
totalized missing-reference case is unreachable in semantic steps by
`ReferenceConsumptionsValid`, matching `executor/src/scalar.rs:1661-1683`.
-/
theorem consumeDescriptorReference_commutes
    (left right : PayloadId)
    (store : List PacketStoreEntry) :
    consumeDescriptorReference left (consumeDescriptorReference right store) =
      consumeDescriptorReference right (consumeDescriptorReference left store) := by
  induction store with
  | nil =>
      simp [consumeDescriptorReference]
  | cons head tail ih =>
      by_cases hl : head.descriptor.id = left
      <;> by_cases hr : head.descriptor.id = right
      <;> by_cases hone : head.references = 1
      <;> simp_all [consumeDescriptorReference, consumeReferenceCount,
        consumeReferenceCountBy]

/--
Inserting before a strictly larger suffix preserves the canonical resident-map position used by
Rust's payload-keyed store at `executor/src/scalar.rs:367-381`.
-/
private theorem incrementDescriptorReference_before
    (descriptor : PacketDescriptor)
    (store : List PacketStoreEntry)
    (hbefore : ∀ entry ∈ store, descriptor.id < entry.descriptor.id) :
    incrementDescriptorReference descriptor store =
      { descriptor, references := 1 } :: store := by
  cases store with
  | nil =>
      rfl
  | cons head tail =>
      have hlt := hbefore head List.mem_cons_self
      simp [incrementDescriptorReference, descriptorLE, Nat.ne_of_lt hlt,
        Nat.le_of_lt hlt]

/--
A payload below every store key has no resident references, as in Rust's payload-keyed lookup at
`executor/src/scalar.rs:367-381`.
-/
private theorem descriptorReferenceCount_eq_zero_of_before
    (payload : PayloadId)
    (store : List PacketStoreEntry)
    (hbefore : ∀ entry ∈ store, payload < entry.descriptor.id) :
    descriptorReferenceCount payload store = 0 := by
  induction store with
  | nil =>
      rfl
  | cons head tail ih =>
      have hlt := hbefore head List.mem_cons_self
      simp only [descriptorReferenceCount]
      rw [if_neg (Nat.ne_of_gt hlt)]
      apply ih
      intro entry hentry
      exact hbefore entry (List.mem_cons_of_mem _ hentry)

/--
Acquisition commutes with consumption of a distinct payload as an exact sorted-store update. The
proof covers erasing the entry that previously determined the insertion point, matching Rust's
payload-keyed increment/decrement/drop operations at `executor/src/scalar.rs:1645-1683`.
-/
theorem increment_consumeDescriptorReference_commutes_of_ne
    (descriptor : PacketDescriptor)
    (payload : PayloadId)
    (hne : descriptor.id ≠ payload)
    (store : List PacketStoreEntry)
    (hsorted : DescriptorStoreSorted store) :
    incrementDescriptorReference descriptor
        (consumeDescriptorReference payload store) =
      consumeDescriptorReference payload
        (incrementDescriptorReference descriptor store) := by
  induction store with
  | nil =>
      simp [incrementDescriptorReference, consumeDescriptorReference, hne]
  | cons head tail ih =>
      have hhead := (List.pairwise_cons.mp hsorted).1
      have htail := (List.pairwise_cons.mp hsorted).2
      have hinduction := ih htail
      by_cases hp : head.descriptor.id = payload
      · rcases Nat.lt_trichotomy descriptor.id head.descriptor.id with hd | hd | hd
        · have hbefore : ∀ entry ∈ tail,
              descriptor.id < entry.descriptor.id := by
            intro entry hentry
            exact Nat.lt_trans hd (hhead entry hentry)
          by_cases hone : head.references = 1
          · simp only [consumeDescriptorReference, hp, hone, ↓reduceIte]
            rw [incrementDescriptorReference_before descriptor tail hbefore]
            simp_all! +arith [incrementDescriptorReference, consumeDescriptorReference,
              incrementReferenceCount, incrementReferenceCountBy,
              consumeReferenceCount, consumeReferenceCountBy, descriptorLE,
              Nat.ne_of_lt, Nat.ne_of_gt, Nat.le_of_lt, Nat.not_le_of_gt]
          · simp_all! +arith [incrementDescriptorReference, consumeDescriptorReference,
              incrementReferenceCount, incrementReferenceCountBy,
              consumeReferenceCount, consumeReferenceCountBy, descriptorLE,
              Nat.ne_of_lt, Nat.ne_of_gt, Nat.le_of_lt, Nat.not_le_of_gt]
        · exact False.elim (hne (hd.trans hp))
        · by_cases hone : head.references = 1
          <;> simp_all! +arith [incrementDescriptorReference, consumeDescriptorReference,
            incrementReferenceCount, incrementReferenceCountBy,
            consumeReferenceCount, consumeReferenceCountBy, descriptorLE,
            Nat.ne_of_lt, Nat.ne_of_gt, Nat.le_of_lt, Nat.not_le_of_gt]
      · rcases Nat.lt_trichotomy descriptor.id head.descriptor.id with hd | hd | hd
        <;> simp_all! +arith [incrementDescriptorReference, consumeDescriptorReference,
          incrementReferenceCount, incrementReferenceCountBy,
          consumeReferenceCount, consumeReferenceCountBy, descriptorLE,
          Nat.ne_of_lt, Nat.ne_of_gt, Nat.le_of_lt, Nat.not_le_of_gt]

/--
At the resident entry, acquisition commutes with one held consumption even across zero-drop and
reinsertion, matching Rust's increment/decrement/drop sites at
`executor/src/scalar.rs:1645-1683`.
-/
private theorem increment_consumeDescriptorReference_commutes_at_head
    (descriptor : PacketDescriptor)
    (references : Nat)
    (tail : List PacketStoreEntry)
    (hpositive : 0 < references)
    (htail : ∀ entry ∈ tail, descriptor.id < entry.descriptor.id) :
    incrementDescriptorReference descriptor
        (consumeDescriptorReference descriptor.id
          ({ descriptor, references } :: tail)) =
      consumeDescriptorReference descriptor.id
        (incrementDescriptorReference descriptor
          ({ descriptor, references } :: tail)) := by
  rcases references with _ | references
  · omega
  · rcases references with _ | references
    · simp only [consumeDescriptorReference, ↓reduceIte]
      rw [incrementDescriptorReference_before descriptor tail htail]
      simp [consumeDescriptorReference, incrementDescriptorReference,
        incrementReferenceCount, incrementReferenceCountBy,
        consumeReferenceCount, consumeReferenceCountBy]
    · simp [consumeDescriptorReference, incrementDescriptorReference,
        incrementReferenceCount, incrementReferenceCountBy,
        consumeReferenceCount, consumeReferenceCountBy]

/--
Acquiring and consuming the same held descriptor commute as exact counted-store updates. Strict
ordering and descriptor immutability cover the zero-drop/reinsert case, while the held premise
excludes Rust's checked-sub underflow at `executor/src/scalar.rs:1645-1683`.
-/
theorem increment_consumeDescriptorReference_commutes
    (descriptor : PacketDescriptor)
    (store : List PacketStoreEntry)
    (hsorted : DescriptorStoreSorted store)
    (hheld : 0 < descriptorReferenceCount descriptor.id store)
    (hcanonical : ∀ entry ∈ store,
      entry.descriptor.id = descriptor.id → entry.descriptor = descriptor) :
    incrementDescriptorReference descriptor
        (consumeDescriptorReference descriptor.id store) =
      consumeDescriptorReference descriptor.id
        (incrementDescriptorReference descriptor store) := by
  induction store with
  | nil =>
      simp [descriptorReferenceCount] at hheld
  | cons head tail ih =>
      have hhead := (List.pairwise_cons.mp hsorted).1
      have htail := (List.pairwise_cons.mp hsorted).2
      by_cases heq : head.descriptor.id = descriptor.id
      · have hdescriptor := hcanonical head List.mem_cons_self heq
        rcases head with ⟨headDescriptor, references⟩
        simp only at hdescriptor heq hheld hhead ⊢
        subst headDescriptor
        apply increment_consumeDescriptorReference_commutes_at_head
        · simpa [descriptorReferenceCount] using hheld
        · exact hhead
      · have hheldTail : 0 < descriptorReferenceCount descriptor.id tail := by
          simpa [descriptorReferenceCount, heq] using hheld
        have hcanonicalTail : ∀ entry ∈ tail,
            entry.descriptor.id = descriptor.id → entry.descriptor = descriptor := by
          intro entry hentry
          exact hcanonical entry (List.mem_cons_of_mem _ hentry)
        have hinduction := ih htail hheldTail hcanonicalTail
        by_cases hle : descriptorLE descriptor head.descriptor
        · have hlt : descriptor.id < head.descriptor.id := by
            change descriptor.id ≤ head.descriptor.id at hle
            exact Std.lt_of_le_of_ne hle (Ne.symm heq)
          have hbefore : ∀ entry ∈ tail,
              descriptor.id < entry.descriptor.id := by
            intro entry hentry
            exact Nat.lt_trans hlt (hhead entry hentry)
          have hzero := descriptorReferenceCount_eq_zero_of_before
            descriptor.id tail hbefore
          rw [hzero] at hheldTail
          omega
        · have hheadlt : head.descriptor.id < descriptor.id := by
            change ¬ descriptor.id ≤ head.descriptor.id at hle
            exact Nat.lt_of_not_ge hle
          have heq' : descriptor.id ≠ head.descriptor.id := Ne.symm heq
          simp [incrementDescriptorReference, consumeDescriptorReference, heq,
            heq', hle, hinduction]

/--
Every descriptor returned by descriptor-only canonical insertion is either the inserted descriptor
or an existing public-result member (`executor/src/scalar.rs:577-590`).
-/
private theorem mem_installDescriptor_cases
    (candidate descriptor : PacketDescriptor)
    (store : List PacketDescriptor)
    (hmem : candidate ∈ installDescriptor descriptor store) :
    candidate = descriptor ∨ candidate ∈ store := by
  induction store with
  | nil =>
      simpa [installDescriptor] using hmem
  | cons head tail ih =>
      simp only [installDescriptor] at hmem
      split at hmem
      next =>
        exact Or.inr hmem
      next =>
        split at hmem
        next =>
          rcases List.mem_cons.mp hmem with rfl | hmem
          · exact Or.inl rfl
          · exact Or.inr hmem
        next =>
          rcases List.mem_cons.mp hmem with rfl | hmem
          · exact Or.inr (List.mem_cons_self)
          · rcases ih hmem with rfl | hold
            · exact Or.inl rfl
            · exact Or.inr (List.mem_cons_of_mem _ hold)

/--
Every counted acquisition returns either its new entry or an existing entry with updated count,
matching `executor/src/scalar.rs:1645-1658`.
-/
private theorem mem_incrementDescriptorReference_cases
    (candidate : PacketStoreEntry)
    (descriptor : PacketDescriptor)
    (store : List PacketStoreEntry)
    (hmem : candidate ∈ incrementDescriptorReference descriptor store) :
    candidate.descriptor = descriptor ∨
      ∃ existing ∈ store, candidate.descriptor = existing.descriptor := by
  induction store with
  | nil =>
      simp [incrementDescriptorReference] at hmem
      subst candidate
      exact Or.inl rfl
  | cons head tail ih =>
      simp only [incrementDescriptorReference] at hmem
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
          · rcases ih hmem with hnew | ⟨existing, hold, heq⟩
            · exact Or.inl hnew
            · exact Or.inr ⟨existing, List.mem_cons_of_mem _ hold, heq⟩

/--
Reference acquisition preserves strict payload ordering, matching Rust's counted
`BTreeMap<PayloadId, ResidentPacket>` at `executor/src/scalar.rs:367-381,1645-1658`.
-/
theorem incrementDescriptorReference_preserves_sorted
    (descriptor : PacketDescriptor)
    (store : List PacketStoreEntry)
    (hsorted : DescriptorStoreSorted store) :
    DescriptorStoreSorted (incrementDescriptorReference descriptor store) := by
  induction store with
  | nil =>
      simp [DescriptorStoreSorted, incrementDescriptorReference]
  | cons head tail ih =>
      have hhead := (List.pairwise_cons.mp hsorted).1
      have htail := (List.pairwise_cons.mp hsorted).2
      simp only [incrementDescriptorReference]
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
              change descriptor.id < head.descriptor.id
              change descriptor.id ≤ head.descriptor.id at hle
              change descriptor.id ≠ head.descriptor.id at hne
              exact Std.lt_of_le_of_ne hle hne
            · have hheadCurrent := hhead current hmem
              change descriptor.id ≤ head.descriptor.id at hle
              change descriptor.id ≠ head.descriptor.id at hne
              change head.descriptor.id < current.descriptor.id at hheadCurrent
              change descriptor.id < current.descriptor.id
              exact Nat.lt_trans (Std.lt_of_le_of_ne hle hne) hheadCurrent
          · exact hsorted
        next hnle =>
          apply List.pairwise_cons.mpr
          constructor
          · intro current hmem
            rcases mem_incrementDescriptorReference_cases current descriptor tail hmem with
              hcurrent | ⟨existing, hexisting, hcurrent⟩
            · rw [hcurrent]
              change head.descriptor.id < descriptor.id
              change ¬ descriptor.id ≤ head.descriptor.id at hnle
              exact Nat.lt_of_not_le hnle
            · change head.descriptor.id < current.descriptor.id
              rw [hcurrent]
              exact hhead existing hexisting
          · exact ih htail

/--
Reference consumption preserves strict payload ordering, including Rust's automatic zero-count
erase after checked decrement at `executor/src/scalar.rs:1661-1683`.
-/
theorem consumeDescriptorReference_preserves_sorted
    (payload : PayloadId)
    (store : List PacketStoreEntry)
    (hsorted : DescriptorStoreSorted store) :
    DescriptorStoreSorted (consumeDescriptorReference payload store) := by
  induction store with
  | nil =>
      simpa [consumeDescriptorReference] using hsorted
  | cons head tail ih =>
      have hhead := (List.pairwise_cons.mp hsorted).1
      have htail := (List.pairwise_cons.mp hsorted).2
      simp only [consumeDescriptorReference]
      split
      next =>
        split
        next =>
          exact htail
        next =>
          simpa [DescriptorStoreSorted] using hsorted
      next =>
        apply List.pairwise_cons.mpr
        constructor
        · intro current hmem
          have hdescriptor :
              ∃ existing ∈ tail, current.descriptor = existing.descriptor := by
            clear hhead htail hsorted ih
            induction tail with
            | nil =>
                simp [consumeDescriptorReference] at hmem
            | cons next rest nested =>
                simp only [consumeDescriptorReference] at hmem
                split at hmem
                next =>
                  split at hmem
                  next =>
                    exact ⟨current, List.mem_cons_of_mem _ hmem, rfl⟩
                  next =>
                    rcases List.mem_cons.mp hmem with rfl | hmem
                    · exact ⟨next, List.mem_cons_self, rfl⟩
                    · exact ⟨_, List.mem_cons_of_mem _ hmem, rfl⟩
                next =>
                  rcases List.mem_cons.mp hmem with rfl | hmem
                  · exact ⟨_, List.mem_cons_self, rfl⟩
                  · rcases nested hmem with ⟨existing, hexisting, heq⟩
                    exact ⟨existing, List.mem_cons_of_mem _ hexisting, heq⟩
          rcases hdescriptor with ⟨existing, hexisting, heq⟩
          change head.descriptor.id < current.descriptor.id
          rw [heq]
          exact hhead existing hexisting
        · exact ih htail

/--
A multiset of payload references is held when its multiplicity never exceeds the resident count.
This replaces the order-sensitive pending scan. Rust rejects transmitter underflow at
`executor/src/scalar.rs:1661-1678`; CPU pins and outboxes preserve other live ownership at
`executor/src/cpu.rs:563-569,627-667`.
-/
def PacketReferencesHeld
    (payloads : List PayloadId)
    (store : List PacketStoreEntry) : Prop :=
  ∀ payload ∈ payloads,
    payloads.count payload ≤ descriptorReferenceCount payload store

/--
Multiplicity-aware held-reference validity is executable for finite counted stores
(`executor/src/scalar.rs:1672-1683`).
-/
instance (payloads : List PayloadId) (store : List PacketStoreEntry) :
    Decidable (PacketReferencesHeld payloads store) := by
  unfold PacketReferencesHeld
  infer_instance

/--
Every held reference has a positive resident count, so a pending event, local child, or buffered
envelope cannot be stranded after Rust's zero-count drop
(`executor/src/scalar.rs:1679-1698`, `executor/src/cpu.rs:627-667`).
-/
theorem descriptorReferenceCount_positive_of_held
    (payloads : List PayloadId)
    (store : List PacketStoreEntry)
    (payload : PayloadId)
    (hheld : PacketReferencesHeld payloads store)
    (hmem : payload ∈ payloads) :
    0 < descriptorReferenceCount payload store := by
  have hcountNe : payloads.count payload ≠ 0 := by
    intro hzero
    exact (List.count_eq_zero.mp hzero) hmem
  have hcount : 0 < payloads.count payload := Nat.zero_lt_of_ne_zero hcountNe
  exact Nat.lt_of_lt_of_le hcount (hheld payload hmem)

/--
One transition cannot consume more references than its processing LP holds, matching Rust's
`checked_sub` error at `executor/src/scalar.rs:1672-1678`.
-/
def ReferenceConsumptionsValid
    (result : TransitionResult State kind)
    (store : List PacketStoreEntry) : Prop :=
  PacketReferencesHeld result.packetReferenceConsumptions store

/-- Checked transition consumption validity is executable (`executor/src/scalar.rs:1672-1678`). -/
instance
    (result : TransitionResult State kind)
    (store : List PacketStoreEntry) :
    Decidable (ReferenceConsumptionsValid result store) := by
  unfold ReferenceConsumptionsValid
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
Apply checked consumptions followed by explicit acquisitions. Zero-count removal is derived rather
than discretionary, matching `executor/src/scalar.rs:1645-1683`.
-/
def applyPacketEffects
    (result : TransitionResult State kind)
    (store : List PacketStoreEntry) : List PacketStoreEntry :=
  result.packetReferenceIncrements.foldl
    (fun current descriptor => incrementDescriptorReference descriptor current)
    (result.packetReferenceConsumptions.foldl
      (fun current payload => consumeDescriptorReference payload current)
      store)

/--
Every emitted child has a positive counted reference at its current holding LP. Locally queued
children and buffered remote envelopes both require descriptor availability at
`executor/src/cpu.rs:627-667`; exchange later transfers remote references.
-/
def ChildDescriptorsAvailable
    (image : SimulationImage State)
    (store : List PacketStoreEntry)
    (children : List Event) : Prop :=
  ∀ child ∈ children,
    ∃ entry ∈ store,
      entry.descriptor = image.packetDescriptor child.payload ∧
        0 < entry.references

/--
Every scalar child has a positive counted reference at its target LP. This is the per-LP projection
of the shared scalar map and immediate future insertion at `executor/src/scalar.rs:344-372`; CPU
execution reaches the same holdings after `executor/src/cpu.rs:4485-4500`.
-/
def ChildReferencesAvailableAtTargets
    (image : SimulationImage State)
    (machine : MachineState State)
    (children : List Event) : Prop :=
  ∀ child ∈ children,
    ∃ target ∈ image.nodes,
      target.id = child.target ∧
        ∃ entry ∈ machine.packetStore target,
          entry.descriptor = image.packetDescriptor child.payload ∧
            0 < entry.references

/--
Acquire references for children addressed to one LP. The scalar projection materializes them when
children enter the shared future list (`executor/src/scalar.rs:344-372`); the CPU projection does
so either locally or from exchanged envelopes at `executor/src/cpu.rs:649-667,4485-4500`.
-/
def installChildDescriptorsFor
    (image : SimulationImage State)
    (target : NodeId)
    (children : List Event)
    (store : List PacketStoreEntry) : List PacketStoreEntry :=
  children.foldl
    (fun current child =>
      if child.target = target then
        incrementDescriptorReference (image.packetDescriptor child.payload) current
      else
        current)
    store

/--
Hold every emitted child reference at its source LP until local insertion or remote exchange. This
is the counted abstraction of the CPU child/outbox loop at `executor/src/cpu.rs:649-667`.
-/
def holdEmittedChildReferences
    (image : SimulationImage State)
    (children : List Event)
    (store : List PacketStoreEntry) : List PacketStoreEntry :=
  children.foldl
    (fun current child =>
      incrementDescriptorReference (image.packetDescriptor child.payload) current)
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
    (result : TransitionResult State node.kind)
    (before after : MachineState State) : Prop :=
  after.localState node = result.nextState ∧
    after.packetStore node =
      holdEmittedChildReferences image result.children
        (applyPacketEffects result (before.packetStore node)) ∧
    (∀ other ∈ image.nodes,
      other.id ≠ node.id →
      after.localState other = before.localState other ∧
        after.packetStore other = before.packetStore other) ∧
    applyRecordedOutput result before after ∧
    ReferenceConsumptionsValid result (before.packetStore node)

/--
Scalar transition application in the counted per-LP projection. A processed event consumes only a
held reference, while each child reference is acquired immediately at its target. The CPU path
temporarily holds remote children at the source and transfers them at exchange; the count
commutation lemmas above make these timing choices converge. Rust sites:
`executor/src/scalar.rs:344-372,1645-1683` and `executor/src/cpu.rs:649-667,4485-4500`.
-/
def AppliesScalarTransitionResult
    (image : SimulationImage State)
    (node : NodeDescriptor)
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
    ReferenceConsumptionsValid result (before.packetStore node)

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
      AppliesScalarTransitionResult image node result before after ∧
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
Canonical initial counted store derived for each event-owning LP. Rust's CPU constructor derives
the ownership partition and in-service holdings at `executor/src/cpu.rs:3039-3197`; the Lean count
also represents event/pin ownership.
-/
def initialPacketStore
    (image : SimulationImage State)
    (node : NodeDescriptor) : List PacketStoreEntry :=
  image.initialPacketStore node.id

/--
Structural machine invariant used at round boundaries: pending events are declared and supported,
their lifetime keys remain allocated below the corresponding cursor, and every target LP holds the
pending payload multiset in positive counted entries. This is the count form of CPU pin/future
ownership at `executor/src/cpu.rs:563-569,627-635,3389-3397`.
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
            roleSupports node.kind event.kind ∧
            PacketReferencesHeld
              ((machine.pending.filter fun pending => pending.target = node.id).map Event.payload)
              (machine.packetStore node)) ∧
    (∀ node ∈ image.nodes,
      DescriptorStoreCoherent image (machine.packetStore node)) ∧
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

/--
Exact `(descriptor, positive reference count)` holdings at every declared LP. Rust's public
`RunResult` forgets these counts during assembly at `executor/src/cpu.rs:3372-3433`, but the
semantic equality retains them to support dependent suffixes.
-/
def PerLPDescriptorStoresEqual
    (image : SimulationImage State)
    (left right : MachineState State) : Prop :=
  ∀ node ∈ image.nodes,
    left.packetStore node = right.packetStore node

/--
Complete normalized-result equality used by all cross-executor claims. Reference counts, descriptor
identity, summary counters, full observations, pending events, and both role arenas must all agree;
only proof ghosts are excluded. The count component models the checked lifetime updates at
`executor/src/scalar.rs:1645-1698`.

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
