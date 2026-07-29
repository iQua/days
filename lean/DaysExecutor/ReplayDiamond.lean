import DaysExecutor.ReplayAlgebra
import DaysExecutor.RoundReplay

namespace DaysExecutor

/-!
Exact-owner and cross-LP scalar replay lemmas.  The final diamond theorem is deliberately stated
only after all intermediate `AvailableEventStep` obligations have been reconstructed; no
enabledness assumption is hidden in the replay relation.
-/

private theorem listBagDifference_sublist [BEq α]
    (source removed : List α) :
    (listBagDifference source removed).Sublist source := by
  induction removed generalizing source with
  | nil =>
      exact .refl _
  | cons head tail ih =>
      simp only [listBagDifference, List.foldl_cons]
      exact (ih (source.erase head)).trans List.erase_sublist

private theorem cons_subbag_append
    [BEq α] [LawfulBEq α]
    (item reference : α)
    (small left right : List α)
    (hitem : item ∈ left)
    (hsmall : small.Sublist right) :
    (item :: small).count reference ≤
      (left ++ right).count reference := by
  have hsmallCount := hsmall.count_le reference
  rw [List.count_append]
  by_cases heq : item = reference
  · subst item
    rw [List.count_cons_self]
    have hpositive : 0 < left.count reference :=
      List.count_pos_iff.mpr hitem
    omega
  · rw [List.count_cons_of_ne heq]
    omega

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

set_option linter.unusedSimpArgs false in
theorem acquire_releaseOwnedReference_commutes_of_distinct_payload_sorted
    (acquired released : OwnedPacketReference)
    (hne : acquired.descriptor.id ≠ released.descriptor.id)
    (store : List PacketStoreEntry)
    (hsorted : DescriptorStoreSorted store) :
    releaseOwnedReference released (acquireOwnedReference acquired store) =
      acquireOwnedReference acquired (releaseOwnedReference released store) := by
  induction store with
  | nil =>
      simp [acquireOwnedReference, releaseOwnedReference, hne]
  | cons head tail ih =>
      have hhead := (List.pairwise_cons.mp hsorted).1
      have htail := (List.pairwise_cons.mp hsorted).2
      rcases Nat.lt_trichotomy acquired.descriptor.id head.descriptor.id with
        ha | ha | ha
      · have hacquiredNe : acquired.descriptor.id ≠ head.descriptor.id :=
          Nat.ne_of_lt ha
        have hacquiredLE :
            descriptorLE acquired.descriptor head.descriptor :=
          Nat.le_of_lt ha
        by_cases hreleased : head.descriptor.id = released.descriptor.id
        · have hreleasedAcquired :
              released.descriptor.id ≠ acquired.descriptor.id := by
            exact fun heq => hne heq.symm
          by_cases hempty :
              (head.owners.erase released.owner).isEmpty = true
          · have hbefore : ∀ entry ∈ tail,
                acquired.descriptor.id < entry.descriptor.id := by
              intro entry hentry
              exact Nat.lt_trans ha (hhead entry hentry)
            simp only [acquireOwnedReference, releaseOwnedReference,
              hacquiredNe, hacquiredLE, hreleased, hne,
              hreleasedAcquired, hempty, ↓reduceIte]
            exact (acquireOwnedReference_before acquired tail hbefore).symm
          · have hbefore :
                ∀ entry ∈
                    ({ head with owners := head.owners.erase released.owner } ::
                      tail),
                  acquired.descriptor.id < entry.descriptor.id := by
              intro entry hentry
              rcases List.mem_cons.mp hentry with rfl | hentry
              · exact ha
              · exact Nat.lt_trans ha (hhead entry hentry)
            simp only [acquireOwnedReference, releaseOwnedReference,
              hacquiredNe, hacquiredLE, hreleased, hne,
              hreleasedAcquired, hempty, ↓reduceIte]
            exact (acquireOwnedReference_before acquired _ hbefore).symm
        · have hnewReleased :
              acquired.descriptor.id ≠ released.descriptor.id := hne
          simp only [acquireOwnedReference, releaseOwnedReference,
            hacquiredNe, hacquiredLE, hreleased, hnewReleased,
            ↓reduceIte]
      · have hheadReleased :
            head.descriptor.id ≠ released.descriptor.id := by
          intro heq
          exact hne (ha.trans heq)
        simp only [acquireOwnedReference, releaseOwnedReference,
          ha, hheadReleased, ↓reduceIte]
      · have hacquiredNe :
            acquired.descriptor.id ≠ head.descriptor.id := Nat.ne_of_gt ha
        have hacquiredNLE :
            ¬ descriptorLE acquired.descriptor head.descriptor :=
          Nat.not_le_of_gt ha
        by_cases hreleased : head.descriptor.id = released.descriptor.id
        · have hacquiredReleased :
              acquired.descriptor.id ≠ released.descriptor.id := hne
          by_cases hempty :
              (head.owners.erase released.owner).isEmpty = true
          · simp [acquireOwnedReference, releaseOwnedReference,
              hacquiredNe, hacquiredNLE, hreleased,
              hacquiredReleased, hempty]
          · have hmodifiedNe :
                acquired.descriptor.id ≠
                  ({ head with owners := head.owners.erase released.owner } :
                    PacketStoreEntry).descriptor.id := hacquiredNe
            have hmodifiedNLE :
                ¬ descriptorLE acquired.descriptor
                  ({ head with owners := head.owners.erase released.owner } :
                    PacketStoreEntry).descriptor := hacquiredNLE
            simp only [acquireOwnedReference, releaseOwnedReference,
              hacquiredNe, hacquiredNLE, hreleased,
              hacquiredReleased, hempty, hmodifiedNe, hmodifiedNLE,
              ↓reduceIte]
            simp [acquireOwnedReference, hmodifiedNe, hmodifiedNLE]
        · simp only [acquireOwnedReference, releaseOwnedReference,
            hacquiredNe, hacquiredNLE, hreleased, ↓reduceIte]
          exact congrArg (head :: ·) (ih htail)

theorem acquire_releaseOwnedReference_commutes_of_oracle
    (image : SimulationImage State)
    (acquired released : OwnedPacketReference)
    (hacquired :
      acquired.descriptor =
        image.packetDescriptor acquired.descriptor.id)
    (hreleased :
      released.descriptor =
        image.packetDescriptor released.descriptor.id)
    (howner : acquired.owner ≠ released.owner)
    (store : List PacketStoreEntry)
    (hcoherent : DescriptorStoreCoherent image store)
    (hheld : 0 < ownedReferenceCount released store) :
    releaseOwnedReference released (acquireOwnedReference acquired store) =
      acquireOwnedReference acquired (releaseOwnedReference released store) := by
  by_cases hid : acquired.descriptor.id = released.descriptor.id
  · have hdescriptor : acquired.descriptor = released.descriptor := by
      rw [hacquired, hreleased, hid]
    apply acquire_releaseOwnedReference_commutes_of_distinct_owner
      acquired released hdescriptor howner store hcoherent.1
    · intro entry hentry hentryId
      rw [hcoherent.2.2 entry hentry |>.2.2, hacquired, hentryId]
    · exact hheld
  · exact
      acquire_releaseOwnedReference_commutes_of_distinct_payload_sorted
        acquired released hid store hcoherent.1

private theorem ownedEventReference_oracle
    (image : SimulationImage State)
    (horacle : DescriptorOracleWellFormed image)
    (event : Event) :
    (ownedEventReference image event).descriptor =
      image.packetDescriptor
        (ownedEventReference image event).descriptor.id := by
  simp only [ownedEventReference]
  rw [(horacle event.payload).1]

private theorem ownedRoleStateReferences_oracle
    (image : SimulationImage State)
    (horacle : DescriptorOracleWellFormed image)
    (node : NodeDescriptor)
    (state : RoleState State node.kind)
    (reference : OwnedPacketReference)
    (hreference :
      reference ∈ ownedRoleStateReferences image node state) :
    reference.descriptor =
      image.packetDescriptor reference.descriptor.id := by
  unfold ownedRoleStateReferences at hreference
  rw [List.mem_append] at hreference
  rcases hreference with hqueue | hservice
  · rcases List.mem_map.mp hqueue with ⟨payload, _, rfl⟩
    simp only [ownedQueueReference]
    rw [(horacle payload).1]
  · rcases List.mem_map.mp hservice with ⟨payload, _, rfl⟩
    simp only [ownedInServiceReference]
    rw [(horacle payload).1]

theorem machineOwnedReferencesFor_oracle
    (image : SimulationImage State)
    (horacle : DescriptorOracleWellFormed image)
    (machine : MachineState State)
    (node : NodeDescriptor)
    (reference : OwnedPacketReference)
    (hreference :
      reference ∈ machineOwnedReferencesFor image machine node) :
    reference.descriptor =
      image.packetDescriptor reference.descriptor.id := by
  unfold machineOwnedReferencesFor at hreference
  rw [List.mem_append] at hreference
  rcases hreference with hpending | hstate
  · rcases List.mem_map.mp hpending with ⟨event, _, rfl⟩
    exact ownedEventReference_oracle image horacle event
  · exact
      ownedRoleStateReferences_oracle image horacle node
        (machine.localState node) reference hstate

/--
At a well-formed scalar machine, the pending event and every role-state owner structurally removed
by its handler are held before the step.  This is the exact-owner fact needed when moving a step to
the left of an independent cross-LP step.
-/
theorem structuralConsumptionsHeld_of_machineWellFormed
    (image : SimulationImage State)
    (machine : MachineState State)
    (hwellFormed : MachineWellFormed image machine)
    (node : NodeDescriptor)
    (hnode : node ∈ image.nodes)
    (event : Event)
    (hevent : event ∈ machine.pending)
    (htarget : event.target = node.id)
    (nextState : RoleState State node.kind) :
    PacketReferencesHeld
      (ownedEventReference image event ::
        stateReferenceConsumptions image node
          (machine.localState node) nextState)
      (machine.packetStore node) := by
  rcases hwellFormed with ⟨_, _, _, _, _, hmatches, _⟩
  have heventFiltered :
      event ∈
        machine.pending.filter
          (fun candidate => candidate.target = node.id) := by
    exact List.mem_filter.mpr
      ⟨hevent, by simpa only [decide_eq_true_eq] using htarget⟩
  have heventOwned :
      ownedEventReference image event ∈
        (machine.pending.filter
          (fun candidate => candidate.target = node.id)).map
            (ownedEventReference image) :=
    List.mem_map_of_mem heventFiltered
  let pendingReferences :=
    (machine.pending.filter
      (fun candidate => candidate.target = node.id)).map
        (ownedEventReference image)
  let stateReferences :=
    ownedRoleStateReferences image node (machine.localState node)
  let consumedStateReferences :=
    stateReferenceConsumptions image node
      (machine.localState node) nextState
  have hconsumedSub :
      consumedStateReferences.Sublist stateReferences := by
    exact listBagDifference_sublist _ _
  have hmatch := hmatches node hnode
  intro reference hreference
  have hincluded :
      (ownedEventReference image event :: consumedStateReferences).count reference ≤
        (pendingReferences ++ stateReferences).count reference := by
    exact cons_subbag_append _ _ _ _ _
      (by simpa [pendingReferences] using heventOwned)
      hconsumedSub
  have hreferenceMachine :
      reference ∈ pendingReferences ++ stateReferences := by
    rcases List.mem_cons.mp hreference with heq | hstate
    · subst reference
      exact List.mem_append_left _
        (by simpa [pendingReferences] using heventOwned)
    · exact List.mem_append_right _ (hconsumedSub.subset hstate)
  unfold machineOwnedReferencesFor at hmatch
  change OwnedReferencesMatchStore
    (pendingReferences ++ stateReferences)
    (machine.packetStore node) at hmatch
  rw [← hmatch.1 reference hreferenceMachine]
  exact hincluded

/--
In an observed inverted cross-LP pair, the smaller second event was already pending before the
first step.  It cannot be a child of the first step because accepted children strictly advance
their parent key.
-/
theorem right_mem_before_of_inverted_steps
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (hadvance : ChildrenAdvanceParent transition)
    (left right : Event)
    (before afterLeft afterLeftRight : MachineState State)
    (hleft :
      AvailableEventStep image transition left before afterLeft)
    (hright :
      AvailableEventStep image transition right afterLeft afterLeftRight)
    (hkey : right.key < left.key) :
    right ∈ before.pending := by
  rcases hleft with
    ⟨_, node, _, result, _, htransition, _, _, _, _, _, hpending, _⟩
  have hrightAfter : right ∈ afterLeft.pending := hright.1
  rw [hpending, mem_insertEvents_iff] at hrightAfter
  rcases hrightAfter with hchild | hremaining
  · have hforward := hadvance node left (before.localState node)
      result htransition right hchild
    exact False.elim
      (EventKey.lt_irrefl right.key
        (EventKey.lt_trans hkey hforward))
  · exact List.mem_of_mem_erase hremaining

end DaysExecutor
