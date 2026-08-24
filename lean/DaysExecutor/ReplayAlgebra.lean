import DaysExecutor.Execution

namespace DaysExecutor

/-!
Pure list and normalization algebra used by CPU/scalar replay proofs.  This module deliberately
contains no machine-step semantics.
-/

theorem EventKey.lt_implies_le {left right : EventKey} (hlt : left < right) :
    left ≤ right := by
  change EventKey.lexLT left right at hlt
  change EventKey.lexLE left right
  unfold EventKey.lexLT at hlt
  unfold EventKey.lexLE
  omega

theorem EventKey.lt_of_le_of_ne
    {left right : EventKey}
    (hle : left ≤ right)
    (hne : left ≠ right) :
    left < right := by
  rcases EventKey.lt_trichotomy left right with hlt | heq | hgt
  · exact hlt
  · exact False.elim (hne heq)
  · exact False.elim (hne (EventKey.le_antisymm hle (EventKey.lt_implies_le hgt)))

theorem EventKey.lt_of_not_ge {left right : EventKey} (hnot : ¬ right ≤ left) :
    left < right := by
  rcases EventKey.lt_trichotomy left right with hlt | heq | hgt
  · exact hlt
  · exact False.elim (hnot (heq ▸ EventKey.le_refl right))
  · exact False.elim (hnot (EventKey.lt_implies_le hgt))

theorem EventKey.not_le_of_lt {left right : EventKey} (hlt : left < right) :
    ¬ right ≤ left := by
  intro hreverse
  have heq :=
    EventKey.le_antisymm (EventKey.lt_implies_le hlt) hreverse
  subst right
  exact EventKey.lt_irrefl left hlt

theorem mem_insertEvent_iff
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
        simp [or_left_comm]

theorem mem_insertEvents_iff
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
      rw [ih, mem_insertEvent_iff]
      simp [or_left_comm, or_assoc]

theorem insertEvent_perm_cons
    (event : Event)
    (pending : List Event) :
    List.Perm (insertEvent event pending) (event :: pending) := by
  induction pending with
  | nil =>
      exact .refl _
  | cons head tail ih =>
      simp only [insertEvent]
      split
      · exact .refl _
      · exact (ih.cons head).trans (List.Perm.swap head event tail).symm

theorem insertEvents_perm_append
    (children pending : List Event) :
    List.Perm (insertEvents children pending) (children ++ pending) := by
  induction children generalizing pending with
  | nil =>
      exact .refl _
  | cons child tail ih =>
      simp only [insertEvents, List.foldl_cons, List.cons_append]
      exact
        (ih (insertEvent child pending)).trans
          (((insertEvent_perm_cons child pending).append_left tail).trans
            List.perm_middle)

theorem canonicalPending_insertEvent
    (event : Event)
    (pending : List Event)
    (hcanonical : CanonicalPending pending)
    (hfresh : ∀ other ∈ pending, event.key ≠ other.key) :
    CanonicalPending (insertEvent event pending) := by
  induction pending with
  | nil =>
      simp [CanonicalPending, insertEvent]
  | cons head tail ih =>
      have hhead := (List.pairwise_cons.mp hcanonical).1
      have htail := (List.pairwise_cons.mp hcanonical).2
      have hkeyNe : event.key ≠ head.key :=
        hfresh head List.mem_cons_self
      simp only [insertEvent]
      split
      next hle =>
        apply List.pairwise_cons.mpr
        constructor
        · intro candidate hcandidate
          rcases List.mem_cons.mp hcandidate with rfl | hcandidate
          · exact EventKey.lt_of_le_of_ne hle hkeyNe
          · exact EventKey.lt_trans
              (EventKey.lt_of_le_of_ne hle hkeyNe)
              (hhead candidate hcandidate)
        · exact hcanonical
      next hnot =>
        apply List.pairwise_cons.mpr
        constructor
        · intro candidate hcandidate
          rw [mem_insertEvent_iff] at hcandidate
          rcases hcandidate with rfl | hcandidate
          · exact EventKey.lt_of_not_ge hnot
          · exact hhead candidate hcandidate
        · apply ih htail
          intro other hother
          exact hfresh other (List.mem_cons_of_mem head hother)

private theorem freshEventKeys_tail_insertEvent
    (head : Event)
    (tail pending : List Event)
    (hnodup : ((head :: tail).map Event.key).Nodup)
    (hfresh : FreshEventKeys (head :: tail) pending) :
    FreshEventKeys tail (insertEvent head pending) := by
  intro child hchild other hother
  rw [mem_insertEvent_iff] at hother
  rcases hother with heq | hother
  · subst other
    have hheadNotMem :
        Event.key head ∉ tail.map Event.key :=
      (List.nodup_cons.mp hnodup).1
    intro heq
    apply hheadNotMem
    rw [List.mem_map]
    exact ⟨child, hchild, heq⟩
  · exact hfresh child (List.mem_cons_of_mem head hchild) other hother

theorem canonicalPending_insertEvents
    (children pending : List Event)
    (hcanonical : CanonicalPending pending)
    (hunique : UniqueEventKeys children)
    (hfresh : FreshEventKeys children pending) :
    CanonicalPending (insertEvents children pending) := by
  induction children generalizing pending with
  | nil =>
      exact hcanonical
  | cons head tail ih =>
      simp only [insertEvents, List.foldl_cons]
      apply ih
      · apply canonicalPending_insertEvent head pending hcanonical
        intro other hother
        exact hfresh head List.mem_cons_self other hother
      · exact (List.nodup_cons.mp hunique).2
      · exact freshEventKeys_tail_insertEvent head tail pending hunique hfresh

theorem canonicalPending_nodup
    {pending : List Event}
    (hcanonical : CanonicalPending pending) :
    pending.Nodup := by
  induction pending with
  | nil =>
      exact .nil
  | cons head tail ih =>
      apply List.nodup_cons.mpr
      constructor
      · intro hmem
        have hlt := (List.pairwise_cons.mp hcanonical).1 head hmem
        exact EventKey.lt_irrefl head.key hlt
      · exact ih (List.pairwise_cons.mp hcanonical).2

theorem perm_of_nodup_of_mem_iff
    [BEq α] [LawfulBEq α]
    {left right : List α}
    (hleft : left.Nodup)
    (hright : right.Nodup)
    (hmem : ∀ item, item ∈ left ↔ item ∈ right) :
    List.Perm left right := by
  induction left generalizing right with
  | nil =>
      have : right = [] := by
        cases right with
        | nil => rfl
        | cons head tail =>
            have := (hmem head).mpr List.mem_cons_self
            simp at this
      subst right
      exact .refl _
  | cons head tail ih =>
      have hheadRight : head ∈ right :=
        (hmem head).mp List.mem_cons_self
      have hrightErase : (right.erase head).Nodup :=
        hright.erase head
      have htail : tail.Nodup :=
        (List.nodup_cons.mp hleft).2
      have htailMem :
          ∀ item, item ∈ tail ↔ item ∈ right.erase head := by
        intro item
        by_cases heq : item = head
        · subst item
          constructor
          · exact fun hitem =>
              False.elim ((List.nodup_cons.mp hleft).1 hitem)
          · exact fun hitem =>
              False.elim (hright.not_mem_erase hitem)
        · rw [hright.mem_erase_iff]
          constructor
          · intro hitem
            exact ⟨heq, (hmem item).mp (List.mem_cons_of_mem head hitem)⟩
          · rintro ⟨_, hitem⟩
            have hleftMem := (hmem item).mpr hitem
            rw [List.mem_cons] at hleftMem
            rcases hleftMem with hsame | hitemTail
            · exact False.elim (heq hsame)
            · exact hitemTail
      exact
        ((ih htail hrightErase htailMem).cons head).trans
          (List.perm_cons_erase hheadRight).symm

theorem canonicalPending_eq_of_perm
    {left right : List Event}
    (hleft : CanonicalPending left)
    (hright : CanonicalPending right)
    (hperm : List.Perm left right) :
    left = right := by
  induction left generalizing right with
  | nil =>
      exact hperm.nil_eq
  | cons head tail ih =>
      cases right with
      | nil =>
          exact False.elim (by simpa using hperm.length_eq)
      | cons other rest =>
          have hheadMem : head ∈ other :: rest :=
            hperm.subset List.mem_cons_self
          have hotherMem : other ∈ head :: tail :=
            hperm.symm.subset List.mem_cons_self
          have hheadEq : head = other := by
            rcases List.mem_cons.mp hheadMem with heq | hheadRest
            · exact heq
            · rcases List.mem_cons.mp hotherMem with heq | hotherTail
              · exact heq.symm
              · have hheadLt :=
                  (List.pairwise_cons.mp hleft).1 other hotherTail
                have hotherLt :=
                  (List.pairwise_cons.mp hright).1 head hheadRest
                exact False.elim
                  (EventKey.lt_irrefl head.key
                    (EventKey.lt_trans hheadLt hotherLt))
          subst other
          congr 1
          exact ih
            (List.pairwise_cons.mp hleft).2
            (List.pairwise_cons.mp hright).2
            hperm.cons_inv

theorem canonicalPending_eq_of_mem_iff
    {left right : List Event}
    (hleft : CanonicalPending left)
    (hright : CanonicalPending right)
    (hmem : ∀ event, event ∈ left ↔ event ∈ right) :
    left = right := by
  apply canonicalPending_eq_of_perm hleft hright
  exact perm_of_nodup_of_mem_iff
    (canonicalPending_nodup hleft)
    (canonicalPending_nodup hright)
    hmem

theorem RunSummary.add_comm (left right : RunSummary) :
    RunSummary.add left right = RunSummary.add right left := by
  cases left
  cases right
  simp [RunSummary.add, Nat.add_comm]

theorem RunSummary.add_assoc (first second third : RunSummary) :
    RunSummary.add (RunSummary.add first second) third =
      RunSummary.add first (RunSummary.add second third) := by
  cases first
  cases second
  cases third
  simp [RunSummary.add, Nat.add_assoc]

theorem RunSummary.add_left_comm (first second third : RunSummary) :
    RunSummary.add (RunSummary.add first second) third =
      RunSummary.add (RunSummary.add first third) second := by
  rw [RunSummary.add_assoc, RunSummary.add_comm second third,
    ← RunSummary.add_assoc]

theorem foldl_runSummary_add_eq_of_perm
    {left right : List RunSummary}
    (hperm : List.Perm left right)
    (initial : RunSummary) :
    left.foldl RunSummary.add initial =
      right.foldl RunSummary.add initial := by
  apply hperm.foldl_eq'
  intro first _ second _ current
  exact RunSummary.add_left_comm current first second

set_option linter.unusedSimpArgs false in
private theorem installDescriptor_commutes_of_distinct_id
    (left right : PacketDescriptor)
    (hne : left.id ≠ right.id)
    (store : List PacketDescriptor) :
    installDescriptor left (installDescriptor right store) =
      installDescriptor right (installDescriptor left store) := by
  induction store with
  | nil =>
      rcases Nat.le_total left.id right.id with hlr | hrl
      · have hnot : ¬ right.id ≤ left.id :=
          fun h => hne (Nat.le_antisymm hlr h)
        simp only [installDescriptor, hne, Ne.symm hne, descriptorLE, hlr,
          hnot, ↓reduceIte]
      · have hnot : ¬ left.id ≤ right.id :=
          fun h => hne (Nat.le_antisymm h hrl)
        simp only [installDescriptor, hne, Ne.symm hne, descriptorLE, hrl,
          hnot, ↓reduceIte]
  | cons head tail ih =>
      by_cases hrEq : right.id = head.id
      · have hlEq : left.id ≠ head.id :=
          fun h => hne (h.trans hrEq.symm)
        by_cases hlLe : left.id ≤ head.id
        · have hnot : ¬ head.id ≤ left.id :=
            fun h => hlEq (Nat.le_antisymm hlLe h)
          simp only [installDescriptor, hrEq, hlEq, Ne.symm hlEq, hne,
            Ne.symm hne, descriptorLE, hlLe, hnot, ↓reduceIte]
        · simp only [installDescriptor, hrEq, hlEq, Ne.symm hlEq, hne,
            Ne.symm hne, descriptorLE, hlLe, ↓reduceIte]
      · by_cases hlEq : left.id = head.id
        · by_cases hrLe : right.id ≤ head.id
          · have hnot : ¬ head.id ≤ right.id :=
              fun h => hrEq (Nat.le_antisymm hrLe h)
            simp only [installDescriptor, hrEq, hlEq, Ne.symm hrEq, hne,
              Ne.symm hne, descriptorLE, hrLe, hnot, ↓reduceIte]
          · simp only [installDescriptor, hrEq, hlEq, Ne.symm hrEq, hne,
              Ne.symm hne, descriptorLE, hrLe, ↓reduceIte]
        · by_cases hrLe : right.id ≤ head.id
          · by_cases hlLe : left.id ≤ head.id
            · rcases Nat.le_total left.id right.id with hlr | hrl
              · have hnot : ¬ right.id ≤ left.id :=
                  fun h => hne (Nat.le_antisymm hlr h)
                simp only [installDescriptor, hrEq, hlEq, Ne.symm hrEq,
                  Ne.symm hlEq, hne, Ne.symm hne, descriptorLE, hrLe,
                  hlLe, hlr, hnot, ↓reduceIte]
              · have hnot : ¬ left.id ≤ right.id :=
                  fun h => hne (Nat.le_antisymm h hrl)
                simp only [installDescriptor, hrEq, hlEq, Ne.symm hrEq,
                  Ne.symm hlEq, hne, Ne.symm hne, descriptorLE, hrLe,
                  hlLe, hrl, hnot, ↓reduceIte]
            · have hnot : ¬ left.id ≤ right.id :=
                fun h => hlLe (Nat.le_trans h hrLe)
              simp only [installDescriptor, hrEq, hlEq, Ne.symm hrEq,
                Ne.symm hlEq, hne, Ne.symm hne, descriptorLE, hrLe, hlLe,
                hnot, ↓reduceIte]
          · by_cases hlLe : left.id ≤ head.id
            · have hnot : ¬ right.id ≤ left.id :=
                fun h => hrLe (Nat.le_trans h hlLe)
              simp only [installDescriptor, hrEq, hlEq, Ne.symm hrEq,
                Ne.symm hlEq, hne, Ne.symm hne, descriptorLE, hrLe, hlLe,
                hnot, ↓reduceIte]
            · simp only [installDescriptor, hrEq, hlEq, descriptorLE, hrLe,
                hlLe, ↓reduceIte, ih]

theorem installDescriptor_commutes
    (left right : PacketDescriptor)
    (hcompatible : left.id = right.id → left = right)
    (store : List PacketDescriptor) :
    installDescriptor left (installDescriptor right store) =
      installDescriptor right (installDescriptor left store) := by
  by_cases hid : left.id = right.id
  · rw [hcompatible hid]
  · exact installDescriptor_commutes_of_distinct_id left right hid store

theorem foldl_installDescriptor_eq_of_perm
    {left right : List PacketDescriptor}
    (hperm : List.Perm left right)
    (hcompatible :
      ∀ first ∈ left, ∀ second ∈ left,
        first.id = second.id → first = second)
    (initial : List PacketDescriptor) :
    left.foldl
        (fun current descriptor => installDescriptor descriptor current)
        initial =
      right.foldl
        (fun current descriptor => installDescriptor descriptor current)
        initial := by
  apply hperm.foldl_eq'
  intro first hfirst second hsecond current
  exact (installDescriptor_commutes first second
    (hcompatible first hfirst second hsecond) current).symm

theorem foldl_installDescriptor_batches_commute
    (left right : List PacketDescriptor)
    (hcompatible :
      ∀ first ∈ left ++ right, ∀ second ∈ left ++ right,
        first.id = second.id → first = second)
    (initial : List PacketDescriptor) :
    right.foldl
        (fun current descriptor => installDescriptor descriptor current)
        (left.foldl
          (fun current descriptor => installDescriptor descriptor current)
          initial) =
      left.foldl
        (fun current descriptor => installDescriptor descriptor current)
        (right.foldl
          (fun current descriptor => installDescriptor descriptor current)
          initial) := by
  simpa only [List.foldl_append] using
    foldl_installDescriptor_eq_of_perm
      (left := left ++ right)
      (right := right ++ left)
      List.perm_append_comm hcompatible initial

theorem insertDeparture_commutes_of_key_ne
    (left right : RecordedDeparture)
    (hne : left.eventKey ≠ right.eventKey)
    (records : List RecordedDeparture) :
    insertDeparture left (insertDeparture right records) =
      insertDeparture right (insertDeparture left records) := by
  induction records with
  | nil =>
      rcases EventKey.le_total left.eventKey right.eventKey with hle | hge
      · have hnot : ¬ right.eventKey ≤ left.eventKey := by
          intro hreverse
          exact hne (EventKey.le_antisymm hle hreverse)
        simp [insertDeparture, hle, hnot]
      · have hnot : ¬ left.eventKey ≤ right.eventKey := by
          intro hforward
          exact hne (EventKey.le_antisymm hforward hge)
        simp [insertDeparture, hge, hnot]
  | cons head tail ih =>
      by_cases hr : right.eventKey ≤ head.eventKey
      · by_cases hl : left.eventKey ≤ head.eventKey
        · rcases EventKey.le_total left.eventKey right.eventKey with hlr | hrl
          · have hnot : ¬ right.eventKey ≤ left.eventKey := by
              intro hreverse
              exact hne (EventKey.le_antisymm hlr hreverse)
            simp only [insertDeparture, hr, hl, hlr, hnot, ↓reduceIte]
          · have hnot : ¬ left.eventKey ≤ right.eventKey := by
              intro hforward
              exact hne (EventKey.le_antisymm hforward hrl)
            simp only [insertDeparture, hr, hl, hrl, hnot, ↓reduceIte]
        · have hnotLR : ¬ left.eventKey ≤ right.eventKey := by
            intro hlr
            exact hl (EventKey.le_trans hlr hr)
          simp only [insertDeparture, hr, hl, hnotLR, ↓reduceIte]
      · by_cases hl : left.eventKey ≤ head.eventKey
        · have hnotRL : ¬ right.eventKey ≤ left.eventKey := by
            intro hrl
            exact hr (EventKey.le_trans hrl hl)
          simp only [insertDeparture, hr, hl, hnotRL, ↓reduceIte]
        · simp only [insertDeparture, hr, hl, ↓reduceIte, ih]

theorem insertArrival_commutes_of_key_ne
    (left right : RecordedArrival)
    (hne : left.eventKey ≠ right.eventKey)
    (records : List RecordedArrival) :
    insertArrival left (insertArrival right records) =
      insertArrival right (insertArrival left records) := by
  induction records with
  | nil =>
      rcases EventKey.le_total left.eventKey right.eventKey with hle | hge
      · have hnot : ¬ right.eventKey ≤ left.eventKey := by
          intro hreverse
          exact hne (EventKey.le_antisymm hle hreverse)
        simp [insertArrival, hle, hnot]
      · have hnot : ¬ left.eventKey ≤ right.eventKey := by
          intro hforward
          exact hne (EventKey.le_antisymm hforward hge)
        simp [insertArrival, hge, hnot]
  | cons head tail ih =>
      by_cases hr : right.eventKey ≤ head.eventKey
      · by_cases hl : left.eventKey ≤ head.eventKey
        · rcases EventKey.le_total left.eventKey right.eventKey with hlr | hrl
          · have hnot : ¬ right.eventKey ≤ left.eventKey := by
              intro hreverse
              exact hne (EventKey.le_antisymm hlr hreverse)
            simp only [insertArrival, hr, hl, hlr, hnot, ↓reduceIte]
          · have hnot : ¬ left.eventKey ≤ right.eventKey := by
              intro hforward
              exact hne (EventKey.le_antisymm hforward hrl)
            simp only [insertArrival, hr, hl, hrl, hnot, ↓reduceIte]
        · have hnotLR : ¬ left.eventKey ≤ right.eventKey := by
            intro hlr
            exact hl (EventKey.le_trans hlr hr)
          simp only [insertArrival, hr, hl, hnotLR, ↓reduceIte]
      · by_cases hl : left.eventKey ≤ head.eventKey
        · have hnotRL : ¬ right.eventKey ≤ left.eventKey := by
            intro hrl
            exact hr (EventKey.le_trans hrl hl)
          simp only [insertArrival, hr, hl, hnotRL, ↓reduceIte]
        · simp only [insertArrival, hr, hl, ↓reduceIte, ih]

private theorem foldl_insertDeparture_commutes_with_one
    (record : RecordedDeparture)
    (batch : List RecordedDeparture)
    (hkeys :
      ∀ other ∈ batch, record.eventKey ≠ other.eventKey)
    (initial : List RecordedDeparture) :
    batch.foldl
        (fun current other => insertDeparture other current)
        (insertDeparture record initial) =
      insertDeparture record
        (batch.foldl
          (fun current other => insertDeparture other current)
          initial) := by
  induction batch generalizing initial with
  | nil =>
      rfl
  | cons head tail ih =>
      simp only [List.foldl_cons]
      rw [← insertDeparture_commutes_of_key_ne record head
        (hkeys head List.mem_cons_self)]
      exact ih
        (fun other hother =>
          hkeys other (List.mem_cons_of_mem head hother))
        (insertDeparture head initial)

theorem foldl_insertDeparture_batches_commute
    (left right : List RecordedDeparture)
    (hkeys :
      ∀ first ∈ left, ∀ second ∈ right,
        first.eventKey ≠ second.eventKey)
    (initial : List RecordedDeparture) :
    right.foldl
        (fun current record => insertDeparture record current)
        (left.foldl
          (fun current record => insertDeparture record current)
          initial) =
      left.foldl
        (fun current record => insertDeparture record current)
        (right.foldl
          (fun current record => insertDeparture record current)
          initial) := by
  induction left generalizing initial with
  | nil =>
      rfl
  | cons head tail ih =>
      simp only [List.foldl_cons]
      rw [ih
        (fun first hfirst second hsecond =>
          hkeys first (List.mem_cons_of_mem head hfirst) second hsecond)
        (insertDeparture head initial)]
      rw [foldl_insertDeparture_commutes_with_one head right
        (hkeys head List.mem_cons_self)]

private theorem foldl_insertArrival_commutes_with_one
    (record : RecordedArrival)
    (batch : List RecordedArrival)
    (hkeys :
      ∀ other ∈ batch, record.eventKey ≠ other.eventKey)
    (initial : List RecordedArrival) :
    batch.foldl
        (fun current other => insertArrival other current)
        (insertArrival record initial) =
      insertArrival record
        (batch.foldl
          (fun current other => insertArrival other current)
          initial) := by
  induction batch generalizing initial with
  | nil =>
      rfl
  | cons head tail ih =>
      simp only [List.foldl_cons]
      rw [← insertArrival_commutes_of_key_ne record head
        (hkeys head List.mem_cons_self)]
      exact ih
        (fun other hother =>
          hkeys other (List.mem_cons_of_mem head hother))
        (insertArrival head initial)

theorem foldl_insertArrival_batches_commute
    (left right : List RecordedArrival)
    (hkeys :
      ∀ first ∈ left, ∀ second ∈ right,
        first.eventKey ≠ second.eventKey)
    (initial : List RecordedArrival) :
    right.foldl
        (fun current record => insertArrival record current)
        (left.foldl
          (fun current record => insertArrival record current)
          initial) =
      left.foldl
        (fun current record => insertArrival record current)
        (right.foldl
          (fun current record => insertArrival record current)
          initial) := by
  induction left generalizing initial with
  | nil =>
      rfl
  | cons head tail ih =>
      simp only [List.foldl_cons]
      rw [ih
        (fun first hfirst second hsecond =>
          hkeys first (List.mem_cons_of_mem head hfirst) second hsecond)
        (insertArrival head initial)]
      rw [foldl_insertArrival_commutes_with_one head right
        (hkeys head List.mem_cons_self)]

theorem foldl_insertDeparture_eq_of_perm
    {left right : List RecordedDeparture}
    (hperm : List.Perm left right)
    (hkeys :
      ∀ first ∈ left, ∀ second ∈ left,
        first ≠ second → first.eventKey ≠ second.eventKey)
    (initial : List RecordedDeparture) :
    left.foldl
        (fun current record => insertDeparture record current)
        initial =
      right.foldl
        (fun current record => insertDeparture record current)
        initial := by
  apply hperm.foldl_eq'
  intro first hfirst second hsecond current
  by_cases heq : first = second
  · subst second
    rfl
  · exact (insertDeparture_commutes_of_key_ne first second
      (hkeys first hfirst second hsecond heq) current).symm

theorem foldl_insertArrival_eq_of_perm
    {left right : List RecordedArrival}
    (hperm : List.Perm left right)
    (hkeys :
      ∀ first ∈ left, ∀ second ∈ left,
        first ≠ second → first.eventKey ≠ second.eventKey)
    (initial : List RecordedArrival) :
    left.foldl
        (fun current record => insertArrival record current)
        initial =
      right.foldl
        (fun current record => insertArrival record current)
        initial := by
  apply hperm.foldl_eq'
  intro first hfirst second hsecond current
  by_cases heq : first = second
  · subst second
    rfl
  · exact (insertArrival_commutes_of_key_ne first second
      (hkeys first hfirst second hsecond heq) current).symm

theorem foldl_eq_of_perm_of_commute
    {f : β → α → β}
    {left right : List α}
    (hperm : List.Perm left right)
    (hcommute :
      ∀ first ∈ left, ∀ second ∈ left, ∀ current,
        f (f current first) second = f (f current second) first)
    (initial : β) :
    left.foldl f initial = right.foldl f initial :=
  hperm.foldl_eq' hcommute initial

theorem perm_swap_adjacent
    (beforePart afterPart : List α)
    (left right : α) :
    List.Perm
      (beforePart ++ left :: right :: afterPart)
      (beforePart ++ right :: left :: afterPart) := by
  exact (List.Perm.swap left right afterPart).symm.append_left beforePart

end DaysExecutor
