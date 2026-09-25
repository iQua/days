import DaysExecutor.Statements

namespace DaysExecutor

/-!
Pure list normalization for F5.  The trace records only adjacent inversions whose events are
independent under the frozen required-order relation.
-/

theorem appearsBefore_mem_left
    (hbefore : AppearsBefore left right events) :
    left ∈ events := by
  rcases hbefore with ⟨beforePart, middle, afterPart, rfl⟩
  simp

theorem appearsBefore_mem_right
    (hbefore : AppearsBefore left right events) :
    right ∈ events := by
  rcases hbefore with ⟨beforePart, middle, afterPart, rfl⟩
  simp

theorem appearsBefore_cons_iff :
    AppearsBefore left right (head :: tail) ↔
      (head = left ∧ right ∈ tail) ∨
        AppearsBefore left right tail := by
  constructor
  · rintro ⟨beforePart, middle, afterPart, heq⟩
    cases beforePart with
    | nil =>
        injection heq with hhead htail
        subst head
        subst tail
        exact Or.inl ⟨rfl, by simp⟩
    | cons first rest =>
        simp only [List.cons_append, List.cons.injEq] at heq
        rcases heq with ⟨_, htail⟩
        exact Or.inr ⟨rest, middle, afterPart, htail⟩
  · rintro (⟨rfl, hright⟩ | hbefore)
    · obtain ⟨middle, afterPart, htail⟩ :=
        List.mem_iff_append.mp hright
      exact ⟨[], middle, afterPart, by simp [htail]⟩
    · rcases hbefore with ⟨beforePart, middle, afterPart, rfl⟩
      exact ⟨head :: beforePart, middle, afterPart, by simp⟩

theorem appearsBefore_asymm
    (hnodup : events.Nodup)
    (hleftRight : AppearsBefore left right events) :
    ¬ AppearsBefore right left events := by
  induction events with
  | nil =>
      exact False.elim (by
        simpa using appearsBefore_mem_left hleftRight)
  | cons head tail ih =>
      have hheadFresh := (List.nodup_cons.mp hnodup).1
      have htailNodup := (List.nodup_cons.mp hnodup).2
      rw [appearsBefore_cons_iff] at hleftRight
      intro hrightLeft
      rw [appearsBefore_cons_iff] at hrightLeft
      rcases hleftRight with ⟨hheadLeft, hrightTail⟩ | hleftRightTail
      · rcases hrightLeft with ⟨hheadRight, hleftTail⟩ | hrightLeftTail
        · apply hheadFresh
          rw [hheadRight]
          exact hrightTail
        · rw [hheadLeft] at hheadFresh
          exact hheadFresh (appearsBefore_mem_right hrightLeftTail)
      · rcases hrightLeft with ⟨hheadRight, hleftTail⟩ | hrightLeftTail
        · rw [hheadRight] at hheadFresh
          exact hheadFresh (appearsBefore_mem_right hleftRightTail)
        · exact ih htailNodup hleftRightTail hrightLeftTail

theorem appearsBefore_of_mem_prefix
    (hmem : left ∈ front) :
    AppearsBefore left pivot (front ++ pivot :: suffix) := by
  obtain ⟨beforePart, afterPart, rfl⟩ :=
    List.mem_iff_append.mp hmem
  exact ⟨beforePart, afterPart, suffix, by simp⟩

theorem appearsBefore_erase_other
    (hleft : removed ≠ left)
    (hright : removed ≠ right)
    (hbefore : AppearsBefore left right events) :
    AppearsBefore left right (events.erase removed) := by
  induction events with
  | nil =>
      exact False.elim (by
        simpa using appearsBefore_mem_left hbefore)
  | cons head tail ih =>
      rw [appearsBefore_cons_iff] at hbefore
      by_cases hhead : head = removed
      · subst head
        simp only [List.erase_cons_head]
        rcases hbefore with ⟨hremovedLeft, _⟩ | hbeforeTail
        · exact False.elim (hleft hremovedLeft)
        · exact hbeforeTail
      · simp only [List.erase_cons]
        rw [if_neg (by simpa using hhead)]
        rw [appearsBefore_cons_iff]
        rcases hbefore with hfirst | htail
        · exact Or.inl ⟨hfirst.1,
            (List.mem_erase_of_ne (Ne.symm hright)).mpr hfirst.2⟩
        · exact Or.inr (ih htail)

/--
Reflexive-transitive adjacent swaps.  `left,right` are in the current order, and `right` has the
smaller canonical key, so every step removes one adjacent inversion.
-/
inductive IndependentAdjacentSwapTrace
    (emissions : List (Event × Event)) : List Event → List Event → Prop
  | refl (events) :
      IndependentAdjacentSwapTrace emissions events events
  | step
      (beforePart afterPart : List Event)
      (left right : Event)
      (hinverted : right.key < left.key)
      (hindependent : IntraRoundIndependent emissions left right)
      (rest :
        IndependentAdjacentSwapTrace emissions
          (beforePart ++ right :: left :: afterPart)
          finish) :
      IndependentAdjacentSwapTrace emissions
        (beforePart ++ left :: right :: afterPart)
        finish

theorem IndependentAdjacentSwapTrace.trans
    (hleft :
      IndependentAdjacentSwapTrace emissions first middle)
    (hright :
      IndependentAdjacentSwapTrace emissions middle finish) :
    IndependentAdjacentSwapTrace emissions first finish := by
  induction hleft with
  | refl =>
      exact hright
  | step beforePart afterPart left right hinverted hindependent rest ih =>
      exact .step beforePart afterPart left right hinverted hindependent
        (ih hright)

theorem IndependentAdjacentSwapTrace.cons
    (head : Event)
    (htrace :
      IndependentAdjacentSwapTrace emissions before after) :
    IndependentAdjacentSwapTrace emissions
      (head :: before) (head :: after) := by
  induction htrace with
  | refl =>
      exact .refl _
  | step beforePart afterPart left right hinverted hindependent rest ih =>
      exact .step (head :: beforePart) afterPart left right
        hinverted hindependent (by simpa using ih)

theorem bubbleCanonicalHead
    (head : Event)
    (front suffix : List Event)
    (hkeys : ∀ event ∈ front, head.key < event.key)
    (hindependent :
      ∀ event ∈ front,
        IntraRoundIndependent emissions event head) :
    IndependentAdjacentSwapTrace emissions
      (front ++ head :: suffix)
      (head :: front ++ suffix) := by
  induction front with
  | nil =>
      exact .refl _
  | cons first rest ih =>
      have htail :
          IndependentAdjacentSwapTrace emissions
            (rest ++ head :: suffix)
            (head :: rest ++ suffix) :=
        ih
          (fun event hevent =>
            hkeys event (List.mem_cons_of_mem first hevent))
          (fun event hevent =>
            hindependent event (List.mem_cons_of_mem first hevent))
      have hlifted :
          IndependentAdjacentSwapTrace emissions
            (first :: rest ++ head :: suffix)
            (first :: head :: rest ++ suffix) :=
        htail.cons first
      have hlast :
          IndependentAdjacentSwapTrace emissions
            (first :: head :: rest ++ suffix)
            (head :: first :: rest ++ suffix) :=
        .step [] (rest ++ suffix) first head
          (hkeys first List.mem_cons_self)
          (hindependent first List.mem_cons_self)
          (.refl _)
      simpa using hlifted.trans hlast

theorem eventsBeforeCanonicalHead_independent
    (emissions : List (Event × Event))
    (head : Event)
    (tail front suffix : List Event)
    (hordered :
      (head :: tail).Pairwise
        (fun left right => left.key < right.key))
    (hpreserves :
      PreservesRequiredIntraRoundOrder emissions
        (head :: tail) (front ++ head :: suffix))
    (hrequiredKey :
      ∀ left ∈ head :: tail, ∀ right ∈ head :: tail,
        RequiredIntraRoundBefore emissions left right →
          left.key < right.key) :
    ∀ event ∈ front,
      head.key < event.key ∧
        IntraRoundIndependent emissions event head := by
  have hcanonicalNodup : (head :: tail).Nodup :=
    hordered.imp (fun {left right} hlt heq => by
      subst right
      exact EventKey.lt_irrefl left.key hlt)
  have hcandidateNodup : (front ++ head :: suffix).Nodup :=
    hpreserves.1.symm.nodup hcanonicalNodup
  intro event hevent
  have heventCandidate : event ∈ front ++ head :: suffix := by
    simp [hevent]
  have heventCanonical : event ∈ head :: tail :=
    hpreserves.1.subset heventCandidate
  have heventNeHead : event ≠ head := by
    have hcross :=
      (List.nodup_append.mp hcandidateNodup).2.2
        event hevent head (by simp)
    exact hcross
  have heventTail : event ∈ tail :=
    (List.mem_cons.mp heventCanonical).resolve_left heventNeHead
  have hheadEvent :
      head.key < event.key :=
    (List.pairwise_cons.mp hordered).1 event heventTail
  refine ⟨hheadEvent, ?_, ?_⟩
  · intro heventHead
    have heventHeadKey :=
      hrequiredKey event heventCanonical head List.mem_cons_self
        heventHead
    exact EventKey.lt_irrefl _
      (EventKey.lt_trans heventHeadKey hheadEvent)
  · intro hheadEventRequired
    have hrequiredAppears :=
      hpreserves.2 head List.mem_cons_self event heventCanonical
        hheadEventRequired
    exact
      appearsBefore_asymm hcandidateNodup
        (appearsBefore_of_mem_prefix hevent) hrequiredAppears

theorem erase_eq_prefix_suffix_of_nodup
    (head : Event)
    (front suffix : List Event)
    (hnodup : (front ++ head :: suffix).Nodup) :
    (front ++ head :: suffix).erase head = front ++ suffix := by
  have hheadNotFront : head ∉ front := by
    intro hmem
    exact
      (List.nodup_append.mp hnodup).2.2
        head hmem head (by simp) rfl
  rw [List.erase_append_right _ hheadNotFront,
    List.erase_cons_head]

theorem preservesRequired_erase_canonicalHead
    (emissions : List (Event × Event))
    (head : Event)
    (tail candidate : List Event)
    (hordered :
      (head :: tail).Pairwise
        (fun left right => left.key < right.key))
    (hpreserves :
      PreservesRequiredIntraRoundOrder emissions
        (head :: tail) candidate) :
    PreservesRequiredIntraRoundOrder emissions
      tail (candidate.erase head) := by
  have hheadFresh : head ∉ tail := by
    intro hhead
    exact EventKey.lt_irrefl _
      ((List.pairwise_cons.mp hordered).1 head hhead)
  constructor
  · simpa [List.erase_cons_head] using hpreserves.1.erase head
  · intro left hleft right hright hrequired
    have hleftNe : head ≠ left := by
      intro heq
      subst left
      exact hheadFresh hleft
    have hrightNe : head ≠ right := by
      intro heq
      subst right
      exact hheadFresh hright
    exact appearsBefore_erase_other hleftNe hrightNe
      (hpreserves.2 left (List.mem_cons_of_mem head hleft)
        right (List.mem_cons_of_mem head hright) hrequired)

/--
Execution-oriented induction step: locate the canonical head, certify every event blocking it as
independent, and bubble it left.  The residual order is exactly `candidate.erase head`.
-/
theorem peelCanonicalHead_by_independent_adjacent_swaps
    (emissions : List (Event × Event))
    (head : Event)
    (tail candidate : List Event)
    (hordered :
      (head :: tail).Pairwise
        (fun left right => left.key < right.key))
    (hpreserves :
      PreservesRequiredIntraRoundOrder emissions
        (head :: tail) candidate)
    (hrequiredKey :
      ∀ left ∈ head :: tail, ∀ right ∈ head :: tail,
        RequiredIntraRoundBefore emissions left right →
          left.key < right.key) :
    ∃ front suffix,
      candidate = front ++ head :: suffix ∧
        (∀ event ∈ front,
          head.key < event.key ∧
            IntraRoundIndependent emissions event head) ∧
        IndependentAdjacentSwapTrace emissions candidate
          (head :: candidate.erase head) := by
  have hheadCandidate : head ∈ candidate :=
    hpreserves.1.symm.subset List.mem_cons_self
  obtain ⟨front, suffix, hcand⟩ :=
    List.mem_iff_append.mp hheadCandidate
  have hheadData :=
    eventsBeforeCanonicalHead_independent emissions head tail
      front suffix hordered (by simpa [hcand] using hpreserves)
      hrequiredKey
  have hcanonicalNodup : (head :: tail).Nodup :=
    hordered.imp (fun {left right} hlt heq => by
      subst right
      exact EventKey.lt_irrefl left.key hlt)
  have hcandidateNodup : (front ++ head :: suffix).Nodup := by
    rw [← hcand]
    exact hpreserves.1.symm.nodup hcanonicalNodup
  refine ⟨front, suffix, hcand, hheadData, ?_⟩
  rw [hcand,
    erase_eq_prefix_suffix_of_nodup head front suffix
      hcandidateNodup]
  exact bubbleCanonicalHead head front suffix
    (fun event hevent => (hheadData event hevent).1)
    (fun event hevent => (hheadData event hevent).2)

/--
Every finite linear extension of the required F5 order normalizes to the strictly key-ordered
canonical list by adjacent independent inversion swaps.  The extra `hrequiredKey` premise is the
trace-validity bridge supplied by child-key advancement: recorded causal edges, like same-node
queue conflicts, point forward in canonical key order.
-/
theorem linearExtension_to_keyOrder_by_independent_adjacent_swaps
    (emissions : List (Event × Event))
    (canonical candidate : List Event)
    (hordered :
      canonical.Pairwise
        (fun left right => left.key < right.key))
    (hpreserves :
      PreservesRequiredIntraRoundOrder emissions canonical candidate)
    (hrequiredKey :
      ∀ left ∈ canonical, ∀ right ∈ canonical,
        RequiredIntraRoundBefore emissions left right →
          left.key < right.key) :
    IndependentAdjacentSwapTrace emissions candidate canonical := by
  induction canonical generalizing candidate with
  | nil =>
      have hcand : candidate = [] :=
        hpreserves.1.eq_nil
      subst candidate
      exact .refl []
  | cons head tail ih =>
      have hheadCandidate : head ∈ candidate :=
        hpreserves.1.symm.subset List.mem_cons_self
      obtain ⟨front, suffix, hcand⟩ :=
        List.mem_iff_append.mp hheadCandidate
      subst candidate
      have hheadData :=
        eventsBeforeCanonicalHead_independent emissions head tail
          front suffix hordered hpreserves hrequiredKey
      have hcanonicalNodup : (head :: tail).Nodup :=
        hordered.imp (fun {left right} hlt heq => by
          subst right
          exact EventKey.lt_irrefl left.key hlt)
      have hcandidateNodup :
          (front ++ head :: suffix).Nodup :=
        hpreserves.1.symm.nodup hcanonicalNodup
      have herase :
          (front ++ head :: suffix).erase head =
            front ++ suffix :=
        erase_eq_prefix_suffix_of_nodup head front suffix
          hcandidateNodup
      have hbubble :
          IndependentAdjacentSwapTrace emissions
            (front ++ head :: suffix)
            (head :: front ++ suffix) :=
        bubbleCanonicalHead head front suffix
          (fun event hevent => (hheadData event hevent).1)
          (fun event hevent => (hheadData event hevent).2)
      have htailPreserves :
          PreservesRequiredIntraRoundOrder emissions tail
            ((front ++ head :: suffix).erase head) :=
        preservesRequired_erase_canonicalHead emissions head tail
          (front ++ head :: suffix) hordered hpreserves
      have htailRequiredKey :
          ∀ left ∈ tail, ∀ right ∈ tail,
            RequiredIntraRoundBefore emissions left right →
              left.key < right.key := by
        intro left hleft right hright hrequired
        exact hrequiredKey left (List.mem_cons_of_mem head hleft)
          right (List.mem_cons_of_mem head hright) hrequired
      have htailTrace :
          IndependentAdjacentSwapTrace emissions
            ((front ++ head :: suffix).erase head) tail :=
        ih ((front ++ head :: suffix).erase head)
          (List.pairwise_cons.mp hordered).2
          htailPreserves htailRequiredKey
      have hlifted :
          IndependentAdjacentSwapTrace emissions
            (head :: (front ++ head :: suffix).erase head)
            (head :: tail) :=
        htailTrace.cons head
      rw [herase] at hlifted
      exact hbubble.trans hlifted

end DaysExecutor
