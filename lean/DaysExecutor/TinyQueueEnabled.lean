import DaysExecutor.Counterexamples
import DaysExecutor.ScalarMaterialization

namespace DaysExecutor

private def tinyQueueSwitchNode : NodeDescriptor :=
  { id := 1, kind := .switch, stateSlot := 0 }

private def tinyQueueTerminalNode : NodeDescriptor :=
  { id := 2, kind := .host, stateSlot := 1 }

private def tinyQueueInitialSwitchState :
    RoleState TinyQueueStateFamily .switch :=
  { privateState := queueCounterexamplePrivateState
    serviceQueue := [12]
    committedService := [] }

private def tinyQueueInitialTerminalState :
    RoleState TinyQueueStateFamily .host :=
  { privateState := queueCounterexampleSourceState
    serviceQueue := []
    committedService := [] }

private def tinyQueueSwitchAfterArrival (eager : Bool) :
    RoleState TinyQueueStateFamily .switch :=
  (tinyQueueTransitionResult eager tinyQueueSwitchNode
    queueCounterexampleArrival tinyQueueInitialSwitchState).nextState

private def tinyQueueSwitchAfterReady (eager : Bool) :
    RoleState TinyQueueStateFamily .switch :=
  (tinyQueueTransitionResult eager tinyQueueSwitchNode
    queueCounterexampleReady (tinyQueueSwitchAfterArrival eager)).nextState

private def tinyQueueCompletion : Event :=
  { key :=
      { timeNs := 11
        phase := eventPhase .txComplete
        originNode := 1
        originSeq := 1 }
    target := 1
    kind := .txComplete
    payload := 12 }

private def tinyQueueRemote : Event :=
  { key :=
      { timeNs := 11
        phase := eventPhase .remoteArrival
        originNode := 1
        originSeq := 2 }
    target := 2
    kind := .remoteArrival
    payload := 12 }

private def tinyQueueTerminalAfterRemote (eager : Bool) :
    RoleState TinyQueueStateFamily .host :=
  (tinyQueueTransitionResult eager tinyQueueTerminalNode
    tinyQueueRemote tinyQueueInitialTerminalState).nextState

private def tinyQueueSwitchAfterCompletion (eager : Bool) :
    RoleState TinyQueueStateFamily .switch :=
  (tinyQueueTransitionResult eager tinyQueueSwitchNode
    tinyQueueCompletion (tinyQueueSwitchAfterReady eager)).nextState

private inductive TinyQueueReachableShape
    (eager : Bool) : MachineState TinyQueueStateFamily → Prop
  | initial
      (hpending :
        machine.pending =
          [queueCounterexampleArrival, queueCounterexampleReady])
      (hswitch :
        machine.localState tinyQueueSwitchNode =
          tinyQueueInitialSwitchState)
      (hterminal :
        machine.localState tinyQueueTerminalNode =
          tinyQueueInitialTerminalState)
      (hcursor : machine.nextOriginSeq 1 = 1) :
      TinyQueueReachableShape eager machine
  | afterArrival
      (hpending : machine.pending = [queueCounterexampleReady])
      (hswitch :
        machine.localState tinyQueueSwitchNode =
          tinyQueueSwitchAfterArrival eager)
      (hterminal :
        machine.localState tinyQueueTerminalNode =
          tinyQueueInitialTerminalState)
      (hcursor : machine.nextOriginSeq 1 = 1) :
      TinyQueueReachableShape eager machine
  | afterReady
      (hpending : machine.pending = [tinyQueueRemote, tinyQueueCompletion])
      (hswitch :
        machine.localState tinyQueueSwitchNode =
          tinyQueueSwitchAfterReady eager)
      (hterminal :
        machine.localState tinyQueueTerminalNode =
          tinyQueueInitialTerminalState) :
      TinyQueueReachableShape eager machine
  | afterRemote
      (hpending : machine.pending = [tinyQueueCompletion])
      (hswitch :
        machine.localState tinyQueueSwitchNode =
          tinyQueueSwitchAfterReady eager)
      (hterminal :
        machine.localState tinyQueueTerminalNode =
          tinyQueueTerminalAfterRemote eager) :
      TinyQueueReachableShape eager machine
  | finished
      (hpending : machine.pending = [])
      (hswitch :
        machine.localState tinyQueueSwitchNode =
          tinyQueueSwitchAfterCompletion eager)
      (hterminal :
        machine.localState tinyQueueTerminalNode =
          tinyQueueTerminalAfterRemote eager) :
      TinyQueueReachableShape eager machine

private theorem queueCounterexample_uniqueNodes :
    UniqueNodeIds queueCounterexampleImage := by
  unfold UniqueNodeIds
  native_decide

private theorem queueCounterexample_descriptorOracle :
    DescriptorOracleWellFormed queueCounterexampleImage := by
  intro payload
  simp [queueCounterexampleImage, queueCounterexampleDescriptor]

private theorem tinyQueueSwitchNode_mem :
    tinyQueueSwitchNode ∈ queueCounterexampleImage.nodes := by
  simp [tinyQueueSwitchNode, queueCounterexampleImage]

private theorem tinyQueueTerminalNode_mem :
    tinyQueueTerminalNode ∈ queueCounterexampleImage.nodes := by
  simp [tinyQueueTerminalNode, queueCounterexampleImage]

private theorem tinyQueueReachableShape_initial
    (machine : MachineState TinyQueueStateFamily)
    (hinitial : InitialMachine queueCounterexampleImage machine) :
    TinyQueueReachableShape eager machine := by
  rcases hinitial with
    ⟨hpending, _, _, _, _, hcursor, _, _, hnodes, _⟩
  apply TinyQueueReachableShape.initial
  · rw [hpending]
    native_decide
  · have hstate :=
      (hnodes tinyQueueSwitchNode tinyQueueSwitchNode_mem).1
    simpa [stateAt?, listGet?, queueCounterexampleImage,
      tinyQueueSwitchNode, tinyQueueInitialSwitchState] using
        (Option.some.inj hstate).symm
  · have hstate :=
      (hnodes tinyQueueTerminalNode tinyQueueTerminalNode_mem).1
    simpa [stateAt?, listGet?, queueCounterexampleImage,
      tinyQueueTerminalNode, tinyQueueInitialTerminalState] using
        (Option.some.inj hstate).symm
  · have := congrFun hcursor 1
    simpa [queueCounterexampleImage] using this

private theorem tinyQueueReachableShape_step
    (hshape : TinyQueueReachableShape eager before)
    (hstep :
      CanonicalSerialStep queueCounterexampleImage
        (tinyQueueTransition eager) (fun _ => True)
        before event after) :
    TinyQueueReachableShape eager after := by
  rcases hstep with ⟨hleast, havailable⟩
  rcases havailable with
    ⟨hevent, node, hnode, result, htarget, htransition, _,
      hallocates, happlies, _, _, hpending, _⟩
  have switchNeTerminal :
      tinyQueueTerminalNode.id ≠ tinyQueueSwitchNode.id := by decide
  have terminalNeSwitch :
      tinyQueueSwitchNode.id ≠ tinyQueueTerminalNode.id := by decide
  cases hshape with
  | initial hbeforePending hbeforeSwitch hbeforeTerminal hbeforeCursor =>
      have heventEq : event = queueCounterexampleArrival := by
        rw [hbeforePending] at hleast
        rcases List.mem_cons.mp hleast.1 with heq | htail
        · exact heq
        · have heq := List.mem_singleton.mp htail
          subst event
          have horder :=
            hleast.2.2 queueCounterexampleArrival (by simp) trivial
          have himpossible :
              ¬ queueCounterexampleReady.key ≤
                queueCounterexampleArrival.key := by
            native_decide
          exact (himpossible horder).elim
      subst event
      have hnodeEq : node = tinyQueueSwitchNode := by
        apply node_eq_of_unique_ids queueCounterexample_uniqueNodes
          hnode tinyQueueSwitchNode_mem
        simpa [tinyQueueSwitchNode, queueCounterexampleArrival] using
          htarget.symm
      subst node
      have hresultEq :
          result =
            tinyQueueTransitionResult eager tinyQueueSwitchNode
              queueCounterexampleArrival
              (before.localState tinyQueueSwitchNode) :=
        htransition.2.2.2.2
      subst result
      apply TinyQueueReachableShape.afterArrival
      · rw [hpending, hbeforePending]
        simp [insertEvents, tinyQueueTransitionResult, tinyQueueServiceChildren,
          queueCounterexampleArrival]
      · rw [happlies.1, hbeforeSwitch]
        rfl
      · rw [(happlies.2.2.1 tinyQueueTerminalNode
          tinyQueueTerminalNode_mem switchNeTerminal).1,
        hbeforeTerminal]
      · have hsourceCursor := hallocates.2.2.2.2.1
        simpa [tinyQueueTransitionResult, tinyQueueServiceChildren,
          queueCounterexampleArrival, tinyQueueSwitchNode,
          hbeforeCursor] using hsourceCursor
  | afterArrival hbeforePending hbeforeSwitch hbeforeTerminal hbeforeCursor =>
      have heventEq : event = queueCounterexampleReady := by
        simpa [hbeforePending] using hevent
      subst event
      have hnodeEq : node = tinyQueueSwitchNode := by
        apply node_eq_of_unique_ids queueCounterexample_uniqueNodes
          hnode tinyQueueSwitchNode_mem
        simpa [tinyQueueSwitchNode, queueCounterexampleReady] using
          htarget.symm
      subst node
      have hresultEq :
          result =
            tinyQueueTransitionResult eager tinyQueueSwitchNode
              queueCounterexampleReady
              (before.localState tinyQueueSwitchNode) :=
        htransition.2.2.2.2
      subst result
      apply TinyQueueReachableShape.afterReady
      · rw [hpending, hbeforePending, hbeforeSwitch]
        cases eager <;>
          native_decide
      · rw [happlies.1, hbeforeSwitch]
        rfl
      · rw [(happlies.2.2.1 tinyQueueTerminalNode
          tinyQueueTerminalNode_mem switchNeTerminal).1,
        hbeforeTerminal]
  | afterReady hbeforePending hbeforeSwitch hbeforeTerminal =>
      have heventEq : event = tinyQueueRemote := by
        rw [hbeforePending] at hleast
        rcases List.mem_cons.mp hleast.1 with heq | htail
        · exact heq
        · have heq := List.mem_singleton.mp htail
          subst event
          have horder := hleast.2.2 tinyQueueRemote (by simp) trivial
          have himpossible :
              ¬ tinyQueueCompletion.key ≤ tinyQueueRemote.key := by
            native_decide
          exact (himpossible horder).elim
      subst event
      have hnodeEq : node = tinyQueueTerminalNode := by
        apply node_eq_of_unique_ids queueCounterexample_uniqueNodes
          hnode tinyQueueTerminalNode_mem
        simpa [tinyQueueTerminalNode, tinyQueueRemote] using htarget.symm
      subst node
      have hresultEq :
          result =
            tinyQueueTransitionResult eager tinyQueueTerminalNode
              tinyQueueRemote
              (before.localState tinyQueueTerminalNode) :=
        htransition.2.2.2.2
      subst result
      apply TinyQueueReachableShape.afterRemote
      · rw [hpending, hbeforePending]
        simp [insertEvents, tinyQueueTransitionResult, tinyQueueServiceChildren,
          tinyQueueRemote]
      · rw [(happlies.2.2.1 tinyQueueSwitchNode
          tinyQueueSwitchNode_mem terminalNeSwitch).1,
        hbeforeSwitch]
      · rw [happlies.1, hbeforeTerminal]
        rfl
  | afterRemote hbeforePending hbeforeSwitch hbeforeTerminal =>
      have heventEq : event = tinyQueueCompletion := by
        simpa [hbeforePending] using hevent
      subst event
      have hnodeEq : node = tinyQueueSwitchNode := by
        apply node_eq_of_unique_ids queueCounterexample_uniqueNodes
          hnode tinyQueueSwitchNode_mem
        simpa [tinyQueueSwitchNode, tinyQueueCompletion] using
          htarget.symm
      subst node
      have hresultEq :
          result =
            tinyQueueTransitionResult eager tinyQueueSwitchNode
              tinyQueueCompletion
              (before.localState tinyQueueSwitchNode) :=
        htransition.2.2.2.2
      subst result
      apply TinyQueueReachableShape.finished
      · rw [hpending, hbeforePending]
        simp [insertEvents, tinyQueueTransitionResult, tinyQueueServiceChildren,
          tinyQueueCompletion]
      · rw [happlies.1, hbeforeSwitch]
        rfl
      · rw [(happlies.2.2.1 tinyQueueTerminalNode
          tinyQueueTerminalNode_mem switchNeTerminal).1,
        hbeforeTerminal]
  | finished hbeforePending _ _ =>
      rw [hbeforePending] at hevent
      simp at hevent

private theorem tinyQueueTransition_generatedRoles (eager : Bool) :
    GeneratedEventsRoleCorrect
      queueCounterexampleImage (tinyQueueTransition eager) := by
  intro node event state result htransition child hchild
  rcases htransition with ⟨hnode, _, _, _, rfl⟩
  simp [queueCounterexampleImage] at hnode
  rcases hnode with rfl | rfl | rfl
  all_goals
    by_cases hkind : event.kind = .txReady
    · simp only [tinyQueueTransitionResult, hkind, ↓reduceIte] at hchild
      cases hselected : tinyQueueSelectedPacket eager state with
      | none =>
          simp [tinyQueueServiceChildren, hselected] at hchild
      | some packet =>
          simp [tinyQueueServiceChildren, hselected] at hchild
          rcases hchild with rfl | rfl <;>
            simp [queueCounterexampleImage, roleSupports]
    · simp [tinyQueueTransitionResult, hkind,
        tinyQueueServiceChildren] at hchild

private theorem tinyQueue_listBagDifference_mem
    [BEq α] [LawfulBEq α]
    (source removed : List α)
    {item : α}
    (hitem : item ∈ listBagDifference source removed) :
    item ∈ source := by
  induction removed generalizing source with
  | nil =>
      exact hitem
  | cons head tail ih =>
      simp only [listBagDifference, List.foldl_cons] at hitem
      exact List.mem_of_mem_erase (ih (source.erase head) hitem)

private theorem tinyQueue_ownedRoleStateReference_oracle
    (node : NodeDescriptor)
    (state : RoleState TinyQueueStateFamily node.kind)
    (reference : OwnedPacketReference)
    (hreference :
      reference ∈
        ownedRoleStateReferences queueCounterexampleImage node state) :
    reference.descriptor =
      queueCounterexampleImage.packetDescriptor reference.descriptor.id := by
  unfold ownedRoleStateReferences at hreference
  rw [List.mem_append] at hreference
  rcases hreference with hqueue | hservice
  · rcases List.mem_map.mp hqueue with ⟨payload, _, rfl⟩
    rfl
  · rcases List.mem_map.mp hservice with ⟨payload, _, rfl⟩
    rfl

private theorem tinyQueueTransition_descriptors (eager : Bool) :
    TransitionDescriptorEffectsCoherent
      queueCounterexampleImage (tinyQueueTransition eager) := by
  intro node event state result htransition
  rcases htransition with ⟨_, _, _, _, rfl⟩
  constructor
  · intro reference hreference
    unfold tinyQueueTransitionResult at hreference
    exact tinyQueue_ownedRoleStateReference_oracle node _ reference
      (tinyQueue_listBagDifference_mem _ _ hreference)
  constructor
  · intro reference hreference
    unfold tinyQueueTransitionResult at hreference
    rcases List.mem_cons.mp hreference with rfl | hreference
    · rfl
    · exact tinyQueue_ownedRoleStateReference_oracle node state reference
        (tinyQueue_listBagDifference_mem _ _ hreference)
  · intro descriptor hdescriptor
    simp [tinyQueueTransitionResult] at hdescriptor

private theorem tinyQueueExecution_preserves_shape
    (hbeforeWellFormed :
      MachineWellFormed queueCounterexampleImage before)
    (hbeforeShape : TinyQueueReachableShape eager before)
    (hexecution :
      CanonicalSerialExecution queueCounterexampleImage
        (tinyQueueTransition eager) (fun _ => True)
        before events after) :
    MachineWellFormed queueCounterexampleImage after ∧
      TinyQueueReachableShape eager after := by
  induction hexecution with
  | refl =>
      exact ⟨hbeforeWellFormed, hbeforeShape⟩
  | step first rest ih =>
      have hmiddleWellFormed :=
        availableEventStep_preserves_machineWellFormed
          queueCounterexampleImage (tinyQueueTransition eager)
          queueCounterexample_uniqueNodes
          queueCounterexample_descriptorOracle
          (tinyQueueTransition_generatedRoles eager)
          (tinyQueueTransition_descriptors eager)
          _ _ _ hbeforeWellFormed first.2
      have hmiddleShape :=
        tinyQueueReachableShape_step hbeforeShape first
      exact ih hmiddleWellFormed hmiddleShape

private theorem tinyQueueReachable_wellFormed_shape
    (hreachable :
      CanonicallyReachableMachine queueCounterexampleImage
        (tinyQueueTransition eager) machine) :
    MachineWellFormed queueCounterexampleImage machine ∧
      TinyQueueReachableShape eager machine := by
  rcases hreachable with ⟨initial, executed, hinitial, hexecution⟩
  exact tinyQueueExecution_preserves_shape
    hinitial.2.2.2.2.2.2.2.2.2
    (tinyQueueReachableShape_initial initial hinitial)
    hexecution

private theorem tinyQueue_materialized_available
    (eager : Bool)
    (machine : MachineState TinyQueueStateFamily)
    (hwellFormed : MachineWellFormed queueCounterexampleImage machine)
    (node : NodeDescriptor)
    (hnode : node ∈ queueCounterexampleImage.nodes)
    (event : Event)
    (hevent : event ∈ machine.pending)
    (htarget : event.target = node.id)
    (hsupport : roleSupports node.kind event.kind)
    (hguard :
      event.kind = .txComplete →
        event.payload ∈ (machine.localState node).committedService)
    (horigin :
      ChildrenUseOriginSequence node.id
        (machine.nextOriginSeq node.id)
        (tinyQueueTransitionResult eager node event
          (machine.localState node)).children)
    (hchildrenFresh :
      ∀ child ∈
          (tinyQueueTransitionResult eager node event
            (machine.localState node)).children,
        child.key ∉ machine.allocatedKeys)
    (hnewStateNodup :
      (ownedRoleStateReferences queueCounterexampleImage node
        (tinyQueueTransitionResult eager node event
          (machine.localState node)).nextState).Nodup) :
    ∃ after,
      AvailableEventStep queueCounterexampleImage
        (tinyQueueTransition eager) event machine after := by
  let result :=
    tinyQueueTransitionResult eager node event (machine.localState node)
  have htransition :
      tinyQueueTransition eager node event
        (machine.localState node) result :=
    ⟨hnode, htarget, hsupport, hguard, rfl⟩
  have hchildKeys : (result.children.map Event.key).Nodup := by
    unfold result
    by_cases hkind : event.kind = .txReady
    · simp only [tinyQueueTransitionResult, hkind, ↓reduceIte]
      cases hselected :
          tinyQueueSelectedPacket eager (machine.localState node) with
      | none =>
          simp [tinyQueueServiceChildren]
      | some packet =>
          simp [tinyQueueServiceChildren]
    · simp [tinyQueueTransitionResult, hkind, tinyQueueServiceChildren]
  have hconsumptions :
      result.packetReferenceConsumptions.Perm
        (ownedEventReference queueCounterexampleImage event ::
          stateReferenceConsumptions queueCounterexampleImage node
            (machine.localState node) result.nextState) := by
    rfl
  have hincrements :
      ReferenceIncrementsValid queueCounterexampleImage node
        (machine.localState node) result := by
    unfold ReferenceIncrementsValid result
    rfl
  have havailable :=
    materializeScalarResult_available
      queueCounterexampleImage (tinyQueueTransition eager)
      queueCounterexample_uniqueNodes
      queueCounterexample_descriptorOracle
      (tinyQueueTransition_generatedRoles eager)
      (tinyQueueTransition_descriptors eager)
      node hnode event result machine hwellFormed hevent htarget
      htransition horigin hchildKeys hchildrenFresh
      hconsumptions hincrements hnewStateNodup
  exact ⟨materializeScalarResult queueCounterexampleImage
      node event result machine, havailable.1⟩

theorem tinyQueueTransition_enabledOnReachable (eager : Bool) :
    TransitionEnabledOnReachable
      queueCounterexampleImage (tinyQueueTransition eager) := by
  intro machine hreachable node hnode event hleast htarget hsupport
  rcases tinyQueueReachable_wellFormed_shape hreachable with
    ⟨hwellFormed, hshape⟩
  have hevent := hleast.1
  cases hshape with
  | initial hpending hswitch _ _ =>
      have heventEq : event = queueCounterexampleArrival := by
        rw [hpending] at hleast
        rcases List.mem_cons.mp hleast.1 with heq | htail
        · exact heq
        · have heq := List.mem_singleton.mp htail
          subst event
          have horder :=
            hleast.2.2 queueCounterexampleArrival (by simp) (by
              simpa [queueCounterexampleArrival] using htarget)
          have himpossible :
              ¬ queueCounterexampleReady.key ≤
                queueCounterexampleArrival.key := by
            native_decide
          exact (himpossible horder).elim
      subst event
      have hnodeEq : node = tinyQueueSwitchNode := by
        apply node_eq_of_unique_ids queueCounterexample_uniqueNodes
          hnode tinyQueueSwitchNode_mem
        simpa [tinyQueueSwitchNode, queueCounterexampleArrival] using
          htarget.symm
      subst node
      refine tinyQueue_materialized_available eager machine hwellFormed
        tinyQueueSwitchNode tinyQueueSwitchNode_mem
        queueCounterexampleArrival hevent htarget hsupport ?_ ?_ ?_ ?_
      · simp [queueCounterexampleArrival]
      · simp [ChildrenUseOriginSequence, tinyQueueTransitionResult,
          tinyQueueServiceChildren,
          queueCounterexampleArrival]
      · simp [tinyQueueTransitionResult, tinyQueueServiceChildren,
          queueCounterexampleArrival]
      · rw [hswitch]
        cases eager <;>
          native_decide
  | afterArrival hpending hswitch _ hcursor =>
      have heventEq : event = queueCounterexampleReady := by
        simpa [hpending] using hevent
      subst event
      have hnodeEq : node = tinyQueueSwitchNode := by
        apply node_eq_of_unique_ids queueCounterexample_uniqueNodes
          hnode tinyQueueSwitchNode_mem
        simpa [tinyQueueSwitchNode, queueCounterexampleReady] using
          htarget.symm
      subst node
      refine tinyQueue_materialized_available eager machine hwellFormed
        tinyQueueSwitchNode tinyQueueSwitchNode_mem
        queueCounterexampleReady hevent htarget hsupport ?_ ?_ ?_ ?_
      · simp [queueCounterexampleReady]
      · rw [hswitch]
        have hcursorAtSwitch :
            machine.nextOriginSeq tinyQueueSwitchNode.id = 1 := by
          simpa [tinyQueueSwitchNode] using hcursor
        rw [hcursorAtSwitch]
        have hchildren :
            (tinyQueueTransitionResult eager tinyQueueSwitchNode
              queueCounterexampleReady
              (tinyQueueSwitchAfterArrival eager)).children =
              [tinyQueueCompletion, tinyQueueRemote] := by
          cases eager <;>
            native_decide
        rw [hchildren]
        simp [ChildrenUseOriginSequence, tinyQueueSwitchNode,
          tinyQueueCompletion, tinyQueueRemote]
      · intro child hchild hallocated
        rw [hswitch] at hchild
        have hchildren :
            (tinyQueueTransitionResult eager tinyQueueSwitchNode
              queueCounterexampleReady
              (tinyQueueSwitchAfterArrival eager)).children =
              [tinyQueueCompletion, tinyQueueRemote] := by
          cases eager <;>
            native_decide
        rw [hchildren] at hchild
        rcases List.mem_cons.mp hchild with heq | htail
        · subst child
          have hlt :=
            (hwellFormed.2.2.1 tinyQueueCompletion.key hallocated).1
          simp [tinyQueueCompletion, hcursor] at hlt
        · have heq := List.mem_singleton.mp htail
          subst child
          have hlt :=
            (hwellFormed.2.2.1 tinyQueueRemote.key hallocated).1
          simp [tinyQueueRemote, hcursor] at hlt
      · rw [hswitch]
        cases eager <;>
          native_decide
  | afterReady hpending hswitch hterminal =>
      rw [hpending] at hevent
      rcases List.mem_cons.mp hevent with heventEq | htail
      · subst event
        have hnodeEq : node = tinyQueueTerminalNode := by
          apply node_eq_of_unique_ids queueCounterexample_uniqueNodes
            hnode tinyQueueTerminalNode_mem
          simpa [tinyQueueTerminalNode, tinyQueueRemote] using htarget.symm
        subst node
        refine tinyQueue_materialized_available eager machine hwellFormed
          tinyQueueTerminalNode tinyQueueTerminalNode_mem
          tinyQueueRemote (by rw [hpending]; simp) htarget hsupport ?_ ?_ ?_ ?_
        · simp [tinyQueueRemote]
        · simp [ChildrenUseOriginSequence, tinyQueueTransitionResult,
            tinyQueueServiceChildren,
            tinyQueueRemote]
        · simp [tinyQueueTransitionResult, tinyQueueServiceChildren,
            tinyQueueRemote]
        · rw [hterminal]
          cases eager <;>
            native_decide
      · have heventEq := List.mem_singleton.mp htail
        subst event
        have hnodeEq : node = tinyQueueSwitchNode := by
          apply node_eq_of_unique_ids queueCounterexample_uniqueNodes
            hnode tinyQueueSwitchNode_mem
          simpa [tinyQueueSwitchNode, tinyQueueCompletion] using htarget.symm
        subst node
        refine tinyQueue_materialized_available eager machine hwellFormed
          tinyQueueSwitchNode tinyQueueSwitchNode_mem
          tinyQueueCompletion (by rw [hpending]; simp) htarget hsupport ?_ ?_ ?_ ?_
        · intro _
          rw [hswitch]
          cases eager <;>
            native_decide
        · simp [ChildrenUseOriginSequence, tinyQueueTransitionResult,
            tinyQueueServiceChildren,
            tinyQueueCompletion]
        · simp [tinyQueueTransitionResult, tinyQueueServiceChildren,
            tinyQueueCompletion]
        · rw [hswitch]
          cases eager <;>
            native_decide
  | afterRemote hpending hswitch _ =>
      have heventEq : event = tinyQueueCompletion := by
        simpa [hpending] using hevent
      subst event
      have hnodeEq : node = tinyQueueSwitchNode := by
        apply node_eq_of_unique_ids queueCounterexample_uniqueNodes
          hnode tinyQueueSwitchNode_mem
        simpa [tinyQueueSwitchNode, tinyQueueCompletion] using htarget.symm
      subst node
      refine tinyQueue_materialized_available eager machine hwellFormed
        tinyQueueSwitchNode tinyQueueSwitchNode_mem
        tinyQueueCompletion hevent htarget hsupport ?_ ?_ ?_ ?_
      · intro _
        rw [hswitch]
        cases eager <;>
          native_decide
      · simp [ChildrenUseOriginSequence, tinyQueueTransitionResult,
          tinyQueueServiceChildren,
          tinyQueueCompletion]
      · simp [tinyQueueTransitionResult, tinyQueueServiceChildren,
          tinyQueueCompletion]
      · rw [hswitch]
        cases eager <;>
          native_decide
  | finished hpending _ _ =>
      rw [hpending] at hevent
      simp at hevent

end DaysExecutor
