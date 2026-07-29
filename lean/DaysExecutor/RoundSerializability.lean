import DaysExecutor.ExecutionCanonical
import DaysExecutor.Statements

namespace DaysExecutor

/-- One valid sequential LP drain/exchange round has a canonical scalar replay of the same cut. -/
theorem roundSerializabilityOverCut_proved
    (image : SimulationImage State)
    (transition : TransitionRelation State) :
    RoundSerializabilityOverCut image transition := by
  intro haccepted _ bounds cut start drainedEvents finish _ hround
  rcases haccepted with
    ⟨⟨hunique, _, _, _, _, _, _, _, _, horacle, _, _, _, _, _, _, _⟩,
      _, haxioms, _⟩
  rcases haxioms with
    ⟨hdeterministic, _, hgenerated, hadvance, _, _, _, _, _,
      hdescriptors, hobservations⟩
  rcases hround with
    ⟨hstart, _, _, _, afterDrain, hdrain, roundEmissions, _,
      hcut, hexchange, hfinish⟩
  obtain ⟨scalarDrainFinish, hscalarDrain, hscalarFinish⟩ :=
    sequentialRoundDrain_materializes_scalar image transition
      hunique horacle hgenerated hdescriptors hdeterministic
      bounds start afterDrain finish drainedEvents
      hstart hdrain hexchange hfinish
  have htargetOrdered :=
    sequentialRoundDrain_targetKeyOrdered image transition
      hunique horacle hgenerated hdescriptors hdeterministic hadvance
      bounds start afterDrain drainedEvents hstart hdrain
  obtain ⟨serialOrder, serialFinish, hserialExecution,
      hserialOrdered, hserialPerm, hsortReplay⟩ :=
    executionInOrder_sort image transition
      hunique horacle hgenerated hdescriptors hdeterministic
      hadvance hobservations start.machine scalarDrainFinish
      drainedEvents hstart.2.1 htargetOrdered hscalarDrain
  have hserialEventsAbsent :=
    executionInOrder_events_not_pending image transition
      hunique horacle hgenerated hdescriptors
      start.machine serialFinish serialOrder hstart.2.1 hserialExecution
  have hnoFinal : NoEligibleEvent cut serialFinish.pending := by
    intro event heventPending heventCut
    have heventDrained : event ∈ drainedEvents :=
      (hcut.2.1 event).mpr heventCut
    have heventSerial : event ∈ serialOrder :=
      hserialPerm.mem_iff.mpr heventDrained
    exact hserialEventsAbsent event heventSerial heventPending
  have hcanonical :=
    executionInOrder_to_canonicalRestricted image transition
      hunique horacle hgenerated hdescriptors cut
      start.machine serialFinish serialOrder hstart.2.1
      hserialOrdered
      (fun event hevent =>
        (hcut.2.1 event).mp (hserialPerm.mem_iff.mp hevent))
      hnoFinal hserialExecution
  refine ⟨serialOrder, serialFinish, hcanonical, ?_, ?_⟩
  · intro event
    exact hserialPerm.mem_iff
  · exact strongMachineReplay_implies_result image
      serialFinish finish.machine
      (strongMachineReplay_trans image
        (strongMachineReplay_symm image hsortReplay)
        hscalarFinish)

end DaysExecutor
