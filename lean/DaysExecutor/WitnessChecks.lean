import DaysExecutor.Counterexamples

namespace DaysExecutor

theorem relayOneHopBoundUnsoundCheck_true :
    relayOneHopBoundUnsoundCheck = true := by
  decide

theorem halfOpenBoundaryCheck_true (horizon : Nat) :
    halfOpenBoundaryCheck horizon = true := by
  simp [halfOpenBoundaryCheck, belowBound, EventKey.lt_timeBoundary_iff,
    eventAtHorizon]

theorem inclusiveStopTranslationCheck_true (stopTimeNs : Nat) :
    inclusiveStopTranslationCheck stopTimeNs = true := by
  simp [inclusiveStopTranslationCheck, withinInclusiveStop, belowBound,
    EventKey.lt_timeBoundary_iff, eventAtHorizon, stopExclusive]

theorem maximumStopTranslationCheck_true :
    maximumStopTranslationCheck = true := by
  decide

theorem queueCounterexampleHorizonSoundCheck_true :
    queueCounterexampleHorizonSoundCheck = true := by
  decide

theorem eagerSelectionCounterexampleCheck_true :
    eagerSelectionCounterexampleCheck = true := by
  decide

theorem descriptorStoreOrderIndependenceCheck_true :
    descriptorStoreOrderIndependenceCheck = true := by
  decide

theorem referenceCountBpacCommutativityCheck_true :
    referenceCountBpacCommutativityCheck = true := by
  decide

theorem fifoPerLPStoreCommutationCheck_true :
    fifoPerLPStoreCommutationCheck = true := by
  decide

theorem fifoStrengthenedServiceContractCheck_true :
    fifoStrengthenedServiceContractCheck = true := by
  decide

theorem packetLifetimeRemovalRaceRejectedCheck_true :
    packetLifetimeRemovalRaceRejectedCheck = true := by
  decide

theorem referenceProvenanceLaundererRejectedCheck_true :
    referenceProvenanceLaundererRejectedCheck = true := by
  decide

theorem aliasedObservationKeyRejectedCheck_true :
    aliasedObservationKeyRejectedCheck = true := by
  decide

theorem eagerSelectionPrivateSmugglingCheck_true :
    eagerSelectionPrivateSmugglingCheck = true := by
  decide

theorem privatePayloadSmugglingCheck_true :
    privatePayloadSmugglingCheck = true := by
  decide

theorem committedServiceErasureCheck_true :
    committedServiceErasureCheck = true := by
  decide

theorem committedServiceDuplicatorCheck_true :
    committedServiceDuplicatorCheck = true := by
  decide

theorem committedServicePartialEraserCheck_true :
    committedServicePartialEraserCheck = true := by
  decide

theorem unsoundReorderingCounterexampleCheck_true :
    unsoundReorderingCounterexampleCheck = true := by
  decide

end DaysExecutor
