import DaysExecutor.Counterexamples

namespace DaysExecutor

theorem relayOneHopBoundUnsoundCheck_true :
    relayOneHopBoundUnsoundCheck = true := by
  native_decide

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
  native_decide

theorem queueCounterexampleHorizonSoundCheck_true :
    queueCounterexampleHorizonSoundCheck = true := by
  native_decide

theorem eagerSelectionCounterexampleCheck_true :
    eagerSelectionCounterexampleCheck = true := by
  native_decide

theorem descriptorStoreOrderIndependenceCheck_true :
    descriptorStoreOrderIndependenceCheck = true := by
  native_decide

theorem referenceCountBpacCommutativityCheck_true :
    referenceCountBpacCommutativityCheck = true := by
  native_decide

theorem fifoPerLPStoreCommutationCheck_true :
    fifoPerLPStoreCommutationCheck = true := by
  native_decide

theorem fifoStrengthenedServiceContractCheck_true :
    fifoStrengthenedServiceContractCheck = true := by
  native_decide

theorem packetLifetimeRemovalRaceRejectedCheck_true :
    packetLifetimeRemovalRaceRejectedCheck = true := by
  native_decide

theorem referenceProvenanceLaundererRejectedCheck_true :
    referenceProvenanceLaundererRejectedCheck = true := by
  native_decide

theorem aliasedObservationKeyRejectedCheck_true :
    aliasedObservationKeyRejectedCheck = true := by
  native_decide

theorem eagerSelectionPrivateSmugglingCheck_true :
    eagerSelectionPrivateSmugglingCheck = true := by
  native_decide

theorem privatePayloadSmugglingCheck_true :
    privatePayloadSmugglingCheck = true := by
  native_decide

theorem committedServiceErasureCheck_true :
    committedServiceErasureCheck = true := by
  native_decide

theorem committedServiceDuplicatorCheck_true :
    committedServiceDuplicatorCheck = true := by
  native_decide

theorem committedServicePartialEraserCheck_true :
    committedServicePartialEraserCheck = true := by
  native_decide

theorem unsoundReorderingCounterexampleCheck_true :
    unsoundReorderingCounterexampleCheck = true := by
  native_decide

end DaysExecutor
