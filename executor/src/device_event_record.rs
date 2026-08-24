#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
use crate::EventKind;

pub(crate) const EVENT_WORDS: usize = 14;
pub(crate) const CHANNEL_EVENT_WORDS: usize = 11;
pub(crate) const SERVICE_EVENT_WORDS: usize = 5;
pub(crate) const GENERATOR_EVENT_WORDS: usize = 10;
pub(crate) const REMOTE_STAGING_EVENT_WORDS: usize = 12;

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
#[allow(dead_code)] // Fallback is exercised by the round-trip control; runtime heaps stay 14 words.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StoredEventClass {
    Channel,
    Service,
    Generator,
    Fallback,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EncodedEventRecord {
    pub(crate) words: [u64; EVENT_WORDS],
    pub(crate) len: usize,
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) const fn stored_event_words(class: StoredEventClass) -> usize {
    match class {
        StoredEventClass::Channel => CHANNEL_EVENT_WORDS,
        StoredEventClass::Service => SERVICE_EVENT_WORDS,
        StoredEventClass::Generator => GENERATOR_EVENT_WORDS,
        StoredEventClass::Fallback => EVENT_WORDS,
    }
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) const fn stream_event_class(
    stream: usize,
    service_stream_base: usize,
    generator_stream_base: usize,
) -> StoredEventClass {
    if stream < service_stream_base {
        StoredEventClass::Channel
    } else if stream < generator_stream_base {
        StoredEventClass::Service
    } else {
        StoredEventClass::Generator
    }
}

#[cfg(test)]
pub(crate) fn encode_event_record(
    record: [u64; EVENT_WORDS],
    class: StoredEventClass,
) -> EncodedEventRecord {
    let mut words = [0; EVENT_WORDS];
    words[..4].copy_from_slice(&record[..4]);
    match class {
        StoredEventClass::Channel => {
            debug_assert_eq!(record[5], EventKind::RemoteArrival as u64);
            words[4] = record[6];
            words[5..11].copy_from_slice(&record[8..14]);
        }
        StoredEventClass::Service => {
            debug_assert!(matches!(
                record[5],
                value if value == EventKind::TxReady as u64
                    || value == EventKind::TxComplete as u64
            ));
            words[4] = record[6];
        }
        StoredEventClass::Generator => {
            debug_assert_eq!(record[5], EventKind::PacketArrival as u64);
            words[4] = record[6];
            words[5..10].copy_from_slice(&record[9..14]);
        }
        StoredEventClass::Fallback => words = record,
    }
    EncodedEventRecord {
        words,
        len: stored_event_words(class),
    }
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) fn decode_event_record(
    encoded: &[u64],
    class: StoredEventClass,
    node: u64,
    flow: Option<u64>,
    in_service: Option<&[u64; EVENT_WORDS]>,
) -> [u64; EVENT_WORDS] {
    debug_assert_eq!(encoded.len(), stored_event_words(class));
    let mut record = [0; EVENT_WORDS];
    match class {
        StoredEventClass::Channel => {
            record[..4].copy_from_slice(&encoded[..4]);
            record[4] = node;
            record[5] = EventKind::RemoteArrival as u64;
            record[6] = encoded[4];
            record[7] = encoded[4];
            record[8..14].copy_from_slice(&encoded[5..11]);
        }
        StoredEventClass::Service => {
            record[..4].copy_from_slice(&encoded[..4]);
            record[4] = node;
            record[5] = if record[1] == 2 {
                EventKind::TxReady as u64
            } else {
                EventKind::TxComplete as u64
            };
            record[6] = encoded[4];
            if record[5] == EventKind::TxComplete as u64 {
                let service = in_service.expect("TX_COMPLETE requires in-service hydration");
                debug_assert_eq!(service[7], record[6]);
                record[7..14].copy_from_slice(&service[7..14]);
            }
        }
        StoredEventClass::Generator => {
            record[..4].copy_from_slice(&encoded[..4]);
            record[4] = node;
            record[5] = EventKind::PacketArrival as u64;
            record[6] = encoded[4];
            record[7] = encoded[4];
            record[8] = flow.expect("generator record requires its stream flow");
            record[9..14].copy_from_slice(&encoded[5..10]);
        }
        StoredEventClass::Fallback => record.copy_from_slice(encoded),
    }
    record
}

#[cfg(test)]
mod tests {
    use super::*;

    const PACKET_ARRIVAL: u64 = 0;
    const TX_READY: u64 = 1;
    const TX_COMPLETE: u64 = 2;
    const REMOTE_ARRIVAL: u64 = 3;
    const RETRANSMISSION_TIMEOUT: u64 = 4;
    const PACING_TIMER: u64 = 5;

    fn record(kind: u64, phase: u64) -> [u64; EVENT_WORDS] {
        [
            101, phase, 202, 303, 404, kind, 505, 505, 606, 707, 2, 808, 909, 1,
        ]
    }

    fn assert_logical_event_eq(left: &[u64; EVENT_WORDS], right: &[u64; EVENT_WORDS]) {
        assert_eq!(&left[..7], &right[..7]);
    }

    #[test]
    fn packet_arrival_round_trips_in_generator_record() {
        let expected = record(PACKET_ARRIVAL, 0);
        let encoded = encode_event_record(expected, StoredEventClass::Generator);
        assert_eq!(encoded.len, GENERATOR_EVENT_WORDS);
        let decoded = decode_event_record(
            &encoded.words[..encoded.len],
            StoredEventClass::Generator,
            expected[4],
            Some(expected[8]),
            None,
        );
        assert_eq!(decoded, expected);
    }

    #[test]
    fn tx_ready_round_trips_logical_event_without_packet_copy() {
        let expected = record(TX_READY, 2);
        let encoded = encode_event_record(expected, StoredEventClass::Service);
        assert_eq!(encoded.len, SERVICE_EVENT_WORDS);
        let decoded = decode_event_record(
            &encoded.words[..encoded.len],
            StoredEventClass::Service,
            expected[4],
            None,
            None,
        );
        assert_logical_event_eq(&decoded, &expected);
        assert_eq!(&decoded[7..], &[0; EVENT_WORDS - 7]);
    }

    #[test]
    fn tx_complete_round_trips_with_in_service_hydration() {
        let expected = record(TX_COMPLETE, 1);
        let mut service = [0; EVENT_WORDS];
        service[7..].copy_from_slice(&expected[7..]);
        let encoded = encode_event_record(expected, StoredEventClass::Service);
        assert_eq!(encoded.len, SERVICE_EVENT_WORDS);
        let decoded = decode_event_record(
            &encoded.words[..encoded.len],
            StoredEventClass::Service,
            expected[4],
            None,
            Some(&service),
        );
        assert_eq!(decoded, expected);
    }

    #[test]
    fn remote_arrival_round_trips_in_channel_record() {
        let expected = record(REMOTE_ARRIVAL, 0);
        let encoded = encode_event_record(expected, StoredEventClass::Channel);
        assert_eq!(encoded.len, CHANNEL_EVENT_WORDS);
        let decoded = decode_event_record(
            &encoded.words[..encoded.len],
            StoredEventClass::Channel,
            expected[4],
            None,
            None,
        );
        assert_eq!(decoded, expected);
    }

    #[test]
    fn fallback_timer_kinds_remain_exact_full_records() {
        for (kind, phase) in [(RETRANSMISSION_TIMEOUT, 1), (PACING_TIMER, 1)] {
            let expected = record(kind, phase);
            let encoded = encode_event_record(expected, StoredEventClass::Fallback);
            assert_eq!(encoded.len, EVENT_WORDS);
            let decoded = decode_event_record(
                &encoded.words[..encoded.len],
                StoredEventClass::Fallback,
                expected[4],
                None,
                None,
            );
            assert_eq!(decoded, expected);
        }
    }

    #[test]
    fn stream_ranges_select_their_physical_record_widths() {
        assert_eq!(
            stored_event_words(stream_event_class(1, 2, 5)),
            CHANNEL_EVENT_WORDS
        );
        assert_eq!(
            stored_event_words(stream_event_class(2, 2, 5)),
            SERVICE_EVENT_WORDS
        );
        assert_eq!(
            stored_event_words(stream_event_class(5, 2, 5)),
            GENERATOR_EVENT_WORDS
        );
        assert_eq!(REMOTE_STAGING_EVENT_WORDS, CHANNEL_EVENT_WORDS + 1);
    }
}
