//! The host future-event list: the pending events of one queue owner (the Scalar run, or one CPU
//! LP), popped in ascending `EventKey` order.
//!
//! Every event kind except `RetransmissionTimeout` lives in a binary min-heap on `EventKey`. The
//! retransmission timeouts keep an ordered map, because a superseded timer is removed from the
//! middle of the queue (`remove_superseded_timer`); pop-min takes the smaller of the two heads.
//!
//! **Exactness.** `EventKey`s are unique by construction (`origin_seq` is allocated per origin
//! node), so every correct min-queue pops the same sequence, and the heap's internal layout can
//! reach no output: the only ways out are `pop` and `into_sorted_vec`, both in key order, and
//! `count_below`, an order-free count.
//!
//! **Duplicate keys.** The ordered map refused a duplicate on insert; the heap cannot see one
//! there. In a valid run the popped keys are strictly increasing: a child is strictly later than
//! its parent (`NonMonotoneChild`), and a remote event lands at or after the round horizon
//! (`RemoteEventBeforeHorizon`). So when one of two equal keys is popped, the other is still
//! pending and is the new minimum: `pop` reports `DuplicateEventKey` then. A duplicate pair still
//! pending when the run stops is reported by `into_sorted_vec`, where the two are adjacent. An
//! image that runs to `Ok` therefore produces the same bytes as with the ordered map; an image
//! with a duplicate key still fails with `DuplicateEventKey`, possibly later in the run.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};

use crate::event::{Event, EventKey, EventKind};
use crate::scalar::{ExecutionError, SupersededTimer, remove_superseded_timer};

/// A pending event ordered by its key, reversed, so that `BinaryHeap`'s maximum is the minimum
/// key. Same size and alignment as `Event`, so draining the heap reuses its buffer.
#[derive(Clone, Copy)]
#[repr(transparent)]
struct Pending(Event);

impl PartialEq for Pending {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0.key == other.0.key
    }
}

impl Eq for Pending {}

impl PartialOrd for Pending {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Pending {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.key.cmp(&self.0.key)
    }
}

/// The pending events of one executor queue owner (the Scalar run, or one CPU LP).
///
/// Holds no iteration API: events leave in key order through `pop` and `into_sorted_vec`.
#[derive(Default)]
pub(crate) struct FutureEvents {
    /// Every pending event but the retransmission timeouts.
    heap: BinaryHeap<Pending>,
    /// The pending retransmission timeouts, which a superseding transition removes by identity.
    timers: BTreeMap<EventKey, Event>,
}

impl FutureEvents {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// A queue for `capacity` events of any kind but retransmission timeouts, allocated once.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(capacity),
            timers: BTreeMap::new(),
        }
    }

    /// Adds one pending event. Only a retransmission timeout's duplicate is seen here; any other
    /// duplicate is reported when it is popped or drained.
    #[inline]
    pub(crate) fn insert(&mut self, event: Event) -> Result<(), ExecutionError> {
        if event.kind == EventKind::RetransmissionTimeout {
            if self.timers.insert(event.key, event).is_some() {
                return Err(ExecutionError::DuplicateEventKey(event.key));
            }
        } else {
            self.heap.push(Pending(event));
        }
        Ok(())
    }

    /// The minimum pending key.
    #[inline]
    pub(crate) fn peek_key(&self) -> Option<EventKey> {
        let queued = self.heap.peek().map(|pending| pending.0.key);
        if self.timers.is_empty() {
            return queued;
        }
        let timer = self.timers.first_key_value().map(|(key, _)| *key);
        match (queued, timer) {
            (Some(queued), Some(timer)) => Some(queued.min(timer)),
            (queued, timer) => queued.or(timer),
        }
    }

    /// Removes and returns the minimum pending event. A pending event with the same key, which
    /// is then the minimum, is a duplicate.
    #[inline]
    pub(crate) fn pop(&mut self) -> Result<Option<Event>, ExecutionError> {
        let from_timers = self.timers.first_key_value().is_some_and(|(timer, _)| {
            self.heap
                .peek()
                .is_none_or(|pending| *timer < pending.0.key)
        });
        let event = if from_timers {
            self.timers.pop_first().map(|(_, event)| event)
        } else {
            self.heap.pop().map(|pending| pending.0)
        };
        if let Some(event) = event {
            if self.peek_key() == Some(event.key) {
                return Err(ExecutionError::DuplicateEventKey(event.key));
            }
        }
        Ok(event)
    }

    /// Removes the pending event that carries a superseded timer identity.
    pub(crate) fn remove_superseded_timer(
        &mut self,
        timer: SupersededTimer,
    ) -> Result<Event, ExecutionError> {
        remove_superseded_timer(&mut self.timers, timer)
    }

    /// The pending events whose time is below `exclusive_horizon_ns`: an exact, order-free count.
    pub(crate) fn count_below(&self, exclusive_horizon_ns: u128) -> usize {
        let below = |key: &EventKey| u128::from(key.time_ns) < exclusive_horizon_ns;
        self.heap
            .iter()
            .filter(|pending| below(&pending.0.key))
            .count()
            + self.timers.keys().take_while(|key| below(key)).count()
    }

    /// The pending events in ascending key order. Two adjacent equal keys are a duplicate.
    ///
    /// The `Vec` is the heap's own buffer and keeps its capacity: a caller that holds it beyond
    /// the drain (the Scalar result) shrinks it.
    pub(crate) fn into_sorted_vec(self) -> Result<Vec<Event>, ExecutionError> {
        // `Pending` is `Event` in a transparent wrapper, so this collect reuses the heap's buffer.
        let mut events = self
            .heap
            .into_vec()
            .into_iter()
            .map(|pending| pending.0)
            .collect::<Vec<_>>();
        events.extend(self.timers.into_values());
        events.sort_unstable_by_key(|event| event.key);
        if let Some(pair) = events.windows(2).find(|pair| pair[0].key == pair[1].key) {
            return Err(ExecutionError::DuplicateEventKey(pair[1].key));
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{NodeId, PayloadId, event_phase};

    fn event(time_ns: u64, origin_seq: u64, kind: EventKind) -> Event {
        Event {
            key: EventKey {
                time_ns,
                phase: event_phase(kind),
                origin_node: NodeId(3),
                origin_seq,
            },
            target: NodeId(3),
            kind,
            payload: PayloadId(origin_seq),
        }
    }

    /// A deterministic xorshift stream for the model tests.
    struct Stream(u64);

    impl Stream {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    const KINDS: [EventKind; 6] = [
        EventKind::PacketArrival,
        EventKind::TxReady,
        EventKind::TxComplete,
        EventKind::RemoteArrival,
        EventKind::RetransmissionTimeout,
        EventKind::PacingTimer,
    ];

    /// Pops, peeks, superseded-timer removals and the final drain agree with the ordered map they
    /// replace, under a monotone churn of every event kind (children strictly later than the
    /// popped event, as a run makes them).
    #[test]
    fn future_events_match_the_ordered_map_under_monotone_churn() {
        let mut queue = FutureEvents::new();
        let mut model = BTreeMap::<EventKey, Event>::new();
        let mut stream = Stream(0x2545_f491_4f6c_dd1d);
        let mut origin_seq = 0_u64;
        let mut now = 0_u64;
        let mut removals = 0_u64;
        for step in 0..60_000_u64 {
            let draw = stream.next();
            if draw.is_multiple_of(3) && !model.is_empty() {
                assert_eq!(
                    queue.peek_key(),
                    model.first_key_value().map(|(key, _)| *key)
                );
                let popped = queue.pop().expect("no duplicate key");
                let expected = model.pop_first().map(|(_, event)| event);
                assert_eq!(popped, expected, "step {step}");
                now = popped.expect("a pending event").key.time_ns;
            } else if draw % 7 == 1 {
                // Supersede the earliest pending retransmission timeout, as a sender does.
                let Some(timer) = model
                    .values()
                    .find(|event| event.kind == EventKind::RetransmissionTimeout)
                    .copied()
                else {
                    continue;
                };
                let superseded = SupersededTimer {
                    target: timer.target,
                    payload: timer.payload,
                    deadline_ns: timer.key.time_ns,
                };
                let removed = queue
                    .remove_superseded_timer(superseded)
                    .expect("the timer is pending");
                assert_eq!(removed, timer);
                model.remove(&timer.key);
                removals += 1;
            } else {
                origin_seq += 1;
                let kind = KINDS[(draw >> 8) as usize % KINDS.len()];
                let child = event(now + 1 + (draw >> 16) % 5_000, origin_seq, kind);
                queue.insert(child).expect("a fresh key");
                assert!(model.insert(child.key, child).is_none());
            }
            if !step.is_multiple_of(64) {
                continue;
            }
            assert_eq!(
                queue.count_below(u128::from(now) + 2_500),
                model
                    .keys()
                    .take_while(|key| u128::from(key.time_ns) < u128::from(now) + 2_500)
                    .count()
            );
        }
        assert!(removals > 100, "the churn removed {removals} timers");
        assert!(model.len() > 100, "the churn left {} pending", model.len());
        assert_eq!(
            queue.into_sorted_vec().expect("no duplicate key"),
            model.into_values().collect::<Vec<_>>()
        );
    }

    /// A missing superseded timer is reported, as the ordered map reported it.
    #[test]
    fn a_missing_superseded_timer_is_reported() {
        let mut queue = FutureEvents::new();
        queue
            .insert(event(10, 1, EventKind::PacingTimer))
            .expect("a fresh key");
        let error = queue
            .remove_superseded_timer(SupersededTimer {
                target: NodeId(3),
                payload: PayloadId(1),
                deadline_ns: 10,
            })
            .expect_err("a pacing timer is not a retransmission timeout");
        assert!(matches!(
            error,
            ExecutionError::SupersededTimerMissing { .. }
        ));
    }

    /// A duplicate key in the heap is reported when the first of the two is popped.
    #[test]
    fn a_duplicate_key_is_reported_when_popped() {
        let mut queue = FutureEvents::new();
        let first = event(10, 1, EventKind::TxReady);
        queue.insert(event(5, 0, EventKind::TxReady)).unwrap();
        queue.insert(first).unwrap();
        queue
            .insert(Event {
                payload: PayloadId(9),
                ..first
            })
            .unwrap();
        assert_eq!(queue.pop().unwrap().map(|event| event.key.time_ns), Some(5));
        assert!(matches!(
            queue.pop(),
            Err(ExecutionError::DuplicateEventKey(key)) if key == first.key
        ));
    }

    /// A duplicate key across the heap and the timer map is reported when popped.
    #[test]
    fn a_duplicate_key_across_heap_and_timers_is_reported_when_popped() {
        let mut queue = FutureEvents::new();
        let timer = event(10, 1, EventKind::RetransmissionTimeout);
        queue.insert(timer).unwrap();
        queue
            .insert(Event {
                kind: EventKind::PacingTimer,
                ..timer
            })
            .unwrap();
        assert!(matches!(
            queue.pop(),
            Err(ExecutionError::DuplicateEventKey(key)) if key == timer.key
        ));
    }

    /// Two retransmission timeouts with one key are refused on insert, as by the ordered map.
    #[test]
    fn a_duplicate_timer_key_is_refused_on_insert() {
        let mut queue = FutureEvents::new();
        let timer = event(10, 1, EventKind::RetransmissionTimeout);
        queue.insert(timer).unwrap();
        assert!(matches!(
            queue.insert(timer),
            Err(ExecutionError::DuplicateEventKey(key)) if key == timer.key
        ));
    }

    /// A duplicate key still pending when the run stops is reported by the drain.
    #[test]
    fn a_pending_duplicate_key_is_reported_by_the_drain() {
        let mut queue = FutureEvents::with_capacity(4);
        let duplicate = event(10, 1, EventKind::RemoteArrival);
        queue.insert(event(20, 2, EventKind::TxReady)).unwrap();
        queue.insert(duplicate).unwrap();
        queue
            .insert(event(7, 3, EventKind::RetransmissionTimeout))
            .unwrap();
        queue.insert(duplicate).unwrap();
        assert!(matches!(
            queue.into_sorted_vec(),
            Err(ExecutionError::DuplicateEventKey(key)) if key == duplicate.key
        ));
    }

    /// Every CPU LP carries one: the heap and the timer map, 24 B each, and nothing else.
    #[test]
    fn the_future_event_list_is_two_containers() {
        assert_eq!(std::mem::size_of::<FutureEvents>(), 48);
    }

    /// The drain reuses the heap's buffer: an empty queue allocates nothing, and the heap's
    /// events come back in the same allocation.
    #[test]
    fn the_drain_reuses_the_heap_buffer() {
        assert_eq!(FutureEvents::new().into_sorted_vec().unwrap().capacity(), 0);
        let mut queue = FutureEvents::with_capacity(8);
        for seq in 0..8 {
            queue
                .insert(event(100 - seq, seq, EventKind::TxComplete))
                .unwrap();
        }
        let buffer = queue.heap.as_slice().as_ptr().cast::<Event>();
        let drained = queue.into_sorted_vec().unwrap();
        assert_eq!(drained.as_ptr(), buffer);
        assert!(drained.windows(2).all(|pair| pair[0].key < pair[1].key));
    }
}
