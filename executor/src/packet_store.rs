//! The host packet store: the packets resident at one executor queue owner (the Scalar run, or one
//! CPU LP), by payload identity.
//!
//! A store of at most `SMALL_LIMIT` packets is an unsorted `Vec` searched linearly: one
//! allocation, like the single `BTreeMap` leaf it replaces, and no index for a switch port's few
//! packets. Above that the packets live in a slab (a `Vec` of slots with a free list, so it never
//! holds more slots than the store's high-water packet count) behind an open-addressing index of
//! 16-B `(PayloadId, slot)` buckets with linear probing. An index growth step moves 16 B per
//! packet, not the packet. Deletion shifts the rest of the probe run back (no tombstones), so
//! churn never forces a growth step. A store that empties drops its slab and index and is an
//! empty `Vec` again, so a queue owner that has drained (a switch port after a burst) holds
//! nothing: on a CPU run the LPs' stores peak at different times, and kept slabs would add up.
//! The index hashes with a fixed multiply-shift (`bucket_of`), so even the layout is a pure
//! function of the operations.
//!
//! **Exactness.** The store answers point lookups only and has no iteration API; the one way out
//! is `into_sorted`, which consumes it and returns the packets in ascending payload order, the
//! order of the `BTreeMap` it replaces. No output can depend on the layout.
//!
//! **Size.** 24 B, the size of the `BTreeMap` it replaces, so `TransitionState` keeps its size.

use crate::event::PayloadId;

/// A packet the store holds, keyed by its own payload identity.
pub(crate) trait Resident {
    fn payload(&self) -> PayloadId;
}

/// Above this many packets a small store becomes an indexed slab, and stays one.
const SMALL_LIMIT: usize = 16;
/// The index's first size in buckets (a power of two).
const FIRST_BUCKETS: usize = 64;
/// A bucket's slot when the bucket holds no packet; never a slot number.
const EMPTY: u32 = u32::MAX;
/// The end of the slab's free list.
const NO_SLOT: u32 = u32::MAX;
/// 2^64 divided by the golden ratio: Fibonacci hashing's multiplier.
const FIBONACCI: u64 = 0x9e37_79b9_7f4a_7c15;

/// The packets resident at one executor queue owner, by payload identity.
pub(crate) struct PacketStore<V>(Repr<V>);

enum Repr<V> {
    Small(Vec<V>),
    Large(Box<Slab<V>>),
}

#[derive(Clone, Copy)]
struct Bucket {
    payload: u64,
    slot: u32,
}

const EMPTY_BUCKET: Bucket = Bucket {
    payload: 0,
    slot: EMPTY,
};

enum Slot<V> {
    Full(V),
    /// A free slot, with the next free slot (or `NO_SLOT`).
    Vacant(u32),
}

struct Slab<V> {
    /// The index: a power-of-two count of buckets, at most 7/8 full.
    buckets: Vec<Bucket>,
    /// 64 minus log2 of the bucket count: `bucket_of`'s shift.
    shift: u32,
    len: usize,
    slots: Vec<Slot<V>>,
    /// The first free slot (or `NO_SLOT`).
    free: u32,
}

/// The home bucket of a payload identity: a fixed multiply-shift hash, the top bits of the
/// product, so payload identities that differ only in their low bits (consecutive sequence
/// numbers of one source, `PayloadId::from_node_sequence`) still spread.
#[inline]
fn bucket_of(payload: u64, shift: u32) -> usize {
    (payload.wrapping_mul(FIBONACCI) >> shift) as usize
}

impl<V> Slab<V> {
    fn with_buckets(buckets: usize) -> Self {
        debug_assert!(buckets.is_power_of_two() && buckets >= 2);
        Self {
            buckets: vec![EMPTY_BUCKET; buckets],
            shift: 64 - buckets.trailing_zeros(),
            len: 0,
            slots: Vec::new(),
            free: NO_SLOT,
        }
    }

    #[inline]
    fn mask(&self) -> usize {
        self.buckets.len() - 1
    }

    /// The bucket holding `payload`, if any.
    #[inline]
    fn find(&self, payload: u64) -> Option<usize> {
        let mask = self.mask();
        let mut index = bucket_of(payload, self.shift);
        loop {
            let bucket = self.buckets[index];
            if bucket.slot == EMPTY {
                return None;
            }
            if bucket.payload == payload {
                return Some(index);
            }
            index = (index + 1) & mask;
        }
    }

    /// Places a payload known to be absent in the first empty bucket of its probe run.
    #[inline]
    fn place(&mut self, payload: u64, slot: u32) {
        let mask = self.mask();
        let mut index = bucket_of(payload, self.shift);
        while self.buckets[index].slot != EMPTY {
            index = (index + 1) & mask;
        }
        self.buckets[index] = Bucket { payload, slot };
    }

    /// Doubles the index and re-places every bucket; the slab does not move.
    fn grow(&mut self) {
        let buckets = self.buckets.len() * 2;
        let old = std::mem::replace(&mut self.buckets, vec![EMPTY_BUCKET; buckets]);
        self.shift = 64 - buckets.trailing_zeros();
        for bucket in old {
            if bucket.slot != EMPTY {
                self.place(bucket.payload, bucket.slot);
            }
        }
    }

    #[inline]
    fn full(slot: &Slot<V>) -> &V {
        match slot {
            Slot::Full(value) => value,
            Slot::Vacant(_) => unreachable!("an indexed slot holds a packet"),
        }
    }

    #[inline]
    fn get(&self, payload: u64) -> Option<&V> {
        let bucket = self.buckets[self.find(payload)?];
        Some(Self::full(&self.slots[bucket.slot as usize]))
    }

    #[inline]
    fn get_mut(&mut self, payload: u64) -> Option<&mut V> {
        let bucket = self.buckets[self.find(payload)?];
        match &mut self.slots[bucket.slot as usize] {
            Slot::Full(value) => Some(value),
            Slot::Vacant(_) => unreachable!("an indexed slot holds a packet"),
        }
    }

    #[inline]
    fn insert(&mut self, payload: u64, value: V) -> Option<V> {
        if let Some(index) = self.find(payload) {
            let slot = self.buckets[index].slot as usize;
            return match std::mem::replace(&mut self.slots[slot], Slot::Full(value)) {
                Slot::Full(old) => Some(old),
                Slot::Vacant(_) => unreachable!("an indexed slot holds a packet"),
            };
        }
        if (self.len + 1) * 8 > self.buckets.len() * 7 {
            self.grow();
        }
        let slot = if self.free == NO_SLOT {
            let slot = u32::try_from(self.slots.len())
                .ok()
                .filter(|slot| *slot != EMPTY)
                .expect("fewer than 2^32 - 1 resident packets");
            self.slots.push(Slot::Full(value));
            slot
        } else {
            let slot = self.free;
            self.free = match std::mem::replace(&mut self.slots[slot as usize], Slot::Full(value)) {
                Slot::Vacant(next) => next,
                Slot::Full(_) => unreachable!("the free list holds vacant slots"),
            };
            slot
        };
        self.place(payload, slot);
        self.len += 1;
        None
    }

    #[inline]
    fn remove(&mut self, payload: u64) -> Option<V> {
        let mut hole = self.find(payload)?;
        let slot = self.buckets[hole].slot;
        let value = match std::mem::replace(&mut self.slots[slot as usize], Slot::Vacant(self.free))
        {
            Slot::Full(value) => value,
            Slot::Vacant(_) => unreachable!("an indexed slot holds a packet"),
        };
        self.free = slot;
        self.len -= 1;
        // Backward-shift deletion: move later members of the probe run into the hole, unless a
        // member's home bucket lies cyclically in (hole, next], where it must stay.
        let mask = self.mask();
        let mut next = (hole + 1) & mask;
        while self.buckets[next].slot != EMPTY {
            let home = bucket_of(self.buckets[next].payload, self.shift);
            let stays = if hole <= next {
                hole < home && home <= next
            } else {
                hole < home || home <= next
            };
            if !stays {
                self.buckets[hole] = self.buckets[next];
                hole = next;
            }
            next = (next + 1) & mask;
        }
        self.buckets[hole] = EMPTY_BUCKET;
        Some(value)
    }
}

impl<V> Default for PacketStore<V> {
    fn default() -> Self {
        Self(Repr::Small(Vec::new()))
    }
}

impl<V: Resident> PacketStore<V> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub(crate) fn get(&self, payload: &PayloadId) -> Option<&V> {
        match &self.0 {
            Repr::Small(packets) => packets.iter().find(|packet| packet.payload() == *payload),
            Repr::Large(slab) => slab.get(payload.0),
        }
    }

    #[inline]
    pub(crate) fn get_mut(&mut self, payload: &PayloadId) -> Option<&mut V> {
        match &mut self.0 {
            Repr::Small(packets) => packets
                .iter_mut()
                .find(|packet| packet.payload() == *payload),
            Repr::Large(slab) => slab.get_mut(payload.0),
        }
    }

    #[inline]
    pub(crate) fn contains_key(&self, payload: &PayloadId) -> bool {
        self.get(payload).is_some()
    }

    /// Stores `packet` under its own payload identity, returning the packet it replaces.
    #[inline]
    pub(crate) fn insert(&mut self, packet: V) -> Option<V> {
        let payload = packet.payload();
        match &mut self.0 {
            Repr::Small(packets) => {
                if let Some(old) = packets.iter_mut().find(|old| old.payload() == payload) {
                    return Some(std::mem::replace(old, packet));
                }
                if packets.len() < SMALL_LIMIT {
                    packets.push(packet);
                    return None;
                }
                let mut slab = Box::new(Slab::with_buckets(FIRST_BUCKETS));
                for old in std::mem::take(packets) {
                    slab.insert(old.payload().0, old);
                }
                slab.insert(payload.0, packet);
                self.0 = Repr::Large(slab);
                None
            }
            Repr::Large(slab) => slab.insert(payload.0, packet),
        }
    }

    #[inline]
    pub(crate) fn remove(&mut self, payload: &PayloadId) -> Option<V> {
        match &mut self.0 {
            Repr::Small(packets) => {
                let position = packets
                    .iter()
                    .position(|packet| packet.payload() == *payload)?;
                Some(packets.swap_remove(position))
            }
            Repr::Large(slab) => {
                let removed = slab.remove(payload.0);
                if slab.len == 0 {
                    // Release the slab and its index: a queue owner that drained keeps nothing.
                    self.0 = Repr::Small(Vec::new());
                }
                removed
            }
        }
    }

    /// Every resident packet in ascending payload order: the one way out of the store.
    pub(crate) fn into_sorted(self) -> Vec<V> {
        let mut packets = match self.0 {
            Repr::Small(packets) => packets,
            Repr::Large(slab) => slab
                .slots
                .into_iter()
                .filter_map(|slot| match slot {
                    Slot::Full(packet) => Some(packet),
                    Slot::Vacant(_) => None,
                })
                .collect(),
        };
        packets.sort_unstable_by_key(Resident::payload);
        packets
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct Packet {
        payload: PayloadId,
        value: u64,
    }

    impl Resident for Packet {
        fn payload(&self) -> PayloadId {
            self.payload
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

    /// The store answers every lookup, insert and remove as the `BTreeMap` it replaces, under a
    /// churn that crosses the small limit and several index growths and wraps probe runs, with
    /// payload identities spaced as `PayloadId::from_node_sequence` spaces them.
    #[test]
    fn the_store_matches_the_ordered_map_under_churn() {
        let mut store = PacketStore::new();
        let mut model = BTreeMap::new();
        let mut stream = Stream(0x2545_f491_4f6c_dd1d);
        let mut largest = 0;
        for step in 0..200_000_u64 {
            let draw = stream.next();
            // Grow towards about 3,000 resident packets, then churn around that size.
            let payload = PayloadId(draw % 4_096 * 1_280 + draw % 7);
            let packet = Packet {
                payload,
                value: step,
            };
            match (draw >> 32) % 5 {
                0 | 1 => assert_eq!(
                    store.insert(packet),
                    model.insert(payload, packet),
                    "step {step}"
                ),
                2 => assert_eq!(
                    store.remove(&payload),
                    model.remove(&payload),
                    "step {step}"
                ),
                3 => {
                    assert_eq!(store.get(&payload), model.get(&payload), "step {step}");
                    assert_eq!(store.contains_key(&payload), model.contains_key(&payload));
                }
                _ => {
                    if let Some(packet) = store.get_mut(&payload) {
                        packet.value += 1;
                    }
                    if let Some(packet) = model.get_mut(&payload) {
                        packet.value += 1;
                    }
                }
            }
            largest = largest.max(model.len());
        }
        assert!(largest > 1_000, "the churn reached {largest} packets");
        assert_eq!(store.into_sorted(), model.into_values().collect::<Vec<_>>());
    }

    /// A store that never exceeds the small limit stays one `Vec`, and drains sorted.
    #[test]
    fn a_small_store_drains_sorted() {
        let mut store = PacketStore::new();
        for payload in [9_u64, 3, 12, 1, 7] {
            assert!(
                store
                    .insert(Packet {
                        payload: PayloadId(payload),
                        value: payload,
                    })
                    .is_none()
            );
        }
        assert!(store.remove(&PayloadId(12)).is_some());
        assert!(store.remove(&PayloadId(12)).is_none());
        assert!(matches!(store.0, Repr::Small(_)));
        let drained = store.into_sorted();
        assert_eq!(
            drained
                .iter()
                .map(|packet| packet.payload.0)
                .collect::<Vec<_>>(),
            [1, 3, 7, 9]
        );
    }

    /// Removing all but one packet of a large store, in insertion order, empties every other
    /// bucket: the backward shift leaves no tombstone and loses no packet on the way. The slab
    /// keeps its slots on its free list while it holds a packet, and reuses them.
    #[test]
    fn removal_leaves_no_tombstone() {
        let mut store = PacketStore::new();
        let payloads = (0..2_000_u64)
            .map(|seq| seq * 1_280 + 5)
            .collect::<Vec<_>>();
        for &payload in &payloads {
            store.insert(Packet {
                payload: PayloadId(payload),
                value: payload,
            });
        }
        let (last, rest) = payloads.split_last().expect("2,000 payloads");
        for &payload in rest {
            assert_eq!(
                store.remove(&PayloadId(payload)).map(|packet| packet.value),
                Some(payload)
            );
        }
        let Repr::Large(slab) = &store.0 else {
            panic!("2,000 packets made the store an indexed slab");
        };
        assert_eq!(slab.len, 1);
        assert_eq!(
            slab.buckets
                .iter()
                .filter(|bucket| bucket.slot != EMPTY)
                .count(),
            1
        );
        let slots = slab.slots.len();
        store.insert(Packet {
            payload: PayloadId(1),
            value: 1,
        });
        let Repr::Large(slab) = &store.0 else {
            unreachable!("a store that never emptied stays a slab")
        };
        assert_eq!(slab.slots.len(), slots);
        assert_eq!(
            store.get(&PayloadId(*last)).map(|packet| packet.value),
            Some(*last)
        );
        assert_eq!(store.into_sorted().len(), 2);
    }

    /// A large store that empties drops its slab and index: a queue owner that has drained (a
    /// switch port after a burst) holds nothing until it fills again.
    #[test]
    fn a_store_that_empties_drops_its_slab() {
        let mut store = PacketStore::new();
        for payload in 0..100_u64 {
            store.insert(Packet {
                payload: PayloadId(payload),
                value: payload,
            });
        }
        for payload in 0..100_u64 {
            assert!(store.remove(&PayloadId(payload)).is_some());
        }
        assert!(
            matches!(&store.0, Repr::Small(packets) if packets.capacity() == 0),
            "an empty store holds no slab and no buffer"
        );
        store.insert(Packet {
            payload: PayloadId(7),
            value: 7,
        });
        assert_eq!(store.into_sorted().len(), 1);
    }

    /// The store is the size of the `BTreeMap` it replaces, so `TransitionState` keeps its size.
    #[test]
    fn the_store_is_the_size_of_an_ordered_map() {
        assert_eq!(
            std::mem::size_of::<PacketStore<Packet>>(),
            std::mem::size_of::<BTreeMap<PayloadId, Packet>>()
        );
        assert_eq!(std::mem::size_of::<PacketStore<Packet>>(), 24);
        assert_eq!(std::mem::size_of::<Bucket>(), 16);
    }
}
