//! Plane construction shared by the device planners (P18 diet).
//!
//! **All-zero planes (c2a).** Ten of a device plan's planes are all zero at upload: the channel and generator stream rings,
//! the remote staging arena, the outbox, the three observation logs, the worklist, the summary
//! counters, the LP state and the merge cursors. A plan holds their lengths, not their words; the
//! device allocates and zeroes them itself, so no host buffer of zeros is built, held beside the
//! rest of the plan, or copied (at the 1,024-GPU flagship, 16.5 GB of the 31.2 GB plan).

/// The length in words of an all-zero plane. Like an uploaded host plane, it is at least one word.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ZeroPlane(usize);

impl ZeroPlane {
    /// `records` records of `words` words each.
    pub(crate) fn records(records: usize, words: usize) -> Option<Self> {
        records.checked_mul(words).map(Self::words)
    }

    /// `words` words.
    pub(crate) fn words(words: usize) -> Self {
        Self(words.max(1))
    }

    pub(crate) fn len(self) -> usize {
        self.0
    }
}

/// The `routes` plane with identical routes stored once (P18 c9).
///
/// A flow's two route rows in the `flows` plane are `(offset, length)` pairs into this plane, and
/// the kernels read only `routes[offset..offset + length]`, so flows with the same route can
/// share one copy. At the 1,024-GPU flagship, 11.56 M routed flows run over 29,824 ECMP host
/// pairs, so most routes repeat. A route's first occurrence, in flow order (forward route, then
/// reverse), places it; the map is keyed by the route itself, so placement depends only on the
/// image. An empty route reads nothing and takes offset 0.
///
/// The map is only looked up, never iterated, so its order cannot reach the plane; it hashes a
/// route's link ids with [`RouteHasher`], a fixed multiplicative hash, so a lookup costs a few
/// multiplies rather than a B-tree descent of slice comparisons (which made planning 5% slower on
/// the MoE smoke).
pub(crate) struct RoutePlane<'image> {
    words: Vec<u64>,
    offsets: std::collections::HashMap<
        &'image [crate::LinkId],
        u64,
        std::hash::BuildHasherDefault<RouteHasher>,
    >,
    /// The last route placed and its offset: consecutive flows often repeat a route (a ring's
    /// steps between one pair), which one comparison answers without hashing.
    last: Option<(&'image [crate::LinkId], u64)>,
}

/// A fixed (unseeded) word hash for route keys: deterministic across runs and processes.
#[derive(Default)]
pub(crate) struct RouteHasher(u64);

impl std::hash::Hasher for RouteHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.write_u64(u64::from(byte));
        }
    }

    fn write_u64(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(0x517c_c1b7_2722_0a95);
    }

    fn write_usize(&mut self, word: usize) {
        self.write_u64(word as u64);
    }
}

impl<'image> RoutePlane<'image> {
    pub(crate) fn new() -> Self {
        Self {
            words: Vec::new(),
            offsets: std::collections::HashMap::default(),
            last: None,
        }
    }

    /// The offset of `route` in the plane, appending it on its first occurrence.
    pub(crate) fn place(&mut self, route: &'image [crate::LinkId]) -> u64 {
        if route.is_empty() {
            return 0;
        }
        if let Some((last, offset)) = self.last {
            if last == route {
                return offset;
            }
        }
        let words = &mut self.words;
        let offset = *self.offsets.entry(route).or_insert_with(|| {
            let offset = words.len() as u64;
            words.extend(route.iter().map(|link| link.0));
            offset
        });
        self.last = Some((route, offset));
        offset
    }

    /// The plane's words: at least one, as every uploaded plane.
    pub(crate) fn into_words(mut self) -> Vec<u64> {
        if self.words.is_empty() {
            self.words.push(0);
        }
        self.words
    }
}
