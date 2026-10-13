//! P18 ring writes (`days-gpu/evidence/P17/partition/design.md` §1): source pins on the streams
//! exchange, and a host model of why direct ring writes leave every ring byte where the retired
//! scatter left it.

const CUDA: &str = include_str!("../src/cuda_kernels.cu");
const METAL: &str = include_str!("../src/metal_kernels.metal");

const CHANNEL_EVENT_WORDS: usize = 11;

fn kernel_body<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
    let start = source
        .find(start)
        .unwrap_or_else(|| panic!("missing kernel marker `{start}`"));
    let tail = &source[start..];
    let end = tail
        .find(end)
        .unwrap_or_else(|| panic!("missing kernel end marker `{end}`"));
    &tail[..end]
}

/// `body` without its `#ifdef DAYS_MERGE_AUDIT` ... `#endif` blocks: what a production build
/// compiles.
fn production(body: &str) -> String {
    let mut kept = String::new();
    let mut rest = body;
    while let Some(start) = rest.find("#ifdef DAYS_MERGE_AUDIT") {
        kept.push_str(&rest[..start]);
        let end = rest[start..]
            .find("#endif")
            .unwrap_or_else(|| panic!("an unterminated hooks block"));
        rest = &rest[start + end + "#endif".len()..];
    }
    kept.push_str(rest);
    kept
}

/// The producer path writes the ring with plain stores: one writer per batch word (the channel's
/// unique producer LP), no atomics or fences, and the slot from the reset's snapshot with one
/// conditional subtraction instead of a 64-bit modulo on the round kernel's hot path.
#[test]
fn producers_write_their_rings_without_atomics_or_a_modulo() {
    for (backend, source, start, end) in [
        (
            "CUDA",
            CUDA,
            "__device__ __forceinline__ bool append_remote(",
            "__device__ __forceinline__ bool build_child(",
        ),
        (
            "Metal",
            METAL,
            "inline bool append_remote(",
            "inline bool emit_child(",
        ),
    ] {
        let body = kernel_body(source, start, end);
        let streams = production(
            body.split("    if (params[P_STREAMS_ENABLED] != 0) {")
                .nth(1)
                .and_then(|branch| branch.split("    ulong base = node * META_WORDS;").next())
                .expect("the streams branch precedes the legacy staging path"),
        );
        for fragment in [
            "ulong tail = stream_state[batch + 1];",
            "ulong pending = stream_state[batch + 2];",
            "if (written >= capacity - pending) {",
            "ARENA_CHANNEL_INBOX,\n                channel,",
            "ulong slot = tail + written;",
            "if (slot >= capacity) {\n            slot -= capacity;",
            "encode_channel_record(record, stream_records + word);",
            "stream_state[batch] = written + 1;",
        ] {
            assert!(
                streams.contains(fragment),
                "{backend} producer must contain `{fragment}`"
            );
        }
        for forbidden in [
            "atomic",
            "volatile",
            "threadfence",
            "memory_order",
            "remote_staging",
            "remote_meta",
            "% max(",
        ] {
            assert!(
                !streams.contains(forbidden),
                "{backend} producer must not contain `{forbidden}`"
            );
        }
    }
}

/// The round-start snapshot: `tail0 = (head + count) mod capacity` and `count0 = count`, taken by
/// the reset before any processing attempt of the round.
#[test]
fn the_reset_snapshots_each_channel_tail_and_count() {
    for (backend, source, start, end) in [
        (
            "CUDA",
            CUDA,
            "extern \"C\" __global__ void days_round_reset(",
            "extern \"C\" __global__ void days_round_prepare(",
        ),
        (
            "Metal",
            METAL,
            "kernel void days_round_reset(",
            "kernel void days_round_prepare(",
        ),
    ] {
        let body = production(kernel_body(source, start, end));
        for fragment in [
            "stream_state[batch] = 0;",
            "stream_state[batch + 1] = (stream_state[stream_base + 2] + count) %",
            "stream_state[batch + 2] = count;",
            "stream_state[batch + 3] = horizon_last;",
        ] {
            assert!(
                body.contains(fragment),
                "{backend} reset must contain `{fragment}`"
            );
        }
    }
}

/// P17 merge (`days-gpu/evidence/P17/merge/design.md`, ruling R1 (d)) and P18: on the streams
/// path the exchange merge keeps the incrementally maintained active list, reads only the LP's
/// inbound channels (the declared list's prefix that ends at the LP's service stream), and
/// publishes each channel's round with `count += b`, appending an entry for a channel that was
/// empty. It must not rebuild the list from every declared stream (one generator stream per sourced
/// flow: 14,443 per host on the flagship), which the hooks-only audit alone may still sweep.
#[test]
fn merge_publishes_the_inbound_channel_prefix() {
    for (backend, body) in [
        (
            "CUDA",
            kernel_body(
                CUDA,
                "extern \"C\" __global__ void days_exchange_merge",
                "    ulong inbound_base = ulong(target) * INBOUND_META_WORDS;",
            ),
        ),
        (
            "Metal",
            kernel_body(
                METAL,
                "kernel void days_exchange_merge",
                "    ulong inbound_base = ulong(target) * INBOUND_META_WORDS;",
            ),
        ),
    ] {
        let production = production(body);
        for fragment in [
            "ulong active_count = stream_state[meta + 3];",
            "if (stream >= params[P_SERVICE_STREAM_BASE]) {\n                break;",
            "params[P_CHANNEL_BATCH_OFFSET] + stream * CHANNEL_BATCH_WORDS",
            "if (written == 0) {\n                continue;",
            "stream_state[stream_base + 3] = count + written;",
            "if (count == 0) {",
        ] {
            assert!(
                production.contains(fragment),
                "{backend} streams merge must contain `{fragment}`"
            );
        }
        for forbidden in ["ulong active_count = 0;", "atomic", "fel_records["] {
            assert!(
                !production.contains(forbidden),
                "{backend} streams merge must not contain `{forbidden}`"
            );
        }
    }
}

/// The staged exchange is the legacy heap path's alone: no kernel reads a staging record in the
/// 12-word streams layout, and the streams-only prefix sweep is gone.
#[test]
fn the_staged_exchange_serves_only_the_legacy_path() {
    for (backend, source) in [("CUDA", CUDA), ("Metal", METAL)] {
        for retired in [
            "days_exchange_prefix_sweep",
            "REMOTE_STAGING_EVENT_WORDS",
            "encode_remote_staging_record",
            "copy_staging_to_channel",
            "scatter_stream_",
            "SCATTER_",
        ] {
            assert!(
                !source.contains(retired),
                "{backend} must not contain the retired `{retired}`"
            );
        }
    }
}

/// One channel ring, as `stream_state` holds it.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Ring {
    capacity: usize,
    head: usize,
    count: usize,
    slots: Vec<Option<[u64; CHANNEL_EVENT_WORDS]>>,
}

impl Ring {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            head: 0,
            count: 0,
            slots: vec![None; capacity],
        }
    }

    fn pop(&mut self) -> [u64; CHANNEL_EVENT_WORDS] {
        let record = self.slots[self.head].expect("a live record");
        self.head = (self.head + 1) % self.capacity;
        self.count -= 1;
        record
    }
}

fn record(round: usize, index: usize) -> [u64; CHANNEL_EVENT_WORDS] {
    std::array::from_fn(|word| ((round as u64) << 40) | ((index as u64) << 8) | word as u64)
}

/// Design §1.3, on a host model. Each round the consumer pops some records (only it moves `head`
/// and `count` during processing) while the producer emits some; the retired path stages the
/// emissions and, after processing, scatters them to `(head + count + j) mod capacity` and adds
/// them to `count`; the direct path writes them during processing to `(tail0 + j) mod capacity`,
/// with `tail0` snapshotted at round start, and publishes `count += b` at the merge. The rings are
/// equal after every round, and the direct path's capacity check, against the snapshot, never
/// admits a record the scatter's after-consumption check would refuse.
#[test]
fn direct_writes_leave_every_ring_byte_where_the_scatter_did() {
    let capacity = 7;
    let mut scattered = Ring::new(capacity);
    let mut direct = Ring::new(capacity);
    // (pops, emissions) per round, interleaved within the round as pop, emit, pop, emit, ...
    // Pops never exceed the round-start count (records written this round are not visible), and
    // `count0 + emissions` never exceeds the capacity; the ring wraps several times.
    let rounds = [
        (0, 3),
        (2, 4),
        (1, 0),
        (4, 2),
        (0, 5),
        (3, 0),
        (4, 3),
        (2, 2),
        (0, 0),
        (3, 4),
    ];
    for (round, &(pops, emissions)) in rounds.iter().enumerate() {
        let tail0 = (direct.head + direct.count) % capacity;
        let count0 = direct.count;
        let mut staged = Vec::new();
        let mut written = 0;
        for step in 0..pops.max(emissions) {
            if step < pops {
                assert_eq!(scattered.pop(), direct.pop(), "round {round}: same pops");
                assert_eq!(
                    (direct.head + direct.count) % capacity,
                    tail0,
                    "a pop leaves the tail where the snapshot put it"
                );
            }
            if step < emissions {
                let emitted = record(round, step);
                staged.push(emitted);
                assert!(written < capacity - count0, "the snapshot check admits it");
                let slot = (tail0 + written) % capacity;
                assert!(
                    (0..direct.count).all(|live| (direct.head + live) % capacity != slot),
                    "round {round}: the producer never writes a slot the consumer can still read"
                );
                direct.slots[slot] = Some(emitted);
                written += 1;
            }
        }
        // The scatter's after-consumption check, which the snapshot check is stricter than.
        assert!(scattered.count + staged.len() <= capacity);
        for (index, emitted) in staged.iter().enumerate() {
            let slot = (scattered.head + scattered.count + index) % capacity;
            scattered.slots[slot] = Some(*emitted);
        }
        scattered.count += staged.len();
        direct.count += written;
        assert_eq!(direct, scattered, "round {round}: identical rings");
    }
}
