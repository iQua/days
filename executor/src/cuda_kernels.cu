// CUDA translation of metal_kernels.metal for the production CUDA backend.
//
// Keep transition bodies, record layouts, reduction shapes, and integer operations aligned with
// the MSL source. The CUDA entry ABI is deliberately uniform so a complete attempt DAG can be
// captured once and replayed as a graph without host-side argument mutation. There are no atomics:
// one lane owns each LP and all cross-LP compaction/reductions have fixed deterministic shapes.
#include <stdint.h>

using uint = uint32_t;
using ulong = uint64_t;

#define min(left, right) ((left) < (right) ? (left) : (right))
#define max(left, right) ((left) > (right) ? (left) : (right))

#define DAYS_BUFFERS \
    ulong *control, const ulong *params, ulong *node_state, ulong *generators, \
    const ulong *flows, const ulong *routes, const ulong *links, ulong *fel_meta, \
    ulong *fel_records, ulong *queue_meta, ulong *queue_records, ulong *in_service, \
    ulong *outbox, ulong *worklist, ulong *summary, ulong *observed, ulong *departures, \
    ulong *arrivals, ulong *lp_state, ulong *remote_meta, ulong *remote_staging, \
    ulong *observation_meta, ulong *inbound_meta, ulong *inbound_producers, \
    ulong *merge_cursors, ulong *stream_state, ulong *stream_records, \
    ulong *scheduler_state, ulong *tcp_state

constexpr uint EVENT_WORDS = 14;
constexpr uint SCATTER_GROUP_WIDTH = 32;
constexpr uint SCATTER_GROUP_MASK = 0xffffffffu;
constexpr uint SCATTER_COOPERATIVE_MIN_RECORDS = 3;
constexpr uint NODE_WORDS = 11;
constexpr uint GENERATOR_WORDS = 43;
constexpr uint FLOW_WORDS = 6;
constexpr uint LINK_WORDS = 4;
constexpr uint META_WORDS = 4;
constexpr uint QUEUE_META_WORDS = 5;
constexpr uint LP_STATE_WORDS = 7;
constexpr uint OBSERVATION_META_WORDS = 12;
constexpr uint INBOUND_META_WORDS = 2;
constexpr uint LP_STREAM_META_WORDS = 4;
constexpr uint OUTBOUND_META_WORDS = 2;
constexpr uint OUTBOUND_ENTRY_WORDS = 2;
constexpr uint CHANNEL_BATCH_WORDS = 4;
constexpr uint ACTIVE_STREAM_ENTRY_WORDS = 5;
constexpr uint TCP_RECEIVER_WORDS = 7;
// T20i ledger-ring metadata layout, mirrored word-for-word by `executor/src/tcp_ledger_ring.rs`:
//   +0 record-arena offset  +1 capacity  +2 count
//   +3 armed fallback-heap slot + 1 (T20g live-state contract)
//   +4 ring head            +5 occupancy high-water mark
constexpr uint TCP_LEDGER_META_WORDS = 6;
constexpr uint TCP_LEDGER_RECORD_WORDS = 5;
constexpr uint TCP_LEDGER_META_HEAD = 4;
constexpr uint TCP_LEDGER_META_HIGH_WATER = 5;
constexpr uint RATIONAL_WORDS = 10;
constexpr uint BIG_LIMBS = 10;
constexpr uint WIDE_LIMBS = 16;
constexpr uint PRODUCT_LIMBS = 20;
constexpr uint SCHEDULER_NODE_WORDS = 29;
constexpr uint SCHEDULER_CLASS_WORDS = 12;
constexpr ulong NONE = 0xfffffffffffffffful;

constexpr uint C_ERROR = 0;
constexpr uint C_ERROR_ARENA = 1;
constexpr uint C_ERROR_NODE = 2;
constexpr uint C_ERROR_CAPACITY = 3;
constexpr uint C_DONE = 4;
constexpr uint C_HORIZON_LO = 5;
constexpr uint C_HORIZON_HI = 6;
constexpr uint C_RUN_END_LO = 7;
constexpr uint C_RUN_END_HI = 8;
constexpr uint C_ROUNDS = 9;
constexpr uint C_TRANSITIONS = 10;
constexpr uint C_OUTBOX = 11;
constexpr uint C_OBSERVED = 12;
constexpr uint C_DEPARTURES = 13;
constexpr uint C_ARRIVALS = 14;
constexpr uint C_ACTIVE = 15;
constexpr uint C_FRONTIER = 16;
constexpr uint C_CONTINUATION = 17;
constexpr uint C_RELAUNCHES = 18;
constexpr uint C_ERROR_DEMAND = 19;

constexpr uint P_NODE_COUNT = 0;
constexpr uint P_FLOW_COUNT = 1;
constexpr uint P_LINK_COUNT = 2;
constexpr uint P_OUTBOX_CAPACITY = 3;
constexpr uint P_WORKLIST_CAPACITY = 4;
constexpr uint P_OBSERVED_CAPACITY = 5;
constexpr uint P_DEPARTURE_CAPACITY = 6;
constexpr uint P_ARRIVAL_CAPACITY = 7;
constexpr uint P_FULL_OBSERVATIONS = 8;
constexpr uint P_LOOKAHEAD = 9;
constexpr uint P_TRANSITION_CAPACITY = 10;
constexpr uint P_STOP_TIME = 11;
constexpr uint P_HAS_LOOKAHEAD = 12;
constexpr uint P_ROUND_CAPACITY = 13;
constexpr uint P_STREAMS_ENABLED = 14;
constexpr uint P_STREAM_COUNT = 15;
constexpr uint P_CHANNEL_COUNT = 16;
constexpr uint P_SERVICE_STREAM_BASE = 17;
constexpr uint P_GENERATOR_STREAM_BASE = 18;
constexpr uint P_LP_STREAM_META_OFFSET = 19;
constexpr uint P_LP_STREAM_IDS_OFFSET = 20;
constexpr uint P_LP_ACTIVE_IDS_OFFSET = 21;
constexpr uint P_OUTBOUND_META_OFFSET = 22;
constexpr uint P_OUTBOUND_ENTRIES_OFFSET = 23;
constexpr uint P_CHANNEL_BATCH_OFFSET = 24;
constexpr uint P_STAGING_CHANNEL_OFFSET = 25;
constexpr uint P_CHANNEL_TARGET_OFFSET = 26;
constexpr uint P_STREAM_ORDER_CHECKS = 27;
constexpr uint P_TCP_RECEIVER_OFFSET = 28;
constexpr uint P_TCP_LEDGER_META_OFFSET = 29;
// T21 fix 2. Word offset, inside `stream_state`, of the per-round FEL root cache: two words per
// LP, `{root time, validity}`. `days_horizon` evaluates `fel_root_time` once per node and writes it
// here; `days_round_prepare`'s count pass and write pass read it back instead of re-running the
// query, which `evidence/P12/perround-upperbound.md` §1.5.2 measured at three full-width
// evaluations per round over a read set nothing between the call sites writes.
//
// The region is device scratch: written and consumed inside one attempt, never decoded by the
// readback (which reads only `stream_state`'s metadata prefix), never part of complete state.
constexpr uint P_ROUND_SCRATCH_OFFSET = 30;
constexpr uint ROUND_SCRATCH_CACHE_WORDS = 2;

// T21 fix 1 — the re-gridded control sweeps. `evidence/P12/aterm-fixes.md` §3.4.
//
// Every phase that swept Θ(nodes + channels) from a grid of ONE block is now a full-grid `_sweep`
// dispatch that publishes one partial per block into the tail of the round scratch region, plus a
// width-1 combine dispatch that reduces those partials and performs the phase's control writes.
// The barrier between the two is the DISPATCH BOUNDARY — nothing here synchronizes across blocks
// inside a dispatch, and there are no atomics, no `volatile` and no fences.
//
// The width is a fixed constant rather than a function of the plan, so the combine knows how many
// partials to read without being told and every slot is rewritten by its own block on every
// dispatch. A slot is therefore never read stale, which is the whole reason this shape is sound
// where the first attempt's in-dispatch combine was not.
constexpr ulong CONTROL_SWEEP_BLOCKS = 128;
constexpr ulong ROUND_SCRATCH_PARTIAL_WORDS = 8;

constexpr uint N_KIND = 0;
constexpr uint N_EGRESS = 1;
constexpr uint N_SEMANTIC_QUEUE_CAPACITY = 2;
constexpr uint N_READY_PENDING = 3;
constexpr uint N_SERVICE_VALID = 4;
constexpr uint N_NEXT_ORIGIN = 5;
constexpr uint N_NEXT_PAYLOAD = 6;
constexpr uint N_COUNTER_0 = 7;
constexpr uint N_COUNTER_1 = 8;
constexpr uint N_COUNTER_2 = 9;

constexpr uint S_KIND = 0;
constexpr uint S_CLASS_COUNT = 1;
constexpr uint S_CLASS_OFFSET = 2;
constexpr uint S_QUEUE_TAG_OFFSET = 3;
constexpr uint S_LAST_UPDATED = 4;
constexpr uint S_VIRTUAL_TIME = 5;
constexpr uint S_IN_SERVICE_TAG = 15;
constexpr uint S_AQM_KIND = 25;
constexpr uint S_AQM_UNIT = 26;
constexpr uint S_AQM_CAPACITY = 27;
constexpr uint S_AQM_THRESHOLD = 28;

constexpr uint SC_VALUE = 0;
constexpr uint SC_ACTIVE = 1;
constexpr uint SC_FINISH = 2;

constexpr uint E_TIME = 0;
constexpr uint E_PHASE = 1;
constexpr uint E_ORIGIN = 2;
constexpr uint E_SEQUENCE = 3;
constexpr uint E_TARGET = 4;
constexpr uint E_KIND = 5;
constexpr uint E_PAYLOAD = 6;
constexpr uint PK_ID = 7;
constexpr uint PK_FLOW = 8;
constexpr uint PK_SIZE = 9;
constexpr uint PK_KIND = 10;
constexpr uint PK_META_0 = 11;
constexpr uint PK_META_1 = 12;
constexpr uint PK_META_2 = 13;
constexpr ulong PK_ECN_FLAG = 1ul << 63;
constexpr ulong PK_KIND_MASK = ~PK_ECN_FLAG;

constexpr ulong HOST = 0;
constexpr ulong SWITCH = 1;
constexpr ulong PACKET_ARRIVAL = 0;
constexpr ulong TX_READY = 1;
constexpr ulong TX_COMPLETE = 2;
constexpr ulong REMOTE_ARRIVAL = 3;
constexpr ulong RETRANSMISSION_TIMEOUT = 4;
constexpr ulong PACING_TIMER = 5;
constexpr ulong DATA_PACKET = 0;
constexpr ulong FEEDBACK_PACKET = 1;
constexpr ulong TCP_DATA_PACKET = 2;
constexpr ulong TCP_ACK_PACKET = 3;
constexpr ulong SCHED_FIFO = 0;
constexpr ulong SCHED_SP = 1;
constexpr ulong SCHED_WFQ = 2;
constexpr ulong SCHED_DRR = 3;
constexpr ulong SCHED_WRR = 4;
constexpr ulong AQM_TAILDROP = 0;
constexpr ulong AQM_ECN = 1;
constexpr ulong AQM_PACKETS = 0;
constexpr ulong AQM_BYTES = 1;

// Generator row ABI. Common words 0..10 and kind tag 11 are shared with the scalar image;
// TCP owns words 12..30 and the controller tag/state at 31..42.
constexpr uint G_VALID = 0;
constexpr uint G_OWNER = 1;
constexpr uint G_PACKETS = 2;
constexpr uint G_BYTES = 3;
constexpr uint G_STATUS = 4;
constexpr uint G_DEPARTURE = 5;
constexpr uint G_PAYLOAD = 6;
constexpr uint G_FEEDBACK = 8;
constexpr uint G_OUTSTANDING = 9;
constexpr uint G_UNACKNOWLEDGED = 10;
constexpr uint G_KIND = 11;
constexpr uint G_TCP_TOTAL = 12;
constexpr uint G_TCP_MSS = 13;
constexpr uint G_TCP_ACK_SIZE = 14;
constexpr uint G_TCP_NEXT = 15;
constexpr uint G_TCP_HIGHEST_ACK = 16;
constexpr uint G_TCP_FLIGHT = 17;
constexpr uint G_TCP_DUP_ACKS = 18;
constexpr uint G_TCP_RECOVERY_HIGH = 19;
constexpr uint G_TCP_LAST_ATTEMPT = 20;
constexpr uint G_TCP_TIMER_GENERATION = 21;
constexpr uint G_TCP_TIMER_ACTIVE = 22;
constexpr uint G_TCP_TIMER_ATTEMPT = 23;
constexpr uint G_TCP_TIMER_SEQUENCE = 24;
constexpr uint G_TCP_TIMER_DEADLINE = 25;
constexpr uint G_TCP_TIMER_STORED_GENERATION = 26;
constexpr uint G_TCP_TIMER_RTO = 27;
constexpr uint G_TCP_SRTT = 28;
constexpr uint G_TCP_RTTVAR = 29;
constexpr uint G_TCP_RTO = 30;
constexpr uint G_CONTROL = 31;
constexpr uint G_RATE_FIRST = 12;
constexpr uint G_RATE_INTERVAL = 13;
constexpr uint G_RATE_PACKET_SIZE = 14;
constexpr uint G_RATE_TOTAL = 15;
constexpr uint G_RATE_NUMERATOR = 16;
constexpr uint G_RATE_DENOMINATOR = 17;
constexpr uint G_RATE_CREDIT_LOW = 18;
constexpr uint G_RATE_CREDIT_HIGH = 19;

constexpr uint CTL_KIND = 0;
constexpr uint CTL_MSS = 1;
constexpr uint CTL_CWND = 2;
constexpr uint CTL_SSTHRESH = 3;
constexpr uint CTL_PHASE = 4;
constexpr uint CTL_DUP_ACKS = 5;
constexpr uint CTL_RECOVERY_HIGH = 6;
constexpr uint CTL_EXTRA_0 = 7;
constexpr uint CTL_W_LAST_MAX = 8;
constexpr uint CTL_EPOCH = 9;
constexpr uint CTL_SRTT = 10;
constexpr uint CTL_K = 11;

constexpr ulong TCP_SLOW_START = 0;
constexpr ulong TCP_CONGESTION_AVOIDANCE = 1;
constexpr ulong TCP_FAST_RECOVERY = 2;
constexpr ulong CUBIC_SCALE = 1000000000ul;
constexpr ulong CUBIC_MAX_WINDOW = 2000000000000000ul;
constexpr ulong TCP_MIN_RTO = 1000000000ul;
constexpr ulong TCP_MAX_RTO = 60000000000ul;
constexpr ulong TCP_RTO_GRANULARITY = 1000000ul;

constexpr ulong ERROR_CAPACITY = 1;
constexpr ulong ERROR_TRANSITION_CAPACITY = 2;
constexpr ulong ERROR_SEMANTIC = 3;
constexpr ulong ERROR_WFQ_ARITHMETIC = 100;
constexpr ulong ARENA_FEL = 1;
constexpr ulong ARENA_QUEUE = 2;
constexpr ulong ARENA_OUTBOX = 3;
constexpr ulong ARENA_WORKLIST = 4;
constexpr ulong ARENA_OBSERVED = 5;
constexpr ulong ARENA_DEPARTURES = 6;
constexpr ulong ARENA_ARRIVALS = 7;
constexpr ulong ARENA_CHANNEL_INBOX = 8;
constexpr ulong ARENA_SERVICE_STREAM = 9;
constexpr ulong ARENA_GENERATOR_STREAM = 10;
constexpr ulong ARENA_TCP_RECEIVER = 11;
constexpr ulong ARENA_TCP_SEGMENT_LEDGER = 12;
constexpr ulong ARENA_REMOTE_STAGING = 13;

constexpr uint L_FINISHED = 0;
constexpr uint L_TRANSITIONS = 1;
constexpr uint L_ERROR = 2;
constexpr uint L_ERROR_ARENA = 3;
constexpr uint L_ERROR_NODE = 4;
constexpr uint L_ERROR_CAPACITY = 5;
constexpr uint L_ERROR_DEMAND = 6;

__device__ __forceinline__ bool key_less(const ulong *left, const ulong *right) {
    if (left[E_TIME] != right[E_TIME]) {
        return left[E_TIME] < right[E_TIME];
    }
    if (left[E_PHASE] != right[E_PHASE]) {
        return left[E_PHASE] < right[E_PHASE];
    }
    if (left[E_ORIGIN] != right[E_ORIGIN]) {
        return left[E_ORIGIN] < right[E_ORIGIN];
    }
    return left[E_SEQUENCE] < right[E_SEQUENCE];
}

__device__ __forceinline__ bool stored_key_less(
    const ulong *records,
    ulong left_slot,
    ulong right_slot
) {
    ulong left = left_slot * EVENT_WORDS;
    ulong right = right_slot * EVENT_WORDS;
    if (records[left + E_TIME] != records[right + E_TIME]) {
        return records[left + E_TIME] < records[right + E_TIME];
    }
    if (records[left + E_PHASE] != records[right + E_PHASE]) {
        return records[left + E_PHASE] < records[right + E_PHASE];
    }
    if (records[left + E_ORIGIN] != records[right + E_ORIGIN]) {
        return records[left + E_ORIGIN] < records[right + E_ORIGIN];
    }
    return records[left + E_SEQUENCE] < records[right + E_SEQUENCE];
}

__device__ __forceinline__ bool stored_cross_key_less(
    const ulong *left_records,
    ulong left_slot,
    const ulong *right_records,
    ulong right_slot
) {
    ulong left = left_slot * EVENT_WORDS;
    ulong right = right_slot * EVENT_WORDS;
    if (left_records[left + E_TIME] != right_records[right + E_TIME]) {
        return left_records[left + E_TIME] < right_records[right + E_TIME];
    }
    if (left_records[left + E_PHASE] != right_records[right + E_PHASE]) {
        return left_records[left + E_PHASE] < right_records[right + E_PHASE];
    }
    if (left_records[left + E_ORIGIN] != right_records[right + E_ORIGIN]) {
        return left_records[left + E_ORIGIN] < right_records[right + E_ORIGIN];
    }
    return left_records[left + E_SEQUENCE] < right_records[right + E_SEQUENCE];
}

__device__ __forceinline__ bool stored_thread_key_less(
    const ulong *left_records,
    ulong left_slot,
    const ulong *right
) {
    ulong left = left_slot * EVENT_WORDS;
    if (left_records[left + E_TIME] != right[E_TIME]) {
        return left_records[left + E_TIME] < right[E_TIME];
    }
    if (left_records[left + E_PHASE] != right[E_PHASE]) {
        return left_records[left + E_PHASE] < right[E_PHASE];
    }
    if (left_records[left + E_ORIGIN] != right[E_ORIGIN]) {
        return left_records[left + E_ORIGIN] < right[E_ORIGIN];
    }
    return left_records[left + E_SEQUENCE] < right[E_SEQUENCE];
}

__device__ __forceinline__ void copy_thread_to_device(
    const ulong *source,
    ulong *target,
    ulong slot
) {
    ulong offset = slot * EVENT_WORDS;
    for (uint word = 0; word < EVENT_WORDS; ++word) {
        target[offset + word] = source[word];
    }
}

__device__ __forceinline__ void copy_device_to_thread(
    const ulong *source,
    ulong slot,
    ulong *target
) {
    ulong offset = slot * EVENT_WORDS;
    for (uint word = 0; word < EVENT_WORDS; ++word) {
        target[word] = source[offset + word];
    }
}

__device__ __forceinline__ void copy_device_record(
    const ulong *source,
    ulong source_slot,
    ulong *target,
    ulong target_slot
) {
    ulong source_offset = source_slot * EVENT_WORDS;
    ulong target_offset = target_slot * EVENT_WORDS;
    for (uint word = 0; word < EVENT_WORDS; ++word) {
        target[target_offset + word] = source[source_offset + word];
    }
}

__device__ __forceinline__ void swap_records(ulong *records, ulong left_slot, ulong right_slot) {
    ulong left = left_slot * EVENT_WORDS;
    ulong right = right_slot * EVENT_WORDS;
    for (uint word = 0; word < EVENT_WORDS; ++word) {
        ulong value = records[left + word];
        records[left + word] = records[right + word];
        records[right + word] = value;
    }
}

__device__ __forceinline__ ulong saturating_add_ulong(ulong left, ulong right) {
    return right > NONE - left ? NONE : left + right;
}

__device__ __forceinline__ void set_capacity_error(
    ulong *error,
    ulong arena,
    ulong node,
    ulong capacity,
    ulong demand
) {
    if (error[L_ERROR] == 0) {
        error[L_ERROR] = ERROR_CAPACITY;
        error[L_ERROR_ARENA] = arena;
        error[L_ERROR_NODE] = node;
        error[L_ERROR_CAPACITY] = capacity;
        error[L_ERROR_DEMAND] = demand;
    }
}

__device__ __forceinline__ void set_semantic_error(ulong *error, ulong code, ulong node) {
    if (error[L_ERROR] == 0) {
        error[L_ERROR] = ERROR_SEMANTIC + code;
        error[L_ERROR_NODE] = node;
    }
}

__device__ __forceinline__ void set_wfq_arithmetic_error(ulong *error, ulong node) {
    if (error[L_ERROR] == 0) {
        error[L_ERROR] = ERROR_WFQ_ARITHMETIC;
        error[L_ERROR_NODE] = node;
    }
}

// T20g item 2 live-state contract.
//
// `pending_events` denotes LIVE events, so a superseded retransmission timeout is removed from the
// fallback heap at the transition that supersedes it. Interior removal needs the record's position,
// which the per-flow TCP ledger metadata word +3 carries as `absolute record slot + 1`; zero means
// the flow owns no heap record. Every heap movement below repairs that word, and the repair is
// conditioned on the moved record already being the flow's recorded owner, so legacy-import residue
// carrying the same flow can never steal ownership.
__device__ __forceinline__ ulong tcp_timer_slot_word(ulong flow, const ulong *params) {
    return params[P_TCP_LEDGER_META_OFFSET] + flow * TCP_LEDGER_META_WORDS + 3;
}

__device__ __forceinline__ ulong heap_timer_owner(
    const ulong *params,
    const ulong *tcp_state,
    const ulong *records,
    ulong slot
) {
    ulong record = slot * EVENT_WORDS;
    if (records[record + E_KIND] != RETRANSMISSION_TIMEOUT) {
        return NONE;
    }
    ulong flow = records[record + PK_FLOW];
    if (flow >= params[P_FLOW_COUNT]) {
        return NONE;
    }
    if (tcp_state[tcp_timer_slot_word(flow, params)] != slot + 1) {
        return NONE;
    }
    return flow;
}

__device__ __forceinline__ void heap_swap(
    const ulong *params,
    ulong *tcp_state,
    ulong *records,
    ulong left_slot,
    ulong right_slot
) {
    ulong left_owner = heap_timer_owner(params, tcp_state, records, left_slot);
    ulong right_owner = heap_timer_owner(params, tcp_state, records, right_slot);
    swap_records(records, left_slot, right_slot);
    if (left_owner != NONE) {
        tcp_state[tcp_timer_slot_word(left_owner, params)] = right_slot + 1;
    }
    if (right_owner != NONE) {
        tcp_state[tcp_timer_slot_word(right_owner, params)] = left_slot + 1;
    }
}

__device__ __forceinline__ void heap_move(
    const ulong *params,
    ulong *tcp_state,
    ulong *records,
    ulong source_slot,
    ulong target_slot
) {
    ulong owner = heap_timer_owner(params, tcp_state, records, source_slot);
    copy_device_record(records, source_slot, records, target_slot);
    if (owner != NONE) {
        tcp_state[tcp_timer_slot_word(owner, params)] = target_slot + 1;
    }
}

__device__ __forceinline__ bool heap_push(
    ulong node,
    const ulong *record,
    ulong *error,
    const ulong *params,
    ulong *meta,
    ulong *records,
    ulong *tcp_state
) {
    ulong base = node * META_WORDS;
    ulong offset = meta[base];
    ulong capacity = meta[base + 1];
    ulong count = meta[base + 3];
    if (count >= capacity) {
        set_capacity_error(error, ARENA_FEL, node, capacity, count + 1);
        return false;
    }
    ulong child = count;
    // A kernel-side timeout push is always the arming transition's new live timer, so it claims
    // the flow's slot before the sift-up starts repairing positions. The slot must be empty first:
    // firing clears it and an eager disarm clears it, so a live slot here is a removal the
    // live-state contract required and some transition did not perform.
    bool arming_timer =
        record[E_KIND] == RETRANSMISSION_TIMEOUT && record[PK_FLOW] < params[P_FLOW_COUNT];
    ulong slot_word = 0;
    if (arming_timer) {
        slot_word = tcp_timer_slot_word(record[PK_FLOW], params);
        if (tcp_state[slot_word] != 0) {
            set_semantic_error(error, 61, node);
            return false;
        }
    }
    copy_thread_to_device(record, records, offset + child);
    meta[base + 3] = count + 1;
    if (arming_timer) {
        tcp_state[slot_word] = offset + child + 1;
    }
    while (child != 0) {
        ulong parent = (child - 1) / 2;
        if (!stored_key_less(records, offset + child, offset + parent)) {
            break;
        }
        heap_swap(params, tcp_state, records, offset + child, offset + parent);
        child = parent;
    }
    return true;
}

__device__ __forceinline__ bool heap_pop(
    ulong node,
    const ulong *params,
    ulong *meta,
    ulong *records,
    ulong *tcp_state,
    ulong *record,
    ulong &timer_owner
) {
    ulong base = node * META_WORDS;
    ulong offset = meta[base];
    ulong count = meta[base + 3];
    timer_owner = NONE;
    if (count == 0) {
        return false;
    }
    // Ownership is resolved before the restructure: sifting can move a different live timer into
    // the vacated root, and the caller must classify the record it actually received.
    timer_owner = heap_timer_owner(params, tcp_state, records, offset);
    if (timer_owner != NONE) {
        tcp_state[tcp_timer_slot_word(timer_owner, params)] = 0;
    }
    copy_device_to_thread(records, offset, record);
    count -= 1;
    meta[base + 3] = count;
    if (count == 0) {
        return true;
    }
    heap_move(params, tcp_state, records, offset + count, offset);
    ulong parent = 0;
    while (true) {
        ulong left = parent * 2 + 1;
        if (left >= count) {
            break;
        }
        ulong right = left + 1;
        ulong child = left;
        if (right < count && stored_key_less(records, offset + right, offset + left)) {
            child = right;
        }
        if (!stored_key_less(records, offset + child, offset + parent)) {
            break;
        }
        heap_swap(params, tcp_state, records, offset + child, offset + parent);
        parent = child;
    }
    return true;
}

__device__ __forceinline__ bool heap_root_time(
    ulong node,
    const ulong *meta,
    const ulong *records,
    ulong &time
) {
    ulong base = node * META_WORDS;
    if (meta[base + 3] == 0) {
        return false;
    }
    time = records[meta[base] * EVENT_WORDS + E_TIME];
    return true;
}

__device__ __forceinline__ ulong stream_arena(ulong stream, const ulong *params) {
    if (stream < params[P_CHANNEL_COUNT]) {
        return ARENA_CHANNEL_INBOX;
    }
    if (stream < params[P_GENERATOR_STREAM_BASE]) {
        return ARENA_SERVICE_STREAM;
    }
    return ARENA_GENERATOR_STREAM;
}

__device__ __forceinline__ bool active_add(
    ulong node,
    ulong stream,
    const ulong *record,
    const ulong *params,
    ulong *error,
    ulong *stream_state
) {
    ulong meta =
        params[P_LP_STREAM_META_OFFSET] + node * LP_STREAM_META_WORDS;
    ulong offset = stream_state[meta + 2];
    ulong count = stream_state[meta + 3];
    ulong capacity = stream_state[meta + 1] + 1;
    if (count >= capacity) {
        set_semantic_error(error, 38, node);
        return false;
    }
    ulong entry = offset + count * ACTIVE_STREAM_ENTRY_WORDS;
    stream_state[entry] = stream;
    for (uint word = 0; word < 4; ++word) {
        stream_state[entry + 1 + word] = record[word];
    }
    stream_state[meta + 3] = count + 1;
    return true;
}

__device__ __forceinline__ void active_update_key(
    ulong node,
    ulong active_index,
    const ulong *params,
    ulong *stream_state,
    const ulong *records,
    ulong slot
) {
    ulong meta =
        params[P_LP_STREAM_META_OFFSET] + node * LP_STREAM_META_WORDS;
    ulong entry =
        stream_state[meta + 2] +
        active_index * ACTIVE_STREAM_ENTRY_WORDS;
    ulong record = slot * EVENT_WORDS;
    for (uint word = 0; word < 4; ++word) {
        stream_state[entry + 1 + word] = records[record + word];
    }
}

__device__ __forceinline__ ulong active_find(
    ulong node,
    ulong stream,
    const ulong *params,
    const ulong *stream_state
) {
    ulong meta =
        params[P_LP_STREAM_META_OFFSET] + node * LP_STREAM_META_WORDS;
    ulong offset = stream_state[meta + 2];
    ulong count = stream_state[meta + 3];
    for (ulong index = 0; index < count; ++index) {
        if (stream_state[offset + index * ACTIVE_STREAM_ENTRY_WORDS] == stream) {
            return index;
        }
    }
    return NONE;
}

__device__ __forceinline__ bool active_refresh_source(
    ulong node,
    ulong stream,
    const ulong *params,
    ulong *error,
    ulong *stream_state,
    const ulong *records,
    ulong slot
) {
    ulong index = active_find(node, stream, params, stream_state);
    if (index == NONE) {
        set_semantic_error(error, 43, node);
        return false;
    }
    active_update_key(
        node,
        index,
        params,
        stream_state,
        records,
        slot
    );
    return true;
}

__device__ __forceinline__ void active_remove(
    ulong node,
    ulong active_index,
    const ulong *params,
    ulong *stream_state
) {
    ulong meta =
        params[P_LP_STREAM_META_OFFSET] + node * LP_STREAM_META_WORDS;
    ulong offset = stream_state[meta + 2];
    ulong count = stream_state[meta + 3];
    if (active_index + 1 < count) {
        ulong target = offset + active_index * ACTIVE_STREAM_ENTRY_WORDS;
        ulong source = offset + (count - 1) * ACTIVE_STREAM_ENTRY_WORDS;
        for (uint word = 0; word < ACTIVE_STREAM_ENTRY_WORDS; ++word) {
            stream_state[target + word] = stream_state[source + word];
        }
    }
    stream_state[meta + 3] = count - 1;
}

// Removes one flow's live retransmission-timeout record from `node`'s fallback heap.
//
// The per-flow slot word makes this O(log n): validate the identity at the recorded slot, fill the
// hole with the last record, and repair in whichever direction the fill violates. Semantic code 61
// reports a slot that does not carry the flow's armed timer, which the live-state contract forbids.
__device__ __forceinline__ bool heap_remove_timer(
    ulong node,
    ulong flow,
    ulong attempt,
    ulong deadline,
    ulong *error,
    const ulong *params,
    ulong *meta,
    ulong *records,
    ulong *stream_state,
    ulong *tcp_state
) {
    ulong slot_word = tcp_timer_slot_word(flow, params);
    ulong stored = tcp_state[slot_word];
    ulong base = node * META_WORDS;
    ulong offset = meta[base];
    ulong count = meta[base + 3];
    if (stored == 0 || stored - 1 < offset || stored - 1 >= offset + count) {
        set_semantic_error(error, 61, node);
        return false;
    }
    ulong slot = stored - 1;
    ulong record = slot * EVENT_WORDS;
    if (records[record + E_KIND] != RETRANSMISSION_TIMEOUT ||
        records[record + E_TARGET] != node ||
        records[record + PK_FLOW] != flow ||
        records[record + E_PAYLOAD] != attempt ||
        records[record + E_TIME] != deadline) {
        set_semantic_error(error, 61, node);
        return false;
    }
    tcp_state[slot_word] = 0;
    ulong index = slot - offset;
    ulong last = count - 1;
    count = last;
    meta[base + 3] = count;
    if (index != last) {
        heap_move(params, tcp_state, records, offset + last, offset + index);
        ulong child = index;
        while (child != 0) {
            ulong parent = (child - 1) / 2;
            if (!stored_key_less(records, offset + child, offset + parent)) {
                break;
            }
            heap_swap(params, tcp_state, records, offset + child, offset + parent);
            child = parent;
        }
        if (child == index) {
            ulong parent = index;
            while (true) {
                ulong left = parent * 2 + 1;
                if (left >= count) {
                    break;
                }
                ulong right = left + 1;
                ulong pick = left;
                if (right < count &&
                    stored_key_less(records, offset + right, offset + left)) {
                    pick = right;
                }
                if (!stored_key_less(records, offset + pick, offset + parent)) {
                    break;
                }
                heap_swap(params, tcp_state, records, offset + pick, offset + parent);
                parent = pick;
            }
        }
    }
    if (params[P_STREAMS_ENABLED] == 0) {
        return true;
    }
    if (count == 0) {
        ulong active_index = active_find(node, NONE, params, stream_state);
        if (active_index == NONE) {
            set_semantic_error(error, 43, node);
            return false;
        }
        active_remove(node, active_index, params, stream_state);
        return true;
    }
    return active_refresh_source(
        node,
        NONE,
        params,
        error,
        stream_state,
        records,
        offset
    );
}

__device__ __forceinline__ bool stream_push(
    ulong node,
    ulong stream,
    const ulong *record,
    bool activate,
    const ulong *params,
    ulong *error,
    ulong *stream_state,
    ulong *stream_records
) {
    ulong base = stream * META_WORDS;
    ulong offset = stream_state[base];
    ulong capacity = stream_state[base + 1];
    ulong head = stream_state[base + 2];
    ulong count = stream_state[base + 3];
    if (count >= capacity) {
        set_capacity_error(
            error,
            stream_arena(stream, params),
            node,
            capacity,
            count + 1
        );
        return false;
    }
    if (params[P_STREAM_ORDER_CHECKS] != 0 && count != 0) {
        ulong tail = (head + count - 1) % max(capacity, 1ul);
        if (!stored_thread_key_less(stream_records, offset + tail, record)) {
            set_semantic_error(error, 39, node);
            return false;
        }
    }
    ulong physical = (head + count) % max(capacity, 1ul);
    copy_thread_to_device(record, stream_records, offset + physical);
    stream_state[base + 3] = count + 1;
    return count != 0 || !activate ||
        active_add(node, stream, record, params, error, stream_state);
}

__device__ __forceinline__ bool fallback_push(
    ulong node,
    const ulong *record,
    const ulong *params,
    ulong *error,
    ulong *fel_meta,
    ulong *fel_records,
    ulong *stream_state,
    ulong *tcp_state
) {
    bool was_empty = fel_meta[node * META_WORDS + 3] == 0;
    if (!heap_push(node, record, error, params, fel_meta, fel_records, tcp_state)) {
        return false;
    }
    if (params[P_STREAMS_ENABLED] == 0) {
        return true;
    }
    if (was_empty) {
        return active_add(
            node,
            NONE,
            record,
            params,
            error,
            stream_state
        );
    }
    return active_refresh_source(
        node,
        NONE,
        params,
        error,
        stream_state,
        fel_records,
        fel_meta[node * META_WORDS]
    );
}

__device__ __forceinline__ bool classified_push(
    ulong node,
    const ulong *record,
    const ulong *params,
    ulong *error,
    ulong *fel_meta,
    ulong *fel_records,
    ulong *stream_state,
    ulong *stream_records,
    ulong *tcp_state
) {
    if (params[P_STREAMS_ENABLED] == 0) {
        return heap_push(node, record, error, params, fel_meta, fel_records, tcp_state);
    }
    ulong kind = record[E_KIND];
    if (kind == PACKET_ARRIVAL && record[PK_FLOW] < params[P_FLOW_COUNT]) {
        return stream_push(
            node,
            params[P_GENERATOR_STREAM_BASE] + record[PK_FLOW],
            record,
            true,
            params,
            error,
            stream_state,
            stream_records
        );
    }
    if (kind == TX_READY || kind == TX_COMPLETE) {
        return stream_push(
            node,
            params[P_SERVICE_STREAM_BASE] + node,
            record,
            true,
            params,
            error,
            stream_state,
            stream_records
        );
    }
    return fallback_push(
        node,
        record,
        params,
        error,
        fel_meta,
        fel_records,
        stream_state,
        tcp_state
    );
}

__device__ __forceinline__ bool active_key_less(
    const ulong *stream_state,
    ulong left,
    ulong right
) {
    for (uint word = 1; word < ACTIVE_STREAM_ENTRY_WORDS; ++word) {
        if (stream_state[left + word] != stream_state[right + word]) {
            return stream_state[left + word] < stream_state[right + word];
        }
    }
    return false;
}

__device__ __forceinline__ bool fel_peek(
    ulong node,
    const ulong *params,
    const ulong *fel_meta,
    const ulong *fel_records,
    const ulong *stream_state,
    const ulong *stream_records,
    ulong *record,
    ulong &selected_active,
    ulong &selected_stream
) {
    if (params[P_STREAMS_ENABLED] == 0) {
        ulong base = node * META_WORDS;
        if (fel_meta[base + 3] == 0) {
            return false;
        }
        copy_device_to_thread(fel_records, fel_meta[base], record);
        selected_active = NONE;
        selected_stream = NONE;
        return true;
    }
    ulong lp_meta =
        params[P_LP_STREAM_META_OFFSET] + node * LP_STREAM_META_WORDS;
    ulong active_offset = stream_state[lp_meta + 2];
    ulong active_count = stream_state[lp_meta + 3];
    if (active_count == 0) {
        return false;
    }
    ulong selected_entry = active_offset;
    selected_active = 0;
    for (ulong index = 1; index < active_count; ++index) {
        ulong candidate =
            active_offset + index * ACTIVE_STREAM_ENTRY_WORDS;
        if (active_key_less(stream_state, candidate, selected_entry)) {
            selected_entry = candidate;
            selected_active = index;
        }
    }
    selected_stream = stream_state[selected_entry];
    if (selected_stream == NONE) {
        ulong heap_base = node * META_WORDS;
        if (fel_meta[heap_base + 3] == 0) {
            return false;
        }
        copy_device_to_thread(fel_records, fel_meta[heap_base], record);
    } else {
        ulong base = selected_stream * META_WORDS;
        if (stream_state[base + 3] == 0) {
            return false;
        }
        ulong capacity = stream_state[base + 1];
        ulong physical = stream_state[base + 2] % max(capacity, 1ul);
        copy_device_to_thread(
            stream_records,
            stream_state[base] + physical,
            record
        );
    }
    return true;
}

__device__ __forceinline__ bool fel_root_time(
    ulong node,
    const ulong *params,
    const ulong *fel_meta,
    const ulong *fel_records,
    const ulong *stream_state,
    const ulong *stream_records,
    ulong &time
) {
    if (params[P_STREAMS_ENABLED] == 0) {
        return heap_root_time(node, fel_meta, fel_records, time);
    }
    ulong record[EVENT_WORDS];
    ulong selected_active;
    ulong selected_stream;
    if (!fel_peek(
        node,
        params,
        fel_meta,
        fel_records,
        stream_state,
        stream_records,
        record,
        selected_active,
        selected_stream
    )) {
        return false;
    }
    time = record[E_TIME];
    return true;
}

__device__ __forceinline__ bool fel_pop_selected(
    ulong node,
    ulong selected_active,
    ulong selected_stream,
    const ulong *params,
    ulong *fel_meta,
    ulong *fel_records,
    ulong *stream_state,
    ulong *stream_records,
    ulong *tcp_state,
    ulong *record,
    ulong &timer_owner
) {
    timer_owner = NONE;
    if (params[P_STREAMS_ENABLED] == 0 || selected_active == NONE) {
        return heap_pop(
            node,
            params,
            fel_meta,
            fel_records,
            tcp_state,
            record,
            timer_owner
        );
    }
    if (selected_stream == NONE) {
        if (!heap_pop(
            node,
            params,
            fel_meta,
            fel_records,
            tcp_state,
            record,
            timer_owner
        )) {
            return false;
        }
        if (fel_meta[node * META_WORDS + 3] == 0) {
            active_remove(node, selected_active, params, stream_state);
        } else {
            active_update_key(
                node,
                selected_active,
                params,
                stream_state,
                fel_records,
                fel_meta[node * META_WORDS]
            );
        }
        return true;
    }
    ulong base = selected_stream * META_WORDS;
    ulong capacity = stream_state[base + 1];
    ulong head = stream_state[base + 2];
    ulong count = stream_state[base + 3] - 1;
    stream_state[base + 2] = (head + 1) % max(capacity, 1ul);
    stream_state[base + 3] = count;
    if (count == 0) {
        active_remove(node, selected_active, params, stream_state);
    } else {
        ulong physical = stream_state[base + 2] % max(capacity, 1ul);
        active_update_key(
            node,
            selected_active,
            params,
            stream_state,
            stream_records,
            stream_state[base] + physical
        );
    }
    return true;
}

__device__ __forceinline__ bool fel_pop(
    ulong node,
    const ulong *params,
    ulong *fel_meta,
    ulong *fel_records,
    ulong *stream_state,
    ulong *stream_records,
    ulong *tcp_state,
    ulong *record,
    ulong &timer_owner
) {
    timer_owner = NONE;
    if (params[P_STREAMS_ENABLED] == 0) {
        return heap_pop(
            node,
            params,
            fel_meta,
            fel_records,
            tcp_state,
            record,
            timer_owner
        );
    }
    ulong selected_active;
    ulong selected_stream;
    if (!fel_peek(
        node,
        params,
        fel_meta,
        fel_records,
        stream_state,
        stream_records,
        record,
        selected_active,
        selected_stream
    )) {
        return false;
    }
    return fel_pop_selected(
        node,
        selected_active,
        selected_stream,
        params,
        fel_meta,
        fel_records,
        stream_state,
        stream_records,
        tcp_state,
        record,
        timer_owner
    );
}

__device__ __forceinline__ bool before_horizon(ulong time, const ulong *control) {
    return control[C_HORIZON_HI] != 0 || time < control[C_HORIZON_LO];
}

__device__ __forceinline__ bool queue_push(
    ulong node,
    const ulong *record,
    ulong *error,
    ulong *meta,
    ulong *records
) {
    ulong base = node * QUEUE_META_WORDS;
    ulong offset = meta[base];
    ulong capacity = meta[base + 1];
    ulong head = meta[base + 2];
    ulong count = meta[base + 3];
    if (count >= capacity) {
        set_capacity_error(error, ARENA_QUEUE, node, capacity, count + 1);
        return false;
    }
    ulong physical = (head + count) % max(capacity, 1ul);
    copy_thread_to_device(record, records, offset + physical);
    meta[base + 3] = count + 1;
    return true;
}

__device__ __forceinline__ bool source_queue_key_less_or_equal(
    const ulong *records,
    ulong left_slot,
    const ulong *right
) {
    ulong left = left_slot * EVENT_WORDS;
    if (records[left + E_PHASE] != right[E_PHASE]) {
        return records[left + E_PHASE] < right[E_PHASE];
    }
    if (records[left + E_TIME] != right[E_TIME]) {
        return records[left + E_TIME] < right[E_TIME];
    }
    if (records[left + PK_FLOW] != right[PK_FLOW]) {
        return records[left + PK_FLOW] < right[PK_FLOW];
    }
    return records[left + PK_ID] <= right[PK_ID];
}

__device__ __forceinline__ bool source_queue_insert(
    ulong node,
    const ulong *record,
    ulong *error,
    ulong *meta,
    ulong *records
) {
    ulong base = node * QUEUE_META_WORDS;
    ulong offset = meta[base];
    ulong capacity = meta[base + 1];
    ulong head = meta[base + 2];
    ulong count = meta[base + 3];
    if (count >= capacity) {
        set_capacity_error(error, ARENA_QUEUE, node, capacity, count + 1);
        return false;
    }
    ulong insertion = count;
    while (insertion != 0) {
        ulong previous = insertion - 1;
        ulong previous_slot = offset + (head + previous) % max(capacity, 1ul);
        if (source_queue_key_less_or_equal(records, previous_slot, record)) {
            break;
        }
        ulong destination = offset + (head + insertion) % max(capacity, 1ul);
        copy_device_record(records, previous_slot, records, destination);
        insertion = previous;
    }
    copy_thread_to_device(
        record,
        records,
        offset + (head + insertion) % max(capacity, 1ul)
    );
    meta[base + 3] = count + 1;
    return true;
}

__device__ __forceinline__ bool queue_pop(
    ulong node,
    ulong *meta,
    const ulong *records,
    ulong *record,
    ulong &physical
) {
    ulong base = node * QUEUE_META_WORDS;
    ulong offset = meta[base];
    ulong capacity = meta[base + 1];
    ulong head = meta[base + 2];
    ulong count = meta[base + 3];
    if (count == 0) {
        return false;
    }
    physical = head;
    copy_device_to_thread(records, offset + physical, record);
    meta[base + 2] = (head + 1) % max(capacity, 1ul);
    meta[base + 3] = count - 1;
    return true;
}

__device__ __forceinline__ bool queue_remove_at(
    ulong node,
    ulong logical,
    ulong *meta,
    ulong *records,
    ulong *record,
    ulong &physical
) {
    ulong base = node * QUEUE_META_WORDS;
    ulong offset = meta[base];
    ulong capacity = meta[base + 1];
    ulong head = meta[base + 2];
    ulong count = meta[base + 3];
    if (logical >= count) {
        return false;
    }
    if (logical == 0) {
        return queue_pop(node, meta, records, record, physical);
    }
    physical = (head + logical) % max(capacity, 1ul);
    copy_device_to_thread(records, offset + physical, record);
    for (ulong cursor = logical; cursor + 1 < count; ++cursor) {
        ulong source = offset + (head + cursor + 1) % max(capacity, 1ul);
        ulong target = offset + (head + cursor) % max(capacity, 1ul);
        copy_device_record(records, source, records, target);
    }
    meta[base + 3] = count - 1;
    return true;
}

__device__ __forceinline__ bool queue_front(
    ulong node,
    const ulong *meta,
    const ulong *records,
    ulong *record
) {
    ulong base = node * QUEUE_META_WORDS;
    if (meta[base + 3] == 0) {
        return false;
    }
    copy_device_to_thread(records, meta[base] + meta[base + 2], record);
    return true;
}

__device__ __forceinline__ ulong event_phase(ulong kind) {
    if (kind == TX_COMPLETE || kind == RETRANSMISSION_TIMEOUT || kind == PACING_TIMER) {
        return 1;
    }
    if (kind == TX_READY) {
        return 2;
    }
    return 0;
}

__device__ __forceinline__ void add_summary(
    ulong *summary,
    ulong node,
    uint counter,
    ulong value
) {
    ulong offset = node * 24 + counter * 2;
    ulong previous = summary[offset];
    ulong next = previous + value;
    summary[offset] = next;
    if (next < previous) {
        summary[offset + 1] += 1;
    }
}

__device__ __forceinline__ bool append_observed(
    ulong node,
    const ulong *packet,
    ulong *error,
    const ulong *params,
    ulong *observation_meta,
    ulong *observed
) {
    if (params[P_FULL_OBSERVATIONS] == 0) {
        return true;
    }
    ulong meta = node * OBSERVATION_META_WORDS;
    ulong index = observation_meta[meta + 3];
    ulong capacity = observation_meta[meta + 1];
    if (index >= capacity) {
        set_capacity_error(error, ARENA_OBSERVED, node, capacity, index + 1);
        return false;
    }
    ulong offset = (observation_meta[meta] + index) * 7;
    for (uint word = 0; word < 7; ++word) {
        observed[offset + word] = packet[PK_ID + word];
    }
    observation_meta[meta + 3] = index + 1;
    return true;
}

__device__ __forceinline__ bool record_sourced(
    ulong node,
    const ulong *packet,
    ulong *error,
    const ulong *params,
    ulong *summary,
    ulong *observation_meta,
    ulong *observed
) {
    add_summary(summary, node, 0, 1);
    add_summary(summary, node, 1, packet[PK_SIZE]);
    return append_observed(
        node,
        packet,
        error,
        params,
        observation_meta,
        observed
    );
}

__device__ __forceinline__ bool record_departure(
    ulong node,
    const ulong *event,
    ulong *error,
    const ulong *params,
    ulong *summary,
    ulong *observation_meta,
    ulong *observed,
    ulong *departures
) {
    add_summary(summary, node, 2, 1);
    add_summary(summary, node, 3, event[PK_SIZE]);
    if (!append_observed(
        node,
        event,
        error,
        params,
        observation_meta,
        observed
    )) {
        return false;
    }
    if (params[P_FULL_OBSERVATIONS] == 0) {
        return true;
    }
    ulong meta = node * OBSERVATION_META_WORDS + META_WORDS;
    ulong index = observation_meta[meta + 3];
    ulong capacity = observation_meta[meta + 1];
    if (index >= capacity) {
        set_capacity_error(error, ARENA_DEPARTURES, node, capacity, index + 1);
        return false;
    }
    ulong offset = (observation_meta[meta] + index) * 12;
    departures[offset] = event[E_TIME];
    departures[offset + 1] = event[E_PHASE];
    departures[offset + 2] = event[E_ORIGIN];
    departures[offset + 3] = event[E_SEQUENCE];
    departures[offset + 4] = event[PK_ID];
    departures[offset + 5] = event[E_TIME];
    departures[offset + 6] = event[PK_FLOW];
    departures[offset + 7] = event[PK_SIZE];
    departures[offset + 8] = event[PK_KIND];
    departures[offset + 9] = event[PK_META_0];
    departures[offset + 10] = event[PK_META_1];
    departures[offset + 11] = event[PK_META_2];
    observation_meta[meta + 3] = index + 1;
    return true;
}

__device__ __forceinline__ bool record_arrival(
    ulong node,
    const ulong *event,
    ulong disposition,
    ulong *error,
    const ulong *params,
    ulong *summary,
    ulong *observation_meta,
    ulong *observed,
    ulong *arrivals
) {
    uint counter = 4;
    if (disposition == 1) {
        counter = 8;
    } else if (disposition == 2) {
        counter = 6;
    } else if (disposition == 3) {
        counter = 10;
    }
    add_summary(summary, node, counter, 1);
    add_summary(summary, node, counter + 1, event[PK_SIZE]);
    if (!append_observed(
        node,
        event,
        error,
        params,
        observation_meta,
        observed
    )) {
        return false;
    }
    if (params[P_FULL_OBSERVATIONS] == 0) {
        return true;
    }
    ulong meta = node * OBSERVATION_META_WORDS + 2 * META_WORDS;
    ulong index = observation_meta[meta + 3];
    ulong capacity = observation_meta[meta + 1];
    if (index >= capacity) {
        set_capacity_error(error, ARENA_ARRIVALS, node, capacity, index + 1);
        return false;
    }
    ulong offset = (observation_meta[meta] + index) * 13;
    arrivals[offset] = event[E_TIME];
    arrivals[offset + 1] = event[E_PHASE];
    arrivals[offset + 2] = event[E_ORIGIN];
    arrivals[offset + 3] = event[E_SEQUENCE];
    arrivals[offset + 4] = event[PK_ID];
    arrivals[offset + 5] = event[E_TIME];
    arrivals[offset + 6] = disposition;
    arrivals[offset + 7] = event[PK_FLOW];
    arrivals[offset + 8] = event[PK_SIZE];
    arrivals[offset + 9] = event[PK_KIND];
    arrivals[offset + 10] = event[PK_META_0];
    arrivals[offset + 11] = event[PK_META_1];
    arrivals[offset + 12] = event[PK_META_2];
    observation_meta[meta + 3] = index + 1;
    return true;
}

__device__ __forceinline__ bool append_remote(
    ulong node,
    const ulong *record,
    ulong *error,
    const ulong *params,
    ulong *remote_meta,
    ulong *remote_staging,
    ulong *stream_state
) {
    ulong base = node * META_WORDS;
    ulong offset = remote_meta[base];
    ulong capacity = remote_meta[base + 1];
    ulong index = remote_meta[base + 3];
    if (index >= capacity) {
        set_capacity_error(error, ARENA_REMOTE_STAGING, node, capacity, index + 1);
        return false;
    }
    copy_thread_to_device(record, remote_staging, offset + index);
    remote_meta[base + 3] = index + 1;
    if (params[P_STREAMS_ENABLED] != 0) {
        ulong outbound_meta =
            params[P_OUTBOUND_META_OFFSET] + node * OUTBOUND_META_WORDS;
        ulong entry = stream_state[outbound_meta];
        ulong entry_count = stream_state[outbound_meta + 1];
        ulong channel = NONE;
        if (entry_count == 1) {
            if (
                params[P_STREAM_ORDER_CHECKS] == 0 ||
                stream_state[entry] == record[E_TARGET]
            ) {
                channel = stream_state[entry + 1];
            }
        } else {
            ulong low = 0;
            ulong high = entry_count;
            while (low < high) {
                ulong middle = low + (high - low) / 2;
                ulong current = entry + middle * OUTBOUND_ENTRY_WORDS;
                ulong target = stream_state[current];
                if (target < record[E_TARGET]) {
                    low = middle + 1;
                } else {
                    high = middle;
                }
            }
            if (low < entry_count) {
                ulong current = entry + low * OUTBOUND_ENTRY_WORDS;
                if (stream_state[current] == record[E_TARGET]) {
                    channel = stream_state[current + 1];
                }
            }
        }
        if (channel == NONE) {
            set_semantic_error(error, 40, node);
            return false;
        }
        ulong batch =
            params[P_CHANNEL_BATCH_OFFSET] + channel * CHANNEL_BATCH_WORDS;
        ulong batch_count = stream_state[batch];
        if (params[P_STREAM_ORDER_CHECKS] != 0 && batch_count != 0) {
            ulong previous_slot = stream_state[batch + 2];
            if (!stored_cross_key_less(
                remote_staging,
                previous_slot,
                remote_staging,
                offset + index
            )) {
                set_semantic_error(error, 41, node);
                return false;
            }
        } else {
            stream_state[batch + 1] = offset + index;
        }
        stream_state[batch] = batch_count + 1;
        stream_state[batch + 2] = offset + index;
        stream_state[params[P_STAGING_CHANNEL_OFFSET] + offset + index] =
            channel;
        return true;
    }
    ulong insertion = index;
    while (
        insertion != 0 &&
        stored_key_less(
            remote_staging,
            offset + insertion,
            offset + insertion - 1
        )
    ) {
        swap_records(
            remote_staging,
            offset + insertion,
            offset + insertion - 1
        );
        insertion -= 1;
    }
    return true;
}

__device__ __forceinline__ bool emit_child(
    ulong node,
    const ulong *parent,
    ulong target,
    ulong kind,
    ulong time,
    const ulong *packet,
    ulong *error,
    const ulong *params,
    ulong *node_state,
    ulong *fel_meta,
    ulong *fel_records,
    ulong *remote_meta,
    ulong *remote_staging,
    ulong *stream_state,
    ulong *stream_records,
    ulong *tcp_state
) {
    ulong node_base = node * NODE_WORDS;
    ulong sequence = node_state[node_base + N_NEXT_ORIGIN];
    if (sequence == NONE) {
        set_semantic_error(error, 1, node);
        return false;
    }
    node_state[node_base + N_NEXT_ORIGIN] = sequence + 1;
    ulong child[EVENT_WORDS];
    child[E_TIME] = time;
    child[E_PHASE] = event_phase(kind);
    child[E_ORIGIN] = node;
    child[E_SEQUENCE] = sequence;
    child[E_TARGET] = target;
    child[E_KIND] = kind;
    child[E_PAYLOAD] = packet[PK_ID];
    child[PK_ID] = packet[PK_ID];
    child[PK_FLOW] = packet[PK_FLOW];
    child[PK_SIZE] = packet[PK_SIZE];
    child[PK_KIND] = packet[PK_KIND];
    child[PK_META_0] = packet[PK_META_0];
    child[PK_META_1] = packet[PK_META_1];
    child[PK_META_2] = packet[PK_META_2];
    if (!key_less(parent, child)) {
        set_semantic_error(error, 2, node);
        return false;
    }
    if (target == node) {
        return classified_push(
            node,
            child,
            params,
            error,
            fel_meta,
            fel_records,
            stream_state,
            stream_records,
            tcp_state
        );
    }
    return append_remote(
        node,
        child,
        error,
        params,
        remote_meta,
        remote_staging,
        stream_state
    );
}

__device__ __forceinline__ bool checked_add(ulong left, ulong right, ulong &result) {
    result = left + right;
    return result >= left;
}

__device__ __forceinline__ bool u128_add(
    ulong left_low, ulong left_high, ulong right_low, ulong right_high,
    ulong &result_low, ulong &result_high
) {
    result_low = left_low + right_low;
    ulong carry = result_low < left_low ? 1ul : 0ul;
    result_high = left_high + right_high;
    if (result_high < left_high || result_high > NONE - carry) {
        return false;
    }
    result_high += carry;
    return true;
}

__device__ __forceinline__ bool u128_mul_u64(
    ulong left_low, ulong left_high, ulong right,
    ulong &result_low, ulong &result_high
) {
    if (left_high != 0 && __umul64hi(left_high, right) != 0) {
        return false;
    }
    result_low = left_low * right;
    ulong low_high = __umul64hi(left_low, right);
    ulong high_low = left_high * right;
    if (low_high > NONE - high_low) {
        return false;
    }
    result_high = low_high + high_low;
    return true;
}

__device__ __forceinline__ bool u128_mul(
    ulong left_low, ulong left_high, ulong right_low, ulong right_high,
    ulong &result_low, ulong &result_high
) {
    if ((left_high != 0 && right_high != 0) ||
        __umul64hi(left_low, right_high) != 0 || __umul64hi(left_high, right_low) != 0) {
        return false;
    }
    result_low = left_low * right_low;
    ulong high = __umul64hi(left_low, right_low);
    ulong cross = left_low * right_high;
    if (high > NONE - cross) {
        return false;
    }
    high += cross;
    cross = left_high * right_low;
    if (high > NONE - cross) {
        return false;
    }
    result_high = high + cross;
    return true;
}

__device__ __forceinline__ bool u128_from_mul_u64(
    ulong left, ulong right, ulong &result_low, ulong &result_high
) {
    result_low = left * right;
    result_high = __umul64hi(left, right);
    return true;
}

__device__ __forceinline__ bool u128_at_least(
    ulong left_low, ulong left_high, ulong right_low, ulong right_high
) {
    return left_high > right_high || (left_high == right_high && left_low >= right_low);
}

__device__ __forceinline__ void u128_sub(
    ulong left_low, ulong left_high, ulong right_low, ulong right_high,
    ulong &result_low, ulong &result_high
) {
    result_low = left_low - right_low;
    result_high = left_high - right_high - (left_low < right_low ? 1ul : 0ul);
}

__device__ __forceinline__ ulong saturating_add_u64(ulong left, ulong right) {
    ulong result = left + right;
    return result < left ? NONE : result;
}

__device__ __forceinline__ ulong saturating_mul_u64(ulong left, ulong right) {
    if (left != 0 && right > NONE / left) {
        return NONE;
    }
    return left * right;
}

__device__ __forceinline__ ulong u128_to_u64(unsigned __int128 value) {
    return value > (unsigned __int128)NONE ? NONE : ulong(value);
}

__device__ __forceinline__ ulong mul_div_u64(
    ulong value,
    ulong numerator,
    ulong denominator
) {
    return u128_to_u64(
        (static_cast<unsigned __int128>(value) * static_cast<unsigned __int128>(numerator)) /
        denominator
    );
}

__device__ __forceinline__ bool cube_le_u128(
    ulong value,
    unsigned __int128 bound
) {
    if (value == 0) {
        return true;
    }
    unsigned __int128 square = static_cast<unsigned __int128>(value) * value;
    return square <= bound / value;
}

__device__ __forceinline__ ulong floor_cube_root_u128(unsigned __int128 value) {
    ulong low = 0;
    ulong high = 1;
    while (high <= NONE / 2 && cube_le_u128(high, value)) {
        high *= 2;
    }
    if (cube_le_u128(high, value)) {
        return NONE;
    }
    while (low + 1 < high) {
        ulong middle = low + (high - low) / 2;
        if (cube_le_u128(middle, value)) {
            low = middle;
        } else {
            high = middle;
        }
    }
    return low;
}

__device__ __forceinline__ ulong cubic_k_ns(ulong w_max_scaled) {
    if (w_max_scaled == 0) {
        return 0;
    }
    // Wmax <= 2e15 makes the exact K radicand at most 111 bits. Candidate cubes are
    // conceptually 192 bits, but comparison divides the 128-bit bound before multiplying the
    // third factor. The general WFQ 320/512/640-bit machinery is unnecessary for this proof.
    unsigned __int128 radicand =
        static_cast<unsigned __int128>(w_max_scaled) * 3 * 1000000000000000000ul / 4;
    return floor_cube_root_u128(radicand);
}

__device__ __forceinline__ ulong cubic_magnitude(ulong distance) {
    constexpr ulong denominator = 5000000000000000000ul;
    // An unrestricted d^3 and 2*d^3 need 192 and 193 bits. Saturation lets us compare against a
    // 127-bit threshold before forming them: if the comparison succeeds, the exact cube and
    // scaled numerator fit in 128 bits; otherwise the scalar BigUint quotient necessarily
    // saturates to u64::MAX as well.
    unsigned __int128 threshold =
        (static_cast<unsigned __int128>(NONE) * denominator + (denominator - 1)) / 2;
    if (!cube_le_u128(distance, threshold)) {
        return NONE;
    }
    unsigned __int128 cube = static_cast<unsigned __int128>(distance) * distance * distance;
    return u128_to_u64(cube * 2 / denominator);
}

__device__ __forceinline__ ulong cubic_window_scaled(
    ulong w_max_scaled,
    ulong k_ns,
    ulong elapsed_ns
) {
    bool negative = elapsed_ns < k_ns;
    ulong distance = elapsed_ns < k_ns ? k_ns - elapsed_ns : elapsed_ns - k_ns;
    ulong magnitude = cubic_magnitude(distance);
    if (negative) {
        ulong reduced = w_max_scaled > magnitude ? w_max_scaled - magnitude : 0;
        return max(reduced, CUBIC_SCALE);
    }
    return min(max(saturating_add_u64(w_max_scaled, magnitude), CUBIC_SCALE), CUBIC_MAX_WINDOW);
}

__device__ __forceinline__ ulong tcp_friendly_window_scaled(
    ulong w_max_scaled,
    ulong elapsed_ns,
    ulong rtt_ns
) {
    ulong base = mul_div_u64(w_max_scaled, 7, 10);
    unsigned __int128 numerator =
        static_cast<unsigned __int128>(CUBIC_SCALE) * 9 * elapsed_ns;
    unsigned __int128 denominator = static_cast<unsigned __int128>(17) * max(rtt_ns, 1ul);
    ulong growth = u128_to_u64(numerator / denominator);
    return min(max(saturating_add_u64(base, growth), CUBIC_SCALE), CUBIC_MAX_WINDOW);
}

__device__ __forceinline__ ulong cubic_ack_step(ulong cwnd, ulong target) {
    ulong distance = cwnd < target ? target - cwnd : cwnd - target;
    ulong delta = u128_to_u64(
        static_cast<unsigned __int128>(distance) * CUBIC_SCALE / max(cwnd, CUBIC_SCALE)
    );
    return target >= cwnd ? saturating_add_u64(cwnd, delta) : (cwnd > delta ? cwnd - delta : 0);
}

__device__ __forceinline__ ulong controller_cwnd_bytes(const ulong *control) {
    if (control[CTL_KIND] == 0) {
        return control[CTL_CWND];
    }
    return u128_to_u64(
        static_cast<unsigned __int128>(control[CTL_CWND]) * max(control[CTL_MSS], 1ul) /
        CUBIC_SCALE
    );
}

__device__ __forceinline__ void controller_recovery_exit(ulong *control) {
    if (control[CTL_KIND] == 0) {
        control[CTL_CWND] = max(control[CTL_SSTHRESH], control[CTL_MSS]);
        control[CTL_EXTRA_0] = 0;
    } else {
        control[CTL_CWND] = min(max(control[CTL_SSTHRESH], CUBIC_SCALE), CUBIC_MAX_WINDOW);
    }
    control[CTL_PHASE] = TCP_CONGESTION_AVOIDANCE;
    control[CTL_DUP_ACKS] = 0;
    control[CTL_RECOVERY_HIGH] = 0;
}

__device__ __forceinline__ void controller_fast_retransmit(
    ulong *control,
    ulong flight,
    ulong now_ns
) {
    if (control[CTL_KIND] == 0) {
        ulong minimum = saturating_mul_u64(control[CTL_MSS], 2);
        control[CTL_SSTHRESH] = max(flight / 2, minimum);
        control[CTL_CWND] = saturating_add_u64(
            control[CTL_SSTHRESH], saturating_mul_u64(control[CTL_MSS], 3)
        );
        control[CTL_EXTRA_0] = 0;
    } else {
        ulong previous_max = control[CTL_W_LAST_MAX];
        ulong current = control[CTL_CWND];
        control[CTL_W_LAST_MAX] = current;
        control[CTL_EXTRA_0] =
            previous_max > 0 && current < previous_max ? mul_div_u64(current, 17, 20) : current;
        ulong flight_scaled = min(
            u128_to_u64(static_cast<unsigned __int128>(flight) * CUBIC_SCALE /
                max(control[CTL_MSS], 1ul)),
            CUBIC_MAX_WINDOW
        );
        ulong reduced = max(mul_div_u64(flight_scaled, 7, 10), CUBIC_SCALE);
        control[CTL_SSTHRESH] = max(reduced, 2 * CUBIC_SCALE);
        control[CTL_CWND] = min(reduced, CUBIC_MAX_WINDOW);
        control[CTL_EPOCH] = now_ns;
        control[CTL_K] = cubic_k_ns(control[CTL_EXTRA_0]);
    }
    control[CTL_PHASE] = TCP_FAST_RECOVERY;
}

__device__ __forceinline__ bool controller_duplicate_ack(
    ulong *control,
    ulong flight,
    ulong now_ns
) {
    control[CTL_DUP_ACKS] = saturating_add_u64(control[CTL_DUP_ACKS], 1);
    if (control[CTL_DUP_ACKS] == 3) {
        controller_fast_retransmit(control, flight, now_ns);
        return true;
    }
    if (control[CTL_DUP_ACKS] > 3 && control[CTL_PHASE] == TCP_FAST_RECOVERY) {
        control[CTL_CWND] = control[CTL_KIND] == 0
            ? saturating_add_u64(control[CTL_CWND], control[CTL_MSS])
            : min(saturating_add_u64(control[CTL_CWND], CUBIC_SCALE), CUBIC_MAX_WINDOW);
    }
    return false;
}

__device__ __forceinline__ void controller_new_ack(
    ulong *control,
    ulong acknowledged_bytes,
    ulong now_ns,
    ulong rtt_sample,
    ulong acknowledgment
) {
    control[CTL_DUP_ACKS] = 0;
    if (control[CTL_KIND] == 0) {
        if (control[CTL_PHASE] == TCP_SLOW_START) {
            control[CTL_CWND] = saturating_add_u64(
                control[CTL_CWND], min(control[CTL_MSS], acknowledged_bytes)
            );
            if (control[CTL_CWND] >= control[CTL_SSTHRESH]) {
                control[CTL_CWND] = control[CTL_SSTHRESH];
                control[CTL_PHASE] = TCP_CONGESTION_AVOIDANCE;
                control[CTL_EXTRA_0] = 0;
            }
        } else if (control[CTL_PHASE] == TCP_CONGESTION_AVOIDANCE) {
            control[CTL_EXTRA_0] = saturating_add_u64(control[CTL_EXTRA_0], acknowledged_bytes);
            while (control[CTL_EXTRA_0] >= max(control[CTL_CWND], 1ul)) {
                control[CTL_EXTRA_0] -= max(control[CTL_CWND], 1ul);
                control[CTL_CWND] = saturating_add_u64(control[CTL_CWND], control[CTL_MSS]);
            }
        } else if (control[CTL_RECOVERY_HIGH] != 0 && acknowledgment >= control[CTL_RECOVERY_HIGH]) {
            controller_recovery_exit(control);
        } else {
            control[CTL_CWND] = saturating_add_u64(control[CTL_SSTHRESH], control[CTL_MSS]);
        }
        return;
    }

    ulong sample = max(rtt_sample, 1ul);
    control[CTL_SRTT] = control[CTL_SRTT] == 0
        ? sample
        : u128_to_u64((static_cast<unsigned __int128>(control[CTL_SRTT]) * 7 + sample) / 8);
    if (control[CTL_PHASE] == TCP_SLOW_START) {
        ulong segments = acknowledged_bytes / max(control[CTL_MSS], 1ul) +
            ulong(acknowledged_bytes % max(control[CTL_MSS], 1ul) != 0);
        control[CTL_CWND] = min(
            saturating_add_u64(control[CTL_CWND], saturating_mul_u64(segments, CUBIC_SCALE)),
            CUBIC_MAX_WINDOW
        );
        if (control[CTL_CWND] >= control[CTL_SSTHRESH]) {
            control[CTL_PHASE] = TCP_CONGESTION_AVOIDANCE;
            control[CTL_EPOCH] = now_ns;
            if (control[CTL_EXTRA_0] == 0) {
                control[CTL_EXTRA_0] = control[CTL_CWND];
                control[CTL_K] = 0;
            }
        }
    } else if (control[CTL_PHASE] == TCP_CONGESTION_AVOIDANCE) {
        if (control[CTL_EPOCH] == NONE) {
            control[CTL_EPOCH] = now_ns;
            if (control[CTL_EXTRA_0] == 0) {
                control[CTL_EXTRA_0] = control[CTL_CWND];
                control[CTL_K] = 0;
            } else {
                control[CTL_K] = cubic_k_ns(control[CTL_EXTRA_0]);
            }
        }
        ulong elapsed = now_ns >= control[CTL_EPOCH] ? now_ns - control[CTL_EPOCH] : 0;
        ulong cubic_now = cubic_window_scaled(control[CTL_EXTRA_0], control[CTL_K], elapsed);
        ulong friendly = tcp_friendly_window_scaled(
            control[CTL_EXTRA_0], elapsed, max(control[CTL_SRTT], 1ul)
        );
        if (cubic_now < friendly) {
            control[CTL_CWND] = min(friendly, CUBIC_MAX_WINDOW);
        } else {
            ulong target = cubic_window_scaled(
                control[CTL_EXTRA_0], control[CTL_K],
                saturating_add_u64(elapsed, max(control[CTL_SRTT], 1ul))
            );
            control[CTL_CWND] = min(
                max(cubic_ack_step(control[CTL_CWND], target), CUBIC_SCALE),
                CUBIC_MAX_WINDOW
            );
        }
    } else if (control[CTL_RECOVERY_HIGH] != 0 && acknowledgment >= control[CTL_RECOVERY_HIGH]) {
        controller_recovery_exit(control);
    }
}

__device__ __forceinline__ void controller_timeout(ulong *control, ulong flight) {
    if (control[CTL_KIND] == 0) {
        control[CTL_SSTHRESH] = max(flight / 2, saturating_mul_u64(control[CTL_MSS], 2));
        control[CTL_CWND] = control[CTL_MSS];
        control[CTL_EXTRA_0] = 0;
    } else {
        ulong flight_scaled = min(
            u128_to_u64(static_cast<unsigned __int128>(flight) * CUBIC_SCALE /
                max(control[CTL_MSS], 1ul)),
            CUBIC_MAX_WINDOW
        );
        control[CTL_SSTHRESH] = min(
            max(mul_div_u64(flight_scaled, 7, 10), 2 * CUBIC_SCALE), CUBIC_MAX_WINDOW
        );
        control[CTL_CWND] = CUBIC_SCALE;
        control[CTL_EXTRA_0] = 0;
        control[CTL_W_LAST_MAX] = 0;
        control[CTL_EPOCH] = NONE;
        control[CTL_K] = 0;
    }
    control[CTL_PHASE] = TCP_SLOW_START;
    control[CTL_DUP_ACKS] = 0;
    control[CTL_RECOVERY_HIGH] = 0;
}

__device__ __forceinline__ ulong update_rto(
    ulong &srtt,
    ulong &rtt_var,
    ulong sample
) {
    sample = max(sample, 1ul);
    if (srtt == 0) {
        srtt = sample;
        rtt_var = sample / 2;
    } else {
        ulong deviation = srtt > sample ? srtt - sample : sample - srtt;
        rtt_var = u128_to_u64((static_cast<unsigned __int128>(rtt_var) * 3 + deviation) / 4);
        srtt = u128_to_u64((static_cast<unsigned __int128>(srtt) * 7 + sample) / 8);
    }
    ulong variance = max(saturating_mul_u64(rtt_var, 4), TCP_RTO_GRANULARITY);
    return min(max(saturating_add_u64(srtt, variance), TCP_MIN_RTO), TCP_MAX_RTO);
}

__device__ __forceinline__ void big_clear(uint *value, uint limbs) {
    for (uint limb = 0; limb < limbs; ++limb) {
        value[limb] = 0;
    }
}

__device__ __forceinline__ bool big_is_zero(const uint *value, uint limbs) {
    for (uint limb = 0; limb < limbs; ++limb) {
        if (value[limb] != 0) {
            return false;
        }
    }
    return true;
}

__device__ __forceinline__ void big_copy(
    const uint *source,
    uint *target,
    uint limbs
) {
    for (uint limb = 0; limb < limbs; ++limb) {
        target[limb] = source[limb];
    }
}

__device__ __forceinline__ void big_load(
    const ulong *source,
    ulong offset,
    uint *target
) {
    for (uint word = 0; word < 5; ++word) {
        ulong packed = source[offset + word];
        target[word * 2] = uint(packed);
        target[word * 2 + 1] = uint(packed >> 32);
    }
}

__device__ __forceinline__ void big_store(
    const uint *source,
    ulong *target,
    ulong offset
) {
    for (uint word = 0; word < 5; ++word) {
        target[offset + word] =
            ulong(source[word * 2]) | (ulong(source[word * 2 + 1]) << 32);
    }
}

__device__ __forceinline__ int big_compare(
    const uint *left,
    const uint *right,
    uint limbs
) {
    for (int limb = int(limbs) - 1; limb >= 0; --limb) {
        if (left[limb] != right[limb]) {
            return left[limb] < right[limb] ? -1 : 1;
        }
    }
    return 0;
}

__device__ __forceinline__ ulong big_remainder_u64(
    const uint *value,
    uint limbs,
    ulong divisor
) {
    ulong remainder = 0;
    for (int bit = int(limbs * 32) - 1; bit >= 0; --bit) {
        bool carry = (remainder >> 63) != 0;
        ulong shifted =
            (remainder << 1) |
            ulong((value[uint(bit) / 32] >> (uint(bit) % 32)) & 1u);
        if (carry || shifted >= divisor) {
            shifted -= divisor;
        }
        remainder = shifted;
    }
    return remainder;
}

__device__ __forceinline__ ulong big_div_u64(
    const uint *value,
    uint limbs,
    ulong divisor,
    uint *quotient
) {
    big_clear(quotient, limbs);
    ulong remainder = 0;
    for (int bit = int(limbs * 32) - 1; bit >= 0; --bit) {
        bool carry = (remainder >> 63) != 0;
        ulong shifted =
            (remainder << 1) |
            ulong((value[uint(bit) / 32] >> (uint(bit) % 32)) & 1u);
        if (carry || shifted >= divisor) {
            shifted -= divisor;
            quotient[uint(bit) / 32] |= 1u << (uint(bit) % 32);
        }
        remainder = shifted;
    }
    return remainder;
}

__device__ __forceinline__ ulong gcd_u64(ulong left, ulong right) {
    while (right != 0) {
        ulong remainder = left % right;
        left = right;
        right = remainder;
    }
    return left;
}

__device__ __forceinline__ void big_multiply(
    const uint *left,
    uint left_limbs,
    const uint *right,
    uint right_limbs,
    uint *product,
    uint product_limbs
) {
    big_clear(product, product_limbs);
    for (uint left_limb = 0; left_limb < left_limbs; ++left_limb) {
        ulong carry = 0;
        for (uint right_limb = 0; right_limb < right_limbs; ++right_limb) {
            uint output = left_limb + right_limb;
            ulong current =
                ulong(left[left_limb]) * ulong(right[right_limb]) +
                ulong(product[output]) + carry;
            product[output] = uint(current);
            carry = current >> 32;
        }
        product[left_limb + right_limbs] = uint(carry);
    }
}

__device__ __forceinline__ bool big_add(
    uint *target,
    const uint *addend,
    uint limbs
) {
    ulong carry = 0;
    for (uint limb = 0; limb < limbs; ++limb) {
        ulong sum = ulong(target[limb]) + ulong(addend[limb]) + carry;
        target[limb] = uint(sum);
        carry = sum >> 32;
    }
    return carry == 0;
}

__device__ __forceinline__ void u64_limbs(ulong value, uint *limbs) {
    limbs[0] = uint(value);
    limbs[1] = uint(value >> 32);
}

__device__ __forceinline__ void multiply_u64(ulong left, ulong right, uint *product) {
    uint left_limbs[2];
    uint right_limbs[2];
    u64_limbs(left, left_limbs);
    u64_limbs(right, right_limbs);
    big_multiply(left_limbs, 2, right_limbs, 2, product, 4);
}

__device__ __forceinline__ bool rational_compare(
    const uint *left_num,
    const uint *left_den,
    const uint *right_num,
    const uint *right_den,
    int &ordering
) {
    if (big_is_zero(left_den, BIG_LIMBS) || big_is_zero(right_den, BIG_LIMBS)) {
        return false;
    }
    uint left_product[PRODUCT_LIMBS];
    uint right_product[PRODUCT_LIMBS];
    big_multiply(
        left_num,
        BIG_LIMBS,
        right_den,
        BIG_LIMBS,
        left_product,
        PRODUCT_LIMBS
    );
    big_multiply(
        right_num,
        BIG_LIMBS,
        left_den,
        BIG_LIMBS,
        right_product,
        PRODUCT_LIMBS
    );
    ordering = big_compare(left_product, right_product, PRODUCT_LIMBS);
    return true;
}

__device__ __forceinline__ void rational_load(
    const ulong *state,
    ulong offset,
    uint *numerator,
    uint *denominator
) {
    big_load(state, offset, numerator);
    big_load(state, offset + 5, denominator);
}

__device__ __forceinline__ void rational_store(
    const uint *numerator,
    const uint *denominator,
    ulong *state,
    ulong offset
) {
    big_store(numerator, state, offset);
    big_store(denominator, state, offset + 5);
}

__device__ __forceinline__ void rational_zero(ulong *state, ulong offset) {
    for (uint word = 0; word < RATIONAL_WORDS; ++word) {
        state[offset + word] = 0;
    }
    state[offset + 5] = 1;
}

__device__ __forceinline__ void rational_copy(
    ulong *state,
    ulong source,
    ulong target
) {
    for (uint word = 0; word < RATIONAL_WORDS; ++word) {
        state[target + word] = state[source + word];
    }
}

__device__ __forceinline__ bool rational_add_small(
    const uint *input_num,
    const uint *input_den,
    const uint *small_num_input,
    ulong small_den_input,
    uint *output_num,
    uint *output_den
) {
    if (small_den_input == 0 || big_is_zero(input_den, BIG_LIMBS)) {
        return false;
    }
    uint small_num[4];
    big_copy(small_num_input, small_num, 4);
    ulong small_den = small_den_input;
    ulong small_gcd = gcd_u64(big_remainder_u64(small_num, 4, small_den), small_den);
    if (small_gcd > 1) {
        uint reduced[4];
        if (big_div_u64(small_num, 4, small_gcd, reduced) != 0) {
            return false;
        }
        big_copy(reduced, small_num, 4);
        small_den /= small_gcd;
    }

    ulong denominator_gcd =
        gcd_u64(big_remainder_u64(input_den, BIG_LIMBS, small_den), small_den);
    uint denominator_base[BIG_LIMBS];
    if (
        big_div_u64(
            input_den,
            BIG_LIMBS,
            denominator_gcd,
            denominator_base
        ) != 0
    ) {
        return false;
    }
    ulong left_scale = small_den / denominator_gcd;
    uint left_scale_limbs[2];
    u64_limbs(left_scale, left_scale_limbs);
    uint numerator[WIDE_LIMBS];
    big_multiply(
        input_num,
        BIG_LIMBS,
        left_scale_limbs,
        2,
        numerator,
        WIDE_LIMBS
    );
    uint right[WIDE_LIMBS];
    big_multiply(
        denominator_base,
        BIG_LIMBS,
        small_num,
        4,
        right,
        WIDE_LIMBS
    );
    if (!big_add(numerator, right, WIDE_LIMBS)) {
        return false;
    }

    ulong result_gcd =
        gcd_u64(big_remainder_u64(numerator, WIDE_LIMBS, denominator_gcd), denominator_gcd);
    uint reduced_num[WIDE_LIMBS];
    if (big_div_u64(numerator, WIDE_LIMBS, result_gcd, reduced_num) != 0) {
        return false;
    }
    ulong right_scale = small_den / result_gcd;
    uint right_scale_limbs[2];
    u64_limbs(right_scale, right_scale_limbs);
    uint reduced_den[WIDE_LIMBS];
    big_multiply(
        denominator_base,
        BIG_LIMBS,
        right_scale_limbs,
        2,
        reduced_den,
        WIDE_LIMBS
    );
    for (uint limb = BIG_LIMBS; limb < WIDE_LIMBS; ++limb) {
        if (reduced_num[limb] != 0 || reduced_den[limb] != 0) {
            return false;
        }
    }
    if (big_is_zero(reduced_den, BIG_LIMBS)) {
        return false;
    }
    big_copy(reduced_num, output_num, BIG_LIMBS);
    big_copy(reduced_den, output_den, BIG_LIMBS);
    return true;
}

__device__ __forceinline__ bool serialization_ns(
    ulong bytes,
    ulong rate,
    ulong &result
) {
    if (rate == 0) {
        return false;
    }
    ulong scale = 8000000000ul;
    ulong high = __umul64hi(bytes, scale);
    ulong low = bytes * scale;
    ulong quotient = 0;
    ulong remainder = 0;
    if (high == 0) {
        quotient = low / rate;
        remainder = low % rate;
    } else {
        if (high >= rate) {
            return false;
        }
        remainder = high;
        for (int bit = 63; bit >= 0; --bit) {
            bool carry = (remainder >> 63) != 0;
            ulong shifted = (remainder << 1) | ((low >> uint(bit)) & 1ul);
            if (carry || shifted >= rate) {
                shifted -= rate;
                quotient |= 1ul << uint(bit);
            }
            remainder = shifted;
        }
    }
    if (remainder != 0) {
        if (quotient == NONE) {
            return false;
        }
        quotient += 1;
    }
    result = quotient;
    return true;
}

__device__ __forceinline__ bool scheduler_active_weight_sum(
    ulong node_base,
    const ulong *scheduler_state,
    ulong &weight_sum
) {
    ulong class_count = scheduler_state[node_base + S_CLASS_COUNT];
    ulong class_offset = scheduler_state[node_base + S_CLASS_OFFSET];
    weight_sum = 0;
    for (ulong class_index = 0; class_index < class_count; ++class_index) {
        ulong class_base = class_offset + class_index * SCHEDULER_CLASS_WORDS;
        if (scheduler_state[class_base + SC_ACTIVE] == 0) {
            continue;
        }
        ulong weight = scheduler_state[class_base + SC_VALUE];
        if (weight_sum > NONE - weight) {
            return false;
        }
        weight_sum += weight;
    }
    return true;
}

__device__ __forceinline__ bool scheduler_first_packet_for_class(
    ulong node,
    ulong class_count,
    ulong class_index,
    const ulong *queue_meta,
    const ulong *queue_records,
    ulong &position,
    ulong &size
) {
    ulong meta_base = node * QUEUE_META_WORDS;
    ulong offset = queue_meta[meta_base];
    ulong capacity = queue_meta[meta_base + 1];
    ulong head = queue_meta[meta_base + 2];
    ulong count = queue_meta[meta_base + 3];
    for (ulong logical = 0; logical < count; ++logical) {
        ulong physical = (head + logical) % max(capacity, 1ul);
        ulong record_base = (offset + physical) * EVENT_WORDS;
        if (queue_records[record_base + PK_FLOW] % class_count == class_index) {
            position = logical;
            size = queue_records[record_base + PK_SIZE];
            return true;
        }
    }
    return false;
}

__device__ __forceinline__ bool drr_select_position(
    ulong node,
    ulong *error,
    const ulong *queue_meta,
    const ulong *queue_records,
    ulong *scheduler_state,
    ulong &position
) {
    ulong node_base = node * SCHEDULER_NODE_WORDS;
    ulong class_count = scheduler_state[node_base + S_CLASS_COUNT];
    ulong class_offset = scheduler_state[node_base + S_CLASS_OFFSET];
    ulong current = scheduler_state[node_base + S_LAST_UPDATED];
    if (class_count == 0 || current >= class_count) {
        set_semantic_error(error, 33, node);
        return false;
    }

    // First finish the scalar cursor's current, possibly partial, scan. No deficit is added
    // until that scan wraps from the last class back to class zero.
    for (ulong class_index = current; class_index < class_count; ++class_index) {
        ulong size = 0;
        ulong candidate = 0;
        ulong class_base = class_offset + class_index * SCHEDULER_CLASS_WORDS;
        ulong deficit = scheduler_state[class_base + SC_ACTIVE];
        if (
            scheduler_first_packet_for_class(
                node,
                class_count,
                class_index,
                queue_meta,
                queue_records,
                candidate,
                size
            ) &&
            deficit > 0 &&
            size <= deficit
        ) {
            scheduler_state[class_base + SC_ACTIVE] = deficit - size;
            scheduler_state[node_base + S_LAST_UPDATED] = class_index;
            position = candidate;
            return true;
        }
    }

    // After the first wrap, every nonempty class gains its quantum once per complete scan.
    // Compute the first selectable round directly; this is exact even when it is u64::MAX.
    ulong selected_class = NONE;
    ulong selected_position = 0;
    ulong selected_size = 0;
    ulong minimum_rounds = NONE;
    for (ulong class_index = 0; class_index < class_count; ++class_index) {
        ulong candidate = 0;
        ulong size = 0;
        if (!scheduler_first_packet_for_class(
            node,
            class_count,
            class_index,
            queue_meta,
            queue_records,
            candidate,
            size
        )) {
            continue;
        }
        ulong class_base = class_offset + class_index * SCHEDULER_CLASS_WORDS;
        ulong quantum = scheduler_state[class_base + SC_VALUE];
        ulong deficit = scheduler_state[class_base + SC_ACTIVE];
        if (quantum == 0) {
            set_semantic_error(error, 33, node);
            return false;
        }
        ulong rounds = 1;
        if (size > deficit) {
            ulong needed = size - deficit;
            rounds = needed / quantum;
            if (needed % quantum != 0) {
                rounds += 1;
            }
        }
        if (selected_class == NONE || rounds < minimum_rounds) {
            selected_class = class_index;
            selected_position = candidate;
            selected_size = size;
            minimum_rounds = rounds;
        }
    }
    if (selected_class == NONE) {
        set_semantic_error(error, 33, node);
        return false;
    }

    for (ulong class_index = 0; class_index < class_count; ++class_index) {
        ulong ignored_position = 0;
        ulong ignored_size = 0;
        ulong class_base = class_offset + class_index * SCHEDULER_CLASS_WORDS;
        if (!scheduler_first_packet_for_class(
            node,
            class_count,
            class_index,
            queue_meta,
            queue_records,
            ignored_position,
            ignored_size
        )) {
            scheduler_state[class_base + SC_ACTIVE] = 0;
            continue;
        }
        ulong quantum = scheduler_state[class_base + SC_VALUE];
        ulong deficit = scheduler_state[class_base + SC_ACTIVE];
        if (quantum == 0 || minimum_rounds > (NONE - deficit) / quantum) {
            set_semantic_error(error, 33, node);
            return false;
        }
        scheduler_state[class_base + SC_ACTIVE] = deficit + minimum_rounds * quantum;
    }
    ulong selected_base = class_offset + selected_class * SCHEDULER_CLASS_WORDS;
    ulong selected_deficit = scheduler_state[selected_base + SC_ACTIVE];
    if (selected_size > selected_deficit) {
        set_semantic_error(error, 33, node);
        return false;
    }
    scheduler_state[selected_base + SC_ACTIVE] = selected_deficit - selected_size;
    scheduler_state[node_base + S_LAST_UPDATED] = selected_class;
    position = selected_position;
    return true;
}

__device__ __forceinline__ bool wrr_select_position(
    ulong node,
    ulong *error,
    const ulong *queue_meta,
    const ulong *queue_records,
    ulong *scheduler_state,
    ulong &position
) {
    ulong node_base = node * SCHEDULER_NODE_WORDS;
    ulong class_count = scheduler_state[node_base + S_CLASS_COUNT];
    ulong class_offset = scheduler_state[node_base + S_CLASS_OFFSET];
    ulong current = scheduler_state[node_base + S_LAST_UPDATED];
    if (class_count == 0 || current >= class_count) {
        set_semantic_error(error, 34, node);
        return false;
    }
    // One scan may reset exhausted nonempty classes; a second scan must then select one.
    for (uint pass = 0; pass < 2; ++pass) {
        for (ulong scanned = 0; scanned < class_count; ++scanned) {
            ulong class_base = class_offset + current * SCHEDULER_CLASS_WORDS;
            ulong weight = scheduler_state[class_base + SC_VALUE];
            ulong sent = scheduler_state[class_base + SC_ACTIVE];
            if (weight == 0 || sent > weight) {
                set_semantic_error(error, 34, node);
                return false;
            }
            ulong candidate = 0;
            ulong ignored_size = 0;
            if (
                sent < weight &&
                scheduler_first_packet_for_class(
                    node,
                    class_count,
                    current,
                    queue_meta,
                    queue_records,
                    candidate,
                    ignored_size
                )
            ) {
                scheduler_state[class_base + SC_ACTIVE] = sent + 1;
                scheduler_state[node_base + S_LAST_UPDATED] = current;
                position = candidate;
                return true;
            }
            scheduler_state[class_base + SC_ACTIVE] = 0;
            current = (current + 1) % class_count;
            scheduler_state[node_base + S_LAST_UPDATED] = current;
        }
    }
    set_semantic_error(error, 34, node);
    return false;
}

__device__ __forceinline__ bool switch_admission_action(
    ulong node,
    const ulong *packet,
    ulong taildrop_capacity,
    ulong *error,
    const ulong *queue_meta,
    const ulong *scheduler_state,
    ulong &action
) {
    ulong scheduler_base = node * SCHEDULER_NODE_WORDS;
    ulong policy = scheduler_state[scheduler_base + S_AQM_KIND];
    ulong meta_base = node * QUEUE_META_WORDS;
    ulong waiting = queue_meta[meta_base + 3];
    ulong queued_bytes = queue_meta[meta_base + 4];
    if (queued_bytes > NONE - packet[PK_SIZE]) {
        action = 2;
        return true;
    }
    ulong post_bytes = queued_bytes + packet[PK_SIZE];
    if (policy == AQM_TAILDROP) {
        action = taildrop_capacity != 0 && waiting >= taildrop_capacity ? 2 : 0;
        return true;
    }
    if (policy != AQM_ECN) {
        set_semantic_error(error, 58, node);
        return false;
    }

    ulong capacity = scheduler_state[scheduler_base + S_AQM_CAPACITY];
    ulong threshold = scheduler_state[scheduler_base + S_AQM_THRESHOLD];
    ulong unit = scheduler_state[scheduler_base + S_AQM_UNIT];
    if (capacity == 0 || threshold == 0 || threshold > capacity || unit > AQM_BYTES) {
        set_semantic_error(error, 58, node);
        return false;
    }
    ulong post_depth = waiting + 1;
    if (unit == AQM_BYTES) {
        post_depth = post_bytes;
    }
    action = post_depth > capacity ? 2 : (post_depth >= threshold ? 1 : 0);
    return true;
}

__device__ __forceinline__ void local_rational_zero(uint *numerator, uint *denominator) {
    big_clear(numerator, BIG_LIMBS);
    big_clear(denominator, BIG_LIMBS);
    denominator[0] = 1;
}

__device__ __forceinline__ bool wfq_advanced_virtual_time(
    ulong node,
    ulong time,
    ulong rate,
    ulong *error,
    ulong *scheduler_state,
    uint *numerator,
    uint *denominator
) {
    ulong node_base = node * SCHEDULER_NODE_WORDS;
    ulong last_updated = scheduler_state[node_base + S_LAST_UPDATED];
    if (time < last_updated) {
        set_semantic_error(error, 26, node);
        return false;
    }
    rational_load(
        scheduler_state,
        node_base + S_VIRTUAL_TIME,
        numerator,
        denominator
    );
    ulong elapsed = time - last_updated;
    if (elapsed == 0) {
        return true;
    }
    ulong weight_sum;
    if (
        !scheduler_active_weight_sum(node_base, scheduler_state, weight_sum) ||
        weight_sum == 0 ||
        weight_sum > NONE / 1000000000ul
    ) {
        set_wfq_arithmetic_error(error, node);
        return false;
    }
    uint increment[4];
    multiply_u64(elapsed, rate, increment);
    uint result_num[BIG_LIMBS];
    uint result_den[BIG_LIMBS];
    if (
        !rational_add_small(
            numerator,
            denominator,
            increment,
            weight_sum * 1000000000ul,
            result_num,
            result_den
        )
    ) {
        set_wfq_arithmetic_error(error, node);
        return false;
    }
    big_copy(result_num, numerator, BIG_LIMBS);
    big_copy(result_den, denominator, BIG_LIMBS);
    return true;
}

__device__ __forceinline__ bool sp_queue_insert(
    ulong node,
    const ulong *record,
    ulong *error,
    ulong *queue_meta,
    ulong *queue_records,
    const ulong *scheduler_state
) {
    ulong meta_base = node * QUEUE_META_WORDS;
    ulong offset = queue_meta[meta_base];
    ulong capacity = queue_meta[meta_base + 1];
    ulong head = queue_meta[meta_base + 2];
    ulong count = queue_meta[meta_base + 3];
    if (count >= capacity) {
        set_capacity_error(error, ARENA_QUEUE, node, capacity, count + 1);
        return false;
    }
    ulong node_base = node * SCHEDULER_NODE_WORDS;
    ulong class_count = scheduler_state[node_base + S_CLASS_COUNT];
    ulong class_offset = scheduler_state[node_base + S_CLASS_OFFSET];
    if (class_count == 0) {
        set_semantic_error(error, 27, node);
        return false;
    }
    ulong incoming_class = record[PK_FLOW] % class_count;
    ulong incoming_priority =
        scheduler_state[
            class_offset + incoming_class * SCHEDULER_CLASS_WORDS + SC_VALUE
        ];
    ulong insertion = count;
    for (ulong logical = 0; logical < count; ++logical) {
        ulong physical = (head + logical) % max(capacity, 1ul);
        ulong record_base = (offset + physical) * EVENT_WORDS;
        ulong queued_class = queue_records[record_base + PK_FLOW] % class_count;
        ulong queued_priority =
            scheduler_state[
                class_offset + queued_class * SCHEDULER_CLASS_WORDS + SC_VALUE
            ];
        if (queued_priority < incoming_priority) {
            insertion = logical;
            break;
        }
    }
    for (ulong logical = count; logical > insertion; --logical) {
        ulong source = offset + (head + logical - 1) % max(capacity, 1ul);
        ulong target = offset + (head + logical) % max(capacity, 1ul);
        copy_device_record(queue_records, source, queue_records, target);
    }
    copy_thread_to_device(
        record,
        queue_records,
        offset + (head + insertion) % max(capacity, 1ul)
    );
    queue_meta[meta_base + 3] = count + 1;
    return true;
}

__device__ __forceinline__ bool wfq_queue_insert(
    ulong node,
    const ulong *record,
    const uint *finish_num,
    const uint *finish_den,
    ulong *error,
    ulong *queue_meta,
    ulong *queue_records,
    ulong *scheduler_state
) {
    ulong meta_base = node * QUEUE_META_WORDS;
    ulong offset = queue_meta[meta_base];
    ulong capacity = queue_meta[meta_base + 1];
    ulong head = queue_meta[meta_base + 2];
    ulong count = queue_meta[meta_base + 3];
    if (count >= capacity) {
        set_capacity_error(error, ARENA_QUEUE, node, capacity, count + 1);
        return false;
    }
    ulong node_base = node * SCHEDULER_NODE_WORDS;
    ulong tag_offset = scheduler_state[node_base + S_QUEUE_TAG_OFFSET];
    ulong insertion = count;
    for (ulong logical = 0; logical < count; ++logical) {
        ulong physical = (head + logical) % max(capacity, 1ul);
        uint queued_num[BIG_LIMBS];
        uint queued_den[BIG_LIMBS];
        rational_load(
            scheduler_state,
            tag_offset + physical * RATIONAL_WORDS,
            queued_num,
            queued_den
        );
        int ordering;
        if (!rational_compare(queued_num, queued_den, finish_num, finish_den, ordering)) {
            set_wfq_arithmetic_error(error, node);
            return false;
        }
        if (ordering > 0) {
            insertion = logical;
            break;
        }
    }
    for (ulong logical = count; logical > insertion; --logical) {
        ulong source_physical = (head + logical - 1) % max(capacity, 1ul);
        ulong target_physical = (head + logical) % max(capacity, 1ul);
        copy_device_record(
            queue_records,
            offset + source_physical,
            queue_records,
            offset + target_physical
        );
        rational_copy(
            scheduler_state,
            tag_offset + source_physical * RATIONAL_WORDS,
            tag_offset + target_physical * RATIONAL_WORDS
        );
    }
    ulong destination = (head + insertion) % max(capacity, 1ul);
    copy_thread_to_device(record, queue_records, offset + destination);
    rational_store(
        finish_num,
        finish_den,
        scheduler_state,
        tag_offset + destination * RATIONAL_WORDS
    );
    queue_meta[meta_base + 3] = count + 1;
    return true;
}

__device__ __forceinline__ bool wfq_enqueue(
    ulong node,
    const ulong *record,
    ulong rate,
    ulong *error,
    ulong *queue_meta,
    ulong *queue_records,
    ulong *scheduler_state
) {
    ulong node_base = node * SCHEDULER_NODE_WORDS;
    ulong class_count = scheduler_state[node_base + S_CLASS_COUNT];
    ulong class_offset = scheduler_state[node_base + S_CLASS_OFFSET];
    if (class_count == 0 || record[E_TIME] < scheduler_state[node_base + S_LAST_UPDATED]) {
        set_semantic_error(error, 28, node);
        return false;
    }
    ulong class_index = record[PK_FLOW] % class_count;
    ulong class_base = class_offset + class_index * SCHEDULER_CLASS_WORDS;
    ulong weight = scheduler_state[class_base + SC_VALUE];
    ulong active = scheduler_state[class_base + SC_ACTIVE];
    if (weight == 0 || active == NONE) {
        set_semantic_error(error, 29, node);
        return false;
    }

    ulong active_weight_sum;
    if (!scheduler_active_weight_sum(node_base, scheduler_state, active_weight_sum)) {
        set_wfq_arithmetic_error(error, node);
        return false;
    }
    bool idle = active_weight_sum == 0;
    uint virtual_num[BIG_LIMBS];
    uint virtual_den[BIG_LIMBS];
    if (idle) {
        local_rational_zero(virtual_num, virtual_den);
    } else if (
        !wfq_advanced_virtual_time(
            node,
            record[E_TIME],
            rate,
            error,
            scheduler_state,
            virtual_num,
            virtual_den
        )
    ) {
        return false;
    }

    uint start_num[BIG_LIMBS];
    uint start_den[BIG_LIMBS];
    if (idle) {
        local_rational_zero(start_num, start_den);
    } else {
        uint class_num[BIG_LIMBS];
        uint class_den[BIG_LIMBS];
        rational_load(
            scheduler_state,
            class_base + SC_FINISH,
            class_num,
            class_den
        );
        int ordering;
        if (!rational_compare(virtual_num, virtual_den, class_num, class_den, ordering)) {
            set_wfq_arithmetic_error(error, node);
            return false;
        }
        if (ordering >= 0) {
            big_copy(virtual_num, start_num, BIG_LIMBS);
            big_copy(virtual_den, start_den, BIG_LIMBS);
        } else {
            big_copy(class_num, start_num, BIG_LIMBS);
            big_copy(class_den, start_den, BIG_LIMBS);
        }
    }

    uint service[4];
    multiply_u64(record[PK_SIZE], 8, service);
    uint finish_num[BIG_LIMBS];
    uint finish_den[BIG_LIMBS];
    if (
        !rational_add_small(
            start_num,
            start_den,
            service,
            weight,
            finish_num,
            finish_den
        )
    ) {
        set_wfq_arithmetic_error(error, node);
        return false;
    }
    if (
        !wfq_queue_insert(
            node,
            record,
            finish_num,
            finish_den,
            error,
            queue_meta,
            queue_records,
            scheduler_state
        )
    ) {
        return false;
    }

    if (idle) {
        rational_zero(scheduler_state, node_base + S_VIRTUAL_TIME);
        for (ulong reset_class = 0; reset_class < class_count; ++reset_class) {
            rational_zero(
                scheduler_state,
                class_offset + reset_class * SCHEDULER_CLASS_WORDS + SC_FINISH
            );
        }
    } else {
        rational_store(
            virtual_num,
            virtual_den,
            scheduler_state,
            node_base + S_VIRTUAL_TIME
        );
    }
    rational_store(
        finish_num,
        finish_den,
        scheduler_state,
        class_base + SC_FINISH
    );
    scheduler_state[class_base + SC_ACTIVE] = active + 1;
    scheduler_state[node_base + S_LAST_UPDATED] = record[E_TIME];
    return true;
}

__device__ __forceinline__ bool wfq_complete(
    ulong node,
    const ulong *record,
    ulong rate,
    ulong *error,
    ulong *scheduler_state
) {
    ulong node_base = node * SCHEDULER_NODE_WORDS;
    ulong class_count = scheduler_state[node_base + S_CLASS_COUNT];
    ulong class_offset = scheduler_state[node_base + S_CLASS_OFFSET];
    if (class_count == 0) {
        set_semantic_error(error, 30, node);
        return false;
    }
    ulong class_index = record[PK_FLOW] % class_count;
    ulong class_base = class_offset + class_index * SCHEDULER_CLASS_WORDS;
    ulong active = scheduler_state[class_base + SC_ACTIVE];
    if (active == 0) {
        set_semantic_error(error, 31, node);
        return false;
    }
    uint virtual_num[BIG_LIMBS];
    uint virtual_den[BIG_LIMBS];
    if (
        !wfq_advanced_virtual_time(
            node,
            record[E_TIME],
            rate,
            error,
            scheduler_state,
            virtual_num,
            virtual_den
        )
    ) {
        return false;
    }

    scheduler_state[class_base + SC_ACTIVE] = active - 1;
    bool any_active = false;
    for (ulong other = 0; other < class_count; ++other) {
        ulong other_base = class_offset + other * SCHEDULER_CLASS_WORDS;
        any_active = any_active || scheduler_state[other_base + SC_ACTIVE] != 0;
    }
    rational_zero(scheduler_state, node_base + S_IN_SERVICE_TAG);
    if (any_active) {
        rational_store(
            virtual_num,
            virtual_den,
            scheduler_state,
            node_base + S_VIRTUAL_TIME
        );
    } else {
        rational_zero(scheduler_state, node_base + S_VIRTUAL_TIME);
        rational_zero(scheduler_state, class_base + SC_FINISH);
    }
    scheduler_state[node_base + S_LAST_UPDATED] = record[E_TIME];
    return true;
}

__device__ __forceinline__ bool flow_route(
    const ulong *packet,
    const ulong *flows,
    ulong &offset,
    ulong &length,
    ulong &terminal
) {
    ulong flow_base = packet[PK_FLOW] * FLOW_WORDS;
    ulong kind = packet[PK_KIND] & PK_KIND_MASK;
    if (kind == DATA_PACKET || kind == TCP_DATA_PACKET) {
        offset = flows[flow_base + 2];
        length = flows[flow_base + 3];
        terminal = flows[flow_base + 1];
    } else {
        offset = flows[flow_base + 4];
        length = flows[flow_base + 5];
        terminal = flows[flow_base];
    }
    return true;
}

__device__ __forceinline__ bool packet_egress(
    ulong node,
    const ulong *packet,
    const ulong *flows,
    const ulong *routes,
    const ulong *links,
    ulong &egress
) {
    ulong offset;
    ulong length;
    ulong terminal;
    flow_route(packet, flows, offset, length, terminal);
    for (ulong index = 0; index < length; ++index) {
        ulong link = routes[offset + index];
        if (links[link * LINK_WORDS] == node) {
            egress = link;
            return true;
        }
    }
    if (terminal == node) {
        egress = NONE;
        return true;
    }
    return false;
}

__device__ __forceinline__ bool packet_remote_target(
    const ulong *packet,
    ulong egress,
    const ulong *flows,
    const ulong *routes,
    const ulong *links,
    ulong &target
) {
    ulong offset;
    ulong length;
    ulong terminal;
    flow_route(packet, flows, offset, length, terminal);
    for (ulong index = 0; index < length; ++index) {
        if (routes[offset + index] == egress) {
            if (index + 1 < length) {
                ulong next = routes[offset + index + 1];
                target = links[next * LINK_WORDS];
            } else {
                target = terminal;
            }
            return true;
        }
    }
    return false;
}

__device__ __forceinline__ void packet_clear(ulong *packet) {
    for (uint word = 0; word < EVENT_WORDS; ++word) {
        packet[word] = 0;
    }
}

__device__ __forceinline__ bool allocate_tcp_payload(
    ulong node,
    ulong *node_state,
    const ulong *params,
    ulong &payload
) {
    ulong node_base = node * NODE_WORDS;
    ulong sequence = node_state[node_base + N_NEXT_PAYLOAD];
    if (sequence == NONE || sequence > (NONE - node) / params[P_NODE_COUNT]) {
        return false;
    }
    payload = sequence * params[P_NODE_COUNT] + node;
    node_state[node_base + N_NEXT_PAYLOAD] = sequence + 1;
    return true;
}

__device__ __forceinline__ void tcp_record_copy(
    const ulong *packet,
    ulong *target
) {
    target[0] = packet[PK_ID];
    target[1] = packet[PK_SIZE];
    target[2] = packet[PK_META_0];
    target[3] = packet[PK_META_1];
    target[4] = packet[PK_META_2];
}

// Absolute word index of logical ledger record `logical`. Logical order — the canonical order the
// readback decodes and the frozen hashes cover — is unchanged by the ring; only the physical slot
// moves. `head + logical <= 2 * capacity - 1` at every call site, so one conditional subtraction
// is a complete modulo.
__device__ __forceinline__ ulong tcp_ledger_slot(
    ulong offset,
    ulong capacity,
    ulong head,
    ulong logical
) {
    ulong span = capacity == 0 ? 1 : capacity;
    ulong physical = head + logical;
    if (physical >= span) {
        physical -= span;
    }
    return offset + physical * TCP_LEDGER_RECORD_WORDS;
}

// First logical index whose sequence is >= `sequence`, by binary search over the ring window.
//
// Equivalent to the pre-ring linear scan: the ledger is strictly ascending in sequence and
// duplicate-free, because inserts replace on an exact sequence match and a partial cumulative ACK
// re-keys the head record to an acknowledgement that stays below the next record's start.
__device__ __forceinline__ ulong tcp_ledger_lower_bound(
    ulong offset,
    ulong capacity,
    ulong head,
    ulong count,
    ulong sequence,
    const ulong *tcp_state
) {
    ulong low = 0;
    ulong high = count;
    while (low < high) {
        ulong mid = low + (high - low) / 2;
        if (tcp_state[tcp_ledger_slot(offset, capacity, head, mid) + 2] < sequence) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low;
}

__device__ __forceinline__ bool tcp_ledger_find(
    ulong flow,
    ulong sequence,
    const ulong *params,
    const ulong *tcp_state,
    ulong &record
) {
    ulong meta = params[P_TCP_LEDGER_META_OFFSET] + flow * TCP_LEDGER_META_WORDS;
    ulong offset = tcp_state[meta];
    ulong capacity = tcp_state[meta + 1];
    ulong count = tcp_state[meta + 2];
    ulong head = tcp_state[meta + TCP_LEDGER_META_HEAD];
    ulong index = tcp_ledger_lower_bound(offset, capacity, head, count, sequence, tcp_state);
    if (index < count) {
        ulong candidate = tcp_ledger_slot(offset, capacity, head, index);
        if (tcp_state[candidate + 2] == sequence) {
            record = candidate;
            return true;
        }
    }
    return false;
}

__device__ __forceinline__ bool tcp_ledger_insert(
    ulong flow,
    const ulong *packet,
    ulong *error,
    const ulong *params,
    ulong *tcp_state
) {
    ulong meta = params[P_TCP_LEDGER_META_OFFSET] + flow * TCP_LEDGER_META_WORDS;
    ulong offset = tcp_state[meta];
    ulong capacity = tcp_state[meta + 1];
    ulong count = tcp_state[meta + 2];
    ulong head = tcp_state[meta + TCP_LEDGER_META_HEAD];
    ulong sequence = packet[PK_META_0];
    ulong insertion = tcp_ledger_lower_bound(offset, capacity, head, count, sequence, tcp_state);
    if (insertion < count) {
        ulong existing = tcp_ledger_slot(offset, capacity, head, insertion);
        if (tcp_state[existing + 2] == sequence) {
            if (tcp_state[existing + 1] != packet[PK_SIZE]) {
                set_semantic_error(error, 40, NONE);
                return false;
            }
            // Mutation site R1: in-place replace. Count, head and high-water are unchanged.
            tcp_record_copy(packet, tcp_state + existing);
            return true;
        }
    }
    if (count >= capacity) {
        set_capacity_error(error, ARENA_TCP_SEGMENT_LEDGER, flow, capacity, count + 1);
        return false;
    }
    ulong target;
    if (insertion == count) {
        // Mutation site R2: monotone append, O(1). This is the frontier's hot path.
        target = tcp_ledger_slot(offset, capacity, head, count);
    } else if (insertion == 0) {
        // Mutation site R3: prefix insert retreats the head, O(1).
        ulong retreated = (head == 0 ? capacity : head) - 1;
        tcp_state[meta + TCP_LEDGER_META_HEAD] = retreated;
        target = offset + retreated * TCP_LEDGER_RECORD_WORDS;
    } else {
        // Mutation site R4: interior insert still shifts, but inside the ring window.
        for (ulong index = count; index > insertion; --index) {
            ulong destination = tcp_ledger_slot(offset, capacity, head, index);
            ulong source = tcp_ledger_slot(offset, capacity, head, index - 1);
            for (uint word = 0; word < TCP_LEDGER_RECORD_WORDS; ++word) {
                tcp_state[destination + word] = tcp_state[source + word];
            }
        }
        target = tcp_ledger_slot(offset, capacity, head, insertion);
    }
    tcp_record_copy(packet, tcp_state + target);
    // Mutation site R5: the only site that raises `count`, hence the only site that can raise the
    // high-water word. High-water is a max over a deterministic execution, so it is pure state.
    ulong grown = count + 1;
    tcp_state[meta + 2] = grown;
    if (grown > tcp_state[meta + TCP_LEDGER_META_HIGH_WATER]) {
        tcp_state[meta + TCP_LEDGER_META_HIGH_WATER] = grown;
    }
    return true;
}

__device__ __forceinline__ bool tcp_ledger_acknowledge(
    ulong flow,
    ulong acknowledgment,
    ulong *error,
    const ulong *params,
    ulong *tcp_state
) {
    ulong meta = params[P_TCP_LEDGER_META_OFFSET] + flow * TCP_LEDGER_META_WORDS;
    ulong offset = tcp_state[meta];
    ulong capacity = tcp_state[meta + 1];
    ulong count = tcp_state[meta + 2];
    ulong head = tcp_state[meta + TCP_LEDGER_META_HEAD];
    ulong keep = 0;
    while (keep < count) {
        ulong record = tcp_ledger_slot(offset, capacity, head, keep);
        ulong sequence = tcp_state[record + 2];
        ulong size = tcp_state[record + 1];
        if (sequence > NONE - size) {
            set_semantic_error(error, 42, NONE);
            return false;
        }
        if (sequence + size > acknowledgment) {
            break;
        }
        keep += 1;
    }
    if (keep < count) {
        ulong first = tcp_ledger_slot(offset, capacity, head, keep);
        ulong sequence = tcp_state[first + 2];
        if (sequence < acknowledgment) {
            ulong acknowledged = acknowledgment - sequence;
            if (acknowledged < tcp_state[first + 1]) {
                // Mutation site R6: partial cumulative ACK re-keys the head record in place.
                tcp_state[first + 1] -= acknowledged;
                tcp_state[first + 2] = acknowledgment;
            } else {
                keep += 1;
            }
        }
    }
    // Mutation site R7: prefix removal advances the head. The O(n) compaction this replaces was
    // the whole reason the ring conversion was folded into T20i.
    ulong span = capacity == 0 ? 1 : capacity;
    ulong advanced = head + keep;
    if (advanced >= span) {
        advanced -= span;
    }
    tcp_state[meta + TCP_LEDGER_META_HEAD] = advanced;
    tcp_state[meta + 2] = count - keep;
    return true;
}

__device__ __forceinline__ bool tcp_receive_range(
    ulong flow,
    ulong start,
    ulong end,
    ulong *error,
    const ulong *params,
    ulong *tcp_state
) {
    ulong row = params[P_TCP_RECEIVER_OFFSET] + flow * TCP_RECEIVER_WORDS;
    ulong next = tcp_state[row + 3];
    if (end <= start || end <= next) {
        return true;
    }
    ulong offset = tcp_state[row + 4];
    ulong capacity = tcp_state[row + 5];
    ulong count = tcp_state[row + 6];
    if (count >= capacity) {
        set_capacity_error(error, ARENA_TCP_RECEIVER, flow, capacity, count + 1);
        return false;
    }
    ulong insertion = count;
    while (insertion != 0) {
        ulong previous = insertion - 1;
        ulong previous_start = tcp_state[offset + previous * 2];
        ulong previous_end = tcp_state[offset + previous * 2 + 1];
        if (previous_start < start || (previous_start == start && previous_end <= end)) {
            break;
        }
        tcp_state[offset + insertion * 2] = previous_start;
        tcp_state[offset + insertion * 2 + 1] = previous_end;
        insertion = previous;
    }
    tcp_state[offset + insertion * 2] = start;
    tcp_state[offset + insertion * 2 + 1] = end;
    count += 1;

    ulong merged = 0;
    for (ulong index = 0; index < count; ++index) {
        ulong range_start = tcp_state[offset + index * 2];
        ulong range_end = tcp_state[offset + index * 2 + 1];
        if (merged != 0 && range_start <= tcp_state[offset + (merged - 1) * 2 + 1]) {
            tcp_state[offset + (merged - 1) * 2 + 1] =
                max(tcp_state[offset + (merged - 1) * 2 + 1], range_end);
        } else {
            tcp_state[offset + merged * 2] = range_start;
            tcp_state[offset + merged * 2 + 1] = range_end;
            merged += 1;
        }
    }
    ulong kept = 0;
    for (ulong index = 0; index < merged; ++index) {
        ulong range_start = tcp_state[offset + index * 2];
        ulong range_end = tcp_state[offset + index * 2 + 1];
        if (range_start <= next) {
            next = max(next, range_end);
        } else {
            tcp_state[offset + kept * 2] = range_start;
            tcp_state[offset + kept * 2 + 1] = range_end;
            kept += 1;
        }
    }
    tcp_state[row + 3] = next;
    tcp_state[row + 6] = kept;
    return true;
}

__device__ __forceinline__ bool tcp_enqueue_attempt(
    ulong node,
    ulong flow,
    ulong sequence,
    ulong size_bytes,
    ulong now_ns,
    bool retransmission,
    const ulong *parent,
    ulong *error,
    const ulong *params,
    ulong *node_state,
    ulong *generators,
    ulong *queue_meta,
    ulong *queue_records,
    ulong *summary,
    ulong *observation_meta,
    ulong *observed,
    ulong *tcp_state
) {
    ulong payload;
    if (!allocate_tcp_payload(node, node_state, params, payload)) {
        set_semantic_error(error, 44, node);
        return false;
    }
    ulong packet[EVENT_WORDS];
    packet_clear(packet);
    packet[E_TIME] = parent[E_TIME];
    packet[E_PHASE] = 1;
    packet[PK_ID] = payload;
    packet[PK_FLOW] = flow;
    packet[PK_SIZE] = size_bytes;
    packet[PK_KIND] = TCP_DATA_PACKET;
    packet[PK_META_0] = sequence;
    packet[PK_META_1] = now_ns;
    packet[PK_META_2] = ulong(retransmission);
    if (!tcp_ledger_insert(flow, packet, error, params, tcp_state) ||
        !source_queue_insert(node, packet, error, queue_meta, queue_records) ||
        !record_sourced(node, packet, error, params, summary, observation_meta, observed)) {
        return false;
    }
    ulong generator = flow * GENERATOR_WORDS;
    generators[generator + G_TCP_LAST_ATTEMPT] = payload;
    if (!retransmission) {
        if (generators[generator + G_PACKETS] == NONE ||
            generators[generator + G_BYTES] > NONE - size_bytes) {
            set_semantic_error(error, 45, node);
            return false;
        }
        generators[generator + G_PACKETS] += 1;
        generators[generator + G_BYTES] += size_bytes;
    }
    ulong node_base = node * NODE_WORDS;
    if (node_state[node_base + N_COUNTER_0] == NONE) {
        set_semantic_error(error, 46, node);
        return false;
    }
    node_state[node_base + N_COUNTER_0] += 1;
    return true;
}

__device__ __forceinline__ bool prepare_tcp_attempts(
    ulong node,
    ulong flow,
    const ulong *parent,
    bool retransmit,
    ulong retransmit_sequence,
    bool fill_window,
    bool preserve_scheduled_send,
    ulong *error,
    const ulong *params,
    ulong *node_state,
    ulong *generators,
    ulong *fel_meta,
    ulong *fel_records,
    ulong *queue_meta,
    ulong *queue_records,
    ulong *remote_meta,
    ulong *remote_staging,
    ulong *stream_state,
    ulong *stream_records,
    ulong *summary,
    ulong *observation_meta,
    ulong *observed,
    ulong *tcp_state
) {
    ulong generator = flow * GENERATOR_WORDS;
    if (retransmit && retransmit_sequence < generators[generator + G_TCP_TOTAL]) {
        ulong record;
        if (!tcp_ledger_find(flow, retransmit_sequence, params, tcp_state, record)) {
            set_semantic_error(error, 47, node);
            return false;
        }
        ulong size = tcp_state[record + 1];
        if (!tcp_enqueue_attempt(
            node, flow, retransmit_sequence, size, parent[E_TIME], true, parent, error,
            params, node_state, generators, queue_meta, queue_records, summary,
            observation_meta, observed, tcp_state
        )) {
            return false;
        }
    }

    if (fill_window) {
        while (true) {
            ulong cwnd = controller_cwnd_bytes(generators + generator + G_CONTROL);
            ulong flight = generators[generator + G_TCP_FLIGHT];
            ulong allowance = cwnd > flight ? cwnd - flight : 0;
            ulong sequence = generators[generator + G_TCP_NEXT];
            ulong total = generators[generator + G_TCP_TOTAL];
            if (allowance == 0 || sequence >= total) {
                break;
            }
            ulong size = min(min(generators[generator + G_TCP_MSS], total - sequence), allowance);
            if (size == 0 || sequence > NONE - size || flight > NONE - size) {
                set_semantic_error(error, 48, node);
                return false;
            }
            generators[generator + G_TCP_NEXT] = sequence + size;
            generators[generator + G_TCP_FLIGHT] = flight + size;
            if (!tcp_enqueue_attempt(
                node, flow, sequence, size, parent[E_TIME], false, parent, error, params,
                node_state, generators, queue_meta, queue_records, summary, observation_meta,
                observed, tcp_state
            )) {
                return false;
            }
        }
    }

    generators[generator + G_OUTSTANDING] = generators[generator + G_TCP_FLIGHT];
    generators[generator + G_UNACKNOWLEDGED] = generators[generator + G_TCP_FLIGHT];
    if (!preserve_scheduled_send) {
        generators[generator + G_STATUS] =
            generators[generator + G_TCP_HIGHEST_ACK] >= generators[generator + G_TCP_TOTAL] ? 2 : 1;
    }

    bool install_timer = !preserve_scheduled_send &&
        generators[generator + G_TCP_TIMER_ACTIVE] == 0 &&
        generators[generator + G_TCP_FLIGHT] != 0;
    if (install_timer) {
        if (generators[generator + G_TCP_TIMER_GENERATION] == NONE) {
            set_semantic_error(error, 49, node);
            return false;
        }
        ulong deadline;
        if (!checked_add(parent[E_TIME], generators[generator + G_TCP_RTO], deadline)) {
            set_semantic_error(error, 50, node);
            return false;
        }
        generators[generator + G_TCP_TIMER_GENERATION] += 1;
        generators[generator + G_TCP_TIMER_ACTIVE] = 1;
        generators[generator + G_TCP_TIMER_ATTEMPT] = generators[generator + G_TCP_LAST_ATTEMPT];
        generators[generator + G_TCP_TIMER_SEQUENCE] = generators[generator + G_TCP_HIGHEST_ACK];
        generators[generator + G_TCP_TIMER_DEADLINE] = deadline;
        generators[generator + G_TCP_TIMER_STORED_GENERATION] =
            generators[generator + G_TCP_TIMER_GENERATION];
        generators[generator + G_TCP_TIMER_RTO] = generators[generator + G_TCP_RTO];
        ulong timer_packet[EVENT_WORDS];
        packet_clear(timer_packet);
        timer_packet[PK_ID] = generators[generator + G_TCP_LAST_ATTEMPT];
        timer_packet[PK_FLOW] = flow;
        timer_packet[PK_KIND] = TCP_DATA_PACKET;
        if (!emit_child(
            node, parent, node, RETRANSMISSION_TIMEOUT, deadline, timer_packet, error, params,
            node_state, fel_meta, fel_records, remote_meta, remote_staging, stream_state,
            stream_records,
            tcp_state)) {
            return false;
        }
    }

    ulong node_base = node * NODE_WORDS;
    if (queue_meta[node * QUEUE_META_WORDS + 3] != 0 &&
        node_state[node_base + N_SERVICE_VALID] == 0 &&
        node_state[node_base + N_READY_PENDING] == 0) {
        ulong ready[EVENT_WORDS];
        if (!queue_front(node, queue_meta, queue_records, ready)) {
            set_semantic_error(error, 51, node);
            return false;
        }
        node_state[node_base + N_READY_PENDING] = 1;
        return emit_child(
            node, parent, node, TX_READY, parent[E_TIME], ready, error, params, node_state,
            fel_meta, fel_records, remote_meta, remote_staging, stream_state, stream_records,
            tcp_state);
    }
    return true;
}

__device__ __forceinline__ bool dispatch_event(
    ulong node,
    ulong *event,
    ulong popped_timer_owner,
    ulong *error,
    const ulong *params,
    ulong *node_state,
    ulong *generators,
    const ulong *flows,
    const ulong *routes,
    const ulong *links,
    ulong *fel_meta,
    ulong *fel_records,
    ulong *queue_meta,
    ulong *queue_records,
    ulong *in_service,
    ulong *scheduler_state,
    ulong *remote_meta,
    ulong *remote_staging,
    ulong *stream_state,
    ulong *stream_records,
    ulong *summary,
    ulong *observation_meta,
    ulong *observed,
    ulong *departures,
    ulong *arrivals,
    ulong *tcp_state
) {
    ulong node_base = node * NODE_WORDS;
    ulong role = node_state[node_base + N_KIND];
    ulong kind = event[E_KIND];

    if (kind == PACING_TIMER && role == HOST) {
        ulong flow = event[PK_FLOW];
        ulong generator = flow * GENERATOR_WORDS;
        if (flow >= params[P_FLOW_COUNT] || generators[generator + G_VALID] == 0 ||
            generators[generator + G_OWNER] != node || generators[generator + G_KIND] != 2) {
            return true;
        }
        ulong status = generators[generator + G_STATUS];
        if ((status != 0 && status != 1) ||
            generators[generator + G_PAYLOAD] != event[E_PAYLOAD] ||
            generators[generator + G_DEPARTURE] != event[E_TIME]) {
            return true;
        }

        ulong scale_low;
        ulong scale_high;
        ulong tick_low;
        ulong tick_high;
        ulong bits_low = event[PK_SIZE] << 3;
        ulong bits_high = event[PK_SIZE] >> 61;
        ulong cost_low;
        ulong cost_high;
        if (!u128_from_mul_u64(
                generators[generator + G_RATE_DENOMINATOR], 1000000000ul,
                scale_low, scale_high) ||
            !u128_from_mul_u64(
                generators[generator + G_RATE_NUMERATOR],
                generators[generator + G_RATE_INTERVAL], tick_low, tick_high) ||
            !u128_mul(bits_low, bits_high, scale_low, scale_high, cost_low, cost_high)) {
            set_semantic_error(error, 59, node);
            return false;
        }
        ulong credit_low;
        ulong credit_high;
        if (!u128_add(
                generators[generator + G_RATE_CREDIT_LOW],
                generators[generator + G_RATE_CREDIT_HIGH], tick_low, tick_high,
                credit_low, credit_high)) {
            set_semantic_error(error, 59, node);
            return false;
        }
        bool emitted = u128_at_least(credit_low, credit_high, cost_low, cost_high);
        if (emitted) {
            u128_sub(credit_low, credit_high, cost_low, cost_high, credit_low, credit_high);
            if (generators[generator + G_PACKETS] == NONE ||
                generators[generator + G_BYTES] > NONE - event[PK_SIZE] ||
                node_state[node_base + N_COUNTER_0] == NONE) {
                set_semantic_error(error, 59, node);
                return false;
            }
            generators[generator + G_PACKETS] += 1;
            generators[generator + G_BYTES] += event[PK_SIZE];
            node_state[node_base + N_COUNTER_0] += 1;
        }
        generators[generator + G_RATE_CREDIT_LOW] = credit_low;
        generators[generator + G_RATE_CREDIT_HIGH] = credit_high;

        bool finished = generators[generator + G_BYTES] >= generators[generator + G_RATE_TOTAL];
        ulong candidate = 0;
        if (!finished && !checked_add(
                event[E_TIME], generators[generator + G_RATE_INTERVAL], candidate)) {
            set_semantic_error(error, 59, node);
            return false;
        }
        if (!finished && candidate <= params[P_STOP_TIME]) {
            ulong payload = event[PK_ID];
            ulong size = event[PK_SIZE];
            if (emitted) {
                ulong sequence = node_state[node_base + N_NEXT_PAYLOAD];
                if (sequence == NONE || sequence > (NONE - node) / params[P_NODE_COUNT]) {
                    set_semantic_error(error, 59, node);
                    return false;
                }
                payload = sequence * params[P_NODE_COUNT] + node;
                node_state[node_base + N_NEXT_PAYLOAD] = sequence + 1;
                ulong remaining = generators[generator + G_RATE_TOTAL] -
                    generators[generator + G_BYTES];
                size = min(generators[generator + G_RATE_PACKET_SIZE], remaining);
            }
            ulong next_bits_low = size << 3;
            ulong next_bits_high = size >> 61;
            ulong next_cost_low;
            ulong next_cost_high;
            if (!u128_mul(
                    next_bits_low, next_bits_high, scale_low, scale_high,
                    next_cost_low, next_cost_high)) {
                set_semantic_error(error, 59, node);
                return false;
            }
            ulong next_credit_low;
            ulong next_credit_high;
            bool next_scheduled = u128_add(
                    credit_low, credit_high, tick_low, tick_high,
                    next_credit_low, next_credit_high) &&
                u128_at_least(next_credit_low, next_credit_high, next_cost_low, next_cost_high);
            generators[generator + G_STATUS] = next_scheduled ? 0 : 1;
            generators[generator + G_DEPARTURE] = candidate;
            generators[generator + G_PAYLOAD] = payload;
            ulong next_packet[EVENT_WORDS];
            packet_clear(next_packet);
            next_packet[PK_ID] = payload;
            next_packet[PK_FLOW] = flow;
            next_packet[PK_SIZE] = size;
            next_packet[PK_KIND] = DATA_PACKET;
            if (!emit_child(
                    node, event, node, PACING_TIMER, candidate, next_packet, error, params,
                    node_state, fel_meta, fel_records, remote_meta, remote_staging,
                    stream_state, stream_records,
                    tcp_state)) {
                return false;
            }
        } else {
            generators[generator + G_STATUS] = finished ? 2 : 3;
            if (!finished) {
                generators[generator + G_DEPARTURE] = candidate;
            }
        }

        if (!emitted) {
            return true;
        }
        ulong sourced_packet[EVENT_WORDS];
        for (uint word = 0; word < EVENT_WORDS; ++word) {
            sourced_packet[word] = event[word];
        }
        sourced_packet[E_PHASE] = 1;
        if (!source_queue_insert(node, sourced_packet, error, queue_meta, queue_records) ||
            !record_sourced(
                node, event, error, params, summary, observation_meta, observed)) {
            return false;
        }
        if (node_state[node_base + N_SERVICE_VALID] == 0 &&
            node_state[node_base + N_READY_PENDING] == 0) {
            node_state[node_base + N_READY_PENDING] = 1;
            return emit_child(
                node, event, node, TX_READY, event[E_TIME], event, error, params,
                node_state, fel_meta, fel_records, remote_meta, remote_staging,
                stream_state, stream_records,
                tcp_state);
        }
        return true;
    }

    if (kind == PACKET_ARRIVAL) {
        if (role != HOST) {
            set_semantic_error(error, 3, node);
            return false;
        }
        ulong generator_base = event[PK_FLOW] * GENERATOR_WORDS;
        bool owns_generator =
            event[PK_FLOW] < params[P_FLOW_COUNT] &&
            generators[generator_base] != 0 &&
            generators[generator_base + 1] == node;
        if (owns_generator && generators[generator_base + G_KIND] == 1) {
            if (generators[generator_base + G_STATUS] != 0 ||
                generators[generator_base + G_DEPARTURE] != event[E_TIME] ||
                generators[generator_base + G_PAYLOAD] != event[PK_ID] ||
                (event[PK_KIND] & PK_KIND_MASK) != TCP_DATA_PACKET ||
                event[PK_META_0] != generators[generator_base + G_TCP_NEXT] ||
                event[PK_META_1] != event[E_TIME] || event[PK_META_2] != 0) {
                set_semantic_error(error, 52, node);
                return false;
            }
            if (generators[generator_base + G_PACKETS] == NONE ||
                generators[generator_base + G_BYTES] > NONE - event[PK_SIZE] ||
                generators[generator_base + G_TCP_NEXT] > NONE - event[PK_SIZE] ||
                generators[generator_base + G_TCP_FLIGHT] > NONE - event[PK_SIZE] ||
                node_state[node_base + N_COUNTER_0] == NONE) {
                set_semantic_error(error, 53, node);
                return false;
            }
            generators[generator_base + G_PACKETS] += 1;
            generators[generator_base + G_BYTES] += event[PK_SIZE];
            generators[generator_base + G_TCP_NEXT] += event[PK_SIZE];
            generators[generator_base + G_TCP_FLIGHT] += event[PK_SIZE];
            generators[generator_base + G_TCP_LAST_ATTEMPT] = event[PK_ID];
            generators[generator_base + G_OUTSTANDING] =
                generators[generator_base + G_TCP_FLIGHT];
            generators[generator_base + G_UNACKNOWLEDGED] =
                generators[generator_base + G_TCP_FLIGHT];
            generators[generator_base + G_STATUS] = 1;
            node_state[node_base + N_COUNTER_0] += 1;
            ulong sourced_packet[EVENT_WORDS];
            for (uint word = 0; word < EVENT_WORDS; ++word) {
                sourced_packet[word] = event[word];
            }
            sourced_packet[E_PHASE] = 1;
            if (!tcp_ledger_insert(event[PK_FLOW], event, error, params, tcp_state) ||
                !source_queue_insert(node, sourced_packet, error, queue_meta, queue_records) ||
                !record_sourced(node, event, error, params, summary, observation_meta, observed)) {
                return false;
            }
            return prepare_tcp_attempts(
                node, event[PK_FLOW], event, false, 0, true, false, error, params, node_state,
                generators, fel_meta, fel_records, queue_meta, queue_records, remote_meta,
                remote_staging, stream_state, stream_records, summary, observation_meta,
                observed, tcp_state
            );
        }
        if (owns_generator) {
            if (
                generators[generator_base + 4] != 0 ||
                generators[generator_base + 5] != event[E_TIME] ||
                generators[generator_base + 6] != event[PK_ID]
            ) {
                set_semantic_error(error, 4, node);
                return false;
            }
            if (
                generators[generator_base + 2] == NONE ||
                generators[generator_base + 3] > NONE - event[PK_SIZE]
            ) {
                set_semantic_error(error, 5, node);
                return false;
            }
            generators[generator_base + 2] += 1;
            generators[generator_base + 3] += event[PK_SIZE];

            ulong candidate = 0;
            bool semantic_next;
            if (generators[generator_base + 15] == 0) {
                semantic_next =
                    generators[generator_base + 3] < generators[generator_base + 16];
                if (
                    semantic_next &&
                    !checked_add(
                        event[E_TIME],
                        generators[generator_base + 13],
                        candidate
                    )
                ) {
                    set_semantic_error(error, 6, node);
                    return false;
                }
            } else {
                ulong end;
                if (!checked_add(
                    generators[generator_base + 12],
                    generators[generator_base + 16],
                    end
                )) {
                    set_semantic_error(error, 6, node);
                    return false;
                }
                if (!checked_add(
                    event[E_TIME],
                    generators[generator_base + 13],
                    candidate
                )) {
                    set_semantic_error(error, 6, node);
                    return false;
                }
                semantic_next = candidate < end;
            }
            if (semantic_next && candidate <= params[P_STOP_TIME]) {
                ulong sequence = node_state[node_base + N_NEXT_PAYLOAD];
                if (sequence == NONE || sequence > (NONE - node) / params[P_NODE_COUNT]) {
                    set_semantic_error(error, 7, node);
                    return false;
                }
                ulong payload = sequence * params[P_NODE_COUNT] + node;
                node_state[node_base + N_NEXT_PAYLOAD] = sequence + 1;
                generators[generator_base + 4] = 0;
                generators[generator_base + 5] = candidate;
                generators[generator_base + 6] = payload;
                ulong next_packet[EVENT_WORDS];
                for (uint word = 0; word < EVENT_WORDS; ++word) {
                    next_packet[word] = 0;
                }
                next_packet[PK_ID] = payload;
                next_packet[PK_FLOW] = event[PK_FLOW];
                next_packet[PK_SIZE] = generators[generator_base + 14];
                next_packet[PK_KIND] = DATA_PACKET;
                if (!emit_child(
                    node,
                    event,
                    node,
                    PACKET_ARRIVAL,
                    candidate,
                    next_packet,
                    error,
                    params,
                    node_state,
                    fel_meta,
                    fel_records,
                    remote_meta,
                    remote_staging,
                    stream_state,
                    stream_records,
                    tcp_state)) {
                    return false;
                }
            } else {
                generators[generator_base + 4] = semantic_next ? 3 : 2;
                if (semantic_next) {
                    generators[generator_base + 5] = candidate;
                }
            }
        }
        if (node_state[node_base + N_COUNTER_0] == NONE) {
            set_semantic_error(error, 8, node);
            return false;
        }
        node_state[node_base + N_COUNTER_0] += 1;
        ulong sourced_packet[EVENT_WORDS];
        for (uint word = 0; word < EVENT_WORDS; ++word) {
            sourced_packet[word] = event[word];
        }
        sourced_packet[E_PHASE] = 1;
        if (!source_queue_insert(
            node,
            sourced_packet,
            error,
            queue_meta,
            queue_records
        )) {
            return false;
        }
        if (!record_sourced(
            node,
            event,
            error,
            params,
            summary,
            observation_meta,
            observed
        )) {
            return false;
        }
        if (
            node_state[node_base + N_SERVICE_VALID] == 0 &&
            node_state[node_base + N_READY_PENDING] == 0
        ) {
            node_state[node_base + N_READY_PENDING] = 1;
            if (!emit_child(
                node,
                event,
                node,
                TX_READY,
                event[E_TIME],
                event,
                error,
                params,
                node_state,
                fel_meta,
                fel_records,
                remote_meta,
                remote_staging,
                stream_state,
                stream_records,
                tcp_state)) {
                return false;
            }
        }
        return true;
    }

    if (kind == TX_READY) {
        node_state[node_base + N_READY_PENDING] = 0;
        if (node_state[node_base + N_SERVICE_VALID] != 0) {
            set_semantic_error(error, 9, node);
            return false;
        }
        ulong selected[EVENT_WORDS];
        ulong selected_physical;
        ulong scheduler_base = node * SCHEDULER_NODE_WORDS;
        ulong scheduler_kind = role == SWITCH
            ? scheduler_state[scheduler_base + S_KIND]
            : SCHED_FIFO;
        ulong selected_position = 0;
        bool selected_position_valid = true;
        if (scheduler_kind == SCHED_DRR) {
            selected_position_valid = drr_select_position(
                node,
                error,
                queue_meta,
                queue_records,
                scheduler_state,
                selected_position
            );
        } else if (scheduler_kind == SCHED_WRR) {
            selected_position_valid = wrr_select_position(
                node,
                error,
                queue_meta,
                queue_records,
                scheduler_state,
                selected_position
            );
        }
        if (!selected_position_valid) {
            return false;
        }
        bool removed = scheduler_kind == SCHED_DRR || scheduler_kind == SCHED_WRR
            ? queue_remove_at(
                node,
                selected_position,
                queue_meta,
                queue_records,
                selected,
                selected_physical
            )
            : queue_pop(node, queue_meta, queue_records, selected, selected_physical);
        if (!removed) {
            if (scheduler_kind == SCHED_DRR || scheduler_kind == SCHED_WRR) {
                set_semantic_error(error, 35, node);
                return false;
            }
            return true;
        }
        if (role == SWITCH) {
            ulong queue_base = node * QUEUE_META_WORDS;
            ulong queued_bytes = queue_meta[queue_base + 4];
            if (queued_bytes < selected[PK_SIZE]) {
                set_semantic_error(error, 60, node);
                return false;
            }
            queue_meta[queue_base + 4] = queued_bytes - selected[PK_SIZE];
        }
        if (role == SWITCH && scheduler_kind == SCHED_WFQ) {
            ulong tag_offset = scheduler_state[scheduler_base + S_QUEUE_TAG_OFFSET];
            rational_copy(
                scheduler_state,
                tag_offset + selected_physical * RATIONAL_WORDS,
                scheduler_base + S_IN_SERVICE_TAG
            );
            rational_zero(
                scheduler_state,
                tag_offset + selected_physical * RATIONAL_WORDS
            );
        }
        node_state[node_base + N_SERVICE_VALID] = 1;
        copy_thread_to_device(selected, in_service, node);
        ulong egress = node_state[node_base + N_EGRESS];
        if (egress == NONE || egress >= params[P_LINK_COUNT]) {
            set_semantic_error(error, 10, node);
            return false;
        }
        ulong link_base = egress * LINK_WORDS;
        if (links[link_base] != node) {
            set_semantic_error(error, 11, node);
            return false;
        }
        ulong serialization;
        if (!serialization_ns(selected[PK_SIZE], links[link_base + 2], serialization)) {
            set_semantic_error(error, 12, node);
            return false;
        }
        ulong departure_time;
        ulong arrival_time;
        if (
            !checked_add(event[E_TIME], serialization, departure_time) ||
            !checked_add(departure_time, links[link_base + 3], arrival_time)
        ) {
            set_semantic_error(error, 13, node);
            return false;
        }
        ulong target;
        if (!packet_remote_target(selected, egress, flows, routes, links, target)) {
            set_semantic_error(error, 14, node);
            return false;
        }
        if (!emit_child(
            node,
            event,
            node,
            TX_COMPLETE,
            departure_time,
            selected,
            error,
            params,
            node_state,
            fel_meta,
            fel_records,
            remote_meta,
            remote_staging,
            stream_state,
            stream_records,
            tcp_state)) {
            return false;
        }
        return emit_child(
            node,
            event,
            target,
            REMOTE_ARRIVAL,
            arrival_time,
            selected,
            error,
            params,
            node_state,
            fel_meta,
            fel_records,
            remote_meta,
            remote_staging,
            stream_state,
            stream_records,
            tcp_state);
    }

    if (kind == TX_COMPLETE) {
        if (
            node_state[node_base + N_SERVICE_VALID] == 0 ||
            in_service[node * EVENT_WORDS + PK_ID] != event[PK_ID]
        ) {
            set_semantic_error(error, 15, node);
            return false;
        }
        ulong scheduler_base = node * SCHEDULER_NODE_WORDS;
        if (role == SWITCH && scheduler_state[scheduler_base + S_KIND] == SCHED_WFQ) {
            ulong egress = node_state[node_base + N_EGRESS];
            if (
                egress == NONE ||
                egress >= params[P_LINK_COUNT] ||
                !wfq_complete(
                    node,
                    event,
                    links[egress * LINK_WORDS + 2],
                    error,
                    scheduler_state
                )
            ) {
                return false;
            }
        }
        node_state[node_base + N_SERVICE_VALID] = 0;
        uint departure_counter = role == HOST ? N_COUNTER_1 : N_COUNTER_2;
        if (node_state[node_base + departure_counter] == NONE) {
            set_semantic_error(error, 16, node);
            return false;
        }
        node_state[node_base + departure_counter] += 1;
        if (!record_departure(
            node,
            event,
            error,
            params,
            summary,
            observation_meta,
            observed,
            departures
        )) {
            return false;
        }
        if (
            queue_meta[node * QUEUE_META_WORDS + 3] != 0 &&
            node_state[node_base + N_READY_PENDING] == 0
        ) {
            ulong next[EVENT_WORDS];
            if (!queue_front(node, queue_meta, queue_records, next)) {
                set_semantic_error(error, 17, node);
                return false;
            }
            node_state[node_base + N_READY_PENDING] = 1;
            return emit_child(
                node,
                event,
                node,
                TX_READY,
                event[E_TIME],
                next,
                error,
                params,
                node_state,
                fel_meta,
                fel_records,
                remote_meta,
                remote_staging,
                stream_state,
                stream_records,
                tcp_state);
        }
        return true;
    }

    if (kind == REMOTE_ARRIVAL && role == SWITCH) {
        if (node_state[node_base + N_COUNTER_0] == NONE) {
            set_semantic_error(error, 18, node);
            return false;
        }
        node_state[node_base + N_COUNTER_0] += 1;
        ulong egress;
        if (!packet_egress(node, event, flows, routes, links, egress)) {
            set_semantic_error(error, 19, node);
            return false;
        }
        if (egress != node_state[node_base + N_EGRESS]) {
            set_semantic_error(error, 20, node);
            return false;
        }
        ulong semantic_capacity = node_state[node_base + N_SEMANTIC_QUEUE_CAPACITY];
        ulong scheduler_base = node * SCHEDULER_NODE_WORDS;
        ulong admission;
        if (!switch_admission_action(
            node,
            event,
            semantic_capacity,
            error,
            queue_meta,
            scheduler_state,
            admission
        )) {
            return false;
        }
        if (admission == 2) {
            if (node_state[node_base + N_COUNTER_1] == NONE) {
                set_semantic_error(error, 21, node);
                return false;
            }
            node_state[node_base + N_COUNTER_1] += 1;
            return record_arrival(
                node,
                event,
                1,
                error,
                params,
                summary,
                observation_meta,
                observed,
                arrivals
            );
        }
        ulong packet_kind = event[PK_KIND] & PK_KIND_MASK;
        if (admission == 1 &&
            (packet_kind == DATA_PACKET || packet_kind == TCP_DATA_PACKET)) {
            event[PK_KIND] |= PK_ECN_FLAG;
        }
        ulong scheduler_kind = scheduler_state[scheduler_base + S_KIND];
        bool inserted;
        if (scheduler_kind == SCHED_FIFO) {
            inserted = queue_push(node, event, error, queue_meta, queue_records);
        } else if (scheduler_kind == SCHED_SP) {
            inserted = sp_queue_insert(
                node,
                event,
                error,
                queue_meta,
                queue_records,
                scheduler_state
            );
        } else if (scheduler_kind == SCHED_WFQ) {
            inserted = wfq_enqueue(
                node,
                event,
                links[egress * LINK_WORDS + 2],
                error,
                queue_meta,
                queue_records,
                scheduler_state
            );
        } else if (scheduler_kind == SCHED_DRR || scheduler_kind == SCHED_WRR) {
            inserted = queue_push(node, event, error, queue_meta, queue_records);
        } else {
            set_semantic_error(error, 32, node);
            return false;
        }
        if (!inserted) {
            return false;
        }
        ulong queue_base = node * QUEUE_META_WORDS;
        ulong queued_bytes = queue_meta[queue_base + 4];
        if (queued_bytes > NONE - event[PK_SIZE]) {
            set_semantic_error(error, 60, node);
            return false;
        }
        queue_meta[queue_base + 4] = queued_bytes + event[PK_SIZE];
        if (!record_arrival(
            node,
            event,
            0,
            error,
            params,
            summary,
            observation_meta,
            observed,
            arrivals
        )) {
            return false;
        }
        if (
            egress != NONE &&
            node_state[node_base + N_SERVICE_VALID] == 0 &&
            node_state[node_base + N_READY_PENDING] == 0
        ) {
            node_state[node_base + N_READY_PENDING] = 1;
            return emit_child(
                node,
                event,
                node,
                TX_READY,
                event[E_TIME],
                event,
                error,
                params,
                node_state,
                fel_meta,
                fel_records,
                remote_meta,
                remote_staging,
                stream_state,
                stream_records,
                tcp_state);
        }
        return true;
    }

    if (kind == REMOTE_ARRIVAL && role == HOST &&
        (event[PK_KIND] & PK_KIND_MASK) == TCP_DATA_PACKET) {
        ulong flow = event[PK_FLOW];
        ulong flow_base = flow * FLOW_WORDS;
        ulong receiver = params[P_TCP_RECEIVER_OFFSET] + flow * TCP_RECEIVER_WORDS;
        if (flow >= params[P_FLOW_COUNT] || flows[flow_base + 1] != node ||
            tcp_state[receiver] == 0 || tcp_state[receiver + 1] != node ||
            event[PK_META_0] > NONE - event[PK_SIZE]) {
            set_semantic_error(error, 54, node);
            return false;
        }
        if (!tcp_receive_range(
            flow, event[PK_META_0], event[PK_META_0] + event[PK_SIZE], error, params, tcp_state
        )) {
            return false;
        }
        ulong ack_payload;
        if (!allocate_tcp_payload(node, node_state, params, ack_payload) ||
            node_state[node_base + N_COUNTER_2] == NONE ||
            node_state[node_base + N_COUNTER_0] == NONE) {
            set_semantic_error(error, 55, node);
            return false;
        }
        node_state[node_base + N_COUNTER_2] += 1;
        node_state[node_base + N_COUNTER_0] += 1;
        if (!record_arrival(
            node, event, 2, error, params, summary, observation_meta, observed, arrivals
        )) {
            return false;
        }
        ulong ack[EVENT_WORDS];
        packet_clear(ack);
        ack[E_TIME] = event[E_TIME];
        ack[E_PHASE] = 1;
        ack[PK_ID] = ack_payload;
        ack[PK_FLOW] = flow;
        ack[PK_SIZE] = tcp_state[receiver + 2];
        ack[PK_KIND] = TCP_ACK_PACKET;
        ack[PK_META_0] = tcp_state[receiver + 3];
        ack[PK_META_1] = event[PK_SIZE];
        ack[PK_META_2] = event[PK_META_1];
        if (!source_queue_insert(node, ack, error, queue_meta, queue_records) ||
            !record_sourced(node, ack, error, params, summary, observation_meta, observed)) {
            return false;
        }
        if (node_state[node_base + N_SERVICE_VALID] == 0 &&
            node_state[node_base + N_READY_PENDING] == 0) {
            node_state[node_base + N_READY_PENDING] = 1;
            return emit_child(
                node, event, node, TX_READY, event[E_TIME], ack, error, params, node_state,
                fel_meta, fel_records, remote_meta, remote_staging, stream_state, stream_records,
                tcp_state);
        }
        return true;
    }

    if (kind == REMOTE_ARRIVAL && role == HOST &&
        (event[PK_KIND] & PK_KIND_MASK) == TCP_ACK_PACKET) {
        ulong flow = event[PK_FLOW];
        ulong flow_base = flow * FLOW_WORDS;
        ulong generator = flow * GENERATOR_WORDS;
        if (flow >= params[P_FLOW_COUNT] || flows[flow_base] != node ||
            generators[generator + G_VALID] == 0 ||
            generators[generator + G_OWNER] != node || generators[generator + G_KIND] != 1) {
            set_semantic_error(error, 56, node);
            return false;
        }
        if (!record_arrival(
            node, event, 3, error, params, summary, observation_meta, observed, arrivals
        ) || generators[generator + G_FEEDBACK] == NONE) {
            set_semantic_error(error, 57, node);
            return false;
        }
        generators[generator + G_FEEDBACK] += 1;
        ulong acknowledgment = min(event[PK_META_0], generators[generator + G_TCP_NEXT]);
        bool handled_ack = false;
        bool retransmit = false;
        ulong retransmit_sequence = 0;
        bool fill = false;
        bool acknowledged_new = false;
        ulong flight_before = generators[generator + G_TCP_FLIGHT];

        if (acknowledgment > generators[generator + G_TCP_HIGHEST_ACK]) {
            ulong acknowledged_bytes =
                acknowledgment - generators[generator + G_TCP_HIGHEST_ACK];
            ulong rtt_sample = event[E_TIME] >= event[PK_META_2]
                ? max(event[E_TIME] - event[PK_META_2], 1ul) : 1;
            generators[generator + G_TCP_RTO] = update_rto(
                generators[generator + G_TCP_SRTT],
                generators[generator + G_TCP_RTTVAR], rtt_sample
            );
            controller_new_ack(
                generators + generator + G_CONTROL, acknowledged_bytes, event[E_TIME],
                rtt_sample, acknowledgment
            );
            generators[generator + G_TCP_FLIGHT] =
                flight_before > acknowledged_bytes ? flight_before - acknowledged_bytes : 0;
            generators[generator + G_TCP_HIGHEST_ACK] = acknowledgment;
            generators[generator + G_TCP_DUP_ACKS] = 0;
            if (generators[generator + G_TCP_TIMER_ACTIVE] != 0) {
                if (!heap_remove_timer(
                    node,
                    flow,
                    generators[generator + G_TCP_TIMER_ATTEMPT],
                    generators[generator + G_TCP_TIMER_DEADLINE],
                    error,
                    params,
                    fel_meta,
                    fel_records,
                    stream_state,
                    tcp_state
                )) {
                    return false;
                }
                generators[generator + G_TCP_TIMER_ACTIVE] = 0;
            }
            if (generators[generator + G_CONTROL + CTL_PHASE] == TCP_FAST_RECOVERY &&
                acknowledgment < generators[generator + G_TCP_RECOVERY_HIGH]) {
                retransmit = true;
                retransmit_sequence = acknowledgment;
            }
            fill = acknowledgment < generators[generator + G_TCP_TOTAL];
            acknowledged_new = true;
            handled_ack = true;
        } else if (acknowledgment == generators[generator + G_TCP_HIGHEST_ACK] &&
            acknowledgment < generators[generator + G_TCP_TOTAL] && flight_before != 0) {
            ulong recovery_high = generators[generator + G_TCP_NEXT];
            bool fast = controller_duplicate_ack(
                generators + generator + G_CONTROL, flight_before, event[E_TIME]
            );
            generators[generator + G_TCP_DUP_ACKS] =
                generators[generator + G_CONTROL + CTL_DUP_ACKS];
            if (fast) {
                generators[generator + G_TCP_RECOVERY_HIGH] = recovery_high;
                generators[generator + G_CONTROL + CTL_RECOVERY_HIGH] = recovery_high;
                if (generators[generator + G_TCP_TIMER_ACTIVE] != 0) {
                    if (!heap_remove_timer(
                        node,
                        flow,
                        generators[generator + G_TCP_TIMER_ATTEMPT],
                        generators[generator + G_TCP_TIMER_DEADLINE],
                        error,
                        params,
                        fel_meta,
                        fel_records,
                        stream_state,
                        tcp_state
                    )) {
                        return false;
                    }
                    generators[generator + G_TCP_TIMER_ACTIVE] = 0;
                }
                retransmit = true;
                retransmit_sequence = acknowledgment;
            } else if (generators[generator + G_TCP_DUP_ACKS] > 3) {
                fill = true;
            }
            handled_ack = true;
        }

        generators[generator + G_OUTSTANDING] = generators[generator + G_TCP_FLIGHT];
        generators[generator + G_UNACKNOWLEDGED] = generators[generator + G_TCP_FLIGHT];
        if (acknowledged_new && !tcp_ledger_acknowledge(
            flow, acknowledgment, error, params, tcp_state
        )) {
            return false;
        }
        if (!handled_ack) {
            return true;
        }
        bool scheduled = generators[generator + G_STATUS] == 0;
        if (scheduled && !retransmit) {
            return true;
        }
        return prepare_tcp_attempts(
            node, flow, event, retransmit, retransmit_sequence, fill && !scheduled, scheduled,
            error, params, node_state, generators, fel_meta, fel_records, queue_meta,
            queue_records, remote_meta, remote_staging, stream_state, stream_records, summary,
            observation_meta, observed, tcp_state
        );
    }

    if (kind == RETRANSMISSION_TIMEOUT && role == HOST) {
        ulong flow = event[PK_FLOW];
        ulong generator = flow * GENERATOR_WORDS;
        bool armed = flow < params[P_FLOW_COUNT] && generators[generator + G_VALID] != 0 &&
            generators[generator + G_OWNER] == node && generators[generator + G_KIND] == 1 &&
            generators[generator + G_TCP_TIMER_ACTIVE] != 0 &&
            generators[generator + G_TCP_TIMER_ATTEMPT] == event[E_PAYLOAD] &&
            generators[generator + G_TCP_TIMER_DEADLINE] == event[E_TIME];
        // Live-state contract (T20g item 2): the flow's owned heap record is live exactly when it
        // matches the armed timer. Eager removal makes the two conditions equivalent, so any
        // disagreement is a corrupted slot index rather than a modelling condition. A record owned
        // by no flow is legacy import residue, which stays lazily recognized.
        bool owned = popped_timer_owner != NONE && popped_timer_owner == flow;
        if (armed != owned) {
            set_semantic_error(error, 61, node);
            return false;
        }
        if (!armed) {
            return true;
        }
        generators[generator + G_TCP_TIMER_ACTIVE] = 0;
        ulong flight = generators[generator + G_TCP_FLIGHT];
        controller_timeout(generators + generator + G_CONTROL, flight);
        generators[generator + G_TCP_RTO] = min(
            saturating_mul_u64(generators[generator + G_TCP_TIMER_RTO], 2), TCP_MAX_RTO
        );
        return prepare_tcp_attempts(
            node, flow, event, true, generators[generator + G_TCP_HIGHEST_ACK], false, false,
            error, params, node_state, generators, fel_meta, fel_records, queue_meta,
            queue_records, remote_meta, remote_staging, stream_state, stream_records, summary,
            observation_meta, observed, tcp_state
        );
    }

    if (kind == REMOTE_ARRIVAL && role == HOST) {
        ulong flow_base = event[PK_FLOW] * FLOW_WORDS;
        ulong disposition = 2;
        if (
            (event[PK_KIND] & PK_KIND_MASK) == FEEDBACK_PACKET &&
            event[PK_FLOW] < params[P_FLOW_COUNT] &&
            generators[event[PK_FLOW] * GENERATOR_WORDS] != 0 &&
            generators[event[PK_FLOW] * GENERATOR_WORDS + 1] == node
        ) {
            ulong generator_base = event[PK_FLOW] * GENERATOR_WORDS;
            if (generators[generator_base + 8] == NONE) {
                set_semantic_error(error, 22, node);
                return false;
            }
            generators[generator_base + 8] += 1;
            disposition = 3;
        } else {
            ulong expected =
                ((event[PK_KIND] & PK_KIND_MASK) == DATA_PACKET ||
                 (event[PK_KIND] & PK_KIND_MASK) == TCP_DATA_PACKET)
                    ? flows[flow_base + 1] : flows[flow_base];
            if (expected != node) {
                set_semantic_error(error, 23, node);
                return false;
            }
            if (node_state[node_base + N_COUNTER_2] == NONE) {
                set_semantic_error(error, 24, node);
                return false;
            }
            node_state[node_base + N_COUNTER_2] += 1;
        }
        return record_arrival(
            node,
            event,
            disposition,
            error,
            params,
            summary,
            observation_meta,
            observed,
            arrivals
        );
    }

    set_semantic_error(error, 25, node);
    return false;
}

// T21 fix 2 — per-round FEL root cache accessors. Transliterated word for word in
// `metal_kernels.metal`; the two must stay in lockstep or the backends diverge.
//
// CORRECTNESS. `fel_peek`'s read set is the LP stream metadata, that LP's active-stream entries,
// the selected stream's header, `fel_meta`, `fel_records` and `stream_records`. Between
// `days_horizon`'s evaluation and `days_round_prepare`'s two reads the only device writes are
// `days_horizon`'s control words and this cache, and `days_round_prepare`'s own `lp_state` error
// words, `remote_meta[node * META_WORDS + 3]` and the channel BATCH quadruple at
// `P_CHANNEL_BATCH_OFFSET + channel * CHANNEL_BATCH_WORDS`. None of those is in the read set — the
// batch quadruple is a different region of `stream_state` from the per-channel stream headers
// `fel_peek` reads — so the cached answer is the answer a recomputed query would return.
static __device__ inline ulong round_scratch_cache(const ulong *params) {
    return params[P_ROUND_SCRATCH_OFFSET];
}

static __device__ inline void store_fel_root(
    ulong *stream_state,
    const ulong *params,
    ulong node,
    bool present,
    ulong time
) {
    ulong slot = round_scratch_cache(params) + node * ROUND_SCRATCH_CACHE_WORDS;
    stream_state[slot] = present ? time : 0;
    stream_state[slot + 1] = present ? 1 : 0;
}

static __device__ inline bool load_fel_root(
    const ulong *stream_state,
    const ulong *params,
    ulong node,
    ulong &time
) {
    ulong slot = round_scratch_cache(params) + node * ROUND_SCRATCH_CACHE_WORDS;
    time = stream_state[slot];
    return stream_state[slot + 1] != 0;
}

// T21 fix 1 — the per-block reduction partials, `CONTROL_SWEEP_BLOCKS * ROUND_SCRATCH_PARTIAL_WORDS`
// words appended after the FEL-root cache in the same scratch region.
//
// A sweep block writes ONLY its own row, and only from lane 0 after that block's own
// `__syncthreads()`. A combine reads rows written by a PREVIOUS dispatch. There is therefore no
// cross-block ordering to establish: the dispatch boundary establishes it.
static __device__ inline ulong round_scratch_partials(const ulong *params) {
    return params[P_ROUND_SCRATCH_OFFSET] +
        params[P_NODE_COUNT] * ROUND_SCRATCH_CACHE_WORDS;
}

static __device__ inline void store_round_partial(
    ulong *stream_state,
    const ulong *params,
    ulong block,
    ulong slot,
    ulong value
) {
    stream_state[
        round_scratch_partials(params) +
        block * ROUND_SCRATCH_PARTIAL_WORDS +
        slot
    ] = value;
}

static __device__ inline ulong load_round_partial(
    const ulong *stream_state,
    const ulong *params,
    ulong block,
    ulong slot
) {
    return stream_state[
        round_scratch_partials(params) +
        block * ROUND_SCRATCH_PARTIAL_WORDS +
        slot
    ];
}

// T21 fix 1 — the horizon's Θ(N) FEL-root sweep, on the whole grid.
//
// This is the most expensive of the five re-gridded phases: it is the one that pays `fel_peek` per
// LP, which `evidence/P12/perround-upperbound.md` §1.5.2 priced at ≈208 useful bytes per LP.
//
// The reduction is `min` under a validity flag. `min` is commutative, associative and idempotent,
// with identity "invalid", so the reduced value is the same for EVERY partition of the LPs — which
// is why moving from one block's 1,024-lane stride to a 128-block grid-stride cannot move a
// complete-state byte. Each block publishes `{minimum, validity}`; `days_horizon` combines them in
// the next dispatch.
extern "C" __global__ void days_horizon_sweep(DAYS_BUFFERS) {
    uint lane = threadIdx.x;
    __shared__ ulong minima[1024];
    __shared__ uint validity[1024];
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 0 ||
        control[C_ROUNDS] >= params[P_ROUND_CAPACITY]
    ) {
        return;
    }
    ulong first = ulong(blockIdx.x) * ulong(blockDim.x) + ulong(threadIdx.x);
    ulong stride = ulong(gridDim.x) * ulong(blockDim.x);
    ulong minimum = 0;
    bool valid = false;
    for (ulong node = first; node < params[P_NODE_COUNT]; node += stride) {
        ulong candidate;
        bool present = fel_root_time(
            node,
            params,
            fel_meta,
            fel_records,
            stream_state,
            stream_records,
            candidate
        );
        // T21 fix 2: the round's single evaluation of this node's FEL root.
        store_fel_root(stream_state, params, node, present, candidate);
        if (present && (!valid || candidate < minimum)) {
            minimum = candidate;
            valid = true;
        }
    }
    minima[lane] = minimum;
    validity[lane] = valid ? 1 : 0;
    __syncthreads();
    for (uint stride_lanes = 512; stride_lanes != 0; stride_lanes >>= 1) {
        if (lane < stride_lanes) {
            if (
                validity[lane + stride_lanes] != 0 &&
                (
                    validity[lane] == 0 ||
                    minima[lane + stride_lanes] < minima[lane]
                )
            ) {
                minima[lane] = minima[lane + stride_lanes];
                validity[lane] = 1;
            }
        }
        __syncthreads();
    }
    if (lane == 0) {
        store_round_partial(stream_state, params, blockIdx.x, 0, minima[0]);
        store_round_partial(stream_state, params, blockIdx.x, 1, ulong(validity[0]));
    }
}

// T21 fix 1 — the horizon combine. Reduces `days_horizon_sweep`'s per-block partials and keeps
// every control write, so the frontier and the exclusive horizon are still decided in one place.
//
// The guard below is `days_horizon_sweep`'s, word for word. Nothing between the two dispatches
// writes a word the guard reads — the sweep writes only the FEL cache and its own partial row —
// so both dispatches take the same branch, and the combine never reads partials the sweep skipped.
extern "C" __global__ void days_horizon(DAYS_BUFFERS) {
    uint lane = threadIdx.x;
    __shared__ ulong minima[1024];
    __shared__ uint validity[1024];
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 0 ||
        control[C_ROUNDS] >= params[P_ROUND_CAPACITY]
    ) {
        return;
    }
    ulong minimum = 0;
    bool valid = false;
    for (
        ulong block = lane;
        block < CONTROL_SWEEP_BLOCKS;
        block += 1024
    ) {
        if (load_round_partial(stream_state, params, block, 1) == 0) {
            continue;
        }
        ulong candidate = load_round_partial(stream_state, params, block, 0);
        if (!valid || candidate < minimum) {
            minimum = candidate;
            valid = true;
        }
    }
    minima[lane] = minimum;
    validity[lane] = valid ? 1 : 0;
    __syncthreads();
    for (uint stride = 512; stride != 0; stride >>= 1) {
        if (lane < stride) {
            if (
                validity[lane + stride] != 0 &&
                (
                    validity[lane] == 0 ||
                    minima[lane + stride] < minima[lane]
                )
            ) {
                minima[lane] = minima[lane + stride];
                validity[lane] = 1;
            }
        }
        __syncthreads();
    }
    if (lane != 0) {
        return;
    }
    minimum = minima[0];
    if (
        validity[0] == 0 ||
        (control[C_RUN_END_HI] == 0 && minimum >= control[C_RUN_END_LO])
    ) {
        control[C_DONE] = 1;
        return;
    }
    control[C_FRONTIER] = minimum;
    ulong horizon_lo = control[C_RUN_END_LO];
    ulong horizon_hi = control[C_RUN_END_HI];
    if (params[P_HAS_LOOKAHEAD] != 0) {
        ulong candidate_lo = minimum + params[P_LOOKAHEAD];
        ulong candidate_hi = candidate_lo < minimum ? 1 : 0;
        if (
            candidate_hi < horizon_hi ||
            (candidate_hi == horizon_hi && candidate_lo < horizon_lo)
        ) {
            horizon_lo = candidate_lo;
            horizon_hi = candidate_hi;
        }
    }
    control[C_HORIZON_LO] = horizon_lo;
    control[C_HORIZON_HI] = horizon_hi;
}

// T21 fix 1 — `days_round_prepare`'s Θ(N) + Θ(C) resets, on the whole grid.
//
// `evidence/P12/aterm-fixes.md` §3.4 item 2. These writes need NO cross-block communication and no
// reduction of any kind: each thread owns whole LPs and whole channels, every word is written by
// exactly one thread, and nothing here reads a word any other thread writes. They are also
// per-word-disjoint from the worklist compaction that used to run beside them — `L_FINISHED` is
// `lp_state` word 0, the reset writes words 2..6 — which is why splitting them out is inert.
//
// It runs immediately before `days_round_prepare` and replays that kernel's guard exactly. Nothing
// between the two dispatches writes a word the guard reads, so both dispatches take the same
// branch. It writes no control word, which is the property `t21_control_regrid.rs` asserts.
extern "C" __global__ void days_round_reset(DAYS_BUFFERS) {
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 0 ||
        control[C_ROUNDS] >= params[P_ROUND_CAPACITY]
    ) {
        return;
    }

    ulong first = ulong(blockIdx.x) * ulong(blockDim.x) + ulong(threadIdx.x);
    ulong stride = ulong(gridDim.x) * ulong(blockDim.x);
    if (params[P_STREAMS_ENABLED] != 0) {
        for (
            ulong channel = first;
            channel < params[P_CHANNEL_COUNT];
            channel += stride
        ) {
            ulong batch =
                params[P_CHANNEL_BATCH_OFFSET] +
                channel * CHANNEL_BATCH_WORDS;
            stream_state[batch] = 0;
            stream_state[batch + 1] = NONE;
            stream_state[batch + 2] = NONE;
            stream_state[batch + 3] = 0;
        }
    }
    for (ulong node = first; node < params[P_NODE_COUNT]; node += stride) {
        ulong state = node * LP_STATE_WORDS;
        lp_state[state + L_ERROR] = 0;
        lp_state[state + L_ERROR_ARENA] = 0;
        lp_state[state + L_ERROR_NODE] = NONE;
        lp_state[state + L_ERROR_CAPACITY] = 0;
        lp_state[state + L_ERROR_DEMAND] = 0;
        remote_meta[node * META_WORDS + 3] = 0;
    }
}

// T32 profile-only decomposition of `days_round_prepare`.
//
// These four kernels are loaded and launched only by `run_profiled_with_observations`. The
// production graph continues to launch the single `days_round_prepare` below. `prepare_profile`
// has 2 * 1024 words: per-lane counts followed by the inclusive prefix. The dispatch boundaries
// are the only cross-block (here, cross-kernel) synchronization added by this diagnostic path.
extern "C" __global__ void days_round_prepare_count_profile(
    DAYS_BUFFERS,
    ulong *prepare_profile
) {
    uint lane = threadIdx.x;
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 0 ||
        control[C_ROUNDS] >= params[P_ROUND_CAPACITY]
    ) {
        return;
    }

    ulong nodes = params[P_NODE_COUNT];
    ulong chunk = nodes / 1024;
    ulong remainder = nodes % 1024;
    ulong start = ulong(lane) * chunk + min(ulong(lane), remainder);
    ulong end = start + chunk + (ulong(lane) < remainder ? 1 : 0);
    ulong local_count = 0;
    for (ulong node = start; node < end; ++node) {
        ulong time;
        if (
            load_fel_root(stream_state, params, node, time) &&
            before_horizon(time, control)
        ) {
            local_count += 1;
        }
    }
    prepare_profile[lane] = local_count;
}

extern "C" __global__ void days_round_prepare_prefix_profile(
    DAYS_BUFFERS,
    ulong *prepare_profile
) {
    uint lane = threadIdx.x;
    __shared__ ulong counts[1024];
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 0 ||
        control[C_ROUNDS] >= params[P_ROUND_CAPACITY]
    ) {
        return;
    }

    counts[lane] = prepare_profile[lane];
    __syncthreads();
    for (uint offset = 1; offset < 1024; offset <<= 1) {
        ulong addend = lane >= offset ? counts[lane - offset] : 0;
        __syncthreads();
        counts[lane] += addend;
        __syncthreads();
    }
    prepare_profile[1024 + lane] = counts[lane];
}

extern "C" __global__ void days_round_prepare_write_profile(
    DAYS_BUFFERS,
    ulong *prepare_profile
) {
    uint lane = threadIdx.x;
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 0 ||
        control[C_ROUNDS] >= params[P_ROUND_CAPACITY]
    ) {
        return;
    }

    ulong nodes = params[P_NODE_COUNT];
    ulong chunk = nodes / 1024;
    ulong remainder = nodes % 1024;
    ulong start = ulong(lane) * chunk + min(ulong(lane), remainder);
    ulong end = start + chunk + (ulong(lane) < remainder ? 1 : 0);
    ulong local_count = prepare_profile[lane];
    ulong write = prepare_profile[1024 + lane] - local_count;
    for (ulong node = start; node < end; ++node) {
        ulong time;
        if (
            load_fel_root(stream_state, params, node, time) &&
            before_horizon(time, control)
        ) {
            if (write >= params[P_WORKLIST_CAPACITY]) {
                if (lane == 0) {
                    control[C_ERROR] = ERROR_CAPACITY;
                    control[C_ERROR_ARENA] = ARENA_WORKLIST;
                    control[C_ERROR_NODE] = NONE;
                    control[C_ERROR_CAPACITY] = params[P_WORKLIST_CAPACITY];
                    control[C_ERROR_DEMAND] = write + 1;
                }
                return;
            }
            worklist[write++] = node;
            lp_state[node * LP_STATE_WORDS + L_FINISHED] = 0;
        }
    }
}

extern "C" __global__ void days_round_prepare_combine_profile(
    DAYS_BUFFERS,
    ulong *prepare_profile
) {
    if (
        threadIdx.x != 0 ||
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 0 ||
        control[C_ROUNDS] >= params[P_ROUND_CAPACITY]
    ) {
        return;
    }
    control[C_ACTIVE] = prepare_profile[2047];
    control[C_OUTBOX] = 0;
    control[C_CONTINUATION] = 1;
}

extern "C" __global__ void days_round_prepare(DAYS_BUFFERS) {
    uint lane = threadIdx.x;
    __shared__ ulong counts[1024];
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 0 ||
        control[C_ROUNDS] >= params[P_ROUND_CAPACITY]
    ) {
        return;
    }

    ulong nodes = params[P_NODE_COUNT];
    ulong chunk = nodes / 1024;
    ulong remainder = nodes % 1024;
    ulong start = ulong(lane) * chunk + min(ulong(lane), remainder);
    ulong end = start + chunk + (ulong(lane) < remainder ? 1 : 0);
    ulong local_count = 0;
    for (ulong node = start; node < end; ++node) {
        ulong time;
        if (
            load_fel_root(stream_state, params, node, time) &&
            before_horizon(time, control)
        ) {
            local_count += 1;
        }
    }

    counts[lane] = local_count;
    __syncthreads();
    for (uint offset = 1; offset < 1024; offset <<= 1) {
        ulong addend = lane >= offset ? counts[lane - offset] : 0;
        __syncthreads();
        counts[lane] += addend;
        __syncthreads();
    }

    ulong write = counts[lane] - local_count;
    for (ulong node = start; node < end; ++node) {
        ulong time;
        if (
            load_fel_root(stream_state, params, node, time) &&
            before_horizon(time, control)
        ) {
            if (write >= params[P_WORKLIST_CAPACITY]) {
                if (lane == 0) {
                    control[C_ERROR] = ERROR_CAPACITY;
                    control[C_ERROR_ARENA] = ARENA_WORKLIST;
                    control[C_ERROR_NODE] = NONE;
                    control[C_ERROR_CAPACITY] = params[P_WORKLIST_CAPACITY];
                    control[C_ERROR_DEMAND] = write + 1;
                }
                return;
            }
            worklist[write++] = node;
            lp_state[node * LP_STATE_WORDS + L_FINISHED] = 0;
        }
    }
    __syncthreads();
    if (lane == 0) {
        control[C_ACTIVE] = counts[1023];
        control[C_OUTBOX] = 0;
        control[C_CONTINUATION] = 1;
    }
}

extern "C" __global__ __launch_bounds__(1024) void days_round(DAYS_BUFFERS) {
    uint active_index = blockIdx.x * blockDim.x + threadIdx.x;
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 1 ||
        active_index >= control[C_ACTIVE]
    ) {
        return;
    }
    ulong node = worklist[active_index];
    ulong *state = lp_state + node * LP_STATE_WORDS;
    if (state[L_FINISHED] != 0 || state[L_ERROR] != 0) {
        return;
    }

    ulong dispatch_transitions = 0;
    while (dispatch_transitions < params[P_TRANSITION_CAPACITY]) {
        ulong event[EVENT_WORDS];
        ulong selected_active;
        ulong selected_stream;
        if (!fel_peek(
            node,
            params,
            fel_meta,
            fel_records,
            stream_state,
            stream_records,
            event,
            selected_active,
            selected_stream
        ) || !before_horizon(event[E_TIME], control)) {
            state[L_FINISHED] = 1;
            return;
        }
        ulong popped_timer_owner;
        if (!fel_pop_selected(
            node,
            selected_active,
            selected_stream,
            params,
            fel_meta,
            fel_records,
            stream_state,
            stream_records,
            tcp_state,
            event,
            popped_timer_owner
        )) {
            set_semantic_error(state, 26, node);
            return;
        }
        if (!dispatch_event(
            node,
            event,
            popped_timer_owner,
            state,
            params,
            node_state,
            generators,
            flows,
            routes,
            links,
            fel_meta,
            fel_records,
            queue_meta,
            queue_records,
            in_service,
            scheduler_state,
            remote_meta,
            remote_staging,
            stream_state,
            stream_records,
            summary,
            observation_meta,
            observed,
            departures,
            arrivals,
            tcp_state
        )) {
            return;
        }
        if (state[L_TRANSITIONS] == NONE) {
            set_semantic_error(state, 27, node);
            return;
        }
        state[L_TRANSITIONS] += 1;
        dispatch_transitions += 1;
    }

}

// T32 read-only drain diagnostics. The production `days_round` entry above is intentionally
// untouched. This counterpart occupies the same dispatch position and geometry, executes the
// same semantic operations in the same order, and writes only a separate diagnostic buffer.
//
// Layout: one flag word per LP, then one disjoint histogram row per LP with a bin for each
// possible active-head count, then one emission counter per immutable outbound channel. An LP is
// owned by one canonical-worklist lane and every channel has exactly one source LP, so all writes
// below have one owner across the stream-ordered continuation dispatches. No atomic or device-wide
// synchronization participates in the observation.
constexpr ulong T32_FLAG_STREAMS_DISABLED = 1ul;
constexpr ulong T32_FLAG_ACTIVE_LAYOUT = 2ul;
constexpr ulong T32_FLAG_ACTIVE_COUNT = 4ul;
constexpr ulong T32_FLAG_HEAD_OVERFLOW = 8ul;
constexpr ulong T32_FLAG_REMOTE_COUNT = 16ul;
constexpr ulong T32_FLAG_STAGING_SLOT = 32ul;
constexpr ulong T32_FLAG_CHANNEL = 64ul;
constexpr ulong T32_FLAG_CHANNEL_OVERFLOW = 128ul;

__device__ __forceinline__ void t32_diagnostic_flag(
    ulong node,
    ulong flag,
    ulong *diagnostics
) {
    diagnostics[node] = diagnostics[node] | flag;
}

__device__ __forceinline__ bool t32_active_entry_capacity(
    const ulong *params,
    ulong &capacity
) {
    ulong active_base = params[P_LP_ACTIVE_IDS_OFFSET];
    ulong outbound_base = params[P_OUTBOUND_META_OFFSET];
    if (outbound_base < active_base) {
        return false;
    }
    ulong words = outbound_base - active_base;
    if (words % ACTIVE_STREAM_ENTRY_WORDS != 0) {
        return false;
    }
    capacity = words / ACTIVE_STREAM_ENTRY_WORDS;
    return true;
}

__device__ __forceinline__ void t32_record_head_visit(
    ulong node,
    ulong active_offset,
    ulong active_count,
    const ulong *params,
    const ulong *stream_state,
    ulong *diagnostics
) {
    ulong active_entry_capacity;
    ulong active_base = params[P_LP_ACTIVE_IDS_OFFSET];
    if (
        !t32_active_entry_capacity(params, active_entry_capacity) ||
        active_offset < active_base
    ) {
        t32_diagnostic_flag(node, T32_FLAG_ACTIVE_LAYOUT, diagnostics);
        return;
    }
    ulong relative = active_offset - active_base;
    if (relative % ACTIVE_STREAM_ENTRY_WORDS != 0) {
        t32_diagnostic_flag(node, T32_FLAG_ACTIVE_LAYOUT, diagnostics);
        return;
    }
    ulong row = relative / ACTIVE_STREAM_ENTRY_WORDS;
    ulong lp_meta = params[P_LP_STREAM_META_OFFSET] + node * LP_STREAM_META_WORDS;
    ulong declared = stream_state[lp_meta + 1];
    if (
        declared == NONE ||
        active_count == 0 ||
        active_count > declared + 1 ||
        row >= active_entry_capacity ||
        active_count > active_entry_capacity - row
    ) {
        t32_diagnostic_flag(node, T32_FLAG_ACTIVE_COUNT, diagnostics);
        return;
    }
    ulong counter = params[P_NODE_COUNT] + row + active_count - 1;
    ulong previous = diagnostics[counter];
    if (previous == NONE) {
        t32_diagnostic_flag(node, T32_FLAG_HEAD_OVERFLOW, diagnostics);
        return;
    }
    diagnostics[counter] = previous + 1;
}

__device__ __forceinline__ void t32_record_remote_emissions(
    ulong node,
    ulong before,
    ulong after,
    const ulong *params,
    const ulong *remote_meta,
    const ulong *stream_state,
    ulong *diagnostics
) {
    ulong active_entry_capacity;
    if (!t32_active_entry_capacity(params, active_entry_capacity)) {
        t32_diagnostic_flag(node, T32_FLAG_ACTIVE_LAYOUT, diagnostics);
        return;
    }
    if (after < before) {
        t32_diagnostic_flag(node, T32_FLAG_REMOTE_COUNT, diagnostics);
        return;
    }
    ulong channel_emissions_offset =
        params[P_NODE_COUNT] + active_entry_capacity;
    ulong staging = remote_meta[node * META_WORDS];
    for (ulong index = before; index < after; ++index) {
        if (staging > NONE - index) {
            t32_diagnostic_flag(node, T32_FLAG_STAGING_SLOT, diagnostics);
            return;
        }
        ulong staging_slot = staging + index;
        ulong channel =
            stream_state[params[P_STAGING_CHANNEL_OFFSET] + staging_slot];
        if (channel >= params[P_CHANNEL_COUNT]) {
            t32_diagnostic_flag(node, T32_FLAG_CHANNEL, diagnostics);
            continue;
        }
        ulong counter = channel_emissions_offset + channel;
        ulong previous = diagnostics[counter];
        if (previous == NONE) {
            t32_diagnostic_flag(node, T32_FLAG_CHANNEL_OVERFLOW, diagnostics);
            continue;
        }
        diagnostics[counter] = previous + 1;
    }
}

extern "C" __global__ __launch_bounds__(1024) void days_round_drain_profile(
    DAYS_BUFFERS, ulong *diagnostics
) {
    uint active_index = blockIdx.x * blockDim.x + threadIdx.x;
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 1 ||
        active_index >= control[C_ACTIVE]
    ) {
        return;
    }
    ulong node = worklist[active_index];
    ulong *state = lp_state + node * LP_STATE_WORDS;
    if (state[L_FINISHED] != 0 || state[L_ERROR] != 0) {
        return;
    }

    ulong dispatch_transitions = 0;
    while (dispatch_transitions < params[P_TRANSITION_CAPACITY]) {
        bool diagnostic_streams = params[P_STREAMS_ENABLED] != 0;
        ulong active_offset = 0;
        ulong active_count = 0;
        if (diagnostic_streams) {
            ulong lp_meta =
                params[P_LP_STREAM_META_OFFSET] + node * LP_STREAM_META_WORDS;
            active_offset = stream_state[lp_meta + 2];
            active_count = stream_state[lp_meta + 3];
        }

        ulong event[EVENT_WORDS];
        ulong selected_active;
        ulong selected_stream;
        if (!fel_peek(
            node,
            params,
            fel_meta,
            fel_records,
            stream_state,
            stream_records,
            event,
            selected_active,
            selected_stream
        ) || !before_horizon(event[E_TIME], control)) {
            state[L_FINISHED] = 1;
            return;
        }
        ulong popped_timer_owner;
        if (!fel_pop_selected(
            node,
            selected_active,
            selected_stream,
            params,
            fel_meta,
            fel_records,
            stream_state,
            stream_records,
            tcp_state,
            event,
            popped_timer_owner
        )) {
            set_semantic_error(state, 26, node);
            return;
        }
        ulong remote_counter = node * META_WORDS + 3;
        ulong remote_before = remote_meta[remote_counter];
        if (!dispatch_event(
            node,
            event,
            popped_timer_owner,
            state,
            params,
            node_state,
            generators,
            flows,
            routes,
            links,
            fel_meta,
            fel_records,
            queue_meta,
            queue_records,
            in_service,
            scheduler_state,
            remote_meta,
            remote_staging,
            stream_state,
            stream_records,
            summary,
            observation_meta,
            observed,
            departures,
            arrivals,
            tcp_state
        )) {
            return;
        }
        if (state[L_TRANSITIONS] == NONE) {
            set_semantic_error(state, 27, node);
            return;
        }
        if (diagnostic_streams) {
            t32_record_head_visit(
                node,
                active_offset,
                active_count,
                params,
                stream_state,
                diagnostics
            );
            t32_record_remote_emissions(
                node,
                remote_before,
                remote_meta[remote_counter],
                params,
                remote_meta,
                stream_state,
                diagnostics
            );
        } else {
            t32_diagnostic_flag(node, T32_FLAG_STREAMS_DISABLED, diagnostics);
        }
        state[L_TRANSITIONS] += 1;
        dispatch_transitions += 1;
    }
}

// T15e diagnostic only. This kernel preserves the production event set and transition body. Its
// uniform mode word optionally adds one push/pop pair for the just-popped event before each real
// transition. Probe-minus-control measures the marginal stress cost; a per-LP counter reports
// real local pushes from the unchanged body.

// T21 fix 1 — the round-control scans, on the whole grid.
//
// One active-worklist scan, two partition-free reductions. `days_round_reset` cleared every LP's
// error state before `days_round_prepare` built this round's canonical ascending unique worklist;
// between prepare and this sweep, only `days_round` can change error or finished state, and it runs
// only worklist LPs. Every LP outside the worklist therefore contributes the identity element.
//
// `first_error` is a `min` over errored worklist LP indices: each thread keeps the first such LP in
// its ascending active-index subsequence, and the tree takes the smallest of those, so the answer
// is the global smallest for every partition. `unfinished` is an `OR`. Neither depends on how the
// worklist was divided, and the active set needs no construction beyond the existing compaction.
extern "C" __global__ void days_round_control_sweep(DAYS_BUFFERS) {
    uint lane = threadIdx.x;
    __shared__ ulong first_errors[1024];
    __shared__ uint unfinished_lanes[1024];
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 1
    ) {
        return;
    }

    ulong first = ulong(blockIdx.x) * ulong(blockDim.x) + ulong(threadIdx.x);
    ulong stride = ulong(gridDim.x) * ulong(blockDim.x);
    ulong first_error = NONE;
    uint unfinished = 0;
    for (ulong active = first; active < control[C_ACTIVE]; active += stride) {
        ulong node = worklist[active];
        ulong state = node * LP_STATE_WORDS;
        if (first_error == NONE && lp_state[state + L_ERROR] != 0) {
            first_error = node;
        }
        if (unfinished == 0 && lp_state[state + L_FINISHED] == 0) {
            unfinished = 1;
        }
        if (first_error != NONE && unfinished != 0) {
            break;
        }
    }
    first_errors[lane] = first_error;
    unfinished_lanes[lane] = unfinished;
    __syncthreads();
    for (uint stride_lanes = 512; stride_lanes != 0; stride_lanes >>= 1) {
        if (lane < stride_lanes) {
            first_errors[lane] = min(first_errors[lane], first_errors[lane + stride_lanes]);
            unfinished_lanes[lane] |= unfinished_lanes[lane + stride_lanes];
        }
        __syncthreads();
    }
    if (lane == 0) {
        store_round_partial(stream_state, params, blockIdx.x, 0, first_errors[0]);
        store_round_partial(
            stream_state,
            params,
            blockIdx.x,
            1,
            ulong(unfinished_lanes[0])
        );
    }
}

// T21 fix 1 — the round-control combine. The guard is `days_round_control_sweep`'s, word for word,
// and nothing between the two dispatches writes a word it reads.
extern "C" __global__ void days_round_control(DAYS_BUFFERS) {
    uint lane = threadIdx.x;
    __shared__ ulong first_errors[1024];
    __shared__ uint unfinished_lanes[1024];
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 1
    ) {
        return;
    }

    ulong first_error = NONE;
    uint unfinished = 0;
    for (
        ulong block = lane;
        block < CONTROL_SWEEP_BLOCKS;
        block += 1024
    ) {
        first_error = min(
            first_error,
            load_round_partial(stream_state, params, block, 0)
        );
        unfinished |= uint(load_round_partial(stream_state, params, block, 1) != 0);
    }
    first_errors[lane] = first_error;
    unfinished_lanes[lane] = unfinished;
    __syncthreads();
    for (uint stride = 512; stride != 0; stride >>= 1) {
        if (lane < stride) {
            first_errors[lane] = min(first_errors[lane], first_errors[lane + stride]);
            unfinished_lanes[lane] |= unfinished_lanes[lane + stride];
        }
        __syncthreads();
    }
    if (lane != 0) {
        return;
    }
    if (first_errors[0] != NONE) {
        ulong state = first_errors[0] * LP_STATE_WORDS;
        control[C_ERROR] = lp_state[state + L_ERROR];
        control[C_ERROR_ARENA] = lp_state[state + L_ERROR_ARENA];
        control[C_ERROR_NODE] = lp_state[state + L_ERROR_NODE];
        control[C_ERROR_CAPACITY] = lp_state[state + L_ERROR_CAPACITY];
        control[C_ERROR_DEMAND] = lp_state[state + L_ERROR_DEMAND];
    } else if (unfinished_lanes[0] != 0) {
        control[C_RELAUNCHES] += 1;
    } else {
        control[C_CONTINUATION] = 2;
    }
}

// T21 fix 1 — the exchange-prefix STREAMS scan, on the whole grid.
//
// This is the kernel the FIRST attempt died in (`evidence/P12/aterm-fixes.md` §3.1-3.3): its
// in-dispatch combine produced `demand: 18446744073709551615`, i.e. this clamped sum saturating on
// garbage, at k=32 width only. Here the combine is a SEPARATE DISPATCH, so there are no partials
// in flight and nothing to order.
//
// Three partition-free reductions: the clamped sum of the per-channel batch counts (associative on
// non-negative values, so `min(true total, 2^64-1)` for every grouping), the `OR` of the capacity
// flags (every tree node's value is its subtree's clamped sum, hence at most the total, and the
// root re-checks the total, so the flag is exactly `total > capacity`), and the `min` over the
// channel indices that failed validation.
//
// The per-channel `stream_state[batch + 3] = 0` reset comes along: it is one write per channel by
// the one thread that owns that channel, and it touches the channel BATCH quadruple, not the
// stream header at `channel * META_WORDS` that the combine re-reads for the failure detail.
//
// The LEGACY (streams-disabled) path is NOT here. It is a node-ordered prefix scan whose output —
// each producer's staging base — depends on the partition, so it stays in the width-1 combine over
// the retained 1,024-lane contiguous chunks.
extern "C" __global__ void days_exchange_prefix_sweep(DAYS_BUFFERS) {
    uint lane = threadIdx.x;
    __shared__ ulong sums[1024];
    __shared__ uint exceeded[1024];
    __shared__ ulong failures[1024];
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 2 ||
        params[P_STREAMS_ENABLED] == 0
    ) {
        return;
    }

    // Braced so the per-channel body below stays a line-for-line copy of the pre-T21 kernel's,
    // including the `capacity` the stream header shadows inside the loop.
    {
        ulong capacity = params[P_OUTBOX_CAPACITY];
        ulong local_sum = 0;
        uint local_exceeded = 0;
        ulong local_failure = NONE;
        ulong first = ulong(blockIdx.x) * ulong(blockDim.x) + ulong(threadIdx.x);
        ulong grid_stride = ulong(gridDim.x) * ulong(blockDim.x);
        for (
            ulong channel = first;
            channel < params[P_CHANNEL_COUNT];
            channel += grid_stride
        ) {
            ulong batch =
                params[P_CHANNEL_BATCH_OFFSET] +
                channel * CHANNEL_BATCH_WORDS;
            ulong batch_count = stream_state[batch];
            local_sum = saturating_add_ulong(local_sum, batch_count);
            local_exceeded |= uint(local_sum > capacity);
            stream_state[batch + 3] = 0;
            ulong stream_base = channel * META_WORDS;
            ulong capacity = stream_state[stream_base + 1];
            ulong count = stream_state[stream_base + 3];
            bool invalid_capacity =
                count > capacity || batch_count > capacity - min(count, capacity);
            bool invalid_order = false;
            if (
                params[P_STREAM_ORDER_CHECKS] != 0 &&
                !invalid_capacity &&
                batch_count != 0
            ) {
                ulong first_slot = stream_state[batch + 1];
                invalid_order = before_horizon(
                    remote_staging[first_slot * EVENT_WORDS + E_TIME],
                    control
                );
                if (!invalid_order && count != 0) {
                    ulong head = stream_state[stream_base + 2];
                    ulong tail = (head + count - 1) % max(capacity, 1ul);
                    invalid_order = !stored_cross_key_less(
                        stream_records,
                        stream_state[stream_base] + tail,
                        remote_staging,
                        first_slot
                    );
                }
            }
            if ((invalid_capacity || invalid_order) && channel < local_failure) {
                local_failure = channel;
            }
        }
        sums[lane] = local_sum;
        exceeded[lane] = local_exceeded;
        failures[lane] = local_failure;
        __syncthreads();
        for (uint stride = 512; stride != 0; stride >>= 1) {
            if (lane < stride) {
                ulong right_sum = sums[lane + stride];
                uint combined_exceeded =
                    exceeded[lane] | exceeded[lane + stride];
                ulong combined_sum = saturating_add_ulong(sums[lane], right_sum);
                combined_exceeded |= uint(combined_sum > capacity);
                sums[lane] = combined_sum;
                exceeded[lane] = combined_exceeded;
                failures[lane] = min(failures[lane], failures[lane + stride]);
            }
            __syncthreads();
        }
        if (lane == 0) {
            store_round_partial(stream_state, params, blockIdx.x, 0, sums[0]);
            store_round_partial(stream_state, params, blockIdx.x, 1, ulong(exceeded[0]));
            store_round_partial(stream_state, params, blockIdx.x, 2, failures[0]);
        }
    }
}

// T21 fix 1 — the exchange-prefix combine, plus the legacy node-ordered prefix scan.
//
// The guard is `days_exchange_prefix_sweep`'s, word for word; nothing between the two dispatches
// writes a word it reads.
extern "C" __global__ void days_exchange_prefix(DAYS_BUFFERS) {
    uint lane = threadIdx.x;
    __shared__ ulong sums[1024];
    __shared__ uint exceeded[1024];
    __shared__ ulong failures[1024];
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 2
    ) {
        return;
    }

    if (params[P_STREAMS_ENABLED] != 0) {
        ulong capacity = params[P_OUTBOX_CAPACITY];
        ulong local_sum = 0;
        uint local_exceeded = 0;
        ulong local_failure = NONE;
        for (
            ulong block = lane;
            block < CONTROL_SWEEP_BLOCKS;
            block += 1024
        ) {
            local_sum = saturating_add_ulong(
                local_sum,
                load_round_partial(stream_state, params, block, 0)
            );
            local_exceeded |= uint(
                load_round_partial(stream_state, params, block, 1) != 0
            );
            local_exceeded |= uint(local_sum > capacity);
            local_failure = min(
                local_failure,
                load_round_partial(stream_state, params, block, 2)
            );
        }
        sums[lane] = local_sum;
        exceeded[lane] = local_exceeded;
        failures[lane] = local_failure;
        __syncthreads();
        for (uint stride = 512; stride != 0; stride >>= 1) {
            if (lane < stride) {
                ulong right_sum = sums[lane + stride];
                uint combined_exceeded =
                    exceeded[lane] | exceeded[lane + stride];
                ulong combined_sum = saturating_add_ulong(sums[lane], right_sum);
                combined_exceeded |= uint(combined_sum > capacity);
                sums[lane] = combined_sum;
                exceeded[lane] = combined_exceeded;
                failures[lane] = min(failures[lane], failures[lane + stride]);
            }
            __syncthreads();
        }
        if (lane == 0) {
            control[C_OUTBOX] = sums[0];
            ulong failure = failures[0];
            if (exceeded[0] != 0) {
                control[C_ERROR] = ERROR_CAPACITY;
                control[C_ERROR_ARENA] = ARENA_OUTBOX;
                control[C_ERROR_NODE] = NONE;
                control[C_ERROR_CAPACITY] = capacity;
                control[C_ERROR_DEMAND] = sums[0];
            } else if (failure != NONE) {
                ulong batch =
                    params[P_CHANNEL_BATCH_OFFSET] +
                    failure * CHANNEL_BATCH_WORDS;
                ulong stream_base = failure * META_WORDS;
                ulong capacity = stream_state[stream_base + 1];
                ulong count = stream_state[stream_base + 3];
                ulong batch_count = stream_state[batch];
                control[C_ERROR_NODE] =
                    stream_state[params[P_CHANNEL_TARGET_OFFSET] + failure];
                if (
                    count > capacity ||
                    batch_count > capacity - min(count, capacity)
                ) {
                    control[C_ERROR] = ERROR_CAPACITY;
                    control[C_ERROR_ARENA] = ARENA_CHANNEL_INBOX;
                    control[C_ERROR_CAPACITY] = capacity;
                    control[C_ERROR_DEMAND] = saturating_add_ulong(count, batch_count);
                } else {
                    control[C_ERROR] = ERROR_SEMANTIC + 42;
                }
            }
        }
        return;
    }

    ulong producers = params[P_NODE_COUNT];
    ulong capacity = params[P_OUTBOX_CAPACITY];
    ulong chunk = producers / 1024;
    ulong remainder = producers % 1024;
    ulong start = ulong(lane) * chunk + min(ulong(lane), remainder);
    ulong end = start + chunk + (ulong(lane) < remainder ? 1 : 0);
    ulong local_sum = 0;
    uint local_exceeded = 0;
    for (ulong producer = start; producer < end; ++producer) {
        ulong base = producer * META_WORDS;
        ulong count = remote_meta[base + 3];
        local_sum = saturating_add_ulong(local_sum, count);
        local_exceeded |= uint(local_sum > capacity);
    }

    sums[lane] = local_sum;
    exceeded[lane] = local_exceeded;
    __syncthreads();
    for (uint offset = 1; offset < 1024; offset <<= 1) {
        ulong left_sum = lane >= offset ? sums[lane - offset] : 0;
        uint left_exceeded = lane >= offset ? exceeded[lane - offset] : 0;
        ulong own_sum = sums[lane];
        uint own_exceeded = exceeded[lane];
        __syncthreads();
        if (lane >= offset) {
            uint combined_exceeded = left_exceeded | own_exceeded;
            ulong combined_sum = saturating_add_ulong(left_sum, own_sum);
            combined_exceeded |= uint(combined_sum > capacity);
            sums[lane] = combined_sum;
            exceeded[lane] = combined_exceeded;
        }
        __syncthreads();
    }

    if (lane == 0 && exceeded[1023] != 0) {
        control[C_ERROR] = ERROR_CAPACITY;
        control[C_ERROR_ARENA] = ARENA_OUTBOX;
        control[C_ERROR_NODE] = NONE;
        control[C_ERROR_CAPACITY] = capacity;
        control[C_ERROR_DEMAND] = sums[1023];
    }
    __syncthreads();
    if (exceeded[1023] != 0) {
        return;
    }

    ulong write = sums[lane] - local_sum;
    for (ulong producer = start; producer < end; ++producer) {
        ulong base = producer * META_WORDS;
        remote_meta[base + 2] = write;
        write += remote_meta[base + 3];
    }
    __syncthreads();
    if (lane == 0) {
        control[C_OUTBOX] = sums[1023];
    }
}

__device__ __forceinline__ ulong scatter_stream_target(
    ulong staging_slot,
    const ulong *params,
    ulong *stream_state
) {
    ulong channel =
        stream_state[params[P_STAGING_CHANNEL_OFFSET] + staging_slot];
    ulong batch =
        params[P_CHANNEL_BATCH_OFFSET] +
        channel * CHANNEL_BATCH_WORDS;
    ulong cursor = stream_state[batch + 3];
    ulong stream_base = channel * META_WORDS;
    ulong capacity = stream_state[stream_base + 1];
    ulong physical = (
        stream_state[stream_base + 2] +
        stream_state[stream_base + 3] +
        cursor
    ) % max(capacity, 1ul);
    stream_state[batch + 3] = cursor + 1;
    return stream_state[stream_base] + physical;
}

__device__ __forceinline__ void scatter_stream_commit(
    ulong producer,
    const ulong *params,
    ulong *stream_state
) {
    ulong outbound_meta =
        params[P_OUTBOUND_META_OFFSET] + producer * OUTBOUND_META_WORDS;
    ulong entry = stream_state[outbound_meta];
    ulong entry_count = stream_state[outbound_meta + 1];
    for (ulong index = 0; index < entry_count; ++index) {
        ulong channel =
            stream_state[entry + index * OUTBOUND_ENTRY_WORDS + 1];
        ulong batch =
            params[P_CHANNEL_BATCH_OFFSET] + channel * CHANNEL_BATCH_WORDS;
        stream_state[channel * META_WORDS + 3] += stream_state[batch];
    }
}

__device__ __forceinline__ void scatter_stream_producer_lane(
    ulong producer,
    ulong count,
    const ulong *params,
    const ulong *remote_meta,
    const ulong *remote_staging,
    ulong *stream_state,
    ulong *stream_records
) {
    ulong base = producer * META_WORDS;
    ulong staging = remote_meta[base];
    for (ulong index = 0; index < count; ++index) {
        ulong target =
            scatter_stream_target(staging + index, params, stream_state);
        copy_device_record(
            remote_staging,
            staging + index,
            stream_records,
            target
        );
    }
    scatter_stream_commit(producer, params, stream_state);
}

__device__ __forceinline__ void scatter_stream_producer(
    ulong producer,
    ulong count,
    uint lane,
    const ulong *params,
    const ulong *remote_meta,
    const ulong *remote_staging,
    ulong *stream_state,
    ulong *stream_records
) {
    // The leader preserves the scalar channel-cursor recurrence. The other
    // lanes only copy words into the disjoint slots that recurrence selects.
    ulong staging = 0;
    if (lane == 0) {
        ulong base = producer * META_WORDS;
        staging = remote_meta[base];
    }
    staging = __shfl_sync(SCATTER_GROUP_MASK, staging, 0);

    for (ulong index = 0; index < count; index += 2) {
        ulong first_target = 0;
        ulong second_target = 0;
        if (lane == 0) {
            first_target = scatter_stream_target(staging + index, params, stream_state);
            if (index + 1 < count) {
                second_target =
                    scatter_stream_target(staging + index + 1, params, stream_state);
            }
        }
        first_target = __shfl_sync(SCATTER_GROUP_MASK, first_target, 0);
        second_target = __shfl_sync(SCATTER_GROUP_MASK, second_target, 0);

        if (lane < 2 * EVENT_WORDS) {
            ulong record = lane / EVENT_WORDS;
            uint word = lane % EVENT_WORDS;
            if (index + record < count) {
                ulong source_slot = staging + index + record;
                ulong target_slot = record == 0 ? first_target : second_target;
                stream_records[target_slot * EVENT_WORDS + word] =
                    remote_staging[source_slot * EVENT_WORDS + word];
            }
        }
    }

    if (lane == 0) {
        scatter_stream_commit(producer, params, stream_state);
    }
}

extern "C" __global__ void days_exchange_scatter(DAYS_BUFFERS) {
    uint global_thread = blockIdx.x * blockDim.x + threadIdx.x;
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 2
    ) {
        return;
    }
    if (
        params[P_STREAMS_ENABLED] != 0 &&
        blockDim.x >= SCATTER_GROUP_WIDTH &&
        blockDim.x % SCATTER_GROUP_WIDTH == 0
    ) {
        // Counts below three fit in at most one two-record copy cluster. Keep
        // those producers on the original lane mapping so one warp can advance
        // up to 32 of them concurrently.
        if (global_thread < params[P_NODE_COUNT]) {
            ulong producer = global_thread;
            ulong count = remote_meta[producer * META_WORDS + 3];
            if (count < SCATTER_COOPERATIVE_MIN_RECORDS) {
                scatter_stream_producer_lane(
                    producer,
                    count,
                    params,
                    remote_meta,
                    remote_staging,
                    stream_state,
                    stream_records
                );
            }
        }

        // Long producers retain one cooperative warp each. The worklist is
        // deterministic and unique; the count predicate makes the lane and
        // cooperative traversals disjoint.
        uint lane = threadIdx.x % SCATTER_GROUP_WIDTH;
        ulong groups_per_block = blockDim.x / SCATTER_GROUP_WIDTH;
        ulong group =
            ulong(blockIdx.x) * groups_per_block +
            threadIdx.x / SCATTER_GROUP_WIDTH;
        ulong group_stride = ulong(gridDim.x) * groups_per_block;
        for (
            ulong active_index = group;
            active_index < control[C_ACTIVE];
            active_index += group_stride
        ) {
            ulong producer = worklist[active_index];
            ulong count = 0;
            if (lane == 0) {
                count = remote_meta[producer * META_WORDS + 3];
            }
            count = __shfl_sync(SCATTER_GROUP_MASK, count, 0);
            if (count >= SCATTER_COOPERATIVE_MIN_RECORDS) {
                scatter_stream_producer(
                    producer,
                    count,
                    lane,
                    params,
                    remote_meta,
                    remote_staging,
                    stream_state,
                    stream_records
                );
            }
        }
        return;
    }
    if (global_thread >= params[P_NODE_COUNT]) {
        return;
    }
    ulong producer = global_thread;
    ulong base = ulong(producer) * META_WORDS;
    ulong staging = remote_meta[base];
    ulong compact = remote_meta[base + 2];
    ulong count = remote_meta[base + 3];
    if (params[P_STREAMS_ENABLED] != 0) {
        for (ulong index = 0; index < count; ++index) {
            ulong staging_slot = staging + index;
            ulong channel =
                stream_state[params[P_STAGING_CHANNEL_OFFSET] + staging_slot];
            ulong batch =
                params[P_CHANNEL_BATCH_OFFSET] +
                channel * CHANNEL_BATCH_WORDS;
            ulong cursor = stream_state[batch + 3];
            ulong stream_base = channel * META_WORDS;
            ulong capacity = stream_state[stream_base + 1];
            ulong physical = (
                stream_state[stream_base + 2] +
                stream_state[stream_base + 3] +
                cursor
            ) % max(capacity, 1ul);
            copy_device_record(
                remote_staging,
                staging_slot,
                stream_records,
                stream_state[stream_base] + physical
            );
            stream_state[batch + 3] = cursor + 1;
        }
        ulong outbound_meta =
            params[P_OUTBOUND_META_OFFSET] +
            ulong(producer) * OUTBOUND_META_WORDS;
        ulong entry = stream_state[outbound_meta];
        ulong entry_count = stream_state[outbound_meta + 1];
        for (ulong index = 0; index < entry_count; ++index) {
            ulong channel =
                stream_state[entry + index * OUTBOUND_ENTRY_WORDS + 1];
            ulong batch =
                params[P_CHANNEL_BATCH_OFFSET] +
                channel * CHANNEL_BATCH_WORDS;
            stream_state[channel * META_WORDS + 3] += stream_state[batch];
        }
        return;
    }
    for (ulong index = 0; index < count; ++index) {
        copy_device_record(
            remote_staging,
            staging + index,
            outbox,
            compact + index
        );
    }
}

extern "C" __global__ void days_exchange_merge(DAYS_BUFFERS) {
    uint target = blockIdx.x * blockDim.x + threadIdx.x;
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 2 ||
        target >= params[P_NODE_COUNT]
    ) {
        return;
    }
    if (params[P_STREAMS_ENABLED] != 0) {
        ulong meta =
            params[P_LP_STREAM_META_OFFSET] +
            ulong(target) * LP_STREAM_META_WORDS;
        ulong declared = stream_state[meta];
        ulong declared_count = stream_state[meta + 1];
        ulong active = stream_state[meta + 2];
        ulong active_count = 0;
        if (fel_meta[ulong(target) * META_WORDS + 3] != 0) {
            ulong entry = active + active_count * ACTIVE_STREAM_ENTRY_WORDS;
            active_count += 1;
            stream_state[entry] = NONE;
            ulong record =
                fel_meta[ulong(target) * META_WORDS] * EVENT_WORDS;
            for (uint word = 0; word < 4; ++word) {
                stream_state[entry + 1 + word] = fel_records[record + word];
            }
        }
        for (ulong index = 0; index < declared_count; ++index) {
            ulong stream = stream_state[declared + index];
            if (stream_state[stream * META_WORDS + 3] != 0) {
                ulong entry =
                    active + active_count * ACTIVE_STREAM_ENTRY_WORDS;
                active_count += 1;
                stream_state[entry] = stream;
                ulong stream_base = stream * META_WORDS;
                ulong capacity = stream_state[stream_base + 1];
                ulong physical =
                    stream_state[stream_base + 2] % max(capacity, 1ul);
                ulong record =
                    (stream_state[stream_base] + physical) * EVENT_WORDS;
                for (uint word = 0; word < 4; ++word) {
                    stream_state[entry + 1 + word] =
                        stream_records[record + word];
                }
            }
        }
        stream_state[meta + 3] = active_count;
        return;
    }
    ulong inbound_base = ulong(target) * INBOUND_META_WORDS;
    ulong edge_start = inbound_meta[inbound_base];
    ulong edge_count = inbound_meta[inbound_base + 1];
    for (ulong edge = edge_start; edge < edge_start + edge_count; ++edge) {
        merge_cursors[edge] = 0;
    }

    ulong *error = lp_state + ulong(target) * LP_STATE_WORDS;
    while (true) {
        ulong best_edge = NONE;
        ulong best_slot = 0;
        for (ulong edge = edge_start; edge < edge_start + edge_count; ++edge) {
            ulong producer = inbound_producers[edge];
            ulong producer_base = producer * META_WORDS;
            ulong count = remote_meta[producer_base + 3];
            ulong cursor = merge_cursors[edge];
            while (cursor < count) {
                ulong slot = remote_meta[producer_base + 2] + cursor;
                if (outbox[slot * EVENT_WORDS + E_TARGET] == target) {
                    break;
                }
                cursor += 1;
            }
            merge_cursors[edge] = cursor;
            if (cursor == count) {
                continue;
            }
            ulong slot = remote_meta[producer_base + 2] + cursor;
            if (
                best_edge == NONE ||
                stored_key_less(outbox, slot, best_slot)
            ) {
                best_edge = edge;
                best_slot = slot;
            }
        }
        if (best_edge == NONE) {
            break;
        }
        ulong event[EVENT_WORDS];
        copy_device_to_thread(outbox, best_slot, event);
        if (before_horizon(event[E_TIME], control)) {
            set_semantic_error(error, 28, target);
            return;
        }
        if (!heap_push(
            target,
            event,
            error,
            params,
            fel_meta,
            fel_records,
            tcp_state
        )) {
            return;
        }
        merge_cursors[best_edge] += 1;
    }
}

// T15e diagnostic counterpart of `days_exchange_merge`. The merge itself is unchanged; a
// read-only pre-scan records actual producer fan-in and remote-event counts per target-round.
// T21 fix 1 — the finalize scans, on the whole grid.
//
// Four partition-free reductions: a `min` over the LP indices whose `L_ERROR` is set, and one
// CLAMPED SUM plus capacity flag for each of the three observation logs.
//
// The clamped sum is associative on non-negative values — `sat(sat(a,b),c) = min(a+b+c, 2^64-1) =
// sat(a,sat(b,c))`, because if `a+b` already saturates then so does the true total — so the
// multi-way clamped sum is `min(true total, 2^64-1)` for EVERY grouping. And every tree node's
// value is the clamped sum of its own subtree, hence at most the total, so a node exceeding the
// capacity implies the total does; the root re-checks the total. The flag is therefore exactly
// `clamped total > capacity`, again for every grouping. Neither the totals nor the flags can tell
// how the LPs were divided.
//
// Seven words per block: `{first_error, total0, exceeded0, total1, exceeded1, total2, exceeded2}`.
extern "C" __global__ void days_round_finalize_sweep(DAYS_BUFFERS) {
    uint lane = threadIdx.x;
    __shared__ ulong values[1024];
    __shared__ uint exceeded[1024];
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 2
    ) {
        return;
    }

    ulong capacities[3] = {
        params[P_OBSERVED_CAPACITY],
        params[P_DEPARTURE_CAPACITY],
        params[P_ARRIVAL_CAPACITY]
    };
    ulong local_totals[3] = {0, 0, 0};
    uint local_exceeded[3] = {0, 0, 0};
    ulong first_error = NONE;
    ulong first = ulong(blockIdx.x) * ulong(blockDim.x) + ulong(threadIdx.x);
    ulong stride = ulong(gridDim.x) * ulong(blockDim.x);
    for (ulong node = first; node < params[P_NODE_COUNT]; node += stride) {
        ulong state = node * LP_STATE_WORDS;
        if (first_error == NONE && lp_state[state + L_ERROR] != 0) {
            first_error = node;
        }
        if (params[P_FULL_OBSERVATIONS] != 0) {
            ulong base = node * OBSERVATION_META_WORDS;
            for (uint log = 0; log < 3; ++log) {
                ulong count = observation_meta[base + log * META_WORDS + 3];
                local_totals[log] = saturating_add_ulong(local_totals[log], count);
                local_exceeded[log] |= uint(local_totals[log] > capacities[log]);
            }
        }
    }

    values[lane] = first_error;
    __syncthreads();
    for (uint stride_lanes = 512; stride_lanes != 0; stride_lanes >>= 1) {
        if (lane < stride_lanes) {
            values[lane] = min(values[lane], values[lane + stride_lanes]);
        }
        __syncthreads();
    }
    if (lane == 0) {
        store_round_partial(stream_state, params, blockIdx.x, 0, values[0]);
    }
    __syncthreads();

    for (uint log = 0; log < 3; ++log) {
        values[lane] = local_totals[log];
        exceeded[lane] = local_exceeded[log];
        __syncthreads();
        for (uint stride_lanes = 512; stride_lanes != 0; stride_lanes >>= 1) {
            if (lane < stride_lanes) {
                ulong left = values[lane];
                ulong right = values[lane + stride_lanes];
                uint combined_exceeded = exceeded[lane] | exceeded[lane + stride_lanes];
                ulong combined_total = saturating_add_ulong(left, right);
                combined_exceeded |= uint(combined_total > capacities[log]);
                values[lane] = combined_total;
                exceeded[lane] = combined_exceeded;
            }
            __syncthreads();
        }
        if (lane == 0) {
            store_round_partial(stream_state, params, blockIdx.x, 1 + log * 2, values[0]);
            store_round_partial(
                stream_state,
                params,
                blockIdx.x,
                2 + log * 2,
                ulong(exceeded[0])
            );
        }
        __syncthreads();
    }
}

// T21 fix 1 — the finalize combine. The guard is `days_round_finalize_sweep`'s, word for word, and
// nothing between the two dispatches writes a word it reads.
extern "C" __global__ void days_round_finalize(DAYS_BUFFERS) {
    uint lane = threadIdx.x;
    __shared__ ulong values[1024];
    __shared__ uint exceeded[1024];
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 2
    ) {
        return;
    }

    // T24: on the streams + Summary path, every possible post-control error is already published
    // directly to `control`, stream merge writes no LP error, and cumulative observation totals are
    // disabled. The host omits the sweep for exactly this lowered-image predicate; this ordered
    // dispatch remains as the publish boundary before the next horizon sweep.
    if (
        params[P_STREAMS_ENABLED] != 0 &&
        params[P_FULL_OBSERVATIONS] == 0
    ) {
        if (lane == 0) {
            control[C_CONTINUATION] = 0;
            control[C_ROUNDS] += 1;
        }
        return;
    }

    ulong capacities[3] = {
        params[P_OBSERVED_CAPACITY],
        params[P_DEPARTURE_CAPACITY],
        params[P_ARRIVAL_CAPACITY]
    };
    ulong first_error = NONE;
    for (
        ulong block = lane;
        block < CONTROL_SWEEP_BLOCKS;
        block += 1024
    ) {
        first_error = min(
            first_error,
            load_round_partial(stream_state, params, block, 0)
        );
    }

    values[lane] = first_error;
    __syncthreads();
    for (uint stride = 512; stride != 0; stride >>= 1) {
        if (lane < stride) {
            values[lane] = min(values[lane], values[lane + stride]);
        }
        __syncthreads();
    }
    if (lane == 0 && values[0] != NONE) {
        ulong state = values[0] * LP_STATE_WORDS;
        control[C_ERROR] = lp_state[state + L_ERROR];
        control[C_ERROR_ARENA] = lp_state[state + L_ERROR_ARENA];
        control[C_ERROR_NODE] = lp_state[state + L_ERROR_NODE];
        control[C_ERROR_CAPACITY] = lp_state[state + L_ERROR_CAPACITY];
        control[C_ERROR_DEMAND] = lp_state[state + L_ERROR_DEMAND];
    }
    __syncthreads();
    if (values[0] != NONE) {
        return;
    }
    // Complete the reduction-result read before reusing the shared scratch planes.
    __syncthreads();
    if (params[P_FULL_OBSERVATIONS] == 0) {
        if (lane == 0) {
            control[C_CONTINUATION] = 0;
            control[C_ROUNDS] += 1;
        }
        return;
    }

    ulong arenas[3] = {ARENA_OBSERVED, ARENA_DEPARTURES, ARENA_ARRIVALS};
    for (uint log = 0; log < 3; ++log) {
        ulong local_total = 0;
        uint local_exceeded = 0;
        for (
            ulong block = lane;
            block < CONTROL_SWEEP_BLOCKS;
            block += 1024
        ) {
            local_total = saturating_add_ulong(
                local_total,
                load_round_partial(stream_state, params, block, 1 + log * 2)
            );
            local_exceeded |= uint(
                load_round_partial(stream_state, params, block, 2 + log * 2) != 0
            );
            local_exceeded |= uint(local_total > capacities[log]);
        }
        values[lane] = local_total;
        exceeded[lane] = local_exceeded;
        __syncthreads();
        for (uint stride = 512; stride != 0; stride >>= 1) {
            if (lane < stride) {
                ulong left = values[lane];
                ulong right = values[lane + stride];
                uint combined_exceeded = exceeded[lane] | exceeded[lane + stride];
                ulong combined_total = saturating_add_ulong(left, right);
                combined_exceeded |= uint(combined_total > capacities[log]);
                values[lane] = combined_total;
                exceeded[lane] = combined_exceeded;
            }
            __syncthreads();
        }
        if (lane == 0 && exceeded[0] != 0 && control[C_ERROR] == 0) {
            control[C_ERROR] = ERROR_CAPACITY;
            control[C_ERROR_ARENA] = arenas[log];
            control[C_ERROR_NODE] = NONE;
            control[C_ERROR_CAPACITY] = capacities[log];
            control[C_ERROR_DEMAND] = values[0];
        }
        __syncthreads();
    }
    if (lane == 0 && control[C_ERROR] == 0) {
        control[C_CONTINUATION] = 0;
        control[C_ROUNDS] += 1;
    }
}

// T20l fix 2 — device-side readback compaction. Transliteration of `days_compact_gather` in
// `metal_kernels.metal`; see that kernel's header for the audit and determinism argument.
//
// AUDIT (T20g style). Exactly ONE device write site, `destination[...]` below. `destination` is a
// buffer allocated for the readback alone: never bound to a simulation kernel, never read by one,
// never part of complete state. `source`, `plan` and `args` are `const`. This kernel is launched
// outside the captured attempt graph, only after the attempt has been screened as successful.
//
// The entry ABI deliberately does NOT follow DAYS_BUFFERS: the gather is not part of the replayed
// attempt DAG, so it takes its own four buffers instead of the uniform 29-plane signature.
constexpr uint COMPACT_PLAN_ROW_WORDS = 5;

extern "C" __global__ void days_compact_gather(
    ulong *destination, const ulong *source, const ulong *plan, const ulong *args
) {
    ulong entity_count = args[0];
    ulong record_words = args[1];
    bool ring = args[2] != 0;
    ulong entity = ulong(blockIdx.x) * ulong(blockDim.x) + ulong(threadIdx.x);
    if (entity >= entity_count) {
        return;
    }
    const ulong *row = plan + entity * COMPACT_PLAN_ROW_WORDS;
    ulong source_words = row[0];
    ulong span = max(row[1], 1ul);
    ulong head = row[2];
    ulong count = row[3];
    ulong destination_words = row[4];
    for (ulong index = 0; index < count; ++index) {
        ulong physical = ring ? (head + index) % span : index;
        ulong from = source_words + physical * record_words;
        ulong to = destination_words + index * record_words;
        for (ulong word = 0; word < record_words; ++word) {
            destination[to + word] = source[from + word];
        }
    }
}
