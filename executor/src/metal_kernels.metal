#include <metal_stdlib>
using namespace metal;

// `MetalBuffers::new` creates and binds one allocation per buffer index used by the attempt
// kernels. Their `__restrict` contract depends on preserving that pairwise-disjoint binding.

constant uint EVENT_WORDS = 14;
constant uint SCATTER_COOPERATIVE_MIN_RECORDS = 3;
constant uint NODE_WORDS = 11;
constant uint GENERATOR_WORDS = 43;
constant uint FLOW_WORDS = 6;
constant uint LINK_WORDS = 4;
constant uint META_WORDS = 4;
constant uint QUEUE_META_WORDS = 5;
constant uint LP_STATE_WORDS = 7;
constant uint OBSERVATION_META_WORDS = 12;
constant uint INBOUND_META_WORDS = 2;
constant uint LP_STREAM_META_WORDS = 4;
constant uint OUTBOUND_META_WORDS = 2;
constant uint OUTBOUND_ENTRY_WORDS = 2;
constant uint CHANNEL_BATCH_WORDS = 4;
constant uint ACTIVE_STREAM_ENTRY_WORDS = 5;
constant uint TCP_RECEIVER_WORDS = 7;
// T20i ledger-ring metadata layout, mirrored word-for-word by `executor/src/tcp_ledger_ring.rs`:
//   +0 record-arena offset  +1 capacity  +2 count
//   +3 armed fallback-heap slot + 1 (T20g live-state contract)
//   +4 ring head            +5 occupancy high-water mark
constant uint TCP_LEDGER_META_WORDS = 6;
constant uint TCP_LEDGER_RECORD_WORDS = 5;
constant uint TCP_LEDGER_META_HEAD = 4;
constant uint TCP_LEDGER_META_HIGH_WATER = 5;
constant uint RATIONAL_WORDS = 10;
constant uint BIG_LIMBS = 10;
constant uint WIDE_LIMBS = 16;
constant uint PRODUCT_LIMBS = 20;
constant uint SCHEDULER_NODE_WORDS = 29;
constant uint SCHEDULER_CLASS_WORDS = 12;
constant ulong NONE = 0xfffffffffffffffful;

constant uint C_ERROR = 0;
constant uint C_ERROR_ARENA = 1;
constant uint C_ERROR_NODE = 2;
constant uint C_ERROR_CAPACITY = 3;
constant uint C_DONE = 4;
constant uint C_HORIZON_LO = 5;
constant uint C_HORIZON_HI = 6;
constant uint C_RUN_END_LO = 7;
constant uint C_RUN_END_HI = 8;
constant uint C_ROUNDS = 9;
constant uint C_TRANSITIONS = 10;
constant uint C_OUTBOX = 11;
constant uint C_OBSERVED = 12;
constant uint C_DEPARTURES = 13;
constant uint C_ARRIVALS = 14;
constant uint C_ACTIVE = 15;
constant uint C_FRONTIER = 16;
constant uint C_CONTINUATION = 17;
constant uint C_RELAUNCHES = 18;
constant uint C_ERROR_DEMAND = 19;
constant uint C_INDIRECT_OFFSET_WORDS = 20;

constant uint P_NODE_COUNT = 0;
constant uint P_FLOW_COUNT = 1;
constant uint P_LINK_COUNT = 2;
constant uint P_OUTBOX_CAPACITY = 3;
constant uint P_WORKLIST_CAPACITY = 4;
constant uint P_OBSERVED_CAPACITY = 5;
constant uint P_DEPARTURE_CAPACITY = 6;
constant uint P_ARRIVAL_CAPACITY = 7;
constant uint P_FULL_OBSERVATIONS = 8;
constant uint P_LOOKAHEAD = 9;
constant uint P_TRANSITION_CAPACITY = 10;
constant uint P_STOP_TIME = 11;
constant uint P_HAS_LOOKAHEAD = 12;
constant uint P_ROUND_CAPACITY = 13;
constant uint P_STREAMS_ENABLED = 14;
constant uint P_STREAM_COUNT = 15;
constant uint P_CHANNEL_COUNT = 16;
constant uint P_SERVICE_STREAM_BASE = 17;
constant uint P_GENERATOR_STREAM_BASE = 18;
constant uint P_LP_STREAM_META_OFFSET = 19;
constant uint P_LP_STREAM_IDS_OFFSET = 20;
constant uint P_LP_ACTIVE_IDS_OFFSET = 21;
constant uint P_OUTBOUND_META_OFFSET = 22;
constant uint P_OUTBOUND_ENTRIES_OFFSET = 23;
constant uint P_CHANNEL_BATCH_OFFSET = 24;
constant uint P_STAGING_CHANNEL_OFFSET = 25;
constant uint P_CHANNEL_TARGET_OFFSET = 26;
constant uint P_STREAM_ORDER_CHECKS = 27;
constant uint P_TCP_RECEIVER_OFFSET = 28;
constant uint P_TCP_LEDGER_META_OFFSET = 29;
constant uint P_ROUND_THREADS = 30;
// T21 fix 2. Word offset, inside `stream_state`, of the per-round FEL root cache: two words per
// LP, `{root time, validity}`. `days_horizon_sweep` evaluates `fel_root_time` once per node and
// writes it here; O1.4's reset/count and prepare/write passes each read it instead of re-running
// the query, which `evidence/P12/perround-upperbound.md` §1.5.2 measured at three full-width
// evaluations per round over a read set nothing between the call sites writes.
//
// The region is device scratch: written and consumed inside one attempt, never decoded by the
// readback (which reads only `stream_state`'s metadata prefix), never part of complete state.
constant uint P_ROUND_SCRATCH_OFFSET = 31;
constant uint ROUND_SCRATCH_CACHE_WORDS = 2;

// Test-hook-only vector offsets appended to the physical metadata buffers after planning. Each
// entity owns its own slot, so ordinary max writes preserve the actor model and need no atomics.
#ifdef DAYS_DOMINANT_ARENA_HIGH_WATER
constant uint P_STREAM_HIGH_WATER_OFFSET = 32;
constant uint P_REMOTE_HIGH_WATER_OFFSET = 33;
constant uint P_QUEUE_HIGH_WATER_OFFSET = 34;
#define RECORD_STREAM_HIGH_WATER(params, state, entity, occupancy) \
    (state)[(params)[P_STREAM_HIGH_WATER_OFFSET] + (entity)] = max( \
        (state)[(params)[P_STREAM_HIGH_WATER_OFFSET] + (entity)], \
        (ulong)(occupancy) \
    )
#define RECORD_REMOTE_HIGH_WATER(params, meta, entity, occupancy) \
    (meta)[(params)[P_REMOTE_HIGH_WATER_OFFSET] + (entity)] = max( \
        (meta)[(params)[P_REMOTE_HIGH_WATER_OFFSET] + (entity)], \
        (ulong)(occupancy) \
    )
#define RECORD_QUEUE_HIGH_WATER(params, meta, entity, occupancy) \
    (meta)[(params)[P_QUEUE_HIGH_WATER_OFFSET] + (entity)] = max( \
        (meta)[(params)[P_QUEUE_HIGH_WATER_OFFSET] + (entity)], \
        (ulong)(occupancy) \
    )
#else
#define RECORD_STREAM_HIGH_WATER(params, state, entity, occupancy) ((void)0)
#define RECORD_REMOTE_HIGH_WATER(params, meta, entity, occupancy) ((void)0)
#define RECORD_QUEUE_HIGH_WATER(params, meta, entity, occupancy) ((void)0)
#endif

// T21 fix 1 — the re-gridded control sweeps. `evidence/P12/aterm-fixes.md` §3.4. Transliterated
// word for word from `cuda_kernels.cu`; see that file for the design note.
//
// Every phase that swept Θ(nodes + channels) from a grid of ONE threadgroup is now a full-grid
// `_sweep` dispatch that publishes one partial per threadgroup into the tail of the round scratch
// region, plus a width-1 combine dispatch that reduces those partials and performs the phase's
// control writes. The barrier between the two is the DISPATCH BOUNDARY, which under
// `MTLDispatchType::Serial` is the same ordering `days_round` already relies on to see the
// `worklist` `days_round_prepare` wrote. Nothing here synchronizes across threadgroups inside a
// dispatch: no atomics, no `volatile`, no device-scope fence used as one.
constant ulong CONTROL_SWEEP_BLOCKS = 128;
constant ulong ROUND_SCRATCH_PARTIAL_WORDS = 8;

constant uint N_KIND = 0;
constant uint N_EGRESS = 1;
constant uint N_SEMANTIC_QUEUE_CAPACITY = 2;
constant uint N_READY_PENDING = 3;
constant uint N_SERVICE_VALID = 4;
constant uint N_NEXT_ORIGIN = 5;
constant uint N_NEXT_PAYLOAD = 6;
constant uint N_COUNTER_0 = 7;
constant uint N_COUNTER_1 = 8;
constant uint N_COUNTER_2 = 9;

constant uint S_KIND = 0;
constant uint S_CLASS_COUNT = 1;
constant uint S_CLASS_OFFSET = 2;
constant uint S_QUEUE_TAG_OFFSET = 3;
constant uint S_LAST_UPDATED = 4;
constant uint S_VIRTUAL_TIME = 5;
constant uint S_IN_SERVICE_TAG = 15;
constant uint S_AQM_KIND = 25;
constant uint S_AQM_UNIT = 26;
constant uint S_AQM_CAPACITY = 27;
constant uint S_AQM_THRESHOLD = 28;

constant uint SC_VALUE = 0;
constant uint SC_ACTIVE = 1;
constant uint SC_FINISH = 2;

constant uint E_TIME = 0;
constant uint E_PHASE = 1;
constant uint E_ORIGIN = 2;
constant uint E_SEQUENCE = 3;
constant uint E_TARGET = 4;
constant uint E_KIND = 5;
constant uint E_PAYLOAD = 6;
constant uint PK_ID = 7;
constant uint PK_FLOW = 8;
constant uint PK_SIZE = 9;
constant uint PK_KIND = 10;
constant uint PK_META_0 = 11;
constant uint PK_META_1 = 12;
constant uint PK_META_2 = 13;
constant ulong PK_ECN_FLAG = 1ul << 63;
constant ulong PK_KIND_MASK = ~PK_ECN_FLAG;

constant ulong HOST = 0;
constant ulong SWITCH = 1;
constant ulong PACKET_ARRIVAL = 0;
constant ulong TX_READY = 1;
constant ulong TX_COMPLETE = 2;
constant ulong REMOTE_ARRIVAL = 3;
constant ulong RETRANSMISSION_TIMEOUT = 4;
constant ulong PACING_TIMER = 5;
constant ulong DATA_PACKET = 0;
constant ulong FEEDBACK_PACKET = 1;
constant ulong TCP_DATA_PACKET = 2;
constant ulong TCP_ACK_PACKET = 3;
constant ulong SCHED_FIFO = 0;
constant ulong SCHED_SP = 1;
constant ulong SCHED_WFQ = 2;
constant ulong SCHED_DRR = 3;
constant ulong SCHED_WRR = 4;
constant ulong AQM_TAILDROP = 0;
constant ulong AQM_ECN = 1;
constant ulong AQM_PACKETS = 0;
constant ulong AQM_BYTES = 1;

// Generator/controller ABI. CUBIC keeps exact 10^9-nanosegment fixed point and all transition
// arithmetic below uses the existing multi-limb integer primitives; no floating point is used.
constant uint G_VALID = 0;
constant uint G_OWNER = 1;
constant uint G_PACKETS = 2;
constant uint G_BYTES = 3;
constant uint G_STATUS = 4;
constant uint G_DEPARTURE = 5;
constant uint G_PAYLOAD = 6;
constant uint G_FEEDBACK = 8;
constant uint G_OUTSTANDING = 9;
constant uint G_UNACKNOWLEDGED = 10;
constant uint G_KIND = 11;
constant uint G_TCP_TOTAL = 12;
constant uint G_TCP_MSS = 13;
constant uint G_TCP_ACK_SIZE = 14;
constant uint G_TCP_NEXT = 15;
constant uint G_TCP_HIGHEST_ACK = 16;
constant uint G_TCP_FLIGHT = 17;
constant uint G_TCP_DUP_ACKS = 18;
constant uint G_TCP_RECOVERY_HIGH = 19;
constant uint G_TCP_LAST_ATTEMPT = 20;
constant uint G_TCP_TIMER_GENERATION = 21;
constant uint G_TCP_TIMER_ACTIVE = 22;
constant uint G_TCP_TIMER_ATTEMPT = 23;
constant uint G_TCP_TIMER_SEQUENCE = 24;
constant uint G_TCP_TIMER_DEADLINE = 25;
constant uint G_TCP_TIMER_STORED_GENERATION = 26;
constant uint G_TCP_TIMER_RTO = 27;
constant uint G_TCP_SRTT = 28;
constant uint G_TCP_RTTVAR = 29;
constant uint G_TCP_RTO = 30;
constant uint G_CONTROL = 31;
constant uint G_RATE_FIRST = 12;
constant uint G_RATE_INTERVAL = 13;
constant uint G_RATE_PACKET_SIZE = 14;
constant uint G_RATE_TOTAL = 15;
constant uint G_RATE_NUMERATOR = 16;
constant uint G_RATE_DENOMINATOR = 17;
constant uint G_RATE_CREDIT_LOW = 18;
constant uint G_RATE_CREDIT_HIGH = 19;

constant uint CTL_KIND = 0;
constant uint CTL_MSS = 1;
constant uint CTL_CWND = 2;
constant uint CTL_SSTHRESH = 3;
constant uint CTL_PHASE = 4;
constant uint CTL_DUP_ACKS = 5;
constant uint CTL_RECOVERY_HIGH = 6;
constant uint CTL_EXTRA_0 = 7;
constant uint CTL_W_LAST_MAX = 8;
constant uint CTL_EPOCH = 9;
constant uint CTL_SRTT = 10;
constant uint CTL_K = 11;

constant ulong TCP_SLOW_START = 0;
constant ulong TCP_CONGESTION_AVOIDANCE = 1;
constant ulong TCP_FAST_RECOVERY = 2;
constant ulong CUBIC_SCALE = 1000000000ul;
constant ulong CUBIC_MAX_WINDOW = 2000000000000000ul;
constant ulong TCP_MIN_RTO = 1000000000ul;
constant ulong TCP_MAX_RTO = 60000000000ul;
constant ulong TCP_RTO_GRANULARITY = 1000000ul;

constant ulong ERROR_CAPACITY = 1;
constant ulong ERROR_TRANSITION_CAPACITY = 2;
constant ulong ERROR_SEMANTIC = 3;
constant ulong ERROR_WFQ_ARITHMETIC = 100;
constant ulong ARENA_FEL = 1;
constant ulong ARENA_QUEUE = 2;
constant ulong ARENA_OUTBOX = 3;
constant ulong ARENA_WORKLIST = 4;
constant ulong ARENA_OBSERVED = 5;
constant ulong ARENA_DEPARTURES = 6;
constant ulong ARENA_ARRIVALS = 7;
constant ulong ARENA_CHANNEL_INBOX = 8;
constant ulong ARENA_SERVICE_STREAM = 9;
constant ulong ARENA_GENERATOR_STREAM = 10;
constant ulong ARENA_TCP_RECEIVER = 11;
constant ulong ARENA_TCP_SEGMENT_LEDGER = 12;
constant ulong ARENA_REMOTE_STAGING = 13;

constant uint L_FINISHED = 0;
constant uint L_TRANSITIONS = 1;
constant uint L_ERROR = 2;
constant uint L_ERROR_ARENA = 3;
constant uint L_ERROR_NODE = 4;
constant uint L_ERROR_CAPACITY = 5;
constant uint L_ERROR_DEMAND = 6;
// Tagged success/error storage: while L_ERROR is zero this word is the cumulative continuation
// count. A capacity error overwrites it with its arena and terminates the discarded attempt.
constant uint L_SAME_TIME_CONTINUATIONS = L_ERROR_ARENA;

inline bool key_less(const thread ulong *left, const thread ulong *right) {
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

inline bool stored_key_less(
    const device ulong *records,
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

inline bool stored_cross_key_less(
    const device ulong *left_records,
    ulong left_slot,
    const device ulong *right_records,
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

inline bool stored_thread_key_less(
    const device ulong *left_records,
    ulong left_slot,
    const thread ulong *right
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

inline void copy_thread_to_device(
    const thread ulong *source,
    device ulong *target,
    ulong slot
) {
    ulong offset = slot * EVENT_WORDS;
    for (uint word = 0; word < EVENT_WORDS; ++word) {
        target[offset + word] = source[word];
    }
}

inline void copy_device_to_thread(
    const device ulong *source,
    ulong slot,
    thread ulong *target
) {
    ulong offset = slot * EVENT_WORDS;
    for (uint word = 0; word < EVENT_WORDS; ++word) {
        target[word] = source[offset + word];
    }
}

inline void copy_device_record(
    const device ulong *source,
    ulong source_slot,
    device ulong *target,
    ulong target_slot
) {
    ulong source_offset = source_slot * EVENT_WORDS;
    ulong target_offset = target_slot * EVENT_WORDS;
    for (uint word = 0; word < EVENT_WORDS; ++word) {
        target[target_offset + word] = source[source_offset + word];
    }
}

inline void swap_records(device ulong *records, ulong left_slot, ulong right_slot) {
    ulong left = left_slot * EVENT_WORDS;
    ulong right = right_slot * EVENT_WORDS;
    for (uint word = 0; word < EVENT_WORDS; ++word) {
        ulong value = records[left + word];
        records[left + word] = records[right + word];
        records[right + word] = value;
    }
}

inline ulong saturating_add_ulong(ulong left, ulong right) {
    return right > NONE - left ? NONE : left + right;
}

inline void set_capacity_error(
    device ulong *error,
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

inline void set_semantic_error(device ulong *error, ulong code, ulong node) {
    if (error[L_ERROR] == 0) {
        error[L_ERROR] = ERROR_SEMANTIC + code;
        error[L_ERROR_NODE] = node;
    }
}

inline void set_wfq_arithmetic_error(device ulong *error, ulong node) {
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
inline ulong tcp_timer_slot_word(ulong flow, const device ulong *params) {
    return params[P_TCP_LEDGER_META_OFFSET] + flow * TCP_LEDGER_META_WORDS + 3;
}

inline ulong heap_timer_owner(
    const device ulong *params,
    const device ulong *tcp_state,
    const device ulong *records,
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

inline void heap_swap(
    const device ulong *params,
    device ulong *tcp_state,
    device ulong *records,
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

inline void heap_move(
    const device ulong *params,
    device ulong *tcp_state,
    device ulong *records,
    ulong source_slot,
    ulong target_slot
) {
    ulong owner = heap_timer_owner(params, tcp_state, records, source_slot);
    copy_device_record(records, source_slot, records, target_slot);
    if (owner != NONE) {
        tcp_state[tcp_timer_slot_word(owner, params)] = target_slot + 1;
    }
}

inline bool heap_push(
    ulong node,
    const thread ulong *record,
    device ulong *error,
    const device ulong *params,
    device ulong *meta,
    device ulong *records,
    device ulong *tcp_state
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

inline bool heap_pop(
    ulong node,
    const device ulong *params,
    device ulong *meta,
    device ulong *records,
    device ulong *tcp_state,
    thread ulong *record,
    thread ulong &timer_owner
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

inline bool heap_root_time(
    ulong node,
    const device ulong *meta,
    const device ulong *records,
    thread ulong &time
) {
    ulong base = node * META_WORDS;
    if (meta[base + 3] == 0) {
        return false;
    }
    time = records[meta[base] * EVENT_WORDS + E_TIME];
    return true;
}

inline ulong stream_arena(ulong stream, const device ulong *params) {
    if (stream < params[P_CHANNEL_COUNT]) {
        return ARENA_CHANNEL_INBOX;
    }
    if (stream < params[P_GENERATOR_STREAM_BASE]) {
        return ARENA_SERVICE_STREAM;
    }
    return ARENA_GENERATOR_STREAM;
}

inline bool active_add(
    ulong node,
    ulong stream,
    const thread ulong *record,
    const device ulong *params,
    device ulong *error,
    device ulong *stream_state
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

inline void active_update_key(
    ulong node,
    ulong active_index,
    const device ulong *params,
    device ulong *stream_state,
    const device ulong *records,
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

inline ulong active_find(
    ulong node,
    ulong stream,
    const device ulong *params,
    const device ulong *stream_state
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

inline bool active_refresh_source(
    ulong node,
    ulong stream,
    const device ulong *params,
    device ulong *error,
    device ulong *stream_state,
    const device ulong *records,
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

inline void active_remove(
    ulong node,
    ulong active_index,
    const device ulong *params,
    device ulong *stream_state
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
inline bool heap_remove_timer(
    ulong node,
    ulong flow,
    ulong attempt,
    ulong deadline,
    device ulong *error,
    const device ulong *params,
    device ulong *meta,
    device ulong *records,
    device ulong *stream_state,
    device ulong *tcp_state
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

inline bool stream_push(
    ulong node,
    ulong stream,
    const thread ulong *record,
    bool activate,
    const device ulong *params,
    device ulong *error,
    device ulong *stream_state,
    device ulong *stream_records
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
    RECORD_STREAM_HIGH_WATER(params, stream_state, stream, count + 1);
    return count != 0 || !activate ||
        active_add(node, stream, record, params, error, stream_state);
}

inline bool fallback_push(
    ulong node,
    const thread ulong *record,
    const device ulong *params,
    device ulong *error,
    device ulong *fel_meta,
    device ulong *fel_records,
    device ulong *stream_state,
    device ulong *tcp_state
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

inline bool classified_push(
    ulong node,
    const thread ulong *record,
    const device ulong *params,
    device ulong *error,
    device ulong *fel_meta,
    device ulong *fel_records,
    device ulong *stream_state,
    device ulong *stream_records,
    device ulong *tcp_state
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

inline bool active_key_less(
    const device ulong *stream_state,
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

inline bool fel_peek(
    ulong node,
    const device ulong *params,
    const device ulong *fel_meta,
    const device ulong *fel_records,
    const device ulong *stream_state,
    const device ulong *stream_records,
    thread ulong *record,
    thread ulong &selected_active,
    thread ulong &selected_stream
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

inline bool fel_root_time(
    ulong node,
    const device ulong *params,
    const device ulong *fel_meta,
    const device ulong *fel_records,
    const device ulong *stream_state,
    const device ulong *stream_records,
    thread ulong &time
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

inline bool fel_pop_selected(
    ulong node,
    ulong selected_active,
    ulong selected_stream,
    const device ulong *params,
    device ulong *fel_meta,
    device ulong *fel_records,
    device ulong *stream_state,
    device ulong *stream_records,
    device ulong *tcp_state,
    thread ulong *record,
    thread ulong &timer_owner
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

inline bool fel_pop(
    ulong node,
    const device ulong *params,
    device ulong *fel_meta,
    device ulong *fel_records,
    device ulong *stream_state,
    device ulong *stream_records,
    device ulong *tcp_state,
    thread ulong *record,
    thread ulong &timer_owner
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

inline bool before_horizon(ulong time, const device ulong *control) {
    return control[C_HORIZON_HI] != 0 || time < control[C_HORIZON_LO];
}

inline bool queue_push(
    ulong node,
    const thread ulong *record,
    device ulong *error,
    const device ulong *params,
    device ulong *meta,
    device ulong *records
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
    RECORD_QUEUE_HIGH_WATER(params, meta, node, count + 1);
    return true;
}

inline bool source_queue_key_less_or_equal(
    const device ulong *records,
    ulong left_slot,
    const thread ulong *right
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

inline bool source_queue_insert(
    ulong node,
    const thread ulong *record,
    device ulong *error,
    const device ulong *params,
    device ulong *meta,
    device ulong *records
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
    RECORD_QUEUE_HIGH_WATER(params, meta, node, count + 1);
    return true;
}

inline bool queue_pop(
    ulong node,
    device ulong *meta,
    const device ulong *records,
    thread ulong *record,
    thread ulong &physical
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

inline bool queue_remove_at(
    ulong node,
    ulong logical,
    device ulong *meta,
    device ulong *records,
    thread ulong *record,
    thread ulong &physical
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

inline bool queue_front(
    ulong node,
    const device ulong *meta,
    const device ulong *records,
    thread ulong *record
) {
    ulong base = node * QUEUE_META_WORDS;
    if (meta[base + 3] == 0) {
        return false;
    }
    copy_device_to_thread(records, meta[base] + meta[base + 2], record);
    return true;
}

inline ulong event_phase(ulong kind) {
    if (kind == TX_COMPLETE || kind == RETRANSMISSION_TIMEOUT || kind == PACING_TIMER) {
        return 1;
    }
    if (kind == TX_READY) {
        return 2;
    }
    return 0;
}

inline void add_summary(
    device ulong *summary,
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

inline bool append_observed(
    ulong node,
    const thread ulong *packet,
    device ulong *error,
    const device ulong *params,
    device ulong *observation_meta,
    device ulong *observed
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

inline bool record_sourced(
    ulong node,
    const thread ulong *packet,
    device ulong *error,
    const device ulong *params,
    device ulong *summary,
    device ulong *observation_meta,
    device ulong *observed
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

inline bool record_departure(
    ulong node,
    const thread ulong *event,
    device ulong *error,
    const device ulong *params,
    device ulong *summary,
    device ulong *observation_meta,
    device ulong *observed,
    device ulong *departures
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

inline bool record_arrival(
    ulong node,
    const thread ulong *event,
    ulong disposition,
    device ulong *error,
    const device ulong *params,
    device ulong *summary,
    device ulong *observation_meta,
    device ulong *observed,
    device ulong *arrivals
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

inline bool append_remote(
    ulong node,
    const thread ulong *record,
    device ulong *error,
    const device ulong *params,
    device ulong *remote_meta,
    device ulong *remote_staging,
    device ulong *stream_state
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
    RECORD_REMOTE_HIGH_WATER(params, remote_meta, node, index + 1);
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

inline bool build_child(
    ulong node,
    const thread ulong *parent,
    ulong target,
    ulong kind,
    ulong time,
    const thread ulong *packet,
    device ulong *error,
    device ulong *node_state,
    thread ulong *child
) {
    ulong node_base = node * NODE_WORDS;
    ulong sequence = node_state[node_base + N_NEXT_ORIGIN];
    if (sequence == NONE) {
        set_semantic_error(error, 1, node);
        return false;
    }
    node_state[node_base + N_NEXT_ORIGIN] = sequence + 1;
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
    return true;
}

inline bool thread_stored_key_less(
    const thread ulong *left,
    const device ulong *right
) {
    for (uint word = E_TIME; word <= E_SEQUENCE; ++word) {
        if (left[word] != right[word]) {
            return left[word] < right[word];
        }
    }
    return false;
}

// The live active-entry cache contains exactly one current head key for every non-empty stream
// plus the fallback heap root. Requiring the child to precede every cached key is therefore
// equivalent to the scalar `child.key < first_future_key` predicate. Heap-only mode compares the
// same child against the live post-pop heap root directly.
inline bool child_precedes_lp_next_key(
    ulong node,
    const thread ulong *child,
    const device ulong *params,
    const device ulong *fel_meta,
    const device ulong *fel_records,
    const device ulong *stream_state
) {
    if (params[P_STREAMS_ENABLED] == 0) {
        ulong heap = node * META_WORDS;
        return fel_meta[heap + 3] == 0 || thread_stored_key_less(
            child,
            fel_records + fel_meta[heap] * EVENT_WORDS
        );
    }
    ulong lp_meta = params[P_LP_STREAM_META_OFFSET] + node * LP_STREAM_META_WORDS;
    ulong active_offset = stream_state[lp_meta + 2];
    ulong active_count = stream_state[lp_meta + 3];
    for (ulong index = 0; index < active_count; ++index) {
        ulong entry = active_offset + index * ACTIVE_STREAM_ENTRY_WORDS;
        if (!thread_stored_key_less(child, stream_state + entry + 1)) {
            return false;
        }
    }
    return true;
}

inline bool is_same_time_tx_ready_continuation(
    ulong node,
    const thread ulong *parent,
    const thread ulong *child,
    const device ulong *params,
    const device ulong *fel_meta,
    const device ulong *fel_records,
    const device ulong *stream_state
) {
    return parent[E_KIND] == TX_COMPLETE &&
        child[E_TARGET] == node &&
        child[E_KIND] == TX_READY &&
        child[E_TIME] == parent[E_TIME] &&
        child[E_PHASE] == event_phase(TX_READY) &&
        child_precedes_lp_next_key(
            node,
            child,
            params,
            fel_meta,
            fel_records,
            stream_state
        );
}

inline bool emit_child(
    ulong node,
    const thread ulong *parent,
    ulong target,
    ulong kind,
    ulong time,
    const thread ulong *packet,
    device ulong *error,
    const device ulong *params,
    device ulong *node_state,
    device ulong *fel_meta,
    device ulong *fel_records,
    device ulong *remote_meta,
    device ulong *remote_staging,
    device ulong *stream_state,
    device ulong *stream_records,
    device ulong *tcp_state
) {
    ulong child[EVENT_WORDS];
    if (!build_child(
        node,
        parent,
        target,
        kind,
        time,
        packet,
        error,
        node_state,
        child
    )) {
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

inline bool checked_add(ulong left, ulong right, thread ulong &result) {
    result = left + right;
    return result >= left;
}

inline bool u128_add(
    ulong left_low,
    ulong left_high,
    ulong right_low,
    ulong right_high,
    thread ulong &result_low,
    thread ulong &result_high
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

inline bool u128_mul_u64(
    ulong left_low,
    ulong left_high,
    ulong right,
    thread ulong &result_low,
    thread ulong &result_high
) {
    if (left_high != 0 && mulhi(left_high, right) != 0) {
        return false;
    }
    result_low = left_low * right;
    ulong low_high = mulhi(left_low, right);
    ulong high_low = left_high * right;
    if (low_high > NONE - high_low) {
        return false;
    }
    result_high = low_high + high_low;
    return true;
}

inline bool u128_mul(
    ulong left_low,
    ulong left_high,
    ulong right_low,
    ulong right_high,
    thread ulong &result_low,
    thread ulong &result_high
) {
    if ((left_high != 0 && right_high != 0) ||
        mulhi(left_low, right_high) != 0 || mulhi(left_high, right_low) != 0) {
        return false;
    }
    result_low = left_low * right_low;
    ulong high = mulhi(left_low, right_low);
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

inline bool u128_from_mul_u64(
    ulong left,
    ulong right,
    thread ulong &result_low,
    thread ulong &result_high
) {
    result_low = left * right;
    result_high = mulhi(left, right);
    return true;
}

inline bool u128_at_least(
    ulong left_low,
    ulong left_high,
    ulong right_low,
    ulong right_high
) {
    return left_high > right_high || (left_high == right_high && left_low >= right_low);
}

inline void u128_sub(
    ulong left_low,
    ulong left_high,
    ulong right_low,
    ulong right_high,
    thread ulong &result_low,
    thread ulong &result_high
) {
    result_low = left_low - right_low;
    result_high = left_high - right_high - (left_low < right_low ? 1ul : 0ul);
}

inline void big_clear(thread uint *value, uint limbs) {
    for (uint limb = 0; limb < limbs; ++limb) {
        value[limb] = 0;
    }
}

inline bool big_is_zero(const thread uint *value, uint limbs) {
    for (uint limb = 0; limb < limbs; ++limb) {
        if (value[limb] != 0) {
            return false;
        }
    }
    return true;
}

inline void big_copy(
    const thread uint *source,
    thread uint *target,
    uint limbs
) {
    for (uint limb = 0; limb < limbs; ++limb) {
        target[limb] = source[limb];
    }
}

inline void big_load(
    const device ulong *source,
    ulong offset,
    thread uint *target
) {
    for (uint word = 0; word < 5; ++word) {
        ulong packed = source[offset + word];
        target[word * 2] = uint(packed);
        target[word * 2 + 1] = uint(packed >> 32);
    }
}

inline void big_store(
    const thread uint *source,
    device ulong *target,
    ulong offset
) {
    for (uint word = 0; word < 5; ++word) {
        target[offset + word] =
            ulong(source[word * 2]) | (ulong(source[word * 2 + 1]) << 32);
    }
}

inline int big_compare(
    const thread uint *left,
    const thread uint *right,
    uint limbs
) {
    for (int limb = int(limbs) - 1; limb >= 0; --limb) {
        if (left[limb] != right[limb]) {
            return left[limb] < right[limb] ? -1 : 1;
        }
    }
    return 0;
}

inline ulong big_remainder_u64(
    const thread uint *value,
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

inline ulong big_div_u64(
    const thread uint *value,
    uint limbs,
    ulong divisor,
    thread uint *quotient
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

inline uint big_div_u32(
    const thread uint *value,
    uint limbs,
    uint divisor,
    thread uint *quotient
) {
    ulong remainder = 0;
    for (int limb = int(limbs) - 1; limb >= 0; --limb) {
        ulong current = (remainder << 32) | ulong(value[uint(limb)]);
        quotient[uint(limb)] = uint(current / ulong(divisor));
        remainder = current % ulong(divisor);
    }
    return uint(remainder);
}

inline ulong gcd_u64(ulong left, ulong right) {
    while (right != 0) {
        ulong remainder = left % right;
        left = right;
        right = remainder;
    }
    return left;
}

inline void big_multiply(
    const thread uint *left,
    uint left_limbs,
    const thread uint *right,
    uint right_limbs,
    thread uint *product,
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

inline bool big_add(
    thread uint *target,
    const thread uint *addend,
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

inline void u64_limbs(ulong value, thread uint *limbs) {
    limbs[0] = uint(value);
    limbs[1] = uint(value >> 32);
}

inline void multiply_u64(ulong left, ulong right, thread uint *product) {
    uint left_limbs[2];
    uint right_limbs[2];
    u64_limbs(left, left_limbs);
    u64_limbs(right, right_limbs);
    big_multiply(left_limbs, 2, right_limbs, 2, product, 4);
}

inline ulong saturating_add_u64(ulong left, ulong right) {
    ulong result = left + right;
    return result < left ? NONE : result;
}

inline ulong saturating_mul_u64(ulong left, ulong right) {
    return left != 0 && right > NONE / left ? NONE : left * right;
}

inline ulong quotient_u64_saturating(const thread uint *quotient, uint limbs) {
    for (uint limb = 2; limb < limbs; ++limb) {
        if (quotient[limb] != 0) {
            return NONE;
        }
    }
    return ulong(quotient[0]) | (ulong(quotient[1]) << 32);
}

inline ulong mul_div_u64(ulong value, ulong numerator, ulong denominator) {
    uint product[4];
    uint quotient[4];
    multiply_u64(value, numerator, product);
    big_div_u64(product, 4, max(denominator, 1ul), quotient);
    return quotient_u64_saturating(quotient, 4);
}

inline ulong weighted_div_u64(ulong value, ulong multiplier, ulong addend, ulong denominator) {
    uint value_limbs[2];
    uint multiplier_limbs[2];
    uint product[4];
    u64_limbs(value, value_limbs);
    u64_limbs(multiplier, multiplier_limbs);
    big_multiply(value_limbs, 2, multiplier_limbs, 2, product, 4);
    ulong sum = ulong(product[0]) + ulong(uint(addend));
    product[0] = uint(sum);
    ulong carry = sum >> 32;
    sum = ulong(product[1]) + ulong(uint(addend >> 32)) + carry;
    product[1] = uint(sum);
    carry = sum >> 32;
    for (uint limb = 2; limb < 4 && carry != 0; ++limb) {
        sum = ulong(product[limb]) + carry;
        product[limb] = uint(sum);
        carry = sum >> 32;
    }
    uint quotient[4];
    big_div_u64(product, 4, max(denominator, 1ul), quotient);
    return quotient_u64_saturating(quotient, 4);
}

inline void cube_u64(ulong value, thread uint *cube) {
    uint square[4];
    uint value_limbs[2];
    multiply_u64(value, value, square);
    u64_limbs(value, value_limbs);
    big_multiply(square, 4, value_limbs, 2, cube, 6);
}

inline ulong cubic_k_ns(ulong w_max_scaled) {
    if (w_max_scaled == 0) {
        return 0;
    }
    // Wmax <= 2e15: the exact K radicand needs at most 111 bits. Candidate cubes need 192 bits.
    // Six 32-bit limbs therefore cover both without overflow (the inherited 320-bit machinery
    // leaves substantial headroom).
    uint first[4];
    multiply_u64(w_max_scaled, 3, first);
    uint scale_limbs[2];
    u64_limbs(1000000000000000000ul, scale_limbs);
    uint product[6];
    big_multiply(first, 4, scale_limbs, 2, product, 6);
    uint radicand[6];
    big_div_u64(product, 6, 4, radicand);

    ulong low = 0;
    ulong high = NONE;
    for (uint iteration = 0; iteration < 64; ++iteration) {
        ulong middle = low + (high - low) / 2;
        uint candidate[6];
        cube_u64(middle, candidate);
        if (big_compare(candidate, radicand, 6) <= 0) {
            low = middle;
        } else {
            high = middle;
        }
    }
    return low;
}

inline ulong cubic_magnitude(ulong distance) {
    // d^3 is 192 bits and the numerator 2*d^3 is 193 bits. Seven limbs cover the exact value;
    // division then saturates only when the scalar BigUint quotient exceeds u64.
    uint cube[6];
    cube_u64(distance, cube);
    uint doubled[7];
    big_clear(doubled, 7);
    uint carry = 0;
    for (uint limb = 0; limb < 6; ++limb) {
        uint next = cube[limb] >> 31;
        doubled[limb] = (cube[limb] << 1) | carry;
        carry = next;
    }
    doubled[6] = carry;
    // Split 5e18 into exact integral divisions. For non-negative integers,
    // nested floor divisions equal division by the product. Keeping each divisor
    // within 32 bits avoids a Metal compiler miscompile observed for larger
    // divisors in large CUBIC congestion-avoidance corpora.
    uint divided_by_five[7];
    uint divided_by_first_billion[7];
    uint quotient[7];
    big_div_u32(doubled, 7, 5, divided_by_five);
    big_div_u32(divided_by_five, 7, 1000000000, divided_by_first_billion);
    big_div_u32(divided_by_first_billion, 7, 1000000000, quotient);
    return quotient_u64_saturating(quotient, 7);
}

inline ulong cubic_window_scaled(ulong w_max_scaled, ulong k_ns, ulong elapsed_ns) {
    bool negative = elapsed_ns < k_ns;
    ulong distance = negative ? k_ns - elapsed_ns : elapsed_ns - k_ns;
    ulong magnitude = cubic_magnitude(distance);
    if (negative) {
        return max(w_max_scaled > magnitude ? w_max_scaled - magnitude : 0, CUBIC_SCALE);
    }
    return min(
        max(saturating_add_u64(w_max_scaled, magnitude), CUBIC_SCALE),
        CUBIC_MAX_WINDOW
    );
}

inline ulong tcp_friendly_window_scaled(
    ulong w_max_scaled,
    ulong elapsed_ns,
    ulong rtt_ns
) {
    ulong base = mul_div_u64(w_max_scaled, 7, 10);
    uint coefficient[2];
    uint elapsed[2];
    uint numerator[4];
    u64_limbs(CUBIC_SCALE * 9, coefficient);
    u64_limbs(elapsed_ns, elapsed);
    big_multiply(coefficient, 2, elapsed, 2, numerator, 4);
    uint by_seventeen[4];
    uint quotient[4];
    big_div_u64(numerator, 4, 17, by_seventeen);
    big_div_u64(by_seventeen, 4, max(rtt_ns, 1ul), quotient);
    ulong growth = quotient_u64_saturating(quotient, 4);
    return min(
        max(saturating_add_u64(base, growth), CUBIC_SCALE),
        CUBIC_MAX_WINDOW
    );
}

inline ulong cubic_ack_step(ulong cwnd, ulong target) {
    ulong distance = target >= cwnd ? target - cwnd : cwnd - target;
    ulong delta = mul_div_u64(distance, CUBIC_SCALE, max(cwnd, CUBIC_SCALE));
    return target >= cwnd
        ? saturating_add_u64(cwnd, delta)
        : (cwnd > delta ? cwnd - delta : 0);
}

inline ulong controller_cwnd_bytes(const thread ulong *control) {
    return control[CTL_KIND] == 0
        ? control[CTL_CWND]
        : mul_div_u64(control[CTL_CWND], max(control[CTL_MSS], 1ul), CUBIC_SCALE);
}

inline void controller_recovery_exit(thread ulong *control) {
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

inline void controller_fast_retransmit(
    thread ulong *control,
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
        control[CTL_EXTRA_0] = previous_max > 0 && current < previous_max
            ? mul_div_u64(current, 17, 20)
            : current;
        ulong flight_scaled = min(
            mul_div_u64(flight, CUBIC_SCALE, max(control[CTL_MSS], 1ul)),
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

inline bool controller_duplicate_ack(thread ulong *control, ulong flight, ulong now_ns) {
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

inline void controller_new_ack(
    thread ulong *control,
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
        } else if (
            control[CTL_RECOVERY_HIGH] != 0 &&
            acknowledgment >= control[CTL_RECOVERY_HIGH]
        ) {
            controller_recovery_exit(control);
        } else {
            control[CTL_CWND] = saturating_add_u64(control[CTL_SSTHRESH], control[CTL_MSS]);
        }
        return;
    }

    ulong sample = max(rtt_sample, 1ul);
    control[CTL_SRTT] = control[CTL_SRTT] == 0
        ? sample
        : weighted_div_u64(control[CTL_SRTT], 7, sample, 8);
    if (control[CTL_PHASE] == TCP_SLOW_START) {
        ulong mss = max(control[CTL_MSS], 1ul);
        ulong segments = acknowledged_bytes / mss + ulong(acknowledged_bytes % mss != 0);
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
                control[CTL_EXTRA_0],
                control[CTL_K],
                saturating_add_u64(elapsed, max(control[CTL_SRTT], 1ul))
            );
            control[CTL_CWND] = min(
                max(cubic_ack_step(control[CTL_CWND], target), CUBIC_SCALE),
                CUBIC_MAX_WINDOW
            );
        }
    } else if (
        control[CTL_RECOVERY_HIGH] != 0 &&
        acknowledgment >= control[CTL_RECOVERY_HIGH]
    ) {
        controller_recovery_exit(control);
    }
}

inline void controller_timeout(thread ulong *control, ulong flight) {
    if (control[CTL_KIND] == 0) {
        control[CTL_SSTHRESH] = max(flight / 2, saturating_mul_u64(control[CTL_MSS], 2));
        control[CTL_CWND] = control[CTL_MSS];
        control[CTL_EXTRA_0] = 0;
    } else {
        ulong flight_scaled = min(
            mul_div_u64(flight, CUBIC_SCALE, max(control[CTL_MSS], 1ul)),
            CUBIC_MAX_WINDOW
        );
        control[CTL_SSTHRESH] = min(
            max(mul_div_u64(flight_scaled, 7, 10), 2 * CUBIC_SCALE),
            CUBIC_MAX_WINDOW
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

inline ulong update_rto(thread ulong &srtt, thread ulong &rtt_var, ulong sample) {
    sample = max(sample, 1ul);
    if (srtt == 0) {
        srtt = sample;
        rtt_var = sample / 2;
    } else {
        ulong deviation = srtt > sample ? srtt - sample : sample - srtt;
        rtt_var = weighted_div_u64(rtt_var, 3, deviation, 4);
        srtt = weighted_div_u64(srtt, 7, sample, 8);
    }
    ulong variance = max(saturating_mul_u64(rtt_var, 4), TCP_RTO_GRANULARITY);
    return min(max(saturating_add_u64(srtt, variance), TCP_MIN_RTO), TCP_MAX_RTO);
}

inline bool rational_compare(
    const thread uint *left_num,
    const thread uint *left_den,
    const thread uint *right_num,
    const thread uint *right_den,
    thread int &ordering
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

inline void rational_load(
    const device ulong *state,
    ulong offset,
    thread uint *numerator,
    thread uint *denominator
) {
    big_load(state, offset, numerator);
    big_load(state, offset + 5, denominator);
}

inline void rational_store(
    const thread uint *numerator,
    const thread uint *denominator,
    device ulong *state,
    ulong offset
) {
    big_store(numerator, state, offset);
    big_store(denominator, state, offset + 5);
}

inline void rational_zero(device ulong *state, ulong offset) {
    for (uint word = 0; word < RATIONAL_WORDS; ++word) {
        state[offset + word] = 0;
    }
    state[offset + 5] = 1;
}

inline void rational_copy(
    device ulong *state,
    ulong source,
    ulong target
) {
    for (uint word = 0; word < RATIONAL_WORDS; ++word) {
        state[target + word] = state[source + word];
    }
}

inline bool rational_add_small(
    const thread uint *input_num,
    const thread uint *input_den,
    const thread uint *small_num_input,
    ulong small_den_input,
    thread uint *output_num,
    thread uint *output_den
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

inline bool serialization_ns(
    ulong bytes,
    ulong rate,
    thread ulong &result
) {
    if (rate == 0) {
        return false;
    }
    ulong scale = 8000000000ul;
    ulong high = mulhi(bytes, scale);
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

inline bool scheduler_active_weight_sum(
    ulong node_base,
    const device ulong *scheduler_state,
    thread ulong &weight_sum
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

inline bool scheduler_first_packet_for_class(
    ulong node,
    ulong class_count,
    ulong class_index,
    const device ulong *queue_meta,
    const device ulong *queue_records,
    thread ulong &position,
    thread ulong &size
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

inline bool drr_select_position(
    ulong node,
    device ulong *error,
    const device ulong *queue_meta,
    const device ulong *queue_records,
    device ulong *scheduler_state,
    thread ulong &position
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

inline bool wrr_select_position(
    ulong node,
    device ulong *error,
    const device ulong *queue_meta,
    const device ulong *queue_records,
    device ulong *scheduler_state,
    thread ulong &position
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

inline bool switch_admission_action(
    ulong node,
    const thread ulong *packet,
    ulong taildrop_capacity,
    device ulong *error,
    const device ulong *queue_meta,
    const device ulong *scheduler_state,
    thread ulong &action
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

inline void local_rational_zero(thread uint *numerator, thread uint *denominator) {
    big_clear(numerator, BIG_LIMBS);
    big_clear(denominator, BIG_LIMBS);
    denominator[0] = 1;
}

inline bool wfq_advanced_virtual_time(
    ulong node,
    ulong time,
    ulong rate,
    device ulong *error,
    device ulong *scheduler_state,
    thread uint *numerator,
    thread uint *denominator
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

inline bool sp_queue_insert(
    ulong node,
    const thread ulong *record,
    device ulong *error,
    const device ulong *params,
    device ulong *queue_meta,
    device ulong *queue_records,
    const device ulong *scheduler_state
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
    RECORD_QUEUE_HIGH_WATER(params, queue_meta, node, count + 1);
    return true;
}

inline bool wfq_queue_insert(
    ulong node,
    const thread ulong *record,
    const thread uint *finish_num,
    const thread uint *finish_den,
    device ulong *error,
    const device ulong *params,
    device ulong *queue_meta,
    device ulong *queue_records,
    device ulong *scheduler_state
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
    RECORD_QUEUE_HIGH_WATER(params, queue_meta, node, count + 1);
    return true;
}

inline bool wfq_enqueue(
    ulong node,
    const thread ulong *record,
    ulong rate,
    device ulong *error,
    const device ulong *params,
    device ulong *queue_meta,
    device ulong *queue_records,
    device ulong *scheduler_state
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
            params,
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

inline bool wfq_complete(
    ulong node,
    const thread ulong *record,
    ulong rate,
    device ulong *error,
    device ulong *scheduler_state
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

inline bool flow_route(
    const thread ulong *packet,
    const device ulong *flows,
    thread ulong &offset,
    thread ulong &length,
    thread ulong &terminal
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

inline bool packet_egress(
    ulong node,
    const thread ulong *packet,
    const device ulong *flows,
    const device ulong *routes,
    const device ulong *links,
    thread ulong &egress
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

inline bool packet_remote_target(
    const thread ulong *packet,
    ulong egress,
    const device ulong *flows,
    const device ulong *routes,
    const device ulong *links,
    thread ulong &target
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

inline void packet_clear(thread ulong *packet) {
    for (uint word = 0; word < EVENT_WORDS; ++word) {
        packet[word] = 0;
    }
}

inline bool allocate_tcp_payload(
    ulong node,
    device ulong *node_state,
    const device ulong *params,
    thread ulong &payload
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

inline void tcp_record_copy(const thread ulong *packet, device ulong *target) {
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
inline ulong tcp_ledger_slot(
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
inline ulong tcp_ledger_lower_bound(
    ulong offset,
    ulong capacity,
    ulong head,
    ulong count,
    ulong sequence,
    const device ulong *tcp_state
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

inline bool tcp_ledger_find(
    ulong flow,
    ulong sequence,
    const device ulong *params,
    const device ulong *tcp_state,
    thread ulong &record
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

inline bool tcp_ledger_insert(
    ulong flow,
    const thread ulong *packet,
    device ulong *error,
    const device ulong *params,
    device ulong *tcp_state
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

inline bool tcp_ledger_acknowledge(
    ulong flow,
    ulong acknowledgment,
    device ulong *error,
    const device ulong *params,
    device ulong *tcp_state
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

inline bool tcp_receive_range(
    ulong flow,
    ulong start,
    ulong end,
    device ulong *error,
    const device ulong *params,
    device ulong *tcp_state
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

inline bool tcp_enqueue_attempt(
    ulong node,
    ulong flow,
    ulong sequence,
    ulong size_bytes,
    ulong now_ns,
    bool retransmission,
    const thread ulong *parent,
    device ulong *error,
    const device ulong *params,
    device ulong *node_state,
    device ulong *generators,
    device ulong *queue_meta,
    device ulong *queue_records,
    device ulong *summary,
    device ulong *observation_meta,
    device ulong *observed,
    device ulong *tcp_state
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
    if (
        !tcp_ledger_insert(flow, packet, error, params, tcp_state) ||
        !source_queue_insert(node, packet, error, params, queue_meta, queue_records) ||
        !record_sourced(node, packet, error, params, summary, observation_meta, observed)
    ) {
        return false;
    }
    ulong generator = flow * GENERATOR_WORDS;
    generators[generator + G_TCP_LAST_ATTEMPT] = payload;
    if (!retransmission) {
        if (
            generators[generator + G_PACKETS] == NONE ||
            generators[generator + G_BYTES] > NONE - size_bytes
        ) {
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

inline bool prepare_tcp_attempts(
    ulong node,
    ulong flow,
    const thread ulong *parent,
    bool retransmit,
    ulong retransmit_sequence,
    bool fill_window,
    bool preserve_scheduled_send,
    device ulong *error,
    const device ulong *params,
    device ulong *node_state,
    device ulong *generators,
    device ulong *fel_meta,
    device ulong *fel_records,
    device ulong *queue_meta,
    device ulong *queue_records,
    device ulong *remote_meta,
    device ulong *remote_staging,
    device ulong *stream_state,
    device ulong *stream_records,
    device ulong *summary,
    device ulong *observation_meta,
    device ulong *observed,
    device ulong *tcp_state
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
            node,
            flow,
            retransmit_sequence,
            size,
            parent[E_TIME],
            true,
            parent,
            error,
            params,
            node_state,
            generators,
            queue_meta,
            queue_records,
            summary,
            observation_meta,
            observed,
            tcp_state
        )) {
            return false;
        }
    }

    if (fill_window) {
        while (true) {
            ulong control[12];
            for (uint word = 0; word < 12; ++word) {
                control[word] = generators[generator + G_CONTROL + word];
            }
            ulong cwnd = controller_cwnd_bytes(control);
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
                node,
                flow,
                sequence,
                size,
                parent[E_TIME],
                false,
                parent,
                error,
                params,
                node_state,
                generators,
                queue_meta,
                queue_records,
                summary,
                observation_meta,
                observed,
                tcp_state
            )) {
                return false;
            }
        }
    }

    generators[generator + G_OUTSTANDING] = generators[generator + G_TCP_FLIGHT];
    generators[generator + G_UNACKNOWLEDGED] = generators[generator + G_TCP_FLIGHT];
    if (!preserve_scheduled_send) {
        generators[generator + G_STATUS] =
            generators[generator + G_TCP_HIGHEST_ACK] >= generators[generator + G_TCP_TOTAL]
                ? 2
                : 1;
    }

    bool install_timer =
        !preserve_scheduled_send &&
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
        generators[generator + G_TCP_TIMER_ATTEMPT] =
            generators[generator + G_TCP_LAST_ATTEMPT];
        generators[generator + G_TCP_TIMER_SEQUENCE] =
            generators[generator + G_TCP_HIGHEST_ACK];
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
            node,
            parent,
            node,
            RETRANSMISSION_TIMEOUT,
            deadline,
            timer_packet,
            error,
            params,
            node_state,
            fel_meta,
            fel_records,
            remote_meta,
            remote_staging,
            stream_state,
            stream_records,
            tcp_state
        )) {
            return false;
        }
    }

    ulong node_base = node * NODE_WORDS;
    if (
        queue_meta[node * QUEUE_META_WORDS + 3] != 0 &&
        node_state[node_base + N_SERVICE_VALID] == 0 &&
        node_state[node_base + N_READY_PENDING] == 0
    ) {
        ulong ready[EVENT_WORDS];
        if (!queue_front(node, queue_meta, queue_records, ready)) {
            set_semantic_error(error, 51, node);
            return false;
        }
        node_state[node_base + N_READY_PENDING] = 1;
        return emit_child(
            node,
            parent,
            node,
            TX_READY,
            parent[E_TIME],
            ready,
            error,
            params,
            node_state,
            fel_meta,
            fel_records,
            remote_meta,
            remote_staging,
            stream_state,
            stream_records,
            tcp_state
        );
    }
    return true;
}

inline bool dispatch_event(
    ulong node,
    thread ulong *event,
    ulong popped_timer_owner,
    device ulong *error,
    const device ulong *params,
    device ulong *node_state,
    device ulong *generators,
    const device ulong *flows,
    const device ulong *routes,
    const device ulong *links,
    device ulong *fel_meta,
    device ulong *fel_records,
    device ulong *queue_meta,
    device ulong *queue_records,
    device ulong *in_service,
    device ulong *scheduler_state,
    device ulong *remote_meta,
    device ulong *remote_staging,
    device ulong *stream_state,
    device ulong *stream_records,
    device ulong *summary,
    device ulong *observation_meta,
    device ulong *observed,
    device ulong *departures,
    device ulong *arrivals,
    device ulong *tcp_state,
    bool retain_continuation,
    thread bool &has_continuation,
    thread bool &counted_continuation
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
        if (!source_queue_insert(
                node, sourced_packet, error, params, queue_meta, queue_records) ||
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
            if (
                generators[generator_base + G_STATUS] != 0 ||
                generators[generator_base + G_DEPARTURE] != event[E_TIME] ||
                generators[generator_base + G_PAYLOAD] != event[PK_ID] ||
                (event[PK_KIND] & PK_KIND_MASK) != TCP_DATA_PACKET ||
                event[PK_META_0] != generators[generator_base + G_TCP_NEXT] ||
                event[PK_META_1] != event[E_TIME] ||
                event[PK_META_2] != 0
            ) {
                set_semantic_error(error, 52, node);
                return false;
            }
            if (
                generators[generator_base + G_PACKETS] == NONE ||
                generators[generator_base + G_BYTES] > NONE - event[PK_SIZE] ||
                generators[generator_base + G_TCP_NEXT] > NONE - event[PK_SIZE] ||
                generators[generator_base + G_TCP_FLIGHT] > NONE - event[PK_SIZE] ||
                node_state[node_base + N_COUNTER_0] == NONE
            ) {
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
            if (
                !tcp_ledger_insert(event[PK_FLOW], event, error, params, tcp_state) ||
                !source_queue_insert(
                    node, sourced_packet, error, params, queue_meta, queue_records) ||
                !record_sourced(
                    node,
                    event,
                    error,
                    params,
                    summary,
                    observation_meta,
                    observed
                )
            ) {
                return false;
            }
            return prepare_tcp_attempts(
                node,
                event[PK_FLOW],
                event,
                false,
                0,
                true,
                false,
                error,
                params,
                node_state,
                generators,
                fel_meta,
                fel_records,
                queue_meta,
                queue_records,
                remote_meta,
                remote_staging,
                stream_state,
                stream_records,
                summary,
                observation_meta,
                observed,
                tcp_state
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
                    tcp_state
                )) {
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
            params,
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
                tcp_state
            )) {
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
            tcp_state
        )) {
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
            tcp_state
        );
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
            ulong child[EVENT_WORDS];
            if (!build_child(
                node,
                event,
                node,
                TX_READY,
                event[E_TIME],
                next,
                error,
                node_state,
                child
            )) {
                return false;
            }
            counted_continuation = is_same_time_tx_ready_continuation(
                node,
                event,
                child,
                params,
                fel_meta,
                fel_records,
                stream_state
            );
            if (counted_continuation && retain_continuation) {
                for (uint word = 0; word < EVENT_WORDS; ++word) {
                    event[word] = child[word];
                }
                has_continuation = true;
                return true;
            }
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
            inserted = queue_push(node, event, error, params, queue_meta, queue_records);
        } else if (scheduler_kind == SCHED_SP) {
            inserted = sp_queue_insert(
                node,
                event,
                error,
                params,
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
                params,
                queue_meta,
                queue_records,
                scheduler_state
            );
        } else if (scheduler_kind == SCHED_DRR || scheduler_kind == SCHED_WRR) {
            inserted = queue_push(node, event, error, params, queue_meta, queue_records);
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
                tcp_state
            );
        }
        return true;
    }

    if (
        kind == REMOTE_ARRIVAL &&
        role == HOST &&
        (event[PK_KIND] & PK_KIND_MASK) == TCP_DATA_PACKET
    ) {
        ulong flow = event[PK_FLOW];
        ulong flow_base = flow * FLOW_WORDS;
        ulong receiver = params[P_TCP_RECEIVER_OFFSET] + flow * TCP_RECEIVER_WORDS;
        if (
            flow >= params[P_FLOW_COUNT] ||
            flows[flow_base + 1] != node ||
            tcp_state[receiver] == 0 ||
            tcp_state[receiver + 1] != node ||
            event[PK_META_0] > NONE - event[PK_SIZE]
        ) {
            set_semantic_error(error, 54, node);
            return false;
        }
        if (!tcp_receive_range(
            flow,
            event[PK_META_0],
            event[PK_META_0] + event[PK_SIZE],
            error,
            params,
            tcp_state
        )) {
            return false;
        }
        ulong ack_payload;
        if (
            !allocate_tcp_payload(node, node_state, params, ack_payload) ||
            node_state[node_base + N_COUNTER_2] == NONE ||
            node_state[node_base + N_COUNTER_0] == NONE
        ) {
            set_semantic_error(error, 55, node);
            return false;
        }
        node_state[node_base + N_COUNTER_2] += 1;
        node_state[node_base + N_COUNTER_0] += 1;
        if (!record_arrival(
            node,
            event,
            2,
            error,
            params,
            summary,
            observation_meta,
            observed,
            arrivals
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
        if (
            !source_queue_insert(node, ack, error, params, queue_meta, queue_records) ||
            !record_sourced(node, ack, error, params, summary, observation_meta, observed)
        ) {
            return false;
        }
        if (
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
                ack,
                error,
                params,
                node_state,
                fel_meta,
                fel_records,
                remote_meta,
                remote_staging,
                stream_state,
                stream_records,
                tcp_state
            );
        }
        return true;
    }

    if (
        kind == REMOTE_ARRIVAL &&
        role == HOST &&
        (event[PK_KIND] & PK_KIND_MASK) == TCP_ACK_PACKET
    ) {
        ulong flow = event[PK_FLOW];
        ulong flow_base = flow * FLOW_WORDS;
        ulong generator = flow * GENERATOR_WORDS;
        if (
            flow >= params[P_FLOW_COUNT] ||
            flows[flow_base] != node ||
            generators[generator + G_VALID] == 0 ||
            generators[generator + G_OWNER] != node ||
            generators[generator + G_KIND] != 1
        ) {
            set_semantic_error(error, 56, node);
            return false;
        }
        if (!record_arrival(
            node,
            event,
            3,
            error,
            params,
            summary,
            observation_meta,
            observed,
            arrivals
        )) {
            return false;
        }
        if (generators[generator + G_FEEDBACK] == NONE) {
            set_semantic_error(error, 57, node);
            return false;
        }
        generators[generator + G_FEEDBACK] += 1;
        ulong acknowledgment = min(event[PK_META_0], generators[generator + G_TCP_NEXT]);
        ulong control[12];
        for (uint word = 0; word < 12; ++word) {
            control[word] = generators[generator + G_CONTROL + word];
        }
        bool processed_ack = false;
        bool retransmit = false;
        ulong retransmit_sequence = 0;
        bool fill = false;
        bool acknowledged_new = false;
        ulong flight_before = generators[generator + G_TCP_FLIGHT];

        if (acknowledgment > generators[generator + G_TCP_HIGHEST_ACK]) {
            ulong acknowledged_bytes =
                acknowledgment - generators[generator + G_TCP_HIGHEST_ACK];
            ulong rtt_sample = event[E_TIME] >= event[PK_META_2]
                ? max(event[E_TIME] - event[PK_META_2], 1ul)
                : 1;
            ulong srtt = generators[generator + G_TCP_SRTT];
            ulong rtt_var = generators[generator + G_TCP_RTTVAR];
            generators[generator + G_TCP_RTO] = update_rto(srtt, rtt_var, rtt_sample);
            generators[generator + G_TCP_SRTT] = srtt;
            generators[generator + G_TCP_RTTVAR] = rtt_var;
            controller_new_ack(
                control,
                acknowledged_bytes,
                event[E_TIME],
                rtt_sample,
                acknowledgment
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
            if (
                control[CTL_PHASE] == TCP_FAST_RECOVERY &&
                acknowledgment < generators[generator + G_TCP_RECOVERY_HIGH]
            ) {
                retransmit = true;
                retransmit_sequence = acknowledgment;
            }
            fill = acknowledgment < generators[generator + G_TCP_TOTAL];
            acknowledged_new = true;
            processed_ack = true;
        } else if (
            acknowledgment == generators[generator + G_TCP_HIGHEST_ACK] &&
            acknowledgment < generators[generator + G_TCP_TOTAL] &&
            flight_before != 0
        ) {
            ulong recovery_high = generators[generator + G_TCP_NEXT];
            bool fast = controller_duplicate_ack(control, flight_before, event[E_TIME]);
            generators[generator + G_TCP_DUP_ACKS] = control[CTL_DUP_ACKS];
            if (fast) {
                generators[generator + G_TCP_RECOVERY_HIGH] = recovery_high;
                control[CTL_RECOVERY_HIGH] = recovery_high;
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
            processed_ack = true;
        }
        for (uint word = 0; word < 12; ++word) {
            generators[generator + G_CONTROL + word] = control[word];
        }
        generators[generator + G_OUTSTANDING] = generators[generator + G_TCP_FLIGHT];
        generators[generator + G_UNACKNOWLEDGED] = generators[generator + G_TCP_FLIGHT];
        if (
            acknowledged_new &&
            !tcp_ledger_acknowledge(flow, acknowledgment, error, params, tcp_state)
        ) {
            return false;
        }
        if (!processed_ack) {
            return true;
        }
        bool scheduled = generators[generator + G_STATUS] == 0;
        if (scheduled && !retransmit) {
            return true;
        }
        return prepare_tcp_attempts(
            node,
            flow,
            event,
            retransmit,
            retransmit_sequence,
            fill && !scheduled,
            scheduled,
            error,
            params,
            node_state,
            generators,
            fel_meta,
            fel_records,
            queue_meta,
            queue_records,
            remote_meta,
            remote_staging,
            stream_state,
            stream_records,
            summary,
            observation_meta,
            observed,
            tcp_state
        );
    }

    if (kind == RETRANSMISSION_TIMEOUT && role == HOST) {
        ulong flow = event[PK_FLOW];
        ulong generator = flow * GENERATOR_WORDS;
        bool armed =
            flow < params[P_FLOW_COUNT] &&
            generators[generator + G_VALID] != 0 &&
            generators[generator + G_OWNER] == node &&
            generators[generator + G_KIND] == 1 &&
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
        ulong control[12];
        for (uint word = 0; word < 12; ++word) {
            control[word] = generators[generator + G_CONTROL + word];
        }
        ulong flight = generators[generator + G_TCP_FLIGHT];
        controller_timeout(control, flight);
        for (uint word = 0; word < 12; ++word) {
            generators[generator + G_CONTROL + word] = control[word];
        }
        generators[generator + G_TCP_RTO] = min(
            saturating_mul_u64(generators[generator + G_TCP_TIMER_RTO], 2),
            TCP_MAX_RTO
        );
        return prepare_tcp_attempts(
            node,
            flow,
            event,
            true,
            generators[generator + G_TCP_HIGHEST_ACK],
            false,
            false,
            error,
            params,
            node_state,
            generators,
            fel_meta,
            fel_records,
            queue_meta,
            queue_records,
            remote_meta,
            remote_staging,
            stream_state,
            stream_records,
            summary,
            observation_meta,
            observed,
            tcp_state
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
                    ? flows[flow_base + 1]
                    : flows[flow_base];
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

// T21 fix 2 — per-round FEL root cache accessors. Transliterated word for word from
// `cuda_kernels.cu`; see that file's header for the read-set argument that makes the cache exact.
static inline ulong round_scratch_cache(const device ulong *params) {
    return params[P_ROUND_SCRATCH_OFFSET];
}

static inline void store_fel_root(
    device ulong *stream_state,
    const device ulong *params,
    ulong node,
    bool present,
    ulong time
) {
    ulong slot = round_scratch_cache(params) + node * ROUND_SCRATCH_CACHE_WORDS;
    stream_state[slot] = present ? time : 0;
    stream_state[slot + 1] = present ? 1 : 0;
}

static inline bool load_fel_root(
    const device ulong *stream_state,
    const device ulong *params,
    ulong node,
    thread ulong &time
) {
    ulong slot = round_scratch_cache(params) + node * ROUND_SCRATCH_CACHE_WORDS;
    time = stream_state[slot];
    return stream_state[slot + 1] != 0;
}

// T21 fix 1 — the per-block reduction partials, `CONTROL_SWEEP_BLOCKS * ROUND_SCRATCH_PARTIAL_WORDS`
// words appended after the FEL-root cache in the same scratch region. Transliterated word for word
// from `cuda_kernels.cu`.
//
// A sweep threadgroup writes ONLY its own row, and only from lane 0 after that threadgroup's own
// `threadgroup_barrier`. A combine reads rows written by a PREVIOUS dispatch. There is therefore
// no cross-threadgroup ordering to establish: the dispatch boundary establishes it. These are
// plain `device ulong` accesses — deliberately NOT `volatile`, which is one of the two suspects
// `evidence/P12/aterm-fixes.md` §3.3 named.
static inline ulong round_scratch_partials(const device ulong *params) {
    return params[P_ROUND_SCRATCH_OFFSET] +
        params[P_NODE_COUNT] * ROUND_SCRATCH_CACHE_WORDS;
}

static inline void store_round_partial(
    device ulong *stream_state,
    const device ulong *params,
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

static inline ulong load_round_partial(
    const device ulong *stream_state,
    const device ulong *params,
    ulong block,
    ulong slot
) {
    return stream_state[
        round_scratch_partials(params) +
        block * ROUND_SCRATCH_PARTIAL_WORDS +
        slot
    ];
}

// O1.4 — one count per threadgroup in the widest configurable per-LP grid. The host sizes this
// tail to max(P_NODE_COUNT, 1), because one thread per threadgroup is the widest supported
// geometry. Dispatch A writes every launched slot; Dispatch B reads only those values.
static inline ulong round_scratch_compaction_counts(const device ulong *params) {
    return round_scratch_partials(params) +
        CONTROL_SWEEP_BLOCKS * ROUND_SCRATCH_PARTIAL_WORDS;
}

static inline void store_compaction_count(
    device ulong *stream_state,
    const device ulong *params,
    ulong block,
    ulong value
) {
    stream_state[round_scratch_compaction_counts(params) + block] = value;
}

static inline ulong load_compaction_count(
    const device ulong *stream_state,
    const device ulong *params,
    ulong block
) {
    return stream_state[round_scratch_compaction_counts(params) + block];
}

// T21 fix 1 — the horizon's Θ(N) FEL-root sweep, on the whole grid. Transliterated word for word
// from `cuda_kernels.cu`; see that kernel's header for the partition-invariance argument.
kernel void days_horizon_sweep(
    const device ulong * __restrict control [[buffer(0)]],
    const device ulong * __restrict params [[buffer(1)]],
    const device ulong * __restrict fel_meta [[buffer(7)]],
    const device ulong * __restrict fel_records [[buffer(8)]],
    device ulong * __restrict stream_state [[buffer(25)]],
    const device ulong * __restrict stream_records [[buffer(26)]],
    uint lane [[thread_index_in_threadgroup]],
    uint block [[threadgroup_position_in_grid]],
    uint block_size [[threads_per_threadgroup]],
    uint blocks [[threadgroups_per_grid]]
) {
    threadgroup ulong minima[1024];
    threadgroup uint validity[1024];
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 0 ||
        control[C_ROUNDS] >= params[P_ROUND_CAPACITY]
    ) {
        return;
    }
    ulong first = ulong(block) * ulong(block_size) + ulong(lane);
    ulong stride = ulong(blocks) * ulong(block_size);
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
    threadgroup_barrier(mem_flags::mem_threadgroup);
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
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    if (lane == 0) {
        store_round_partial(stream_state, params, block, 0, minima[0]);
        store_round_partial(stream_state, params, block, 1, ulong(validity[0]));
    }
}

// T21 fix 1 — the horizon combine. Transliterated word for word from `cuda_kernels.cu`.
kernel void days_horizon(
    device ulong * __restrict control [[buffer(0)]],
    const device ulong * __restrict params [[buffer(1)]],
    const device ulong * __restrict stream_state [[buffer(25)]],
    uint lane [[thread_index_in_threadgroup]]
) {
    threadgroup ulong minima[1024];
    threadgroup uint validity[1024];
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
    threadgroup_barrier(mem_flags::mem_threadgroup);
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
        threadgroup_barrier(mem_flags::mem_threadgroup);
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

// O1.4 dispatch A — `days_round_prepare`'s Θ(N) + Θ(C) resets plus one eligible count per
// configurable per-LP threadgroup. Transliterated from CUDA; see that kernel for the guard-snapshot
// and disjointness arguments.
kernel void days_round_reset(
    const device ulong * __restrict control [[buffer(0)]],
    const device ulong * __restrict params [[buffer(1)]],
    device ulong * __restrict lp_state [[buffer(18)]],
    device ulong * __restrict remote_meta [[buffer(19)]],
    device ulong * __restrict stream_state [[buffer(25)]],
    uint lane [[thread_index_in_threadgroup]],
    uint block [[threadgroup_position_in_grid]],
    uint block_size [[threads_per_threadgroup]],
    uint blocks [[threadgroups_per_grid]]
) {
    threadgroup ulong counts[1024];
    bool enabled = !(
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 0 ||
        control[C_ROUNDS] >= params[P_ROUND_CAPACITY]
    );
    if (!enabled) {
        if (lane == 0) {
            store_compaction_count(stream_state, params, block, NONE);
        }
        return;
    }

    ulong first = ulong(block) * ulong(block_size) + ulong(lane);
    ulong stride = ulong(blocks) * ulong(block_size);
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
    ulong local_count = 0;
    if (first < params[P_NODE_COUNT]) {
        ulong node = first;
        ulong state = node * LP_STATE_WORDS;
        lp_state[state + L_ERROR] = 0;
        // L_ERROR_ARENA is tagged as the cumulative continuation count while L_ERROR is zero.
        // A capacity error overwrites it and terminates the discarded attempt.
        lp_state[state + L_ERROR_NODE] = NONE;
        lp_state[state + L_ERROR_CAPACITY] = 0;
        lp_state[state + L_ERROR_DEMAND] = 0;
        remote_meta[node * META_WORDS + 3] = 0;
        ulong time;
        local_count = ulong(
            load_fel_root(stream_state, params, node, time) &&
            before_horizon(time, control)
        );
    }

    counts[lane] = local_count;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint offset = 1; offset < block_size; offset <<= 1) {
        ulong addend = lane >= offset ? counts[lane - offset] : 0;
        threadgroup_barrier(mem_flags::mem_threadgroup);
        counts[lane] += addend;
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    if (lane + 1 == block_size) {
        store_compaction_count(stream_state, params, block, counts[lane]);
    }
}

// O1.4 dispatch B — stable write from counts and the guard snapshot published by Dispatch A.
kernel void days_round_prepare(
    device ulong * __restrict control [[buffer(0)]],
    const device ulong * __restrict params [[buffer(1)]],
    const device ulong * __restrict fel_meta [[buffer(7)]],
    const device ulong * __restrict fel_records [[buffer(8)]],
    device ulong * __restrict worklist [[buffer(13)]],
    device ulong * __restrict lp_state [[buffer(18)]],
    device ulong * __restrict remote_meta [[buffer(19)]],
    device ulong * __restrict stream_state [[buffer(25)]],
    const device ulong * __restrict stream_records [[buffer(26)]],
    uint lane [[thread_index_in_threadgroup]],
    uint block [[threadgroup_position_in_grid]],
    uint block_size [[threads_per_threadgroup]],
    uint blocks [[threadgroups_per_grid]]
) {
    threadgroup ulong counts[1024];
    threadgroup ulong block_base;
    threadgroup ulong active_total;
    ulong block_count = load_compaction_count(stream_state, params, block);
    if (block_count == NONE) {
        return;
    }

    ulong node = ulong(block) * ulong(block_size) + ulong(lane);
    ulong local_count = 0;
    if (node < params[P_NODE_COUNT]) {
        ulong time;
        if (
            load_fel_root(stream_state, params, node, time) &&
            before_horizon(time, control)
        ) {
            local_count += 1;
        }
    }

    counts[lane] = local_count;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint offset = 1; offset < block_size; offset <<= 1) {
        ulong addend = lane >= offset ? counts[lane - offset] : 0;
        threadgroup_barrier(mem_flags::mem_threadgroup);
        counts[lane] += addend;
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }

    if (lane == 0) {
        ulong base = 0;
        ulong total = 0;
        for (ulong candidate = 0; candidate < blocks; ++candidate) {
            ulong count = load_compaction_count(stream_state, params, candidate);
            if (candidate < block) {
                base += count;
            }
            total += count;
        }
        block_base = base;
        active_total = total;
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);

    if (active_total > params[P_WORKLIST_CAPACITY]) {
        if (block == 0 && lane == 0) {
            control[C_ERROR] = ERROR_CAPACITY;
            control[C_ERROR_ARENA] = ARENA_WORKLIST;
            control[C_ERROR_NODE] = NONE;
            control[C_ERROR_CAPACITY] = params[P_WORKLIST_CAPACITY];
            control[C_ERROR_DEMAND] = active_total;
            device uint *drain_dispatch =
                reinterpret_cast<device uint *>(control + C_INDIRECT_OFFSET_WORDS);
            drain_dispatch[0] = 0;
            drain_dispatch[1] = 1;
            drain_dispatch[2] = 1;
        }
        return;
    }

    if (local_count != 0) {
        ulong write = block_base + counts[lane] - local_count;
        worklist[write] = node;
        lp_state[node * LP_STATE_WORDS + L_FINISHED] = 0;
    }
    threadgroup_barrier(mem_flags::mem_device | mem_flags::mem_threadgroup);
    if (lane == 0 && block + 1 == blocks) {
        ulong active = active_total;
        device uint *drain_dispatch =
            reinterpret_cast<device uint *>(control + C_INDIRECT_OFFSET_WORDS);
        control[C_ACTIVE] = active;
        control[C_OUTBOX] = 0;
        control[C_CONTINUATION] = 1;
        drain_dispatch[0] = uint(
            (active + params[P_ROUND_THREADS] - 1) / params[P_ROUND_THREADS]
        );
        drain_dispatch[1] = 1;
        drain_dispatch[2] = 1;
    }
}

kernel void days_round(
    const device ulong * __restrict control [[buffer(0)]],
    const device ulong * __restrict params [[buffer(1)]],
    device ulong * __restrict node_state [[buffer(2)]],
    device ulong * __restrict generators [[buffer(3)]],
    const device ulong * __restrict flows [[buffer(4)]],
    const device ulong * __restrict routes [[buffer(5)]],
    const device ulong * __restrict links [[buffer(6)]],
    device ulong * __restrict fel_meta [[buffer(7)]],
    device ulong * __restrict fel_records [[buffer(8)]],
    device ulong * __restrict queue_meta [[buffer(9)]],
    device ulong * __restrict queue_records [[buffer(10)]],
    device ulong * __restrict in_service [[buffer(11)]],
    const device ulong * __restrict worklist [[buffer(13)]],
    device ulong * __restrict summary [[buffer(14)]],
    device ulong * __restrict observed [[buffer(15)]],
    device ulong * __restrict departures [[buffer(16)]],
    device ulong * __restrict arrivals [[buffer(17)]],
    device ulong * __restrict lp_state [[buffer(18)]],
    device ulong * __restrict remote_meta [[buffer(19)]],
    device ulong * __restrict remote_staging [[buffer(20)]],
    device ulong * __restrict observation_meta [[buffer(21)]],
    device ulong * __restrict stream_state [[buffer(25)]],
    device ulong * __restrict stream_records [[buffer(26)]],
    device ulong * __restrict scheduler_state [[buffer(27)]],
    device ulong * __restrict tcp_state [[buffer(30)]],
    uint active_index [[thread_position_in_grid]]
) {
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 1 ||
        active_index >= control[C_ACTIVE]
    ) {
        return;
    }
    ulong node = worklist[active_index];
    device ulong *state = lp_state + node * LP_STATE_WORDS;
    if (state[L_FINISHED] != 0 || state[L_ERROR] != 0) {
        return;
    }

    ulong event[EVENT_WORDS];
    bool has_continuation = false;
    ulong dispatch_transitions = 0;
    while (dispatch_transitions < params[P_TRANSITION_CAPACITY]) {
        ulong popped_timer_owner = NONE;
        if (has_continuation) {
            has_continuation = false;
        } else {
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
        }
        bool counted_continuation = false;
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
            tcp_state,
            dispatch_transitions + 1 < params[P_TRANSITION_CAPACITY],
            has_continuation,
            counted_continuation
        )) {
            return;
        }
        if (counted_continuation && state[L_SAME_TIME_CONTINUATIONS] != NONE) {
            state[L_SAME_TIME_CONTINUATIONS] += 1;
        }
        if (state[L_TRANSITIONS] == NONE) {
            set_semantic_error(state, 27, node);
            return;
        }
        state[L_TRANSITIONS] += 1;
        dispatch_transitions += 1;
    }
}

// T21 fix 1 — the round-control scan, on the whole grid. Transliterated word for word from
// `cuda_kernels.cu`; see that kernel's header for the partition-invariance argument.
kernel void days_round_control_sweep(
    const device ulong * __restrict control [[buffer(0)]],
    const device ulong * __restrict params [[buffer(1)]],
    const device ulong * __restrict worklist [[buffer(13)]],
    const device ulong * __restrict lp_state [[buffer(18)]],
    device ulong * __restrict stream_state [[buffer(25)]],
    uint lane [[thread_index_in_threadgroup]],
    uint block [[threadgroup_position_in_grid]],
    uint block_size [[threads_per_threadgroup]],
    uint blocks [[threadgroups_per_grid]]
) {
    threadgroup ulong first_errors[1024];
    threadgroup uint unfinished_lanes[1024];
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 1
    ) {
        return;
    }

    ulong first = ulong(block) * ulong(block_size) + ulong(lane);
    ulong stride = ulong(blocks) * ulong(block_size);
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
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint stride_lanes = 512; stride_lanes != 0; stride_lanes >>= 1) {
        if (lane < stride_lanes) {
            first_errors[lane] = min(first_errors[lane], first_errors[lane + stride_lanes]);
            unfinished_lanes[lane] |= unfinished_lanes[lane + stride_lanes];
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    if (lane == 0) {
        store_round_partial(stream_state, params, block, 0, first_errors[0]);
        store_round_partial(
            stream_state,
            params,
            block,
            1,
            ulong(unfinished_lanes[0])
        );
    }
}

// T21 fix 1 — the round-control combine. Transliterated word for word from `cuda_kernels.cu`.
kernel void days_round_control(
    device ulong * __restrict control [[buffer(0)]],
    const device ulong * __restrict params [[buffer(1)]],
    const device ulong * __restrict lp_state [[buffer(18)]],
    const device ulong * __restrict stream_state [[buffer(25)]],
    uint lane [[thread_index_in_threadgroup]]
) {
    threadgroup ulong first_errors[1024];
    threadgroup uint unfinished_lanes[1024];
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
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint stride = 512; stride != 0; stride >>= 1) {
        if (lane < stride) {
            first_errors[lane] = min(first_errors[lane], first_errors[lane + stride]);
            unfinished_lanes[lane] |= unfinished_lanes[lane + stride];
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
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

// T21 fix 1 — the exchange-prefix STREAMS scan, on the whole grid. Transliterated word for word
// from `cuda_kernels.cu`; see that kernel's header, which also records that this is the kernel the
// FIRST attempt's in-dispatch combine failed in.
kernel void days_exchange_prefix_sweep(
    const device ulong * __restrict control [[buffer(0)]],
    const device ulong * __restrict params [[buffer(1)]],
    const device ulong * __restrict remote_staging [[buffer(20)]],
    device ulong * __restrict stream_state [[buffer(25)]],
    const device ulong * __restrict stream_records [[buffer(26)]],
    uint lane [[thread_index_in_threadgroup]],
    uint block [[threadgroup_position_in_grid]],
    uint block_size [[threads_per_threadgroup]],
    uint blocks [[threadgroups_per_grid]]
) {
    threadgroup ulong sums[1024];
    threadgroup uint exceeded[1024];
    threadgroup ulong failures[1024];
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
        ulong first = ulong(block) * ulong(block_size) + ulong(lane);
        ulong grid_stride = ulong(blocks) * ulong(block_size);
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
        threadgroup_barrier(mem_flags::mem_threadgroup);
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
            threadgroup_barrier(mem_flags::mem_threadgroup);
        }
        if (lane == 0) {
            store_round_partial(stream_state, params, block, 0, sums[0]);
            store_round_partial(stream_state, params, block, 1, ulong(exceeded[0]));
            store_round_partial(stream_state, params, block, 2, failures[0]);
        }
    }
}

// T21 fix 1 — the exchange-prefix combine, plus the legacy node-ordered prefix scan.
// Transliterated word for word from `cuda_kernels.cu`.
kernel void days_exchange_prefix(
    device ulong * __restrict control [[buffer(0)]],
    const device ulong * __restrict params [[buffer(1)]],
    device ulong * __restrict remote_meta [[buffer(19)]],
    const device ulong * __restrict stream_state [[buffer(25)]],
    uint lane [[thread_index_in_threadgroup]]
) {
    threadgroup ulong sums[1024];
    threadgroup uint exceeded[1024];
    threadgroup ulong failures[1024];
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
        threadgroup_barrier(mem_flags::mem_threadgroup);
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
            threadgroup_barrier(mem_flags::mem_threadgroup);
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
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint offset = 1; offset < 1024; offset <<= 1) {
        ulong left_sum = lane >= offset ? sums[lane - offset] : 0;
        uint left_exceeded = lane >= offset ? exceeded[lane - offset] : 0;
        ulong own_sum = sums[lane];
        uint own_exceeded = exceeded[lane];
        threadgroup_barrier(mem_flags::mem_threadgroup);
        if (lane >= offset) {
            uint combined_exceeded = left_exceeded | own_exceeded;
            ulong combined_sum = saturating_add_ulong(left_sum, own_sum);
            combined_exceeded |= uint(combined_sum > capacity);
            sums[lane] = combined_sum;
            exceeded[lane] = combined_exceeded;
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }

    if (lane == 0 && exceeded[1023] != 0) {
        control[C_ERROR] = ERROR_CAPACITY;
        control[C_ERROR_ARENA] = ARENA_OUTBOX;
        control[C_ERROR_NODE] = NONE;
        control[C_ERROR_CAPACITY] = capacity;
        control[C_ERROR_DEMAND] = sums[1023];
    }
    threadgroup_barrier(mem_flags::mem_device | mem_flags::mem_threadgroup);
    if (exceeded[1023] != 0) {
        return;
    }

    ulong write = sums[lane] - local_sum;
    for (ulong producer = start; producer < end; ++producer) {
        ulong base = producer * META_WORDS;
        remote_meta[base + 2] = write;
        write += remote_meta[base + 3];
    }
    threadgroup_barrier(mem_flags::mem_device | mem_flags::mem_threadgroup);
    if (lane == 0) {
        control[C_OUTBOX] = sums[1023];
    }
}

inline ulong scatter_simd_broadcast_ulong(ulong value) {
    uint low = simd_broadcast(uint(value), 0);
    uint high = simd_broadcast(uint(value >> 32), 0);
    return ulong(low) | (ulong(high) << 32);
}

inline ulong scatter_stream_target(
    ulong staging_slot,
    const device ulong *params,
    device ulong *stream_state
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

inline void scatter_stream_commit(
    ulong producer,
    const device ulong *params,
    device ulong *stream_state
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
        ulong count = stream_state[channel * META_WORDS + 3] + stream_state[batch];
        stream_state[channel * META_WORDS + 3] = count;
        RECORD_STREAM_HIGH_WATER(params, stream_state, channel, count);
    }
}

inline void scatter_stream_producer_lane(
    ulong producer,
    ulong count,
    const device ulong *params,
    const device ulong *remote_meta,
    const device ulong *remote_staging,
    device ulong *stream_state,
    device ulong *stream_records
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

kernel void days_exchange_scatter(
    const device ulong * __restrict control [[buffer(0)]],
    const device ulong * __restrict params [[buffer(1)]],
    device ulong * __restrict outbox [[buffer(12)]],
    const device ulong * __restrict worklist [[buffer(13)]],
    const device ulong * __restrict remote_meta [[buffer(19)]],
    const device ulong * __restrict remote_staging [[buffer(20)]],
    device ulong * __restrict stream_state [[buffer(25)]],
    device ulong * __restrict stream_records [[buffer(26)]],
    uint global_thread [[thread_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]],
    uint group_in_block [[simdgroup_index_in_threadgroup]],
    uint groups_per_block [[simdgroups_per_threadgroup]],
    uint block [[threadgroup_position_in_grid]],
    uint blocks [[threadgroups_per_grid]]
) {
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 2
    ) {
        return;
    }
    if (params[P_STREAMS_ENABLED] != 0) {
        // Counts below three fit in at most one two-record copy cluster. Keep
        // those producers on the original lane mapping so one SIMD group can
        // advance one producer per lane concurrently.
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

        // Long producers retain one cooperative SIMD group each. The worklist
        // is deterministic and unique; the count predicate makes the lane and
        // cooperative traversals disjoint.
        ulong group = ulong(block) * groups_per_block + group_in_block;
        ulong group_stride = ulong(blocks) * groups_per_block;
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
            count = scatter_simd_broadcast_ulong(count);

            if (count >= SCATTER_COOPERATIVE_MIN_RECORDS) {
                ulong staging = 0;
                if (lane == 0) {
                    staging = remote_meta[producer * META_WORDS];
                }
                staging = scatter_simd_broadcast_ulong(staging);
                for (ulong index = 0; index < count; index += 2) {
                    ulong first_target = 0;
                    ulong second_target = 0;
                    if (lane == 0) {
                        first_target = scatter_stream_target(
                            staging + index,
                            params,
                            stream_state
                        );
                        if (index + 1 < count) {
                            second_target = scatter_stream_target(
                                staging + index + 1,
                                params,
                                stream_state
                            );
                        }
                    }
                    first_target = scatter_simd_broadcast_ulong(first_target);
                    second_target = scatter_simd_broadcast_ulong(second_target);

                    if (lane < 2 * EVENT_WORDS) {
                        ulong record = lane / EVENT_WORDS;
                        uint word = lane % EVENT_WORDS;
                        if (index + record < count) {
                            ulong source_slot = staging + index + record;
                            ulong target_slot =
                                record == 0 ? first_target : second_target;
                            stream_records[target_slot * EVENT_WORDS + word] =
                                remote_staging[source_slot * EVENT_WORDS + word];
                        }
                    }
                }

                if (lane == 0) {
                    scatter_stream_commit(producer, params, stream_state);
                }
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
    for (ulong index = 0; index < count; ++index) {
        copy_device_record(
            remote_staging,
            staging + index,
            outbox,
            compact + index
        );
    }
}

kernel void days_exchange_merge(
    const device ulong * __restrict control [[buffer(0)]],
    const device ulong * __restrict params [[buffer(1)]],
    device ulong * __restrict fel_meta [[buffer(7)]],
    device ulong * __restrict fel_records [[buffer(8)]],
    const device ulong * __restrict outbox [[buffer(12)]],
    device ulong * __restrict lp_state [[buffer(18)]],
    const device ulong * __restrict remote_meta [[buffer(19)]],
    const device ulong * __restrict inbound_meta [[buffer(22)]],
    const device ulong * __restrict inbound_producers [[buffer(23)]],
    device ulong * __restrict merge_cursors [[buffer(24)]],
    device ulong * __restrict stream_state [[buffer(25)]],
    const device ulong * __restrict stream_records [[buffer(26)]],
    device ulong * __restrict tcp_state [[buffer(30)]],
    uint target [[thread_position_in_grid]]
) {
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

    device ulong *error = lp_state + ulong(target) * LP_STATE_WORDS;
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

// T21 fix 1 — the finalize scans, on the whole grid. Transliterated word for word from
// `cuda_kernels.cu`; see that kernel's header for the clamped-sum associativity argument.
kernel void days_round_finalize_sweep(
    const device ulong * __restrict control [[buffer(0)]],
    const device ulong * __restrict params [[buffer(1)]],
    const device ulong * __restrict lp_state [[buffer(18)]],
    const device ulong * __restrict observation_meta [[buffer(21)]],
    device ulong * __restrict stream_state [[buffer(25)]],
    uint lane [[thread_index_in_threadgroup]],
    uint block [[threadgroup_position_in_grid]],
    uint block_size [[threads_per_threadgroup]],
    uint blocks [[threadgroups_per_grid]]
) {
    threadgroup ulong values[1024];
    threadgroup uint exceeded[1024];
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
    ulong first = ulong(block) * ulong(block_size) + ulong(lane);
    ulong stride = ulong(blocks) * ulong(block_size);
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
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint stride_lanes = 512; stride_lanes != 0; stride_lanes >>= 1) {
        if (lane < stride_lanes) {
            values[lane] = min(values[lane], values[lane + stride_lanes]);
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    if (lane == 0) {
        store_round_partial(stream_state, params, block, 0, values[0]);
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);

    for (uint log = 0; log < 3; ++log) {
        values[lane] = local_totals[log];
        exceeded[lane] = local_exceeded[log];
        threadgroup_barrier(mem_flags::mem_threadgroup);
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
            threadgroup_barrier(mem_flags::mem_threadgroup);
        }
        if (lane == 0) {
            store_round_partial(stream_state, params, block, 1 + log * 2, values[0]);
            store_round_partial(
                stream_state,
                params,
                block,
                2 + log * 2,
                ulong(exceeded[0])
            );
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
}

// T21 fix 1 — the finalize combine. Transliterated word for word from `cuda_kernels.cu`.
kernel void days_round_finalize(
    device ulong * __restrict control [[buffer(0)]],
    const device ulong * __restrict params [[buffer(1)]],
    const device ulong * __restrict lp_state [[buffer(18)]],
    const device ulong * __restrict stream_state [[buffer(25)]],
    uint lane [[thread_index_in_threadgroup]]
) {
    threadgroup ulong values[1024];
    threadgroup uint exceeded[1024];
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 2
    ) {
        return;
    }

    // T24: the host omits the sweep only for streams + Summary. Transliterated word for word from
    // CUDA; the retained dispatch is the ordered round-state publication boundary.
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
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint stride = 512; stride != 0; stride >>= 1) {
        if (lane < stride) {
            values[lane] = min(values[lane], values[lane + stride]);
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    if (lane == 0 && values[0] != NONE) {
        ulong state = values[0] * LP_STATE_WORDS;
        control[C_ERROR] = lp_state[state + L_ERROR];
        control[C_ERROR_ARENA] = lp_state[state + L_ERROR_ARENA];
        control[C_ERROR_NODE] = lp_state[state + L_ERROR_NODE];
        control[C_ERROR_CAPACITY] = lp_state[state + L_ERROR_CAPACITY];
        control[C_ERROR_DEMAND] = lp_state[state + L_ERROR_DEMAND];
    }
    threadgroup_barrier(mem_flags::mem_device | mem_flags::mem_threadgroup);
    if (values[0] != NONE) {
        return;
    }
    // Complete the reduction-result read before reusing the threadgroup scratch planes.
    threadgroup_barrier(mem_flags::mem_threadgroup);
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
        threadgroup_barrier(mem_flags::mem_threadgroup);
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
            threadgroup_barrier(mem_flags::mem_threadgroup);
        }
        if (lane == 0 && exceeded[0] != 0 && control[C_ERROR] == 0) {
            control[C_ERROR] = ERROR_CAPACITY;
            control[C_ERROR_ARENA] = arenas[log];
            control[C_ERROR_NODE] = NONE;
            control[C_ERROR_CAPACITY] = capacities[log];
            control[C_ERROR_DEMAND] = values[0];
        }
        threadgroup_barrier(mem_flags::mem_device | mem_flags::mem_threadgroup);
    }
    if (lane == 0 && control[C_ERROR] == 0) {
        control[C_CONTINUATION] = 0;
        control[C_ROUNDS] += 1;
    }
}

// T20l fix 2 — device-side readback compaction.
//
// Every striped result arena is sized for worst-case occupancy and is therefore mostly empty:
// T20l phase 1 §4.2 measured the frontier attempt copying 18.6-19.2 GB back to recover 487,958
// pending events and 2,931,779 packet descriptors, and §8 recorded that the meta planes bounding
// those live records are already on the device before the copy starts. This kernel gathers one
// arena's live records into a dense destination so the host copies the live words instead of the
// arena.
//
// AUDIT (T20g style). This kernel has exactly ONE device write site, `destination[...]` below.
// `destination` is a buffer allocated for the readback alone: it is never bound to any simulation
// kernel, never read by one, and never part of complete state. `source`, `plan` and `args` are all
// `const device`. The kernel therefore cannot alter a single semantic word, and it runs only after
// the attempt has been screened as successful, so it cannot alter a fault payload either.
//
// DETERMINISM. One thread owns one entity; destinations are disjoint by construction because the
// host lays them out as the exclusive prefix sum of the device-written live counts, in the order
// the host decode walks the entities. There are no atomics, and the output does not depend on
// dispatch order, thread scheduling or threadgroup size.
//
// The slot arithmetic mirrors `executor/src/device_compaction.rs::live_record_slots`, which in turn
// reproduces `read_queue`, the stream decode loop and `tcp_ledger_ring::ledger_record_slot`.
constant uint COMPACT_PLAN_ROW_WORDS = 5;

kernel void days_compact_gather(
    device ulong *destination [[buffer(0)]],
    const device ulong *source [[buffer(1)]],
    const device ulong *plan [[buffer(2)]],
    const device ulong *args [[buffer(3)]],
    uint entity [[thread_position_in_grid]]
) {
    ulong entity_count = args[0];
    ulong record_words = args[1];
    bool ring = args[2] != 0;
    if (ulong(entity) >= entity_count) {
        return;
    }
    const device ulong *row = plan + ulong(entity) * COMPACT_PLAN_ROW_WORDS;
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
