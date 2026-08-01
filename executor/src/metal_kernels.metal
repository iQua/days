#include <metal_stdlib>
using namespace metal;

constant uint EVENT_WORDS = 14;
constant uint NODE_WORDS = 11;
constant uint GENERATOR_WORDS = 43;
constant uint FLOW_WORDS = 6;
constant uint LINK_WORDS = 4;
constant uint META_WORDS = 4;
constant uint LP_STATE_WORDS = 6;
constant uint OBSERVATION_META_WORDS = 12;
constant uint INBOUND_META_WORDS = 2;
constant uint LP_STREAM_META_WORDS = 4;
constant uint OUTBOUND_META_WORDS = 2;
constant uint OUTBOUND_ENTRY_WORDS = 2;
constant uint CHANNEL_BATCH_WORDS = 4;
constant uint ACTIVE_STREAM_ENTRY_WORDS = 5;
constant uint TCP_RECEIVER_WORDS = 7;
constant uint TCP_LEDGER_META_WORDS = 4;
constant uint TCP_LEDGER_RECORD_WORDS = 5;
constant uint TCP_TRANSITION_META_WORDS = 4;
constant uint TCP_TRANSITION_WORDS = 36;
constant uint RATIONAL_WORDS = 10;
constant uint BIG_LIMBS = 10;
constant uint WIDE_LIMBS = 16;
constant uint PRODUCT_LIMBS = 20;
constant uint SCHEDULER_NODE_WORDS = 25;
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
constant uint P_TCP_TRANSITION_META_OFFSET = 30;

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

constant ulong HOST = 0;
constant ulong SWITCH = 1;
constant ulong PACKET_ARRIVAL = 0;
constant ulong TX_READY = 1;
constant ulong TX_COMPLETE = 2;
constant ulong REMOTE_ARRIVAL = 3;
constant ulong RETRANSMISSION_TIMEOUT = 4;
constant ulong DATA_PACKET = 0;
constant ulong FEEDBACK_PACKET = 1;
constant ulong TCP_DATA_PACKET = 2;
constant ulong TCP_ACK_PACKET = 3;
constant ulong SCHED_FIFO = 0;
constant ulong SCHED_SP = 1;
constant ulong SCHED_WFQ = 2;

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
constant ulong ARENA_TCP_TRANSITIONS = 13;

constant uint L_FINISHED = 0;
constant uint L_TRANSITIONS = 1;
constant uint L_ERROR = 2;
constant uint L_ERROR_ARENA = 3;
constant uint L_ERROR_NODE = 4;
constant uint L_ERROR_CAPACITY = 5;

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

inline void set_capacity_error(
    device ulong *error,
    ulong arena,
    ulong node,
    ulong capacity
) {
    if (error[L_ERROR] == 0) {
        error[L_ERROR] = ERROR_CAPACITY;
        error[L_ERROR_ARENA] = arena;
        error[L_ERROR_NODE] = node;
        error[L_ERROR_CAPACITY] = capacity;
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

inline bool heap_push(
    ulong node,
    const thread ulong *record,
    device ulong *error,
    device ulong *meta,
    device ulong *records
) {
    ulong base = node * META_WORDS;
    ulong offset = meta[base];
    ulong capacity = meta[base + 1];
    ulong count = meta[base + 3];
    if (count >= capacity) {
        set_capacity_error(error, ARENA_FEL, node, capacity);
        return false;
    }
    ulong child = count;
    copy_thread_to_device(record, records, offset + child);
    meta[base + 3] = count + 1;
    while (child != 0) {
        ulong parent = (child - 1) / 2;
        if (!stored_key_less(records, offset + child, offset + parent)) {
            break;
        }
        swap_records(records, offset + child, offset + parent);
        child = parent;
    }
    return true;
}

inline bool heap_pop(
    ulong node,
    device ulong *meta,
    device ulong *records,
    thread ulong *record
) {
    ulong base = node * META_WORDS;
    ulong offset = meta[base];
    ulong count = meta[base + 3];
    if (count == 0) {
        return false;
    }
    copy_device_to_thread(records, offset, record);
    count -= 1;
    meta[base + 3] = count;
    if (count == 0) {
        return true;
    }
    copy_device_record(records, offset + count, records, offset);
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
        swap_records(records, offset + child, offset + parent);
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

inline bool active_refresh_source(
    ulong node,
    ulong stream,
    const device ulong *params,
    device ulong *error,
    device ulong *stream_state,
    const device ulong *records,
    ulong slot
) {
    ulong meta =
        params[P_LP_STREAM_META_OFFSET] + node * LP_STREAM_META_WORDS;
    ulong offset = stream_state[meta + 2];
    ulong count = stream_state[meta + 3];
    for (ulong index = 0; index < count; ++index) {
        ulong entry = offset + index * ACTIVE_STREAM_ENTRY_WORDS;
        if (stream_state[entry] == stream) {
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
    }
    set_semantic_error(error, 43, node);
    return false;
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
            capacity
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

inline bool fallback_push(
    ulong node,
    const thread ulong *record,
    const device ulong *params,
    device ulong *error,
    device ulong *fel_meta,
    device ulong *fel_records,
    device ulong *stream_state
) {
    bool was_empty = fel_meta[node * META_WORDS + 3] == 0;
    if (!heap_push(node, record, error, fel_meta, fel_records)) {
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
    device ulong *stream_records
) {
    if (params[P_STREAMS_ENABLED] == 0) {
        return heap_push(node, record, error, fel_meta, fel_records);
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
        stream_state
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
    thread ulong *record
) {
    if (params[P_STREAMS_ENABLED] == 0 || selected_active == NONE) {
        return heap_pop(node, fel_meta, fel_records, record);
    }
    if (selected_stream == NONE) {
        if (!heap_pop(node, fel_meta, fel_records, record)) {
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
    thread ulong *record
) {
    if (params[P_STREAMS_ENABLED] == 0) {
        return heap_pop(node, fel_meta, fel_records, record);
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
        record
    );
}

inline bool before_horizon(ulong time, const device ulong *control) {
    return control[C_HORIZON_HI] != 0 || time < control[C_HORIZON_LO];
}

inline bool queue_push(
    ulong node,
    const thread ulong *record,
    device ulong *error,
    device ulong *meta,
    device ulong *records
) {
    ulong base = node * META_WORDS;
    ulong offset = meta[base];
    ulong capacity = meta[base + 1];
    ulong head = meta[base + 2];
    ulong count = meta[base + 3];
    if (count >= capacity) {
        set_capacity_error(error, ARENA_QUEUE, node, capacity);
        return false;
    }
    ulong physical = (head + count) % max(capacity, 1ul);
    copy_thread_to_device(record, records, offset + physical);
    meta[base + 3] = count + 1;
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
    device ulong *meta,
    device ulong *records
) {
    ulong base = node * META_WORDS;
    ulong offset = meta[base];
    ulong capacity = meta[base + 1];
    ulong head = meta[base + 2];
    ulong count = meta[base + 3];
    if (count >= capacity) {
        set_capacity_error(error, ARENA_QUEUE, node, capacity);
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

inline bool queue_pop(
    ulong node,
    device ulong *meta,
    const device ulong *records,
    thread ulong *record,
    thread ulong &physical
) {
    ulong base = node * META_WORDS;
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

inline bool queue_front(
    ulong node,
    const device ulong *meta,
    const device ulong *records,
    thread ulong *record
) {
    ulong base = node * META_WORDS;
    if (meta[base + 3] == 0) {
        return false;
    }
    copy_device_to_thread(records, meta[base] + meta[base + 2], record);
    return true;
}

inline ulong event_phase(ulong kind) {
    if (kind == TX_COMPLETE || kind == RETRANSMISSION_TIMEOUT) {
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
        set_capacity_error(error, ARENA_OBSERVED, NONE, params[P_OBSERVED_CAPACITY]);
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
        set_capacity_error(error, ARENA_DEPARTURES, NONE, params[P_DEPARTURE_CAPACITY]);
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
        set_capacity_error(error, ARENA_ARRIVALS, NONE, params[P_ARRIVAL_CAPACITY]);
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
        set_capacity_error(error, ARENA_OUTBOX, NONE, params[P_OUTBOX_CAPACITY]);
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
    device ulong *stream_records
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
            stream_records
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
    device ulong *queue_meta,
    device ulong *queue_records,
    const device ulong *scheduler_state
) {
    ulong meta_base = node * META_WORDS;
    ulong offset = queue_meta[meta_base];
    ulong capacity = queue_meta[meta_base + 1];
    ulong head = queue_meta[meta_base + 2];
    ulong count = queue_meta[meta_base + 3];
    if (count >= capacity) {
        set_capacity_error(error, ARENA_QUEUE, node, capacity);
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

inline bool wfq_queue_insert(
    ulong node,
    const thread ulong *record,
    const thread uint *finish_num,
    const thread uint *finish_den,
    device ulong *error,
    device ulong *queue_meta,
    device ulong *queue_records,
    device ulong *scheduler_state
) {
    ulong meta_base = node * META_WORDS;
    ulong offset = queue_meta[meta_base];
    ulong capacity = queue_meta[meta_base + 1];
    ulong head = queue_meta[meta_base + 2];
    ulong count = queue_meta[meta_base + 3];
    if (count >= capacity) {
        set_capacity_error(error, ARENA_QUEUE, node, capacity);
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

inline bool wfq_enqueue(
    ulong node,
    const thread ulong *record,
    ulong rate,
    device ulong *error,
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
    if (packet[PK_KIND] == DATA_PACKET || packet[PK_KIND] == TCP_DATA_PACKET) {
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

inline bool tcp_ledger_find(
    ulong flow,
    ulong sequence,
    const device ulong *params,
    const device ulong *tcp_state,
    thread ulong &record
) {
    ulong meta = params[P_TCP_LEDGER_META_OFFSET] + flow * TCP_LEDGER_META_WORDS;
    ulong offset = tcp_state[meta];
    ulong count = tcp_state[meta + 2];
    for (ulong index = 0; index < count; ++index) {
        ulong candidate = offset + index * TCP_LEDGER_RECORD_WORDS;
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
    ulong sequence = packet[PK_META_0];
    ulong insertion = 0;
    while (
        insertion < count &&
        tcp_state[offset + insertion * TCP_LEDGER_RECORD_WORDS + 2] < sequence
    ) {
        insertion += 1;
    }
    if (
        insertion < count &&
        tcp_state[offset + insertion * TCP_LEDGER_RECORD_WORDS + 2] == sequence
    ) {
        if (tcp_state[offset + insertion * TCP_LEDGER_RECORD_WORDS + 1] != packet[PK_SIZE]) {
            set_semantic_error(error, 40, NONE);
            return false;
        }
        tcp_record_copy(packet, tcp_state + offset + insertion * TCP_LEDGER_RECORD_WORDS);
        return true;
    }
    if (count >= capacity) {
        set_capacity_error(error, ARENA_TCP_SEGMENT_LEDGER, flow, capacity);
        return false;
    }
    for (ulong index = count; index > insertion; --index) {
        for (uint word = 0; word < TCP_LEDGER_RECORD_WORDS; ++word) {
            tcp_state[offset + index * TCP_LEDGER_RECORD_WORDS + word] =
                tcp_state[offset + (index - 1) * TCP_LEDGER_RECORD_WORDS + word];
        }
    }
    tcp_record_copy(packet, tcp_state + offset + insertion * TCP_LEDGER_RECORD_WORDS);
    tcp_state[meta + 2] = count + 1;
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
    ulong count = tcp_state[meta + 2];
    ulong keep = 0;
    while (keep < count) {
        ulong record = offset + keep * TCP_LEDGER_RECORD_WORDS;
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
        ulong first = offset + keep * TCP_LEDGER_RECORD_WORDS;
        ulong sequence = tcp_state[first + 2];
        if (sequence < acknowledgment) {
            ulong acknowledged = acknowledgment - sequence;
            if (acknowledged < tcp_state[first + 1]) {
                tcp_state[first + 1] -= acknowledged;
                tcp_state[first + 2] = acknowledgment;
            } else {
                keep += 1;
            }
        }
    }
    ulong remaining = count - keep;
    for (ulong index = 0; index < remaining; ++index) {
        for (uint word = 0; word < TCP_LEDGER_RECORD_WORDS; ++word) {
            tcp_state[offset + index * TCP_LEDGER_RECORD_WORDS + word] =
                tcp_state[offset + (keep + index) * TCP_LEDGER_RECORD_WORDS + word];
        }
    }
    tcp_state[meta + 2] = remaining;
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
        set_capacity_error(error, ARENA_TCP_RECEIVER, flow, capacity);
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

inline bool record_tcp_transition(
    ulong node,
    ulong flow,
    const thread ulong *event,
    ulong input_kind,
    ulong input_0,
    ulong input_1,
    ulong input_2,
    ulong input_3,
    const thread ulong *before,
    const thread ulong *after,
    device ulong *error,
    const device ulong *params,
    device ulong *tcp_state
) {
    if (params[P_FULL_OBSERVATIONS] == 0) {
        return true;
    }
    ulong meta = params[P_TCP_TRANSITION_META_OFFSET] + node * TCP_TRANSITION_META_WORDS;
    ulong count = tcp_state[meta + 3];
    ulong capacity = tcp_state[meta + 1];
    if (count >= capacity) {
        set_capacity_error(error, ARENA_TCP_TRANSITIONS, node, capacity);
        return false;
    }
    ulong record = tcp_state[meta] + count * TCP_TRANSITION_WORDS;
    tcp_state[record] = event[E_TIME];
    tcp_state[record + 1] = event[E_PHASE];
    tcp_state[record + 2] = event[E_ORIGIN];
    tcp_state[record + 3] = event[E_SEQUENCE];
    tcp_state[record + 4] = node;
    tcp_state[record + 5] = flow;
    tcp_state[record + 6] = before[CTL_MSS];
    tcp_state[record + 7] = input_kind;
    tcp_state[record + 8] = input_0;
    tcp_state[record + 9] = input_1;
    tcp_state[record + 10] = input_2;
    tcp_state[record + 11] = input_3;
    for (uint word = 0; word < 12; ++word) {
        tcp_state[record + 12 + word] = before[word];
        tcp_state[record + 24 + word] = after[word];
    }
    tcp_state[meta + 3] = count + 1;
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
        !source_queue_insert(node, packet, error, queue_meta, queue_records) ||
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
            stream_records
        )) {
            return false;
        }
    }

    ulong node_base = node * NODE_WORDS;
    if (
        queue_meta[node * META_WORDS + 3] != 0 &&
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
            stream_records
        );
    }
    return true;
}

inline bool dispatch_event(
    ulong node,
    thread ulong *event,
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
    device ulong *tcp_state
) {
    ulong node_base = node * NODE_WORDS;
    ulong role = node_state[node_base + N_KIND];
    ulong kind = event[E_KIND];

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
                event[PK_KIND] != TCP_DATA_PACKET ||
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
                !source_queue_insert(node, sourced_packet, error, queue_meta, queue_records) ||
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
                    stream_records
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
                stream_records
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
        if (!queue_pop(node, queue_meta, queue_records, selected, selected_physical)) {
            return true;
        }
        ulong scheduler_base = node * SCHEDULER_NODE_WORDS;
        if (role == SWITCH && scheduler_state[scheduler_base + S_KIND] == SCHED_WFQ) {
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
            stream_records
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
            stream_records
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
            queue_meta[node * META_WORDS + 3] != 0 &&
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
                stream_records
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
        ulong waiting = queue_meta[node * META_WORDS + 3];
        ulong semantic_capacity = node_state[node_base + N_SEMANTIC_QUEUE_CAPACITY];
        if (semantic_capacity != 0 && waiting >= semantic_capacity) {
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
        ulong scheduler_base = node * SCHEDULER_NODE_WORDS;
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
        } else {
            set_semantic_error(error, 32, node);
            return false;
        }
        if (!inserted) {
            return false;
        }
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
                stream_records
            );
        }
        return true;
    }

    if (kind == REMOTE_ARRIVAL && role == HOST && event[PK_KIND] == TCP_DATA_PACKET) {
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
            !source_queue_insert(node, ack, error, queue_meta, queue_records) ||
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
                stream_records
            );
        }
        return true;
    }

    if (kind == REMOTE_ARRIVAL && role == HOST && event[PK_KIND] == TCP_ACK_PACKET) {
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
        ulong before[12];
        ulong control[12];
        for (uint word = 0; word < 12; ++word) {
            before[word] = generators[generator + G_CONTROL + word];
            control[word] = before[word];
        }
        bool has_transition = false;
        bool retransmit = false;
        ulong retransmit_sequence = 0;
        bool fill = false;
        bool acknowledged_new = false;
        ulong flight_before = generators[generator + G_TCP_FLIGHT];
        ulong input_kind = 0;
        ulong input_0 = 0;
        ulong input_1 = 0;
        ulong input_2 = 0;
        ulong input_3 = 0;

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
            generators[generator + G_TCP_TIMER_ACTIVE] = 0;
            if (
                control[CTL_PHASE] == TCP_FAST_RECOVERY &&
                acknowledgment < generators[generator + G_TCP_RECOVERY_HIGH]
            ) {
                retransmit = true;
                retransmit_sequence = acknowledgment;
            }
            fill = acknowledgment < generators[generator + G_TCP_TOTAL];
            acknowledged_new = true;
            has_transition = true;
            input_kind = 0;
            input_0 = acknowledged_bytes;
            input_1 = rtt_sample;
            input_2 = flight_before;
            input_3 = acknowledgment;
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
                generators[generator + G_TCP_TIMER_ACTIVE] = 0;
                retransmit = true;
                retransmit_sequence = acknowledgment;
            } else if (generators[generator + G_TCP_DUP_ACKS] > 3) {
                fill = true;
            }
            has_transition = true;
            input_kind = 1;
            input_0 = flight_before;
            input_1 = recovery_high;
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
        if (!has_transition) {
            return true;
        }
        if (!record_tcp_transition(
            node,
            flow,
            event,
            input_kind,
            input_0,
            input_1,
            input_2,
            input_3,
            before,
            control,
            error,
            params,
            tcp_state
        )) {
            return false;
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
        if (
            flow >= params[P_FLOW_COUNT] ||
            generators[generator + G_VALID] == 0 ||
            generators[generator + G_OWNER] != node ||
            generators[generator + G_KIND] != 1 ||
            generators[generator + G_TCP_TIMER_ACTIVE] == 0 ||
            generators[generator + G_TCP_TIMER_ATTEMPT] != event[E_PAYLOAD] ||
            generators[generator + G_TCP_TIMER_DEADLINE] != event[E_TIME]
        ) {
            return true;
        }
        generators[generator + G_TCP_TIMER_ACTIVE] = 0;
        ulong before[12];
        ulong control[12];
        for (uint word = 0; word < 12; ++word) {
            before[word] = generators[generator + G_CONTROL + word];
            control[word] = before[word];
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
        if (!record_tcp_transition(
            node,
            flow,
            event,
            2,
            flight,
            0,
            0,
            0,
            before,
            control,
            error,
            params,
            tcp_state
        )) {
            return false;
        }
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
            event[PK_KIND] == FEEDBACK_PACKET &&
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
                (event[PK_KIND] == DATA_PACKET || event[PK_KIND] == TCP_DATA_PACKET)
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

kernel void days_horizon(
    device ulong *control [[buffer(0)]],
    const device ulong *params [[buffer(1)]],
    const device ulong *fel_meta [[buffer(7)]],
    const device ulong *fel_records [[buffer(8)]],
    const device ulong *stream_state [[buffer(25)]],
    const device ulong *stream_records [[buffer(26)]],
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
    for (ulong node = lane; node < params[P_NODE_COUNT]; node += 1024) {
        ulong candidate;
        if (
            fel_root_time(
                node,
                params,
                fel_meta,
                fel_records,
                stream_state,
                stream_records,
                candidate
            ) &&
            (!valid || candidate < minimum)
        ) {
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

kernel void days_round_prepare(
    device ulong *control [[buffer(0)]],
    const device ulong *params [[buffer(1)]],
    const device ulong *fel_meta [[buffer(7)]],
    const device ulong *fel_records [[buffer(8)]],
    device ulong *worklist [[buffer(13)]],
    device ulong *lp_state [[buffer(18)]],
    device ulong *remote_meta [[buffer(19)]],
    device ulong *stream_state [[buffer(25)]],
    const device ulong *stream_records [[buffer(26)]],
    uint lane [[thread_index_in_threadgroup]]
) {
    threadgroup ulong counts[1024];
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
    if (params[P_STREAMS_ENABLED] != 0) {
        for (
            ulong channel = lane;
            channel < params[P_CHANNEL_COUNT];
            channel += 1024
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
    for (ulong node = start; node < end; ++node) {
        ulong state = node * LP_STATE_WORDS;
        lp_state[state + L_ERROR] = 0;
        lp_state[state + L_ERROR_ARENA] = 0;
        lp_state[state + L_ERROR_NODE] = NONE;
        lp_state[state + L_ERROR_CAPACITY] = 0;
        remote_meta[node * META_WORDS + 3] = 0;
        ulong time;
        if (
            fel_root_time(
                node,
                params,
                fel_meta,
                fel_records,
                stream_state,
                stream_records,
                time
            ) &&
            before_horizon(time, control)
        ) {
            local_count += 1;
        }
    }

    counts[lane] = local_count;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint offset = 1; offset < 1024; offset <<= 1) {
        ulong addend = lane >= offset ? counts[lane - offset] : 0;
        threadgroup_barrier(mem_flags::mem_threadgroup);
        counts[lane] += addend;
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }

    ulong write = counts[lane] - local_count;
    for (ulong node = start; node < end; ++node) {
        ulong time;
        if (
            fel_root_time(
                node,
                params,
                fel_meta,
                fel_records,
                stream_state,
                stream_records,
                time
            ) &&
            before_horizon(time, control)
        ) {
            if (write >= params[P_WORKLIST_CAPACITY]) {
                if (lane == 0) {
                    control[C_ERROR] = ERROR_CAPACITY;
                    control[C_ERROR_ARENA] = ARENA_WORKLIST;
                    control[C_ERROR_NODE] = NONE;
                    control[C_ERROR_CAPACITY] = params[P_WORKLIST_CAPACITY];
                }
                return;
            }
            worklist[write++] = node;
            lp_state[node * LP_STATE_WORDS + L_FINISHED] = 0;
        }
    }
    threadgroup_barrier(mem_flags::mem_device | mem_flags::mem_threadgroup);
    if (lane == 0) {
        control[C_ACTIVE] = counts[1023];
        control[C_OUTBOX] = 0;
        control[C_CONTINUATION] = 1;
    }
}

kernel void days_round(
    device ulong *control [[buffer(0)]],
    const device ulong *params [[buffer(1)]],
    device ulong *node_state [[buffer(2)]],
    device ulong *generators [[buffer(3)]],
    const device ulong *flows [[buffer(4)]],
    const device ulong *routes [[buffer(5)]],
    const device ulong *links [[buffer(6)]],
    device ulong *fel_meta [[buffer(7)]],
    device ulong *fel_records [[buffer(8)]],
    device ulong *queue_meta [[buffer(9)]],
    device ulong *queue_records [[buffer(10)]],
    device ulong *in_service [[buffer(11)]],
    const device ulong *worklist [[buffer(13)]],
    device ulong *summary [[buffer(14)]],
    device ulong *observed [[buffer(15)]],
    device ulong *departures [[buffer(16)]],
    device ulong *arrivals [[buffer(17)]],
    device ulong *lp_state [[buffer(18)]],
    device ulong *remote_meta [[buffer(19)]],
    device ulong *remote_staging [[buffer(20)]],
    device ulong *observation_meta [[buffer(21)]],
    device ulong *stream_state [[buffer(25)]],
    device ulong *stream_records [[buffer(26)]],
    device ulong *scheduler_state [[buffer(27)]],
    device ulong *tcp_state [[buffer(30)]],
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
        if (!fel_pop_selected(
            node,
            selected_active,
            selected_stream,
            params,
            fel_meta,
            fel_records,
            stream_state,
            stream_records,
            event
        )) {
            set_semantic_error(state, 26, node);
            return;
        }
        if (!dispatch_event(
            node,
            event,
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

// T15e diagnostic only. This kernel preserves the production event set and transition body. Its
// uniform mode word optionally adds one push/pop pair for the just-popped event before each real
// transition. Probe-minus-control measures the marginal stress cost; a per-LP counter reports
// real local pushes from the unchanged body.
#if defined(DAYS_T15E_DIAGNOSTICS)
inline bool diagnostic_record_fel_counts(
    ulong node,
    ulong local_pushes,
    ulong injected_round_trips,
    const device ulong *params,
    device ulong *state,
    device ulong *diagnostic_counts
) {
    ulong local_counter = node + 1;
    ulong previous_local = diagnostic_counts[local_counter];
    ulong next_local = previous_local + local_pushes;
    if (next_local < previous_local) {
        set_semantic_error(state, 33, node);
        return false;
    }
    ulong injected_counter = params[P_NODE_COUNT] + node + 1;
    ulong previous_injected = diagnostic_counts[injected_counter];
    ulong next_injected = previous_injected + injected_round_trips;
    if (next_injected < previous_injected) {
        set_semantic_error(state, 37, node);
        return false;
    }
    diagnostic_counts[local_counter] = next_local;
    diagnostic_counts[injected_counter] = next_injected;
    return true;
}

// Control and stress-probe calls use this same compiled PSO. A uniform diagnostic-buffer word
// selects whether to inject the net-zero heap pair, keeping compiler and register layout matched.
kernel void days_round_fel_probe(
    device ulong *control [[buffer(0)]],
    const device ulong *params [[buffer(1)]],
    device ulong *node_state [[buffer(2)]],
    device ulong *generators [[buffer(3)]],
    const device ulong *flows [[buffer(4)]],
    const device ulong *routes [[buffer(5)]],
    const device ulong *links [[buffer(6)]],
    device ulong *fel_meta [[buffer(7)]],
    device ulong *fel_records [[buffer(8)]],
    device ulong *queue_meta [[buffer(9)]],
    device ulong *queue_records [[buffer(10)]],
    device ulong *in_service [[buffer(11)]],
    const device ulong *worklist [[buffer(13)]],
    device ulong *summary [[buffer(14)]],
    device ulong *observed [[buffer(15)]],
    device ulong *departures [[buffer(16)]],
    device ulong *arrivals [[buffer(17)]],
    device ulong *lp_state [[buffer(18)]],
    device ulong *remote_meta [[buffer(19)]],
    device ulong *remote_staging [[buffer(20)]],
    device ulong *observation_meta [[buffer(21)]],
    device ulong *stream_state [[buffer(25)]],
    device ulong *stream_records [[buffer(26)]],
    device ulong *scheduler_state [[buffer(27)]],
    device ulong *diagnostic_counts [[buffer(28)]],
    device ulong *tcp_state [[buffer(30)]],
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

    ulong dispatch_transitions = 0;
    ulong diagnostic_local_pushes = 0;
    ulong diagnostic_injected_round_trips = 0;
    bool inject_round_trip = diagnostic_counts[0] != 0;
    while (dispatch_transitions < params[P_TRANSITION_CAPACITY]) {
        ulong time;
        if (
            !heap_root_time(node, fel_meta, fel_records, time) ||
            !before_horizon(time, control)
        ) {
            if (!diagnostic_record_fel_counts(
                node,
                diagnostic_local_pushes,
                diagnostic_injected_round_trips,
                params,
                state,
                diagnostic_counts
            )) {
                return;
            }
            state[L_FINISHED] = 1;
            return;
        }

        ulong event[EVENT_WORDS];
        if (!heap_pop(node, fel_meta, fel_records, event)) {
            set_semantic_error(state, 29, node);
            return;
        }
        if (inject_round_trip) {
            if (!heap_push(node, event, state, fel_meta, fel_records)) {
                return;
            }
            if (!heap_pop(node, fel_meta, fel_records, event)) {
                set_semantic_error(state, 30, node);
                return;
            }
            ulong next_round_trips = diagnostic_injected_round_trips + 1;
            if (next_round_trips < diagnostic_injected_round_trips) {
                set_semantic_error(state, 37, node);
                return;
            }
            diagnostic_injected_round_trips = next_round_trips;
        }

        ulong fel_count_before = fel_meta[node * META_WORDS + 3];
        if (!dispatch_event(
            node,
            event,
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
        ulong fel_count_after = fel_meta[node * META_WORDS + 3];
        if (fel_count_after < fel_count_before) {
            set_semantic_error(state, 32, node);
            return;
        }
        ulong local_pushes = fel_count_after - fel_count_before;
        ulong next_local_pushes = diagnostic_local_pushes + local_pushes;
        if (next_local_pushes < diagnostic_local_pushes) {
            set_semantic_error(state, 33, node);
            return;
        }
        diagnostic_local_pushes = next_local_pushes;

        if (state[L_TRANSITIONS] == NONE) {
            set_semantic_error(state, 27, node);
            return;
        }
        state[L_TRANSITIONS] += 1;
        dispatch_transitions += 1;
    }
    if (!diagnostic_record_fel_counts(
        node,
        diagnostic_local_pushes,
        diagnostic_injected_round_trips,
        params,
        state,
        diagnostic_counts
    )) {
        return;
    }
}
#endif

kernel void days_round_control(
    device ulong *control [[buffer(0)]],
    const device ulong *params [[buffer(1)]],
    const device ulong *worklist [[buffer(13)]],
    const device ulong *lp_state [[buffer(18)]],
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
    for (ulong node = lane; node < params[P_NODE_COUNT]; node += 1024) {
        ulong state = node * LP_STATE_WORDS;
        if (lp_state[state + L_ERROR] != 0) {
            first_error = node;
            break;
        }
    }
    uint unfinished = 0;
    for (ulong active = lane; active < control[C_ACTIVE]; active += 1024) {
        ulong node = worklist[active];
        if (lp_state[node * LP_STATE_WORDS + L_FINISHED] == 0) {
            unfinished = 1;
            break;
        }
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
    } else if (unfinished_lanes[0] != 0) {
        control[C_RELAUNCHES] += 1;
    } else {
        control[C_CONTINUATION] = 2;
    }
}

kernel void days_exchange_prefix(
    device ulong *control [[buffer(0)]],
    const device ulong *params [[buffer(1)]],
    device ulong *remote_meta [[buffer(19)]],
    const device ulong *remote_staging [[buffer(20)]],
    device ulong *stream_state [[buffer(25)]],
    const device ulong *stream_records [[buffer(26)]],
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
            ulong channel = lane;
            channel < params[P_CHANNEL_COUNT];
            channel += 1024
        ) {
            ulong batch =
                params[P_CHANNEL_BATCH_OFFSET] +
                channel * CHANNEL_BATCH_WORDS;
            ulong batch_count = stream_state[batch];
            if (
                local_exceeded == 0 &&
                (local_sum > capacity || batch_count > capacity - local_sum)
            ) {
                local_sum = capacity;
                local_exceeded = 1;
            } else if (local_exceeded == 0) {
                local_sum += batch_count;
            }
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
                if (
                    combined_exceeded == 0 &&
                    (sums[lane] > capacity || right_sum > capacity - sums[lane])
                ) {
                    combined_exceeded = 1;
                }
                sums[lane] =
                    combined_exceeded != 0 ? capacity : sums[lane] + right_sum;
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
        if (
            local_exceeded == 0 &&
            (local_sum > capacity || count > capacity - local_sum)
        ) {
            local_sum = capacity;
            local_exceeded = 1;
        } else if (local_exceeded == 0) {
            local_sum += count;
        }
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
            if (
                combined_exceeded == 0 &&
                (left_sum > capacity || own_sum > capacity - left_sum)
            ) {
                combined_exceeded = 1;
            }
            sums[lane] =
                combined_exceeded != 0 ? capacity : left_sum + own_sum;
            exceeded[lane] = combined_exceeded;
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }

    if (lane == 0 && exceeded[1023] != 0) {
        control[C_ERROR] = ERROR_CAPACITY;
        control[C_ERROR_ARENA] = ARENA_OUTBOX;
        control[C_ERROR_NODE] = NONE;
        control[C_ERROR_CAPACITY] = capacity;
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

kernel void days_exchange_scatter(
    device ulong *control [[buffer(0)]],
    const device ulong *params [[buffer(1)]],
    device ulong *outbox [[buffer(12)]],
    const device ulong *remote_meta [[buffer(19)]],
    const device ulong *remote_staging [[buffer(20)]],
    device ulong *stream_state [[buffer(25)]],
    device ulong *stream_records [[buffer(26)]],
    uint producer [[thread_position_in_grid]]
) {
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_CONTINUATION] != 2 ||
        producer >= params[P_NODE_COUNT]
    ) {
        return;
    }
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

kernel void days_exchange_merge(
    device ulong *control [[buffer(0)]],
    const device ulong *params [[buffer(1)]],
    device ulong *fel_meta [[buffer(7)]],
    device ulong *fel_records [[buffer(8)]],
    const device ulong *outbox [[buffer(12)]],
    device ulong *lp_state [[buffer(18)]],
    const device ulong *remote_meta [[buffer(19)]],
    const device ulong *inbound_meta [[buffer(22)]],
    const device ulong *inbound_producers [[buffer(23)]],
    device ulong *merge_cursors [[buffer(24)]],
    device ulong *stream_state [[buffer(25)]],
    const device ulong *stream_records [[buffer(26)]],
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
        if (!heap_push(target, event, error, fel_meta, fel_records)) {
            return;
        }
        merge_cursors[best_edge] += 1;
    }
}

// T15e diagnostic counterpart of `days_exchange_merge`. The merge itself is unchanged; a
// read-only pre-scan records actual producer fan-in and remote-event counts per target-round.
#if defined(DAYS_T15E_DIAGNOSTICS)
kernel void days_exchange_merge_fan_in_probe(
    device ulong *control [[buffer(0)]],
    const device ulong *params [[buffer(1)]],
    device ulong *fel_meta [[buffer(7)]],
    device ulong *fel_records [[buffer(8)]],
    const device ulong *outbox [[buffer(12)]],
    device ulong *lp_state [[buffer(18)]],
    const device ulong *remote_meta [[buffer(19)]],
    const device ulong *inbound_meta [[buffer(22)]],
    const device ulong *inbound_producers [[buffer(23)]],
    device ulong *merge_cursors [[buffer(24)]],
    device ulong *diagnostic_fan_in [[buffer(29)]],
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
    ulong inbound_base = ulong(target) * INBOUND_META_WORDS;
    ulong edge_start = inbound_meta[inbound_base];
    ulong edge_count = inbound_meta[inbound_base + 1];
    ulong active_fan_in = 0;
    ulong target_events = 0;
    for (ulong edge = edge_start; edge < edge_start + edge_count; ++edge) {
        merge_cursors[edge] = 0;
        ulong producer = inbound_producers[edge];
        ulong producer_base = producer * META_WORDS;
        ulong count = remote_meta[producer_base + 3];
        bool active = false;
        for (ulong cursor = 0; cursor < count; ++cursor) {
            ulong slot = remote_meta[producer_base + 2] + cursor;
            if (outbox[slot * EVENT_WORDS + E_TARGET] == target) {
                active = true;
                ulong next_target_events = target_events + 1;
                if (next_target_events < target_events) {
                    set_semantic_error(
                        lp_state + ulong(target) * LP_STATE_WORDS,
                        34,
                        target
                    );
                    return;
                }
                target_events = next_target_events;
            }
        }
        if (active) {
            ulong next_active_fan_in = active_fan_in + 1;
            if (next_active_fan_in < active_fan_in) {
                set_semantic_error(
                    lp_state + ulong(target) * LP_STATE_WORDS,
                    35,
                    target
                );
                return;
            }
            active_fan_in = next_active_fan_in;
        }
    }
    device ulong *error = lp_state + ulong(target) * LP_STATE_WORDS;
    if (active_fan_in != 0) {
        ulong diagnostic_base = ulong(target) * 4;
        ulong previous_rounds = diagnostic_fan_in[diagnostic_base];
        ulong previous_fan_in = diagnostic_fan_in[diagnostic_base + 1];
        ulong previous_events = diagnostic_fan_in[diagnostic_base + 2];
        ulong next_rounds = previous_rounds + 1;
        ulong next_fan_in = previous_fan_in + active_fan_in;
        ulong next_events = previous_events + target_events;
        if (
            next_rounds < previous_rounds ||
            next_fan_in < previous_fan_in ||
            next_events < previous_events
        ) {
            set_semantic_error(error, 36, target);
            return;
        }
        diagnostic_fan_in[diagnostic_base] = next_rounds;
        diagnostic_fan_in[diagnostic_base + 1] = next_fan_in;
        diagnostic_fan_in[diagnostic_base + 2] = next_events;
        diagnostic_fan_in[diagnostic_base + 3] =
            max(diagnostic_fan_in[diagnostic_base + 3], active_fan_in);
    }

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
        if (!heap_push(target, event, error, fel_meta, fel_records)) {
            return;
        }
        merge_cursors[best_edge] += 1;
    }
}
#endif

kernel void days_round_finalize(
    device ulong *control [[buffer(0)]],
    const device ulong *params [[buffer(1)]],
    const device ulong *lp_state [[buffer(18)]],
    const device ulong *observation_meta [[buffer(21)]],
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

    ulong capacities[3] = {
        params[P_OBSERVED_CAPACITY],
        params[P_DEPARTURE_CAPACITY],
        params[P_ARRIVAL_CAPACITY]
    };
    ulong local_totals[3] = {0, 0, 0};
    uint local_exceeded[3] = {0, 0, 0};
    ulong first_error = NONE;
    for (ulong node = lane; node < params[P_NODE_COUNT]; node += 1024) {
        ulong state = node * LP_STATE_WORDS;
        if (first_error == NONE && lp_state[state + L_ERROR] != 0) {
            first_error = node;
        }
        if (params[P_FULL_OBSERVATIONS] != 0) {
            ulong base = node * OBSERVATION_META_WORDS;
            for (uint log = 0; log < 3; ++log) {
                ulong count = observation_meta[base + log * META_WORDS + 3];
                if (
                    local_exceeded[log] == 0 &&
                    (
                        local_totals[log] > capacities[log] ||
                        count > capacities[log] - local_totals[log]
                    )
                ) {
                    local_totals[log] = capacities[log];
                    local_exceeded[log] = 1;
                } else if (local_exceeded[log] == 0) {
                    local_totals[log] += count;
                }
            }
        }
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
        values[lane] = local_totals[log];
        exceeded[lane] = local_exceeded[log];
        threadgroup_barrier(mem_flags::mem_threadgroup);
        for (uint stride = 512; stride != 0; stride >>= 1) {
            if (lane < stride) {
                ulong left = values[lane];
                ulong right = values[lane + stride];
                uint combined_exceeded = exceeded[lane] | exceeded[lane + stride];
                if (
                    combined_exceeded == 0 &&
                    (left > capacities[log] || right > capacities[log] - left)
                ) {
                    combined_exceeded = 1;
                }
                values[lane] =
                    combined_exceeded != 0 ? capacities[log] : left + right;
                exceeded[lane] = combined_exceeded;
            }
            threadgroup_barrier(mem_flags::mem_threadgroup);
        }
        if (lane == 0 && exceeded[0] != 0 && control[C_ERROR] == 0) {
            control[C_ERROR] = ERROR_CAPACITY;
            control[C_ERROR_ARENA] = arenas[log];
            control[C_ERROR_NODE] = NONE;
            control[C_ERROR_CAPACITY] = capacities[log];
        }
        threadgroup_barrier(mem_flags::mem_device | mem_flags::mem_threadgroup);
    }
    if (lane == 0 && control[C_ERROR] == 0) {
        control[C_CONTINUATION] = 0;
        control[C_ROUNDS] += 1;
    }
}
