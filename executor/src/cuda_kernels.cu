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
    ulong *merge_cursors, ulong *stream_state, ulong *stream_records

constexpr uint EVENT_WORDS = 11;
constexpr uint NODE_WORDS = 11;
constexpr uint GENERATOR_WORDS = 16;
constexpr uint FLOW_WORDS = 6;
constexpr uint LINK_WORDS = 4;
constexpr uint META_WORDS = 4;
constexpr uint LP_STATE_WORDS = 6;
constexpr uint OBSERVATION_META_WORDS = 12;
constexpr uint INBOUND_META_WORDS = 2;
constexpr uint LP_STREAM_META_WORDS = 4;
constexpr uint OUTBOUND_META_WORDS = 2;
constexpr uint OUTBOUND_ENTRY_WORDS = 2;
constexpr uint CHANNEL_BATCH_WORDS = 4;
constexpr uint ACTIVE_STREAM_ENTRY_WORDS = 5;
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

constexpr ulong HOST = 0;
constexpr ulong SWITCH = 1;
constexpr ulong PACKET_ARRIVAL = 0;
constexpr ulong TX_READY = 1;
constexpr ulong TX_COMPLETE = 2;
constexpr ulong REMOTE_ARRIVAL = 3;
constexpr ulong DATA_PACKET = 0;
constexpr ulong FEEDBACK_PACKET = 1;

constexpr ulong ERROR_CAPACITY = 1;
constexpr ulong ERROR_TRANSITION_CAPACITY = 2;
constexpr ulong ERROR_SEMANTIC = 3;
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

constexpr uint L_FINISHED = 0;
constexpr uint L_TRANSITIONS = 1;
constexpr uint L_ERROR = 2;
constexpr uint L_ERROR_ARENA = 3;
constexpr uint L_ERROR_NODE = 4;
constexpr uint L_ERROR_CAPACITY = 5;

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

__device__ __forceinline__ void set_capacity_error(
    ulong *error,
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

__device__ __forceinline__ void set_semantic_error(ulong *error, ulong code, ulong node) {
    if (error[L_ERROR] == 0) {
        error[L_ERROR] = ERROR_SEMANTIC + code;
        error[L_ERROR_NODE] = node;
    }
}

__device__ __forceinline__ bool heap_push(
    ulong node,
    const ulong *record,
    ulong *error,
    ulong *meta,
    ulong *records
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

__device__ __forceinline__ bool heap_pop(
    ulong node,
    ulong *meta,
    ulong *records,
    ulong *record
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

__device__ __forceinline__ bool active_refresh_source(
    ulong node,
    ulong stream,
    const ulong *params,
    ulong *error,
    ulong *stream_state,
    const ulong *records,
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

__device__ __forceinline__ bool fallback_push(
    ulong node,
    const ulong *record,
    const ulong *params,
    ulong *error,
    ulong *fel_meta,
    ulong *fel_records,
    ulong *stream_state
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

__device__ __forceinline__ bool classified_push(
    ulong node,
    const ulong *record,
    const ulong *params,
    ulong *error,
    ulong *fel_meta,
    ulong *fel_records,
    ulong *stream_state,
    ulong *stream_records
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
    ulong *record
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

__device__ __forceinline__ bool fel_pop(
    ulong node,
    const ulong *params,
    ulong *fel_meta,
    ulong *fel_records,
    ulong *stream_state,
    ulong *stream_records,
    ulong *record
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

__device__ __forceinline__ bool queue_pop(
    ulong node,
    ulong *meta,
    const ulong *records,
    ulong *record
) {
    ulong base = node * META_WORDS;
    ulong offset = meta[base];
    ulong capacity = meta[base + 1];
    ulong head = meta[base + 2];
    ulong count = meta[base + 3];
    if (count == 0) {
        return false;
    }
    copy_device_to_thread(records, offset + head, record);
    meta[base + 2] = (head + 1) % max(capacity, 1ul);
    meta[base + 3] = count - 1;
    return true;
}

__device__ __forceinline__ bool queue_front(
    ulong node,
    const ulong *meta,
    const ulong *records,
    ulong *record
) {
    ulong base = node * META_WORDS;
    if (meta[base + 3] == 0) {
        return false;
    }
    copy_device_to_thread(records, meta[base] + meta[base + 2], record);
    return true;
}

__device__ __forceinline__ ulong event_phase(ulong kind) {
    if (kind == TX_COMPLETE) {
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
        set_capacity_error(error, ARENA_OBSERVED, NONE, params[P_OBSERVED_CAPACITY]);
        return false;
    }
    ulong offset = (observation_meta[meta] + index) * 4;
    observed[offset] = packet[PK_ID];
    observed[offset + 1] = packet[PK_FLOW];
    observed[offset + 2] = packet[PK_SIZE];
    observed[offset + 3] = packet[PK_KIND];
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
        set_capacity_error(error, ARENA_DEPARTURES, NONE, params[P_DEPARTURE_CAPACITY]);
        return false;
    }
    ulong offset = (observation_meta[meta] + index) * 9;
    departures[offset] = event[E_TIME];
    departures[offset + 1] = event[E_PHASE];
    departures[offset + 2] = event[E_ORIGIN];
    departures[offset + 3] = event[E_SEQUENCE];
    departures[offset + 4] = event[PK_ID];
    departures[offset + 5] = event[E_TIME];
    departures[offset + 6] = event[PK_FLOW];
    departures[offset + 7] = event[PK_SIZE];
    departures[offset + 8] = event[PK_KIND];
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
        set_capacity_error(error, ARENA_ARRIVALS, NONE, params[P_ARRIVAL_CAPACITY]);
        return false;
    }
    ulong offset = (observation_meta[meta] + index) * 10;
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
    ulong *stream_records
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

__device__ __forceinline__ bool checked_add(ulong left, ulong right, ulong &result) {
    result = left + right;
    return result >= left;
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

__device__ __forceinline__ bool flow_route(
    const ulong *packet,
    const ulong *flows,
    ulong &offset,
    ulong &length,
    ulong &terminal
) {
    ulong flow_base = packet[PK_FLOW] * FLOW_WORDS;
    if (packet[PK_KIND] == DATA_PACKET) {
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

__device__ __forceinline__ bool dispatch_event(
    ulong node,
    ulong *event,
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
    ulong *remote_meta,
    ulong *remote_staging,
    ulong *stream_state,
    ulong *stream_records,
    ulong *summary,
    ulong *observation_meta,
    ulong *observed,
    ulong *departures,
    ulong *arrivals
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
            if (generators[generator_base + 14] == 0) {
                semantic_next =
                    generators[generator_base + 3] < generators[generator_base + 15];
                if (
                    semantic_next &&
                    !checked_add(
                        event[E_TIME],
                        generators[generator_base + 12],
                        candidate
                    )
                ) {
                    set_semantic_error(error, 6, node);
                    return false;
                }
            } else {
                ulong end;
                if (!checked_add(
                    generators[generator_base + 11],
                    generators[generator_base + 15],
                    end
                )) {
                    set_semantic_error(error, 6, node);
                    return false;
                }
                if (!checked_add(
                    event[E_TIME],
                    generators[generator_base + 12],
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
                next_packet[PK_SIZE] = generators[generator_base + 13];
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
        if (!queue_pop(node, queue_meta, queue_records, selected)) {
            return true;
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
        if (!queue_push(node, event, error, queue_meta, queue_records)) {
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
                event[PK_KIND] == DATA_PACKET ? flows[flow_base + 1] : flows[flow_base];
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
            remote_meta,
            remote_staging,
            stream_state,
            stream_records,
            summary,
            observation_meta,
            observed,
            departures,
            arrivals
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
    } else if (unfinished_lanes[0] != 0) {
        control[C_RELAUNCHES] += 1;
    } else {
        control[C_CONTINUATION] = 2;
    }
}

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
        __syncthreads();
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
    __syncthreads();
    for (uint offset = 1; offset < 1024; offset <<= 1) {
        ulong left_sum = lane >= offset ? sums[lane - offset] : 0;
        uint left_exceeded = lane >= offset ? exceeded[lane - offset] : 0;
        ulong own_sum = sums[lane];
        uint own_exceeded = exceeded[lane];
        __syncthreads();
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
        __syncthreads();
    }

    if (lane == 0 && exceeded[1023] != 0) {
        control[C_ERROR] = ERROR_CAPACITY;
        control[C_ERROR_ARENA] = ARENA_OUTBOX;
        control[C_ERROR_NODE] = NONE;
        control[C_ERROR_CAPACITY] = capacity;
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

extern "C" __global__ void days_exchange_scatter(DAYS_BUFFERS) {
    uint producer = blockIdx.x * blockDim.x + threadIdx.x;
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
        if (!heap_push(target, event, error, fel_meta, fel_records)) {
            return;
        }
        merge_cursors[best_edge] += 1;
    }
}

// T15e diagnostic counterpart of `days_exchange_merge`. The merge itself is unchanged; a
// read-only pre-scan records actual producer fan-in and remote-event counts per target-round.
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
        values[lane] = local_totals[log];
        exceeded[lane] = local_exceeded[log];
        __syncthreads();
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
            __syncthreads();
        }
        if (lane == 0 && exceeded[0] != 0 && control[C_ERROR] == 0) {
            control[C_ERROR] = ERROR_CAPACITY;
            control[C_ERROR_ARENA] = arenas[log];
            control[C_ERROR_NODE] = NONE;
            control[C_ERROR_CAPACITY] = capacities[log];
        }
        __syncthreads();
    }
    if (lane == 0 && control[C_ERROR] == 0) {
        control[C_CONTINUATION] = 0;
        control[C_ROUNDS] += 1;
    }
}
