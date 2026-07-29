#include <metal_stdlib>
using namespace metal;

constant uint EVENT_WORDS = 11;
constant uint NODE_WORDS = 11;
constant uint GENERATOR_WORDS = 16;
constant uint FLOW_WORDS = 6;
constant uint LINK_WORDS = 4;
constant uint META_WORDS = 4;
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

constant ulong HOST = 0;
constant ulong SWITCH = 1;
constant ulong PACKET_ARRIVAL = 0;
constant ulong TX_READY = 1;
constant ulong TX_COMPLETE = 2;
constant ulong REMOTE_ARRIVAL = 3;
constant ulong DATA_PACKET = 0;
constant ulong FEEDBACK_PACKET = 1;

constant ulong ERROR_CAPACITY = 1;
constant ulong ERROR_TRANSITION_CAPACITY = 2;
constant ulong ERROR_SEMANTIC = 3;
constant ulong ARENA_FEL = 1;
constant ulong ARENA_QUEUE = 2;
constant ulong ARENA_OUTBOX = 3;
constant ulong ARENA_WORKLIST = 4;
constant ulong ARENA_OBSERVED = 5;
constant ulong ARENA_DEPARTURES = 6;
constant ulong ARENA_ARRIVALS = 7;

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
    device ulong *control,
    ulong arena,
    ulong node,
    ulong capacity
) {
    if (control[C_ERROR] == 0) {
        control[C_ERROR] = ERROR_CAPACITY;
        control[C_ERROR_ARENA] = arena;
        control[C_ERROR_NODE] = node;
        control[C_ERROR_CAPACITY] = capacity;
    }
}

inline void set_semantic_error(device ulong *control, ulong code, ulong node) {
    if (control[C_ERROR] == 0) {
        control[C_ERROR] = ERROR_SEMANTIC + code;
        control[C_ERROR_NODE] = node;
    }
}

inline bool heap_push(
    ulong node,
    const thread ulong *record,
    device ulong *control,
    device ulong *meta,
    device ulong *records
) {
    ulong base = node * META_WORDS;
    ulong offset = meta[base];
    ulong capacity = meta[base + 1];
    ulong count = meta[base + 3];
    if (count >= capacity) {
        set_capacity_error(control, ARENA_FEL, node, capacity);
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

inline bool before_horizon(ulong time, const device ulong *control) {
    return control[C_HORIZON_HI] != 0 || time < control[C_HORIZON_LO];
}

inline bool queue_push(
    ulong node,
    const thread ulong *record,
    device ulong *control,
    device ulong *meta,
    device ulong *records
) {
    ulong base = node * META_WORDS;
    ulong offset = meta[base];
    ulong capacity = meta[base + 1];
    ulong head = meta[base + 2];
    ulong count = meta[base + 3];
    if (count >= capacity) {
        set_capacity_error(control, ARENA_QUEUE, node, capacity);
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
    device ulong *control,
    device ulong *meta,
    device ulong *records
) {
    ulong base = node * META_WORDS;
    ulong offset = meta[base];
    ulong capacity = meta[base + 1];
    ulong head = meta[base + 2];
    ulong count = meta[base + 3];
    if (count >= capacity) {
        set_capacity_error(control, ARENA_QUEUE, node, capacity);
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
    thread ulong *record
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
    if (kind == TX_COMPLETE) {
        return 1;
    }
    if (kind == TX_READY) {
        return 2;
    }
    return 0;
}

inline void add_summary(device ulong *summary, uint counter, ulong value) {
    uint offset = counter * 2;
    ulong previous = summary[offset];
    ulong next = previous + value;
    summary[offset] = next;
    if (next < previous) {
        summary[offset + 1] += 1;
    }
}

inline bool append_observed(
    const thread ulong *packet,
    device ulong *control,
    const device ulong *params,
    device ulong *observed
) {
    if (params[P_FULL_OBSERVATIONS] == 0) {
        return true;
    }
    ulong index = control[C_OBSERVED];
    ulong capacity = params[P_OBSERVED_CAPACITY];
    if (index >= capacity) {
        set_capacity_error(control, ARENA_OBSERVED, NONE, capacity);
        return false;
    }
    ulong offset = index * 4;
    observed[offset] = packet[PK_ID];
    observed[offset + 1] = packet[PK_FLOW];
    observed[offset + 2] = packet[PK_SIZE];
    observed[offset + 3] = packet[PK_KIND];
    control[C_OBSERVED] = index + 1;
    return true;
}

inline bool record_sourced(
    const thread ulong *packet,
    device ulong *control,
    const device ulong *params,
    device ulong *summary,
    device ulong *observed
) {
    add_summary(summary, 0, 1);
    add_summary(summary, 1, packet[PK_SIZE]);
    return append_observed(packet, control, params, observed);
}

inline bool record_departure(
    const thread ulong *event,
    device ulong *control,
    const device ulong *params,
    device ulong *summary,
    device ulong *observed,
    device ulong *departures
) {
    add_summary(summary, 2, 1);
    add_summary(summary, 3, event[PK_SIZE]);
    if (!append_observed(event, control, params, observed)) {
        return false;
    }
    if (params[P_FULL_OBSERVATIONS] == 0) {
        return true;
    }
    ulong index = control[C_DEPARTURES];
    ulong capacity = params[P_DEPARTURE_CAPACITY];
    if (index >= capacity) {
        set_capacity_error(control, ARENA_DEPARTURES, NONE, capacity);
        return false;
    }
    ulong offset = index * 9;
    departures[offset] = event[E_TIME];
    departures[offset + 1] = event[E_PHASE];
    departures[offset + 2] = event[E_ORIGIN];
    departures[offset + 3] = event[E_SEQUENCE];
    departures[offset + 4] = event[PK_ID];
    departures[offset + 5] = event[E_TIME];
    departures[offset + 6] = event[PK_FLOW];
    departures[offset + 7] = event[PK_SIZE];
    departures[offset + 8] = event[PK_KIND];
    control[C_DEPARTURES] = index + 1;
    return true;
}

inline bool record_arrival(
    const thread ulong *event,
    ulong disposition,
    device ulong *control,
    const device ulong *params,
    device ulong *summary,
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
    add_summary(summary, counter, 1);
    add_summary(summary, counter + 1, event[PK_SIZE]);
    if (!append_observed(event, control, params, observed)) {
        return false;
    }
    if (params[P_FULL_OBSERVATIONS] == 0) {
        return true;
    }
    ulong index = control[C_ARRIVALS];
    ulong capacity = params[P_ARRIVAL_CAPACITY];
    if (index >= capacity) {
        set_capacity_error(control, ARENA_ARRIVALS, NONE, capacity);
        return false;
    }
    ulong offset = index * 10;
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
    control[C_ARRIVALS] = index + 1;
    return true;
}

inline bool append_remote(
    const thread ulong *record,
    device ulong *control,
    const device ulong *params,
    device ulong *outbox
) {
    ulong index = control[C_OUTBOX];
    ulong capacity = params[P_OUTBOX_CAPACITY];
    if (index >= capacity) {
        set_capacity_error(control, ARENA_OUTBOX, NONE, capacity);
        return false;
    }
    copy_thread_to_device(record, outbox, index);
    control[C_OUTBOX] = index + 1;
    return true;
}

inline bool emit_child(
    ulong node,
    const thread ulong *parent,
    ulong target,
    ulong kind,
    ulong time,
    const thread ulong *packet,
    device ulong *control,
    const device ulong *params,
    device ulong *node_state,
    device ulong *fel_meta,
    device ulong *fel_records,
    device ulong *outbox
) {
    ulong node_base = node * NODE_WORDS;
    ulong sequence = node_state[node_base + N_NEXT_ORIGIN];
    if (sequence == NONE) {
        set_semantic_error(control, 1, node);
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
        set_semantic_error(control, 2, node);
        return false;
    }
    if (target == node) {
        return heap_push(node, child, control, fel_meta, fel_records);
    }
    return append_remote(child, control, params, outbox);
}

inline bool checked_add(ulong left, ulong right, thread ulong &result) {
    result = left + right;
    return result >= left;
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

inline bool flow_route(
    const thread ulong *packet,
    const device ulong *flows,
    thread ulong &offset,
    thread ulong &length,
    thread ulong &terminal
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

inline bool dispatch_event(
    ulong node,
    thread ulong *event,
    device ulong *control,
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
    device ulong *outbox,
    device ulong *summary,
    device ulong *observed,
    device ulong *departures,
    device ulong *arrivals
) {
    ulong node_base = node * NODE_WORDS;
    ulong role = node_state[node_base + N_KIND];
    ulong kind = event[E_KIND];

    if (kind == PACKET_ARRIVAL) {
        if (role != HOST) {
            set_semantic_error(control, 3, node);
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
                set_semantic_error(control, 4, node);
                return false;
            }
            if (
                generators[generator_base + 2] == NONE ||
                generators[generator_base + 3] > NONE - event[PK_SIZE]
            ) {
                set_semantic_error(control, 5, node);
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
                    set_semantic_error(control, 6, node);
                    return false;
                }
            } else {
                ulong end;
                if (!checked_add(
                    generators[generator_base + 11],
                    generators[generator_base + 15],
                    end
                )) {
                    set_semantic_error(control, 6, node);
                    return false;
                }
                if (!checked_add(
                    event[E_TIME],
                    generators[generator_base + 12],
                    candidate
                )) {
                    set_semantic_error(control, 6, node);
                    return false;
                }
                semantic_next = candidate < end;
            }
            if (semantic_next && candidate <= params[P_STOP_TIME]) {
                ulong sequence = node_state[node_base + N_NEXT_PAYLOAD];
                if (sequence == NONE || sequence > (NONE - node) / params[P_NODE_COUNT]) {
                    set_semantic_error(control, 7, node);
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
                    control,
                    params,
                    node_state,
                    fel_meta,
                    fel_records,
                    outbox
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
            set_semantic_error(control, 8, node);
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
            control,
            queue_meta,
            queue_records
        )) {
            return false;
        }
        if (!record_sourced(event, control, params, summary, observed)) {
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
                control,
                params,
                node_state,
                fel_meta,
                fel_records,
                outbox
            )) {
                return false;
            }
        }
        return true;
    }

    if (kind == TX_READY) {
        node_state[node_base + N_READY_PENDING] = 0;
        if (node_state[node_base + N_SERVICE_VALID] != 0) {
            set_semantic_error(control, 9, node);
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
            set_semantic_error(control, 10, node);
            return false;
        }
        ulong link_base = egress * LINK_WORDS;
        if (links[link_base] != node) {
            set_semantic_error(control, 11, node);
            return false;
        }
        ulong serialization;
        if (!serialization_ns(selected[PK_SIZE], links[link_base + 2], serialization)) {
            set_semantic_error(control, 12, node);
            return false;
        }
        ulong departure_time;
        ulong arrival_time;
        if (
            !checked_add(event[E_TIME], serialization, departure_time) ||
            !checked_add(departure_time, links[link_base + 3], arrival_time)
        ) {
            set_semantic_error(control, 13, node);
            return false;
        }
        ulong target;
        if (!packet_remote_target(selected, egress, flows, routes, links, target)) {
            set_semantic_error(control, 14, node);
            return false;
        }
        if (!emit_child(
            node,
            event,
            node,
            TX_COMPLETE,
            departure_time,
            selected,
            control,
            params,
            node_state,
            fel_meta,
            fel_records,
            outbox
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
            control,
            params,
            node_state,
            fel_meta,
            fel_records,
            outbox
        );
    }

    if (kind == TX_COMPLETE) {
        if (
            node_state[node_base + N_SERVICE_VALID] == 0 ||
            in_service[node * EVENT_WORDS + PK_ID] != event[PK_ID]
        ) {
            set_semantic_error(control, 15, node);
            return false;
        }
        node_state[node_base + N_SERVICE_VALID] = 0;
        uint departure_counter = role == HOST ? N_COUNTER_1 : N_COUNTER_2;
        if (node_state[node_base + departure_counter] == NONE) {
            set_semantic_error(control, 16, node);
            return false;
        }
        node_state[node_base + departure_counter] += 1;
        if (!record_departure(
            event,
            control,
            params,
            summary,
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
                set_semantic_error(control, 17, node);
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
                control,
                params,
                node_state,
                fel_meta,
                fel_records,
                outbox
            );
        }
        return true;
    }

    if (kind == REMOTE_ARRIVAL && role == SWITCH) {
        if (node_state[node_base + N_COUNTER_0] == NONE) {
            set_semantic_error(control, 18, node);
            return false;
        }
        node_state[node_base + N_COUNTER_0] += 1;
        ulong egress;
        if (!packet_egress(node, event, flows, routes, links, egress)) {
            set_semantic_error(control, 19, node);
            return false;
        }
        if (egress != node_state[node_base + N_EGRESS]) {
            set_semantic_error(control, 20, node);
            return false;
        }
        ulong waiting = queue_meta[node * META_WORDS + 3];
        ulong semantic_capacity = node_state[node_base + N_SEMANTIC_QUEUE_CAPACITY];
        if (semantic_capacity != 0 && waiting >= semantic_capacity) {
            if (node_state[node_base + N_COUNTER_1] == NONE) {
                set_semantic_error(control, 21, node);
                return false;
            }
            node_state[node_base + N_COUNTER_1] += 1;
            return record_arrival(
                event,
                1,
                control,
                params,
                summary,
                observed,
                arrivals
            );
        }
        if (!queue_push(node, event, control, queue_meta, queue_records)) {
            return false;
        }
        if (!record_arrival(
            event,
            0,
            control,
            params,
            summary,
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
                control,
                params,
                node_state,
                fel_meta,
                fel_records,
                outbox
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
                set_semantic_error(control, 22, node);
                return false;
            }
            generators[generator_base + 8] += 1;
            disposition = 3;
        } else {
            ulong expected =
                event[PK_KIND] == DATA_PACKET ? flows[flow_base + 1] : flows[flow_base];
            if (expected != node) {
                set_semantic_error(control, 23, node);
                return false;
            }
            if (node_state[node_base + N_COUNTER_2] == NONE) {
                set_semantic_error(control, 24, node);
                return false;
            }
            node_state[node_base + N_COUNTER_2] += 1;
        }
        return record_arrival(
            event,
            disposition,
            control,
            params,
            summary,
            observed,
            arrivals
        );
    }

    set_semantic_error(control, 25, node);
    return false;
}

kernel void days_horizon(
    device ulong *control [[buffer(0)]],
    const device ulong *params [[buffer(1)]],
    const device ulong *fel_meta [[buffer(7)]],
    const device ulong *fel_records [[buffer(8)]],
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
            heap_root_time(node, fel_meta, fel_records, candidate) &&
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
    device ulong *outbox [[buffer(12)]],
    device ulong *worklist [[buffer(13)]],
    device ulong *summary [[buffer(14)]],
    device ulong *observed [[buffer(15)]],
    device ulong *departures [[buffer(16)]],
    device ulong *arrivals [[buffer(17)]]
) {
    if (
        control[C_ERROR] != 0 ||
        control[C_DONE] != 0 ||
        control[C_ROUNDS] >= params[P_ROUND_CAPACITY]
    ) {
        return;
    }
    if (control[C_CONTINUATION] == 0) {
        ulong active = 0;
        for (ulong node = 0; node < params[P_NODE_COUNT]; ++node) {
            ulong time;
            if (
                heap_root_time(node, fel_meta, fel_records, time) &&
                before_horizon(time, control)
            ) {
                if (active >= params[P_WORKLIST_CAPACITY]) {
                    set_capacity_error(
                        control,
                        ARENA_WORKLIST,
                        NONE,
                        params[P_WORKLIST_CAPACITY]
                    );
                    return;
                }
                worklist[active++] = node;
            }
        }
        control[C_ACTIVE] = active;
        control[C_OUTBOX] = 0;
        control[C_CONTINUATION] = 1;
    } else {
        control[C_RELAUNCHES] += 1;
    }

    ulong active = control[C_ACTIVE];
    ulong active_index = control[C_CONTINUATION] - 1;
    ulong dispatch_transitions = 0;
    for (; active_index < active; ++active_index) {
        ulong node = worklist[active_index];
        while (true) {
            ulong time;
            if (
                !heap_root_time(node, fel_meta, fel_records, time) ||
                !before_horizon(time, control)
            ) {
                break;
            }
            if (dispatch_transitions >= params[P_TRANSITION_CAPACITY]) {
                control[C_CONTINUATION] = active_index + 1;
                return;
            }
            ulong event[EVENT_WORDS];
            if (!heap_pop(node, fel_meta, fel_records, event)) {
                set_semantic_error(control, 26, node);
                return;
            }
            if (!dispatch_event(
                node,
                event,
                control,
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
                outbox,
                summary,
                observed,
                departures,
                arrivals
            )) {
                return;
            }
            dispatch_transitions += 1;
            if (control[C_TRANSITIONS] == NONE) {
                set_semantic_error(control, 27, node);
                return;
            }
            control[C_TRANSITIONS] += 1;
        }
    }

    ulong remote_count = control[C_OUTBOX];
    for (ulong index = 0; index < remote_count; ++index) {
        ulong event[EVENT_WORDS];
        copy_device_to_thread(outbox, index, event);
        if (before_horizon(event[E_TIME], control)) {
            set_semantic_error(control, 28, event[E_TARGET]);
            return;
        }
        if (!heap_push(
            event[E_TARGET],
            event,
            control,
            fel_meta,
            fel_records
        )) {
            return;
        }
    }
    control[C_CONTINUATION] = 0;
    control[C_ROUNDS] += 1;
}
