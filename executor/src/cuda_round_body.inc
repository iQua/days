// P14: the body of both round kernels, `days_round` and `days_round_mechanisms` in
// `cuda_kernels.cu`, included verbatim by each after its block-scope `constexpr bool MECHANISMS`.
// Statements only: the including kernel supplies the `DAYS_BUFFERS` parameters.
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
        if (!hydrate_tx_complete_event(node, event, state, in_service)) {
            return;
        }
        if (!dispatch_event<MECHANISMS>(
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
        // P16 G1: the stage pass of this transition, after its own emissions. A transition that
        // left a continuation in `event` (a same-time TX_READY after TX_COMPLETE) is not a stage
        // transition, and the pass ignores the continuation's kind.
        if (MECHANISMS && params[P_STAGE_OFFSET] != NONE && !stage_after_event(
                node, event, state, params, node_state, generators, fel_meta, fel_records,
                queue_meta, queue_records, remote_meta, remote_staging, stream_state,
                stream_records, summary, observation_meta, observed, tcp_state)) {
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
