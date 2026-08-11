#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

readonly BUSY_PCT="20.0"
readonly PRE_GATE_PROBES=3
readonly POST_GATE_PROBES=1
readonly SAMPLE_COUNT=5
readonly CUDA_TIMEOUT_SECONDS=7200
readonly CAPACITY_CONTRACT="fallback_fel:16384,queue:2048,channel:2048,remote_staging:2048,outbox:2000000,tcp_ranges:64,tcp_ledger:4096,observation:512"
readonly PRODUCTION_DISPATCH_NAMES="days_horizon_sweep,days_horizon,days_round_reset,days_round_prepare,days_round,days_round_control_sweep,days_round_control,days_exchange_prefix_sweep,days_exchange_prefix,days_exchange_scatter,days_exchange_merge,days_round_finalize"
readonly SPLIT_DISPATCH_NAMES="days_horizon_sweep,days_horizon,days_round_reset,days_round_prepare_count_profile,days_round_prepare_prefix_profile,days_round_prepare_write_profile,days_round_prepare_combine_profile,days_round,days_round_control_sweep,days_round_control,days_exchange_prefix_sweep,days_exchange_prefix,days_exchange_scatter,days_exchange_merge,days_round_finalize"

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd -P)
REPO=$(git -C "$SCRIPT_DIR/.." rev-parse --show-toplevel 2>/dev/null || true)
MANIFEST="$SCRIPT_DIR/t32-instrumentation-fixtures.tsv"
ANALYZER="$SCRIPT_DIR/t32-instrumentation-analyze.py"
CURRENT_LOG=""

fail() {
    local reason="$1" code="$2" detail="${3:-none}"
    local line="T32_PROTOCOL_FAIL reason=$reason detail=$detail"
    printf '%s\n' "$line" >&2
    if [[ -n "$CURRENT_LOG" && -e "$CURRENT_LOG" ]]; then
        printf '%s\n' "$line" >>"$CURRENT_LOG"
    fi
    exit "$code"
}

sha256_file() {
    local path="$1"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$path" | awk '{print $1}'
    else
        shasum -a 256 "$path" | awk '{print $1}'
    fi
}

sha256_stdin() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum | awk '{print $1}'
    else
        shasum -a 256 | awk '{print $1}'
    fi
}

append_record() {
    printf '%s\n' "$1" | tee -a "$CURRENT_LOG"
}

require_literal_commit() {
    local pin="$1"
    [[ "$pin" =~ ^[0-9a-f]{40}$ ]] || fail INVALID_PINNED_COMMIT 68 "value=$pin"
    git -C "$REPO" cat-file -e "${pin}^{commit}" 2>/dev/null \
        || fail INVALID_PINNED_COMMIT 68 "unknown_commit=$pin"
}

verify_tooling_at_pin() {
    local pin="$1" relative working_hash pinned_hash
    for relative in \
        scripts/t32-instrumentation-protocol.sh \
        scripts/t32-instrumentation-analyze.py \
        scripts/t32-instrumentation-fixtures.tsv; do
        git -C "$REPO" cat-file -e "$pin:$relative" 2>/dev/null \
            || fail TOOLING_DRIFT 67 "missing_at_pin=$relative"
        working_hash=$(sha256_file "$REPO/$relative")
        pinned_hash=$(git -C "$REPO" show "$pin:$relative" | sha256_stdin)
        [[ "$working_hash" == "$pinned_hash" ]] \
            || fail TOOLING_DRIFT 67 "path=$relative expected=$pinned_hash actual=$working_hash"
    done
}

verify_fixture_manifest() {
    local count=0 name path expected_sha rounds transitions bytes fnv actual_sha
    local seen=" "
    while IFS=$'\t' read -r name path expected_sha rounds transitions bytes fnv; do
        [[ -n "$name" ]] || continue
        count=$((count + 1))
        [[ "$path" != /* && "$path" != *".."* ]] \
            || fail FIXTURE_MANIFEST_INVALID 72 "fixture=$name path=$path"
        [[ "$seen" != *" $name "* ]] \
            || fail FIXTURE_MANIFEST_INVALID 72 "duplicate_fixture=$name"
        seen="$seen$name "
        git -C "$REPO" cat-file -e "HEAD:$path" 2>/dev/null \
            || fail FIXTURE_MANIFEST_INVALID 72 "fixture=$name untracked_or_missing=$path"
        [[ -f "$REPO/$path" ]] \
            || fail FIXTURE_SHA256_MISMATCH 72 "fixture=$name missing=$path"
        actual_sha=$(sha256_file "$REPO/$path")
        [[ "$actual_sha" == "$expected_sha" ]] \
            || fail FIXTURE_SHA256_MISMATCH 72 "fixture=$name expected=$expected_sha actual=$actual_sha"
    done <"$MANIFEST"
    [[ "$count" -eq 7 ]] || fail FIXTURE_MANIFEST_INVALID 72 "fixture_count=$count"
}

verify_source_state() {
    local pin="$1" actual status
    require_literal_commit "$pin"
    actual=$(git -C "$REPO" rev-parse --verify 'HEAD^{commit}')
    [[ "$actual" == "$pin" ]] \
        || fail PINNED_COMMIT_MISMATCH 71 "expected=$pin actual=$actual"
    verify_tooling_at_pin "$pin"
    verify_fixture_manifest
    status=$(git -C "$REPO" status --porcelain=v1 --untracked-files=all)
    [[ -z "$status" ]] || fail DIRTY_WORKTREE 70 "entries=$(printf '%s\n' "$status" | wc -l | tr -d ' ')"
}

preflight() {
    local pin="$1" actual manifest_sha
    verify_source_state "$pin"
    actual=$(git -C "$REPO" rev-parse --verify 'HEAD^{commit}')
    manifest_sha=$(sha256_file "$MANIFEST")
    printf 'record=t32_protocol_source expected_commit=%s actual_commit=%s clean=true\n' "$pin" "$actual"
    printf 'record=t32_protocol_preflight status=PASS fixture_count=7 worktree=clean manifest_sha256=%s\n' "$manifest_sha"
}

reject_gate_overrides() {
    local variable
    for variable in BUSY_PCT GATE_CLEAR_REQUIRED GATE_MAX_PROBES GATE_RETRY_SECONDS; do
        if printenv "$variable" >/dev/null 2>&1; then
            fail QUIET_GATE_OVERRIDE 81 "variable=$variable"
        fi
    done
}

own_process_tree() {
    local snapshot="$1"
    awk -v self="$$" '
        { parent[$1] = $2 }
        END {
            for (pid in parent) {
                cursor = pid
                for (depth = 0; depth < 128 && cursor != "" && cursor != "0" && cursor != "1"; depth++) {
                    if (cursor == self) { print pid; break }
                    cursor = parent[cursor]
                }
            }
            print self
        }' "$snapshot" | sort -n -u
}

quiet_probe() {
    local sample_id="$1" surface="$2" phase="$3" probe="$4"
    local snapshot tree busy gpu gpu_external process_rows gpu_rows external_busy external_gpu
    local tree_line busy_lines gpu_lines gpu_external_lines tree_sha process_sha busy_sha gpu_sha gpu_external_sha
    snapshot=$(mktemp "${TMPDIR:-/tmp}/t32-processes.XXXXXX")
    ps -Ao pid=,ppid=,%cpu=,command= >"$snapshot"
    tree=$(own_process_tree "$snapshot" | tr '\n' ' ')
    busy=$(awk -v limit="$BUSY_PCT" -v tree=" $tree " \
        'NF >= 3 && $3 + 0 >= limit + 0 && index(tree, " " $1 " ") == 0 { print }' "$snapshot")
    gpu=""
    if command -v nvidia-smi >/dev/null 2>&1; then
        gpu=$(nvidia-smi --query-compute-apps=pid,used_memory,name --format=csv,noheader 2>/dev/null || true)
    fi
    gpu_external=$(printf '%s\n' "$gpu" | awk -F', *' -v tree=" $tree " \
        'NF && index(tree, " " $1 " ") == 0 { print }')
    process_rows=$(wc -l <"$snapshot" | tr -d ' ')
    if [[ -n "$gpu" ]]; then
        gpu_rows=$(printf '%s\n' "$gpu" | awk 'NF { count++ } END { print count + 0 }')
    else
        gpu_rows=0
    fi
    if [[ -n "$busy" ]]; then
        external_busy=$(printf '%s\n' "$busy" | awk 'NF { count++ } END { print count + 0 }')
    else
        external_busy=0
    fi
    if [[ -n "$gpu_external" ]]; then
        external_gpu=$(printf '%s\n' "$gpu_external" | awk 'NF { count++ } END { print count + 0 }')
    else
        external_gpu=0
    fi
    tree_line="runner_descendant_pids=$tree"
    busy_lines="${busy:-none}"
    gpu_lines="${gpu:-none}"
    gpu_external_lines="${gpu_external:-none}"
    tree_sha=$(printf '%s\n' "$tree_line" | sha256_stdin)
    process_sha=$(sha256_file "$snapshot")
    busy_sha=$(printf '%s\n' "$busy_lines" | sha256_stdin)
    gpu_sha=$(printf '%s\n' "$gpu_lines" | sha256_stdin)
    gpu_external_sha=$(printf '%s\n' "$gpu_external_lines" | sha256_stdin)
    {
        printf '%s\n' "--- T32_QUIET_CAPTURE sample_id=$sample_id surface=$surface phase=$phase probe=$probe ---"
        printf '%s\n' "$tree_line"
        printf '%s\n' 'process_snapshot_begin'
        cat "$snapshot"
        printf '%s\n' 'process_snapshot_end'
        printf '%s\n' 'external_busy_begin'
        printf '%s\n' "$busy_lines"
        printf '%s\n' 'external_busy_end'
        printf '%s\n' 'gpu_compute_apps_begin'
        printf '%s\n' "$gpu_lines"
        printf '%s\n' 'gpu_compute_apps_end'
        printf '%s\n' 'external_gpu_begin'
        printf '%s\n' "$gpu_external_lines"
        printf '%s\n' 'external_gpu_end'
        printf '%s\n' "--- T32_QUIET_CAPTURE_END sample_id=$sample_id surface=$surface phase=$phase probe=$probe ---"
    } >>"$CURRENT_LOG"
    rm -f "$snapshot"
    if [[ "$external_busy" -ne 0 || "$external_gpu" -ne 0 ]]; then
        append_record "record=t32_protocol_quiet_capture sample_id=$sample_id surface=$surface phase=$phase probe=$probe threshold_pct=$BUSY_PCT lineage=runner_descendants process_rows=$process_rows gpu_rows=$gpu_rows external_busy=$external_busy external_gpu=$external_gpu tree_sha256=$tree_sha process_sha256=$process_sha external_busy_sha256=$busy_sha gpu_sha256=$gpu_sha external_gpu_sha256=$gpu_external_sha status=BUSY"
        fail QUIET_GATE_BUSY 82 "sample_id=$sample_id surface=$surface phase=$phase probe=$probe"
    fi
    append_record "record=t32_protocol_quiet_capture sample_id=$sample_id surface=$surface phase=$phase probe=$probe threshold_pct=$BUSY_PCT lineage=runner_descendants process_rows=$process_rows gpu_rows=$gpu_rows external_busy=0 external_gpu=0 tree_sha256=$tree_sha process_sha256=$process_sha external_busy_sha256=$busy_sha gpu_sha256=$gpu_sha external_gpu_sha256=$gpu_external_sha status=CLEAR"
}

quiet_boundary() {
    local sample_id="$1" surface="$2" phase="$3" probes="$4" probe
    for ((probe = 1; probe <= probes; probe++)); do
        quiet_probe "$sample_id" "$surface" "$phase" "$probe"
        if [[ "$probe" -lt "$probes" ]]; then
            sleep 1
        fi
    done
    append_record "record=t32_protocol_quiet_summary sample_id=$sample_id surface=$surface phase=$phase threshold_pct=$BUSY_PCT lineage=runner_descendants probes=$probes captures=$probes status=CLEAR"
}

field() {
    local line="$1" key="$2"
    printf '%s\n' "$line" | awk -v wanted="$key" '
        {
            for (i = 1; i <= NF; i++) {
                split($i, pair, "=")
                if (pair[1] == wanted) {
                    sub("^[^=]*=", "", $i)
                    print $i
                    found++
                }
            }
        }
        END { if (found != 1) exit 1 }'
}

reason_code() {
    case "$1" in
        IDENTITY_MISMATCH) printf '74\n' ;;
        MISSING_PREPARE_COMPARISON) printf '78\n' ;;
        COUNTER_DIGEST_MISMATCH) printf '79\n' ;;
        PROTOCOL_CONTRACT_MISMATCH) printf '80\n' ;;
        *) printf '69\n' ;;
    esac
}

single_record() {
    local capture="$1" record_name="$2" reason="${3:-IDENTITY_MISMATCH}" count
    count=$(grep -c "^record=${record_name} " "$capture" || true)
    [[ "$count" -eq 1 ]] \
        || fail "$reason" "$(reason_code "$reason")" "record=$record_name count=$count"
    grep "^record=${record_name} " "$capture"
}

require_field() {
    local line="$1" key="$2" expected="$3" reason="${4:-IDENTITY_MISMATCH}" actual
    actual=$(field "$line" "$key" 2>/dev/null || true)
    [[ "$actual" == "$expected" ]] \
        || fail "$reason" "$(reason_code "$reason")" "field=$key expected=$expected actual=${actual:-missing}"
}

append_capture() {
    local capture="$1"
    while IFS= read -r line || [[ -n "$line" ]]; do
        printf '%s\n' "$line" >>"$CURRENT_LOG"
    done <"$capture"
}

run_captured() {
    local capture="$1"
    shift
    local rc=0
    "$@" >"$capture" 2>&1 || rc=$?
    append_capture "$capture"
    return "$rc"
}

assert_binary_hashes() {
    local phase_path="$1" phase_sha="$2" counter_path="$3" counter_sha="$4"
    if [[ -n "$phase_path" ]]; then
        [[ "$(sha256_file "$phase_path")" == "$phase_sha" ]] \
            || fail BINARY_HASH_DRIFT 76 "role=phase"
    fi
    [[ "$(sha256_file "$counter_path")" == "$counter_sha" ]] \
        || fail BINARY_HASH_DRIFT 76 "role=counter"
}

source_check() {
    local sample_id="$1" surface="$2" pin="$3" phase_path="$4" phase_sha="$5" counter_path="$6" counter_sha="$7"
    verify_source_state "$pin"
    assert_binary_hashes "$phase_path" "$phase_sha" "$counter_path" "$counter_sha"
    append_record "record=t32_protocol_source_check sample_id=$sample_id surface=$surface status=PASS expected_commit=$pin actual_commit=$pin fixture_manifest=PASS worktree=clean binary_hashes=PASS"
}

validate_counter_capture() {
    local capture="$1" sample_id="$2" name="$3" rounds="$4" transitions="$5" bytes="$6" fnv="$7" scalar_required="$8"
    local identity contract digest canonical_count root_rows
    identity=$(single_record "$capture" p12t32_identity)
    require_field "$identity" fixture "$name"
    require_field "$identity" rounds "$rounds"
    require_field "$identity" transitions "$transitions"
    require_field "$identity" result_bytes "$bytes"
    require_field "$identity" result_fnv1a64 "$fnv"
    require_field "$identity" instrumentation_off_on_equal true
    contract=$(single_record "$capture" p12t32_counter_contract PROTOCOL_CONTRACT_MISMATCH)
    require_field "$contract" fixture "$name" PROTOCOL_CONTRACT_MISMATCH
    require_field "$contract" capacity_caps "$CAPACITY_CONTRACT" PROTOCOL_CONTRACT_MISMATCH
    require_field "$contract" max_capacity_retries 16 PROTOCOL_CONTRACT_MISMATCH
    require_field "$contract" observation_mode summary PROTOCOL_CONTRACT_MISMATCH
    canonical_count=$(grep -Ec '^record=p12t32_(drain_totals|head_histogram|outbound_degree|lookup_histogram) ' "$capture" || true)
    [[ "$canonical_count" -gt 0 ]] || fail COUNTER_DIGEST_MISMATCH 79 "sample_id=$sample_id missing_canonical_rows"
    digest=$(LC_ALL=C grep -E '^record=p12t32_(drain_totals|head_histogram|outbound_degree|lookup_histogram) ' "$capture" | sha256_stdin)
    append_record "record=t32_protocol_identity_check sample_id=$sample_id surface=counter fixture=$name rounds=$rounds transitions=$transitions result_bytes=$bytes result_fnv1a64=$fnv instrumentation_off_on_equal=true status=PASS"
    append_record "record=t32_protocol_counter_digest sample_id=$sample_id fixture=$name sha256=$digest status=PASS"
    if [[ "$scalar_required" == true ]]; then
        root_rows=$(grep -c '^record=p12t32_root_trace fixture=e1_10 ' "$capture" || true)
        [[ "$root_rows" -gt 0 ]] || fail IDENTITY_MISMATCH 74 "sample_id=$sample_id missing_scalar_root_trace"
        append_record "record=t32_protocol_scalar_reference sample_id=$sample_id fixture=e1_10 status=PASS"
    fi
}

validate_phase_capture() {
    local capture="$1" sample_id="$2" fixture_path="$3" sample="$4" name="$5" rounds="$6" transitions="$7" bytes="$8" fnv="$9"
    local identity protocol contract comparison expected_order
    local split unsplit difference numerator denominator attempts within attribution
    local count_ns prefix_ns write_ns combine_ns arithmetic
    identity=$(single_record "$capture" t17c_cuda_phase_profile_identity)
    require_field "$identity" fixture "$fixture_path"
    require_field "$identity" sample_index "$sample"
    require_field "$identity" rounds "$rounds"
    require_field "$identity" transitions "$transitions"
    require_field "$identity" result_bytes "$bytes"
    require_field "$identity" result_fnv1a64 "$fnv"
    require_field "$identity" production_unsplit_split_equal true
    require_field "$identity" instrumentation_off_on_equal true
    require_field "$identity" roster_equal true
    protocol=$(single_record "$capture" t17c_cuda_phase_profile_protocol PROTOCOL_CONTRACT_MISMATCH)
    require_field "$protocol" profile_dispatches 15 PROTOCOL_CONTRACT_MISMATCH
    require_field "$protocol" unsplit_dispatches 12 PROTOCOL_CONTRACT_MISMATCH
    require_field "$protocol" production_dispatches 12 PROTOCOL_CONTRACT_MISMATCH
    require_field "$protocol" maximum_profile_dispatches 16 PROTOCOL_CONTRACT_MISMATCH
    require_field "$protocol" maximum_unsplit_dispatches 13 PROTOCOL_CONTRACT_MISMATCH
    require_field "$protocol" maximum_production_dispatches 13 PROTOCOL_CONTRACT_MISMATCH
    contract=$(single_record "$capture" t17c_cuda_phase_profile_contract PROTOCOL_CONTRACT_MISMATCH)
    require_field "$contract" capacity_caps "$CAPACITY_CONTRACT" PROTOCOL_CONTRACT_MISMATCH
    require_field "$contract" max_capacity_retries 16 PROTOCOL_CONTRACT_MISMATCH
    require_field "$contract" production_dispatch_names "$PRODUCTION_DISPATCH_NAMES" PROTOCOL_CONTRACT_MISMATCH
    require_field "$contract" split_dispatch_names "$SPLIT_DISPATCH_NAMES" PROTOCOL_CONTRACT_MISMATCH
    comparison=$(single_record "$capture" t17c_cuda_prepare_comparison MISSING_PREPARE_COMPARISON)
    expected_order=unsplit_then_split
    if ((sample % 2 == 1)); then
        expected_order=split_then_unsplit
    fi
    require_field "$comparison" fixture "$fixture_path" MISSING_PREPARE_COMPARISON
    require_field "$comparison" sample_index "$sample" MISSING_PREPARE_COMPARISON
    require_field "$comparison" clock cuda_device_events MISSING_PREPARE_COMPARISON
    require_field "$comparison" method direct_launch_events MISSING_PREPARE_COMPARISON
    require_field "$comparison" order "$expected_order" MISSING_PREPARE_COMPARISON
    require_field "$comparison" tolerance_basis_points 500 MISSING_PREPARE_COMPARISON
    require_field "$comparison" production_subcost_attribution false MISSING_PREPARE_COMPARISON
    split=$(field "$comparison" split_prepare_sum_ns)
    unsplit=$(field "$comparison" unsplit_prepare_ns)
    difference=$(field "$comparison" split_minus_unsplit_ns)
    numerator=$(field "$comparison" split_over_unsplit_numerator_ns)
    denominator=$(field "$comparison" split_over_unsplit_denominator_ns)
    attempts=$(field "$comparison" recorded_attempts)
    within=$(field "$comparison" within_tolerance)
    attribution=$(field "$comparison" split_total_attribution)
    count_ns=$(grep '^record=t17c_cuda_phase_profile_phase ' "$capture" | grep ' phase=split_prepare_count ' | awk '{for(i=1;i<=NF;i++) if($i~/^elapsed_ns=/){sub("elapsed_ns=","",$i);print $i}}')
    prefix_ns=$(grep '^record=t17c_cuda_phase_profile_phase ' "$capture" | grep ' phase=split_prepare_prefix ' | awk '{for(i=1;i<=NF;i++) if($i~/^elapsed_ns=/){sub("elapsed_ns=","",$i);print $i}}')
    write_ns=$(grep '^record=t17c_cuda_phase_profile_phase ' "$capture" | grep ' phase=split_prepare_write ' | awk '{for(i=1;i<=NF;i++) if($i~/^elapsed_ns=/){sub("elapsed_ns=","",$i);print $i}}')
    combine_ns=$(grep '^record=t17c_cuda_phase_profile_phase ' "$capture" | grep ' phase=split_prepare_combine ' | awk '{for(i=1;i<=NF;i++) if($i~/^elapsed_ns=/){sub("elapsed_ns=","",$i);print $i}}')
    arithmetic=$(python3 - "$split" "$unsplit" "$difference" "$numerator" "$denominator" "$attempts" "$within" "$attribution" "$count_ns" "$prefix_ns" "$write_ns" "$combine_ns" <<'PY'
import sys
try:
    split, unsplit, difference, numerator, denominator, attempts = map(int, sys.argv[1:7])
    within, attribution = sys.argv[7:9]
    parts = list(map(int, sys.argv[9:13]))
except (ValueError, IndexError):
    raise SystemExit(1)
expected_within = abs(split - unsplit) * 10_000 <= unsplit * 500
valid = (
    unsplit > 0
    and attempts > 0
    and split == sum(parts)
    and difference == split - unsplit
    and numerator == split
    and denominator == unsplit
    and within == str(expected_within).lower()
    and attribution == ("within_bound" if expected_within else "perturbed_unusable")
)
print("PASS" if valid else "FAIL")
PY
)
    [[ "$arithmetic" == PASS ]] || fail MISSING_PREPARE_COMPARISON 78 "sample_id=$sample_id arithmetic_or_predicate"
    append_record "record=t32_protocol_identity_check sample_id=$sample_id surface=phase fixture=$name rounds=$rounds transitions=$transitions result_bytes=$bytes result_fnv1a64=$fnv production_unsplit_split_equal=true instrumentation_off_on_equal=true status=PASS"
    append_record "record=t32_protocol_prepare_check sample_id=$sample_id clock=cuda_device_events method=direct_launch_events order=$expected_order recorded_attempts=$attempts split_prepare_sum_ns=$split unsplit_prepare_ns=$unsplit split_minus_unsplit_ns=$difference split_over_unsplit_numerator_ns=$numerator split_over_unsplit_denominator_ns=$denominator tolerance_basis_points=500 within_tolerance=$within split_total_attribution=$attribution production_subcost_attribution=false status=PASS"
    append_record "record=t32_protocol_contract_check sample_id=$sample_id capacity_caps=PASS dispatch_names=PASS dispatch_counts=PASS status=PASS"
}

load_manifest() {
    NAMES=()
    PATHS=()
    ROUNDS=()
    TRANSITIONS=()
    BYTES=()
    FNVS=()
    local name path sha rounds transitions bytes fnv
    while IFS=$'\t' read -r name path sha rounds transitions bytes fnv; do
        [[ -n "$name" ]] || continue
        NAMES+=("$name")
        PATHS+=("$path")
        ROUNDS+=("$rounds")
        TRANSITIONS+=("$transitions")
        BYTES+=("$bytes")
        FNVS+=("$fnv")
    done <"$MANIFEST"
}

create_log() {
    local log="$1"
    [[ "$log" == /* ]] || fail INVALID_LOG_PATH 75 "log_must_be_absolute=$log"
    [[ "$log" != "$REPO"/* ]] || fail INVALID_LOG_PATH 75 "log_must_be_outside_worktree=$log"
    mkdir -p "$(dirname "$log")"
    if ! (set -o noclobber; : >"$log") 2>/dev/null; then
        fail LOG_ALREADY_EXISTS 75 "path=$log"
    fi
    CURRENT_LOG="$log"
}

record_build_provenance() {
    local mode="$1"
    append_record "record=t32_protocol_toolchain mode=$mode rustc=$(rustc -V | tr ' ' '_') cargo=$(cargo -V | tr ' ' '_')"
    append_record "record=t32_protocol_machine mode=$mode host=$(hostname | tr ' ' '_') kernel=$(uname -sr | tr ' ' '_')"
    if [[ "$mode" == cuda ]]; then
        append_record "record=t32_protocol_cuda nvcc=$(nvcc --version | tail -1 | tr ' ' '_')"
        nvidia-smi --query-gpu=name,uuid,driver_version,compute_cap --format=csv,noheader >>"$CURRENT_LOG"
    fi
}

run_local() {
    local pin="$1" log="$2" target_dir counter_bin counter_sha
    local i sample_id capture scalar_required invocation_rc
    create_log "$log"
    reject_gate_overrides
    append_record "record=t32_protocol_start mode=local scope=local_correctness non_hardware=true expected_commit=$pin actual_commit=$(git -C "$REPO" rev-parse HEAD)"
    preflight "$pin" 2>&1 | tee -a "$CURRENT_LOG"
    target_dir="${CARGO_TARGET_DIR:-$REPO/target}"
    append_record "record=t32_protocol_build mode=local target_dir=$target_dir command=cargo_build_locked_release_metal-spike_t32_drain_profile"
    CARGO_TARGET_DIR="$target_dir" cargo build --locked --release --features metal-spike --bin t32_drain_profile >>"$CURRENT_LOG" 2>&1
    counter_bin="$target_dir/release/t32_drain_profile"
    counter_sha=$(sha256_file "$counter_bin")
    record_build_provenance local
    append_record "record=t32_protocol_binary role=counter sha256=$counter_sha"
    load_manifest
    for ((i = 0; i < 7; i++)); do
        sample_id="local_s0_i${i}_a1"
        source_check "$sample_id" counter "$pin" "" "" "$counter_bin" "$counter_sha"
        quiet_boundary "$sample_id" counter pre "$PRE_GATE_PROBES"
        append_record "record=t32_protocol_sample_begin sample_id=$sample_id mode=local surface=counter sample_index=0 fixture_index=$i fixture=${NAMES[$i]} attempt=1"
        capture=$(mktemp "${TMPDIR:-/tmp}/t32-counter.XXXXXX")
        scalar_required=false
        if [[ "${NAMES[$i]}" == e1_10 ]]; then
            scalar_required=true
            invocation_rc=0
            run_captured "$capture" "$counter_bin" --root-trace "${PATHS[$i]}" \
                || invocation_rc=$?
        else
            invocation_rc=0
            run_captured "$capture" "$counter_bin" "${PATHS[$i]}" \
                || invocation_rc=$?
        fi
        quiet_boundary "$sample_id" counter post "$POST_GATE_PROBES"
        [[ "$invocation_rc" -eq 0 ]] \
            || fail INVOCATION_FAILED 77 "sample_id=$sample_id surface=counter rc=$invocation_rc"
        validate_counter_capture "$capture" "$sample_id" "${NAMES[$i]}" "${ROUNDS[$i]}" "${TRANSITIONS[$i]}" "${BYTES[$i]}" "${FNVS[$i]}" "$scalar_required"
        rm -f "$capture"
        append_record "record=t32_protocol_sample_accept sample_id=$sample_id mode=local sample_index=0 fixture_index=$i fixture=${NAMES[$i]} attempt=1"
    done
    append_record "record=t32_protocol_complete mode=local accepted=7 status=PASS"
    python3 "$ANALYZER" local "$CURRENT_LOG" | tee -a "$CURRENT_LOG"
    chmod 0444 "$CURRENT_LOG"
}

run_cuda() {
    local machine="$1" pin="$2" log="$3" target_dir phase_bin counter_bin phase_sha counter_sha
    local sample position index sample_id phase_capture counter_capture invocation_rc
    create_log "$log"
    reject_gate_overrides
    append_record "record=t32_protocol_start mode=cuda machine=$machine expected_commit=$pin actual_commit=$(git -C "$REPO" rev-parse HEAD)"
    preflight "$pin" 2>&1 | tee -a "$CURRENT_LOG"
    target_dir="${CARGO_TARGET_DIR:-$REPO/target}"
    append_record "record=t32_protocol_build mode=cuda target_dir=$target_dir command=cargo_build_locked_release_cuda_t17c_and_t32"
    CARGO_TARGET_DIR="$target_dir" cargo build --locked --release --features cuda --bin t17c_cuda_profile --bin t32_drain_profile >>"$CURRENT_LOG" 2>&1
    phase_bin="$target_dir/release/t17c_cuda_profile"
    counter_bin="$target_dir/release/t32_drain_profile"
    phase_sha=$(sha256_file "$phase_bin")
    counter_sha=$(sha256_file "$counter_bin")
    record_build_provenance cuda
    append_record "record=t32_protocol_binary role=phase sha256=$phase_sha"
    append_record "record=t32_protocol_binary role=counter sha256=$counter_sha"
    load_manifest
    for ((sample = 0; sample < SAMPLE_COUNT; sample++)); do
        for ((position = 0; position < 7; position++)); do
            index=$(((sample + position) % 7))
            sample_id="${machine}_s${sample}_i${index}_a1"
            source_check "$sample_id" phase "$pin" "$phase_bin" "$phase_sha" "$counter_bin" "$counter_sha"
            quiet_boundary "$sample_id" phase pre "$PRE_GATE_PROBES"
            append_record "record=t32_protocol_sample_begin sample_id=$sample_id mode=cuda surface=phase sample_index=$sample fixture_index=$index fixture=${NAMES[$index]} attempt=1"
            phase_capture=$(mktemp "${TMPDIR:-/tmp}/t32-phase.XXXXXX")
            invocation_rc=0
            run_captured "$phase_capture" timeout --signal=TERM "$CUDA_TIMEOUT_SECONDS" "$phase_bin" "${PATHS[$index]}" "$sample" \
                || invocation_rc=$?
            quiet_boundary "$sample_id" phase post "$POST_GATE_PROBES"
            [[ "$invocation_rc" -eq 0 ]] \
                || fail INVOCATION_FAILED 77 "sample_id=$sample_id surface=phase rc=$invocation_rc"
            validate_phase_capture "$phase_capture" "$sample_id" "${PATHS[$index]}" "$sample" "${NAMES[$index]}" "${ROUNDS[$index]}" "${TRANSITIONS[$index]}" "${BYTES[$index]}" "${FNVS[$index]}"
            rm -f "$phase_capture"

            source_check "$sample_id" counter "$pin" "$phase_bin" "$phase_sha" "$counter_bin" "$counter_sha"
            quiet_boundary "$sample_id" counter pre "$PRE_GATE_PROBES"
            append_record "record=t32_protocol_sample_begin sample_id=$sample_id mode=cuda surface=counter sample_index=$sample fixture_index=$index fixture=${NAMES[$index]} attempt=1"
            counter_capture=$(mktemp "${TMPDIR:-/tmp}/t32-counter.XXXXXX")
            invocation_rc=0
            run_captured "$counter_capture" timeout --signal=TERM "$CUDA_TIMEOUT_SECONDS" "$counter_bin" "${PATHS[$index]}" \
                || invocation_rc=$?
            quiet_boundary "$sample_id" counter post "$POST_GATE_PROBES"
            [[ "$invocation_rc" -eq 0 ]] \
                || fail INVOCATION_FAILED 77 "sample_id=$sample_id surface=counter rc=$invocation_rc"
            validate_counter_capture "$counter_capture" "$sample_id" "${NAMES[$index]}" "${ROUNDS[$index]}" "${TRANSITIONS[$index]}" "${BYTES[$index]}" "${FNVS[$index]}" false
            rm -f "$counter_capture"
            append_record "record=t32_protocol_sample_accept sample_id=$sample_id mode=cuda sample_index=$sample fixture_index=$index fixture=${NAMES[$index]} attempt=1"
        done
    done
    append_record "record=t32_protocol_complete mode=cuda accepted=35 status=COLLECTED"
    python3 "$ANALYZER" cuda "$CURRENT_LOG" | tee -a "$CURRENT_LOG"
    chmod 0444 "$CURRENT_LOG"
}

compare_cuda() {
    local boston_log="$1" madrid_log="$2" local_log="$3" role fixture
    local boston_sha madrid_sha local_sha boston_pin madrid_pin local_pin boston_machine madrid_machine
    python3 "$ANALYZER" cuda "$boston_log"
    python3 "$ANALYZER" cuda "$madrid_log"
    python3 "$ANALYZER" local "$local_log"
    boston_machine=$(awk '/^record=t32_protocol_start /{for(i=1;i<=NF;i++)if($i~/^machine=/){sub("machine=","",$i);print $i}}' "$boston_log")
    madrid_machine=$(awk '/^record=t32_protocol_start /{for(i=1;i<=NF;i++)if($i~/^machine=/){sub("machine=","",$i);print $i}}' "$madrid_log")
    [[ "$boston_machine" == boston ]] \
        || fail CROSS_MACHINE_ROLE_MISMATCH 83 "first_log_machine=$boston_machine"
    [[ "$madrid_machine" == madrid ]] \
        || fail CROSS_MACHINE_ROLE_MISMATCH 83 "second_log_machine=$madrid_machine"
    boston_pin=$(awk '/^record=t32_protocol_start /{for(i=1;i<=NF;i++)if($i~/^expected_commit=/){sub("expected_commit=","",$i);print $i}}' "$boston_log")
    madrid_pin=$(awk '/^record=t32_protocol_start /{for(i=1;i<=NF;i++)if($i~/^expected_commit=/){sub("expected_commit=","",$i);print $i}}' "$madrid_log")
    local_pin=$(awk '/^record=t32_protocol_start /{for(i=1;i<=NF;i++)if($i~/^expected_commit=/){sub("expected_commit=","",$i);print $i}}' "$local_log")
    [[ "$boston_pin" == "$madrid_pin" && "$boston_pin" == "$local_pin" ]] \
        || fail CROSS_MACHINE_COMMIT_MISMATCH 83 "boston=$boston_pin madrid=$madrid_pin local=$local_pin"
    for role in phase counter; do
        boston_sha=$(awk -v role="$role" '/^record=t32_protocol_binary /{r="";s="";for(i=1;i<=NF;i++){if($i~/^role=/){r=$i;sub("role=","",r)}if($i~/^sha256=/){s=$i;sub("sha256=","",s)}}if(r==role)print s}' "$boston_log")
        madrid_sha=$(awk -v role="$role" '/^record=t32_protocol_binary /{r="";s="";for(i=1;i<=NF;i++){if($i~/^role=/){r=$i;sub("role=","",r)}if($i~/^sha256=/){s=$i;sub("sha256=","",s)}}if(r==role)print s}' "$madrid_log")
        [[ "$boston_sha" == "$madrid_sha" ]] || fail CROSS_MACHINE_BINARY_MISMATCH 84 "role=$role boston=$boston_sha madrid=$madrid_sha"
    done
    for fixture in e1_10 e1_30 e1_60 e1_90 k48 frontier e5; do
        boston_sha=$(awk -v fixture="$fixture" '/^record=t32_protocol_counter_digest /{f="";s="";for(i=1;i<=NF;i++){if($i~/^fixture=/){f=$i;sub("fixture=","",f)}if($i~/^sha256=/){s=$i;sub("sha256=","",s)}}if(f==fixture)print s}' "$boston_log" | sort -u)
        madrid_sha=$(awk -v fixture="$fixture" '/^record=t32_protocol_counter_digest /{f="";s="";for(i=1;i<=NF;i++){if($i~/^fixture=/){f=$i;sub("fixture=","",f)}if($i~/^sha256=/){s=$i;sub("sha256=","",s)}}if(f==fixture)print s}' "$madrid_log" | sort -u)
        local_sha=$(awk -v fixture="$fixture" '/^record=t32_protocol_counter_digest /{f="";s="";for(i=1;i<=NF;i++){if($i~/^fixture=/){f=$i;sub("fixture=","",f)}if($i~/^sha256=/){s=$i;sub("sha256=","",s)}}if(f==fixture)print s}' "$local_log" | sort -u)
        [[ "$boston_sha" == "$madrid_sha" && "$boston_sha" == "$local_sha" && -n "$boston_sha" ]] \
            || fail CROSS_MACHINE_COUNTER_MISMATCH 85 "fixture=$fixture boston=$boston_sha madrid=$madrid_sha local=$local_sha"
    done
    printf 'record=t32_protocol_cross_machine status=PASS commit=%s binary_hashes_equal=true counter_digests_equal=true local_reference_equal=true\n' "$boston_pin"
}

usage() {
    printf '%s\n' \
        "usage:" \
        "  $0 local <literal-40-hex-commit> <new-absolute-log>" \
        "  $0 cuda-boston <literal-40-hex-commit> <new-absolute-log>" \
        "  $0 cuda-madrid <literal-40-hex-commit> <new-absolute-log>" \
        "  $0 compare-cuda <boston-log> <madrid-log> <local-reference-log>" >&2
    exit 64
}

[[ -n "$REPO" ]] || fail NOT_A_GIT_WORKTREE 66
cd "$REPO"
case "${1:-}" in
    preflight)
        [[ "$#" -eq 2 ]] || usage
        preflight "$2"
        ;;
    local)
        [[ "$#" -eq 3 ]] || usage
        run_local "$2" "$3"
        ;;
    cuda-boston)
        [[ "$#" -eq 3 ]] || usage
        run_cuda boston "$2" "$3"
        ;;
    cuda-madrid)
        [[ "$#" -eq 3 ]] || usage
        run_cuda madrid "$2" "$3"
        ;;
    compare-cuda)
        [[ "$#" -eq 4 ]] || usage
        compare_cuda "$2" "$3" "$4"
        ;;
    *) usage ;;
esac
