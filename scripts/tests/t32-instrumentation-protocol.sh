#!/usr/bin/env bash
set -euo pipefail

REPO=$(git rev-parse --show-toplevel)
DRIVER="$REPO/scripts/t32-instrumentation-protocol.sh"
ANALYZER="$REPO/scripts/t32-instrumentation-analyze.py"
MANIFEST="$REPO/scripts/t32-instrumentation-fixtures.tsv"

grep -Fq 'mode=local surface=counter sample_index=0' "$DRIVER"

PROOF_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/t32-protocol-test.XXXXXX")
trap 'rm -rf "$PROOF_ROOT"' EXIT

expect_failure() {
    local reason="$1" output="$2" failure expected
    shift 2
    set +e
    "$@" >"$output" 2>&1
    local rc=$?
    set -e
    if [[ "$rc" -eq 0 ]]; then
        echo "expected nonzero failure reason=$reason" >&2
        return 1
    fi
    case "$reason" in
        DIRTY_WORKTREE) expected=70 ;;
        FIXTURE_SHA256_MISMATCH) expected=72 ;;
        MISSING_QUIET_CAPTURE) expected=73 ;;
        IDENTITY_MISMATCH) expected=74 ;;
        MISSING_PREPARE_COMPARISON) expected=78 ;;
        PREPARE_TOLERANCE_EXCEEDED) expected=86 ;;
        QUIET_PROBE_FAILED) expected=87 ;;
        SOURCE_CONTENT_DRIFT) expected=88 ;;
        *) expected="$rc" ;;
    esac
    if [[ "$rc" -ne "$expected" ]]; then
        echo "wrong exit reason=$reason expected=$expected actual=$rc" >&2
        return 1
    fi
    failure=$(grep -Fm1 "T32_PROTOCOL_FAIL reason=$reason" "$output")
    printf '%s\n' "$failure"
    printf 'proof=%s exit=%s status=PASS\n' "$reason" "$rc"
}

make_repo() {
    local target="$1"
    mkdir -p "$target/scripts" "$target/src/bin"
    cp "$DRIVER" "$ANALYZER" "$MANIFEST" "$target/scripts/"
    cp "$REPO/src/bin/t32_drain_profile.rs" "$target/src/bin/"
    while IFS=$'\t' read -r name path sha rounds transitions bytes fnv; do
        [[ -n "$name" ]] || continue
        mkdir -p "$target/$(dirname "$path")"
        cp "$REPO/$path" "$target/$path"
    done <"$MANIFEST"
    git -C "$target" init -q
    git -C "$target" config user.name "T32 Protocol Test"
    git -C "$target" config user.email "t32-protocol@example.invalid"
    git -C "$target" add scripts configs src
    git -C "$target" commit -qm "Create protocol proof repository"
}

make_valid_local_log() {
    local log="$1"
    local pin=1111111111111111111111111111111111111111
    : >"$log"
    printf 'record=t32_protocol_start mode=local scope=local_correctness non_hardware=true expected_commit=%s actual_commit=%s\n' "$pin" "$pin" >>"$log"
    printf 'record=t32_protocol_preflight status=PASS fixture_count=7 worktree=clean\n' >>"$log"
    printf 'record=t32_protocol_binary role=counter sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n' >>"$log"
    local i=0
    while IFS=$'\t' read -r name path sha rounds transitions bytes fnv; do
        [[ -n "$name" ]] || continue
        local sample_id="local_s0_i${i}_a1"
        printf 'record=t32_protocol_source_check sample_id=%s surface=counter status=PASS expected_commit=%s actual_commit=%s fixture_manifest=PASS worktree=clean binary_hashes=PASS\n' "$sample_id" "$pin" "$pin" >>"$log"
        emit_quiet "$log" "$sample_id" counter pre 3
        printf 'record=t32_protocol_sample_begin sample_id=%s mode=local surface=counter sample_index=0 fixture_index=%s fixture=%s attempt=1\n' "$sample_id" "$i" "$name" >>"$log"
        emit_quiet "$log" "$sample_id" counter post 1
        printf 'record=t32_protocol_identity_check sample_id=%s surface=counter fixture=%s rounds=%s transitions=%s result_bytes=%s result_fnv1a64=%s instrumentation_off_on_equal=true status=PASS\n' "$sample_id" "$name" "$rounds" "$transitions" "$bytes" "$fnv" >>"$log"
        printf 'record=t32_protocol_counter_digest sample_id=%s fixture=%s sha256=%064d status=PASS\n' "$sample_id" "$name" 0 >>"$log"
        if [[ "$name" == e1_10 ]]; then
            printf 'record=t32_protocol_scalar_reference sample_id=%s fixture=e1_10 status=PASS\n' "$sample_id" >>"$log"
        fi
        printf 'record=t32_protocol_sample_accept sample_id=%s mode=local sample_index=0 fixture_index=%s fixture=%s attempt=1\n' "$sample_id" "$i" "$name" >>"$log"
        i=$((i + 1))
    done <"$MANIFEST"
    printf 'record=t32_protocol_complete mode=local accepted=7 status=PASS\n' >>"$log"
}

emit_quiet() {
    local log="$1" sample_id="$2" surface="$3" phase="$4" probes="$5" probe
    local tree_line='runner_descendant_pids=1 ' process_line='1 0 0.0 protocol-test' none='none'
    local tree_sha process_sha none_sha
    if command -v sha256sum >/dev/null 2>&1; then
        tree_sha=$(printf '%s\n' "$tree_line" | sha256sum | awk '{print $1}')
        process_sha=$(printf '%s\n' "$process_line" | sha256sum | awk '{print $1}')
        none_sha=$(printf '%s\n' "$none" | sha256sum | awk '{print $1}')
    else
        tree_sha=$(printf '%s\n' "$tree_line" | shasum -a 256 | awk '{print $1}')
        process_sha=$(printf '%s\n' "$process_line" | shasum -a 256 | awk '{print $1}')
        none_sha=$(printf '%s\n' "$none" | shasum -a 256 | awk '{print $1}')
    fi
    for ((probe = 1; probe <= probes; probe++)); do
        {
            printf '%s\n' "--- T32_QUIET_CAPTURE sample_id=$sample_id surface=$surface phase=$phase probe=$probe ---"
            printf '%s\n' "$tree_line" process_snapshot_begin "$process_line" process_snapshot_end
            printf '%s\n' external_busy_begin none external_busy_end
            printf '%s\n' gpu_compute_apps_begin none gpu_compute_apps_end
            printf '%s\n' external_gpu_begin none external_gpu_end
            printf '%s\n' "--- T32_QUIET_CAPTURE_END sample_id=$sample_id surface=$surface phase=$phase probe=$probe ---"
        } >>"$log"
        printf 'record=t32_protocol_quiet_capture sample_id=%s surface=%s phase=%s probe=%s threshold_pct=20.0 lineage=runner_descendants process_rows=1 gpu_rows=0 external_busy=0 external_gpu=0 tree_sha256=%s process_sha256=%s external_busy_sha256=%s gpu_sha256=%s external_gpu_sha256=%s status=CLEAR\n' \
            "$sample_id" "$surface" "$phase" "$probe" "$tree_sha" "$process_sha" "$none_sha" "$none_sha" "$none_sha" >>"$log"
    done
    printf 'record=t32_protocol_quiet_summary sample_id=%s surface=%s phase=%s threshold_pct=20.0 lineage=runner_descendants probes=%s captures=%s status=CLEAR\n' \
        "$sample_id" "$surface" "$phase" "$probes" "$probes" >>"$log"
}

make_valid_cuda_log() {
    local log="$1" machine="${2:-boston}"
    local pin=2222222222222222222222222222222222222222
    : >"$log"
    printf 'record=t32_protocol_start mode=cuda machine=%s expected_commit=%s actual_commit=%s\n' "$machine" "$pin" "$pin" >>"$log"
    printf 'record=t32_protocol_preflight status=PASS fixture_count=7 worktree=clean\n' >>"$log"
    printf 'record=t32_protocol_binary role=phase sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n' >>"$log"
    printf 'record=t32_protocol_binary role=counter sha256=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n' >>"$log"
    local sample position i name path sha rounds transitions bytes fnv sample_id order
    for sample in 0 1 2 3 4; do
        for position in 0 1 2 3 4 5 6; do
            i=$(((sample + position) % 7))
            IFS=$'\t' read -r name path sha rounds transitions bytes fnv < <(sed -n "$((i + 1))p" "$MANIFEST")
            sample_id="${machine}_s${sample}_i${i}_a1"
            order=unsplit_then_split
            if ((sample % 2 == 1)); then
                order=split_then_unsplit
            fi
            printf 'record=t32_protocol_source_check sample_id=%s surface=phase status=PASS expected_commit=%s actual_commit=%s fixture_manifest=PASS worktree=clean binary_hashes=PASS\n' "$sample_id" "$pin" "$pin" >>"$log"
            emit_quiet "$log" "$sample_id" phase pre 3
            printf 'record=t32_protocol_sample_begin sample_id=%s mode=cuda surface=phase sample_index=%s fixture_index=%s fixture=%s attempt=1\n' "$sample_id" "$sample" "$i" "$name" >>"$log"
            emit_quiet "$log" "$sample_id" phase post 1
            printf 'record=t32_protocol_identity_check sample_id=%s surface=phase fixture=%s rounds=%s transitions=%s result_bytes=%s result_fnv1a64=%s production_unsplit_split_equal=true instrumentation_off_on_equal=true status=PASS\n' "$sample_id" "$name" "$rounds" "$transitions" "$bytes" "$fnv" >>"$log"
            printf 'record=t32_protocol_prepare_check sample_id=%s clock=cuda_device_events method=direct_launch_events order=%s recorded_attempts=10 split_prepare_sum_ns=1050 unsplit_prepare_ns=1000 split_minus_unsplit_ns=50 split_over_unsplit_numerator_ns=1050 split_over_unsplit_denominator_ns=1000 tolerance_basis_points=500 within_tolerance=true split_total_attribution=within_bound production_subcost_attribution=false status=PASS\n' "$sample_id" "$order" >>"$log"
            printf 'record=t32_protocol_contract_check sample_id=%s capacity_caps=PASS dispatch_names=PASS dispatch_counts=PASS status=PASS\n' "$sample_id" >>"$log"
            printf 'record=t32_protocol_source_check sample_id=%s surface=counter status=PASS expected_commit=%s actual_commit=%s fixture_manifest=PASS worktree=clean binary_hashes=PASS\n' "$sample_id" "$pin" "$pin" >>"$log"
            emit_quiet "$log" "$sample_id" counter pre 3
            printf 'record=t32_protocol_sample_begin sample_id=%s mode=cuda surface=counter sample_index=%s fixture_index=%s fixture=%s attempt=1\n' "$sample_id" "$sample" "$i" "$name" >>"$log"
            emit_quiet "$log" "$sample_id" counter post 1
            printf 'record=t32_protocol_identity_check sample_id=%s surface=counter fixture=%s rounds=%s transitions=%s result_bytes=%s result_fnv1a64=%s instrumentation_off_on_equal=true status=PASS\n' "$sample_id" "$name" "$rounds" "$transitions" "$bytes" "$fnv" >>"$log"
            printf 'record=t32_protocol_counter_digest sample_id=%s fixture=%s sha256=%064d status=PASS\n' "$sample_id" "$name" 0 >>"$log"
            printf 'record=t32_protocol_sample_accept sample_id=%s mode=cuda sample_index=%s fixture_index=%s fixture=%s attempt=1\n' "$sample_id" "$sample" "$i" "$name" >>"$log"
        done
    done
    printf 'record=t32_protocol_complete mode=cuda accepted=35 status=COLLECTED\n' >>"$log"
}

dirty_repo="$PROOF_ROOT/dirty-repo"
make_repo "$dirty_repo"
dirty_pin=$(git -C "$dirty_repo" rev-parse HEAD)
touch "$dirty_repo/untracked-drift"
expect_failure DIRTY_WORKTREE "$PROOF_ROOT/dirty.out" \
    "$dirty_repo/scripts/t32-instrumentation-protocol.sh" preflight "$dirty_pin"

clean_source_repo="$PROOF_ROOT/clean-source-repo"
make_repo "$clean_source_repo"
clean_source_pin=$(git -C "$clean_source_repo" rev-parse HEAD)
"$clean_source_repo/scripts/t32-instrumentation-protocol.sh" preflight "$clean_source_pin" \
    >"$PROOF_ROOT/clean-source.out"
grep -Fq 'record=t32_protocol_preflight status=PASS' "$PROOF_ROOT/clean-source.out"
echo 'proof=SOURCE_CONTENT_CLEAN exit=0 status=PASS'

visible_source_repo="$PROOF_ROOT/visible-source-repo"
make_repo "$visible_source_repo"
visible_source_pin=$(git -C "$visible_source_repo" rev-parse HEAD)
printf '\n// visible tracked drift retains its existing refusal\n' \
    >>"$visible_source_repo/src/bin/t32_drain_profile.rs"
expect_failure DIRTY_WORKTREE "$PROOF_ROOT/visible-source.out" \
    "$visible_source_repo/scripts/t32-instrumentation-protocol.sh" preflight "$visible_source_pin"

hidden_source_repo="$PROOF_ROOT/hidden-source-repo"
make_repo "$hidden_source_repo"
hidden_source_pin=$(git -C "$hidden_source_repo" rev-parse HEAD)
printf '\n// assume-unchanged must not hide this drift\n' \
    >>"$hidden_source_repo/src/bin/t32_drain_profile.rs"
git -C "$hidden_source_repo" update-index --assume-unchanged src/bin/t32_drain_profile.rs
hidden_status=$(git -C "$hidden_source_repo" status --porcelain=v1 --untracked-files=all)
[[ -z "$hidden_status" ]]
echo 'proof=ASSUME_UNCHANGED_OLD_STATUS exit=0 status=PASS'
expect_failure SOURCE_CONTENT_DRIFT "$PROOF_ROOT/hidden-source.out" \
    "$hidden_source_repo/scripts/t32-instrumentation-protocol.sh" preflight "$hidden_source_pin"

fixture_repo="$PROOF_ROOT/fixture-repo"
make_repo "$fixture_repo"
fixture_pin=$(git -C "$fixture_repo" rev-parse HEAD)
printf '\n# altered fixture\n' >>"$fixture_repo/configs/benchmarks/p12/e1_open_k32_load_10.toml"
expect_failure FIXTURE_SHA256_MISMATCH "$PROOF_ROOT/fixture.out" \
    "$fixture_repo/scripts/t32-instrumentation-protocol.sh" preflight "$fixture_pin"

valid_log="$PROOF_ROOT/valid.log"
make_valid_local_log "$valid_log"
python3 "$ANALYZER" local "$valid_log" >"$PROOF_ROOT/valid.out"
grep -Fq 'record=t32_protocol_analysis mode=local status=PASS' "$PROOF_ROOT/valid.out"

missing_quiet_log="$PROOF_ROOT/missing-quiet.log"
sed '/sample_id=local_s0_i0_a1 surface=counter phase=pre/d' "$valid_log" >"$missing_quiet_log"
expect_failure MISSING_QUIET_CAPTURE "$PROOF_ROOT/missing-quiet.out" \
    python3 "$ANALYZER" local "$missing_quiet_log"

failed_probe_log="$PROOF_ROOT/failed-probe.log"
: >"$failed_probe_log"
expect_failure QUIET_PROBE_FAILED "$PROOF_ROOT/failed-probe.out" \
    bash -c '
        eval "$(sed '\''/^\[\[ -n /,$d'\'' "$1")"
        CURRENT_LOG="$2"
        ps() { printf '\''%s 1 0.0 protocol-test\n'\'' "$$"; }
        nvidia-smi() { return 23; }
        quiet_probe failed_probe counter pre 1
    ' bash "$DRIVER" "$failed_probe_log"

successful_probe_log="$PROOF_ROOT/successful-probe.log"
: >"$successful_probe_log"
bash -c '
    eval "$(sed '\''/^\[\[ -n /,$d'\'' "$1")"
    CURRENT_LOG="$2"
    ps() { printf '\''%s 1 0.0 protocol-test\n'\'' "$$"; }
    nvidia-smi() { return 0; }
    quiet_probe successful_probe counter pre 1
' bash "$DRIVER" "$successful_probe_log" >"$PROOF_ROOT/successful-probe.out"
grep -Fq 'sample_id=successful_probe' "$successful_probe_log"
grep -Fq 'gpu_rows=0 external_busy=0 external_gpu=0' "$successful_probe_log"
grep -Fq 'status=CLEAR' "$successful_probe_log"
echo 'proof=QUIET_PROBE_EMPTY_SUCCESS exit=0 status=PASS'

identity_log="$PROOF_ROOT/identity-mismatch.log"
sed 's/sample_id=local_s0_i0_a1 surface=counter fixture=e1_10 rounds=18/sample_id=local_s0_i0_a1 surface=counter fixture=e1_10 rounds=19/' \
    "$valid_log" >"$identity_log"
expect_failure IDENTITY_MISMATCH "$PROOF_ROOT/identity.out" \
    python3 "$ANALYZER" local "$identity_log"

cuda_log="$PROOF_ROOT/cuda.log"
make_valid_cuda_log "$cuda_log"
python3 "$ANALYZER" cuda "$cuda_log" >"$PROOF_ROOT/cuda.out"
grep -Fq 'record=t32_protocol_analysis mode=cuda status=COLLECTED accepted=35' "$PROOF_ROOT/cuda.out"

madrid_log="$PROOF_ROOT/madrid.log"
make_valid_cuda_log "$madrid_log" madrid
compare_local_log="$PROOF_ROOT/compare-local.log"
sed 's/1111111111111111111111111111111111111111/2222222222222222222222222222222222222222/g' \
    "$valid_log" >"$compare_local_log"
"$DRIVER" compare-cuda "$cuda_log" "$madrid_log" "$compare_local_log" \
    >"$PROOF_ROOT/compare-valid.out"
grep -Fq 'record=t32_protocol_cross_machine status=PASS' "$PROOF_ROOT/compare-valid.out"
echo 'proof=PREPARE_TOLERANCE_BOUNDARY exit=0 status=PASS'

outlier_log="$PROOF_ROOT/outlier.log"
sed \
    -e '/sample_id=boston_s0_i0_a1 clock=cuda_device_events/ s/split_prepare_sum_ns=1050/split_prepare_sum_ns=1051/' \
    -e '/sample_id=boston_s0_i0_a1 clock=cuda_device_events/ s/split_minus_unsplit_ns=50/split_minus_unsplit_ns=51/' \
    -e '/sample_id=boston_s0_i0_a1 clock=cuda_device_events/ s/split_over_unsplit_numerator_ns=1050/split_over_unsplit_numerator_ns=1051/' \
    -e '/sample_id=boston_s0_i0_a1 clock=cuda_device_events/ s/within_tolerance=true/within_tolerance=false/' \
    -e '/sample_id=boston_s0_i0_a1 clock=cuda_device_events/ s/split_total_attribution=within_bound/split_total_attribution=perturbed_unusable/' \
    "$cuda_log" >"$outlier_log"
expect_failure PREPARE_TOLERANCE_EXCEEDED "$PROOF_ROOT/outlier.out" \
    "$DRIVER" compare-cuda "$outlier_log" "$madrid_log" "$compare_local_log"

missing_prepare_log="$PROOF_ROOT/missing-prepare.log"
sed '/sample_id=boston_s0_i0_a1 clock=cuda_device_events/d' "$cuda_log" >"$missing_prepare_log"
expect_failure MISSING_PREPARE_COMPARISON "$PROOF_ROOT/missing-prepare.out" \
    python3 "$ANALYZER" cuda "$missing_prepare_log"

echo 'record=t32_protocol_enforcement_tests status=PASS'
