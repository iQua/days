#!/usr/bin/env python3
"""Fail-closed analyzer for the append-only T32 instrumentation sample log."""

from __future__ import annotations

import re
import sys
import hashlib
from collections import defaultdict
from pathlib import Path


ROSTER = (
    ("e1_10", 18, 2_227_879, 70_809_309, "951cad2c3d9f39f8"),
    ("e1_30", 18, 6_333_069, 131_534_072, "2dbbd522d2433b86"),
    ("e1_60", 18, 10_373_881, 256_696_554, "b1ba5a9d872dabbc"),
    ("e1_90", 18, 12_951_185, 379_505_175, "8ae19e3f4c91b029"),
    ("k48", 1_002, 4_245_398_171, 2_227_821_985, "c04b51a57fc0d763"),
    ("frontier", 1_151, 674_774_349, 2_274_074_943, "475b25a56369d8f6"),
    ("e5", 664, 212_378_014, 50_572_617, "56f7b24157e2e852"),
)
SHA256 = re.compile(r"^[0-9a-f]{64}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")

EXIT_CODES = {
    "MALFORMED_LOG": 69,
    "SOURCE_DRIFT": 70,
    "MISSING_QUIET_CAPTURE": 73,
    "IDENTITY_MISMATCH": 74,
    "MISSING_PREPARE_COMPARISON": 78,
    "COUNTER_DIGEST_MISMATCH": 79,
    "PROTOCOL_CONTRACT_MISMATCH": 80,
}


def fail(reason: str, detail: str) -> None:
    print(f"T32_PROTOCOL_FAIL reason={reason} detail={detail}", file=sys.stderr)
    raise SystemExit(EXIT_CODES[reason])


def parse(path: Path) -> tuple[list[dict[str, str | int]], list[str]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        fail("MALFORMED_LOG", f"read_error={error}")
    records: list[dict[str, str | int]] = []
    for line_number, line in enumerate(lines, 1):
        if not line.startswith("record="):
            continue
        fields: dict[str, str | int] = {"_line": line_number}
        for token in line.split():
            if "=" not in token:
                fail("MALFORMED_LOG", f"line={line_number} token={token}")
            key, value = token.split("=", 1)
            if key in fields:
                fail("MALFORMED_LOG", f"line={line_number} duplicate_key={key}")
            fields[key] = value
        records.append(fields)
    return records, lines


def content_sha256(lines: list[str]) -> str:
    return hashlib.sha256(("\n".join(lines) + "\n").encode()).hexdigest()


def parse_quiet_blocks(lines: list[str]) -> dict[tuple[str, str, str, int], dict[str, str | int]]:
    header = re.compile(
        r"^--- T32_QUIET_CAPTURE sample_id=([^ ]+) surface=([^ ]+) phase=(pre|post) probe=([0-9]+) ---$"
    )
    blocks: dict[tuple[str, str, str, int], dict[str, str | int]] = {}
    index = 0
    while index < len(lines):
        match = header.fullmatch(lines[index])
        if not match:
            index += 1
            continue
        sample_id, surface, phase, probe_text = match.groups()
        probe = int(probe_text)
        key = (sample_id, surface, phase, probe)
        if key in blocks:
            fail("MISSING_QUIET_CAPTURE", f"duplicate_raw_capture={key}")
        start_line = index + 1
        index += 1
        if index >= len(lines) or not lines[index].startswith("runner_descendant_pids="):
            fail("MISSING_QUIET_CAPTURE", f"capture={key} missing_runner_tree")
        tree_lines = [lines[index]]
        index += 1
        sections: dict[str, list[str]] = {}
        for section in ("process_snapshot", "external_busy", "gpu_compute_apps", "external_gpu"):
            if index >= len(lines) or lines[index] != f"{section}_begin":
                fail("MISSING_QUIET_CAPTURE", f"capture={key} missing={section}_begin")
            index += 1
            content: list[str] = []
            while index < len(lines) and lines[index] != f"{section}_end":
                content.append(lines[index])
                index += 1
            if index >= len(lines) or not content:
                fail("MISSING_QUIET_CAPTURE", f"capture={key} incomplete={section}")
            sections[section] = content
            index += 1
        expected_end = (
            f"--- T32_QUIET_CAPTURE_END sample_id={sample_id} surface={surface} "
            f"phase={phase} probe={probe} ---"
        )
        if index >= len(lines) or lines[index] != expected_end:
            fail("MISSING_QUIET_CAPTURE", f"capture={key} missing_end")
        blocks[key] = {
            "start_line": start_line,
            "end_line": index + 1,
            "tree_sha256": content_sha256(tree_lines),
            "process_sha256": content_sha256(sections["process_snapshot"]),
            "external_busy_sha256": content_sha256(sections["external_busy"]),
            "gpu_sha256": content_sha256(sections["gpu_compute_apps"]),
            "external_gpu_sha256": content_sha256(sections["external_gpu"]),
            "process_rows": len(sections["process_snapshot"]),
            "gpu_rows": 0
            if sections["gpu_compute_apps"] == ["none"]
            else sum(bool(row) for row in sections["gpu_compute_apps"]),
            "external_busy": 0
            if sections["external_busy"] == ["none"]
            else sum(bool(row) for row in sections["external_busy"]),
            "external_gpu": 0
            if sections["external_gpu"] == ["none"]
            else sum(bool(row) for row in sections["external_gpu"]),
        }
        index += 1
    return blocks


def records_named(records: list[dict[str, str | int]], name: str) -> list[dict[str, str | int]]:
    return [record for record in records if record.get("record") == name]


def exactly_one(
    records: list[dict[str, str | int]],
    name: str,
    reason: str,
    **fields: str,
) -> dict[str, str | int]:
    matches = [
        record
        for record in records_named(records, name)
        if all(record.get(key) == value for key, value in fields.items())
    ]
    if len(matches) != 1:
        selector = ",".join(f"{key}={value}" for key, value in fields.items())
        fail(reason, f"record={name} selector={selector} count={len(matches)}")
    return matches[0]


def require_values(record: dict[str, str | int], reason: str, **fields: str) -> None:
    for key, expected in fields.items():
        if record.get(key) != expected:
            fail(
                reason,
                f"line={record['_line']} field={key} expected={expected} actual={record.get(key)}",
            )


def integer(record: dict[str, str | int], field: str, reason: str) -> int:
    try:
        return int(str(record[field]))
    except (KeyError, ValueError):
        fail(reason, f"line={record['_line']} invalid_integer={field}")


def validate_quiet(
    records: list[dict[str, str | int]],
    raw_blocks: dict[tuple[str, str, str, int], dict[str, str | int]],
    sample_id: str,
    surface: str,
    phase: str,
    probes: int,
) -> int:
    summary = exactly_one(
        records,
        "t32_protocol_quiet_summary",
        "MISSING_QUIET_CAPTURE",
        sample_id=sample_id,
        surface=surface,
        phase=phase,
    )
    require_values(
        summary,
        "MISSING_QUIET_CAPTURE",
        threshold_pct="20.0",
        lineage="runner_descendants",
        probes=str(probes),
        captures=str(probes),
        status="CLEAR",
    )
    captures = [
        record
        for record in records_named(records, "t32_protocol_quiet_capture")
        if record.get("sample_id") == sample_id
        and record.get("surface") == surface
        and record.get("phase") == phase
    ]
    if len(captures) != probes:
        fail(
            "MISSING_QUIET_CAPTURE",
            f"sample_id={sample_id} surface={surface} phase={phase} captures={len(captures)}",
        )
    seen = set()
    for capture in captures:
        require_values(
            capture,
            "MISSING_QUIET_CAPTURE",
            threshold_pct="20.0",
            lineage="runner_descendants",
            status="CLEAR",
        )
        probe = integer(capture, "probe", "MISSING_QUIET_CAPTURE")
        seen.add(probe)
        key = (sample_id, surface, phase, probe)
        raw = raw_blocks.get(key)
        if raw is None:
            fail("MISSING_QUIET_CAPTURE", f"sample_id={sample_id} missing_raw_capture={key}")
        for field in (
            "tree_sha256",
            "process_sha256",
            "external_busy_sha256",
            "gpu_sha256",
            "external_gpu_sha256",
        ):
            if capture.get(field) != raw[field]:
                fail("MISSING_QUIET_CAPTURE", f"sample_id={sample_id} raw_hash_mismatch={field}")
        for field in ("process_rows", "gpu_rows", "external_busy", "external_gpu"):
            if integer(capture, field, "MISSING_QUIET_CAPTURE") != raw[field]:
                fail("MISSING_QUIET_CAPTURE", f"sample_id={sample_id} raw_count_mismatch={field}")
        if int(raw["end_line"]) >= int(capture["_line"]):
            fail("MISSING_QUIET_CAPTURE", f"sample_id={sample_id} raw_record_order")
        if integer(capture, "external_busy", "MISSING_QUIET_CAPTURE") != 0:
            fail("MISSING_QUIET_CAPTURE", f"sample_id={sample_id} external_busy_nonzero")
        if integer(capture, "external_gpu", "MISSING_QUIET_CAPTURE") != 0:
            fail("MISSING_QUIET_CAPTURE", f"sample_id={sample_id} external_gpu_nonzero")
    if seen != set(range(1, probes + 1)):
        fail("MISSING_QUIET_CAPTURE", f"sample_id={sample_id} probes={sorted(seen)}")
    return integer(summary, "_line", "MISSING_QUIET_CAPTURE")


def validate_identity(
    records: list[dict[str, str | int]],
    sample_id: str,
    surface: str,
    expected: tuple[str, int, int, int, str],
) -> int:
    name, rounds, transitions, result_bytes, fnv = expected
    identity = exactly_one(
        records,
        "t32_protocol_identity_check",
        "IDENTITY_MISMATCH",
        sample_id=sample_id,
        surface=surface,
    )
    require_values(
        identity,
        "IDENTITY_MISMATCH",
        fixture=name,
        rounds=str(rounds),
        transitions=str(transitions),
        result_bytes=str(result_bytes),
        result_fnv1a64=fnv,
        instrumentation_off_on_equal="true",
        status="PASS",
    )
    if surface == "phase":
        require_values(identity, "IDENTITY_MISMATCH", production_unsplit_split_equal="true")
    return integer(identity, "_line", "IDENTITY_MISMATCH")


def validate_prepare(records: list[dict[str, str | int]], sample_id: str, sample: int) -> tuple[int, bool]:
    comparison = exactly_one(
        records,
        "t32_protocol_prepare_check",
        "MISSING_PREPARE_COMPARISON",
        sample_id=sample_id,
    )
    expected_order = "unsplit_then_split" if sample % 2 == 0 else "split_then_unsplit"
    require_values(
        comparison,
        "MISSING_PREPARE_COMPARISON",
        clock="cuda_device_events",
        method="direct_launch_events",
        order=expected_order,
        tolerance_basis_points="500",
        production_subcost_attribution="false",
        status="PASS",
    )
    split = integer(comparison, "split_prepare_sum_ns", "MISSING_PREPARE_COMPARISON")
    unsplit = integer(comparison, "unsplit_prepare_ns", "MISSING_PREPARE_COMPARISON")
    numerator = integer(
        comparison, "split_over_unsplit_numerator_ns", "MISSING_PREPARE_COMPARISON"
    )
    denominator = integer(
        comparison, "split_over_unsplit_denominator_ns", "MISSING_PREPARE_COMPARISON"
    )
    difference = integer(comparison, "split_minus_unsplit_ns", "MISSING_PREPARE_COMPARISON")
    attempts = integer(comparison, "recorded_attempts", "MISSING_PREPARE_COMPARISON")
    if unsplit <= 0 or attempts <= 0:
        fail("MISSING_PREPARE_COMPARISON", f"sample_id={sample_id} nonpositive_total_or_attempts")
    if numerator != split or denominator != unsplit or difference != split - unsplit:
        fail("MISSING_PREPARE_COMPARISON", f"sample_id={sample_id} arithmetic_mismatch")
    within = abs(difference) * 10_000 <= unsplit * 500
    expected_within = "true" if within else "false"
    expected_attribution = "within_bound" if within else "perturbed_unusable"
    require_values(
        comparison,
        "MISSING_PREPARE_COMPARISON",
        within_tolerance=expected_within,
        split_total_attribution=expected_attribution,
    )
    return integer(comparison, "_line", "MISSING_PREPARE_COMPARISON"), within


def main() -> None:
    if len(sys.argv) != 3 or sys.argv[1] not in {"local", "cuda"}:
        fail("MALFORMED_LOG", "usage=t32-instrumentation-analyze.py_<local|cuda>_<log>")
    mode = sys.argv[1]
    records, lines = parse(Path(sys.argv[2]))
    raw_blocks = parse_quiet_blocks(lines)
    structured_capture_keys = set()
    for capture in records_named(records, "t32_protocol_quiet_capture"):
        try:
            structured_capture_keys.add(
                (
                    str(capture["sample_id"]),
                    str(capture["surface"]),
                    str(capture["phase"]),
                    int(str(capture["probe"])),
                )
            )
        except (KeyError, ValueError):
            fail("MISSING_QUIET_CAPTURE", f"line={capture['_line']} malformed_capture_key")
    if set(raw_blocks) != structured_capture_keys:
        fail("MISSING_QUIET_CAPTURE", "raw_and_structured_capture_rosters_differ")
    start = exactly_one(records, "t32_protocol_start", "MALFORMED_LOG")
    require_values(start, "MALFORMED_LOG", mode=mode)
    if mode == "local":
        require_values(
            start,
            "PROTOCOL_CONTRACT_MISMATCH",
            scope="local_correctness",
            non_hardware="true",
        )
    elif start.get("machine") not in {"boston", "madrid"}:
        fail("PROTOCOL_CONTRACT_MISMATCH", "invalid_cuda_machine")
    expected_commit = str(start.get("expected_commit", ""))
    actual_commit = str(start.get("actual_commit", ""))
    if not COMMIT.fullmatch(expected_commit) or actual_commit != expected_commit:
        fail("SOURCE_DRIFT", "session_commit_mismatch")
    preflight = exactly_one(records, "t32_protocol_preflight", "SOURCE_DRIFT")
    require_values(
        preflight,
        "SOURCE_DRIFT",
        status="PASS",
        fixture_count="7",
        worktree="clean",
    )
    required_binary_roles = {"counter"} if mode == "local" else {"phase", "counter"}
    binaries = records_named(records, "t32_protocol_binary")
    if (
        len(binaries) != len(required_binary_roles)
        or {str(record.get("role")) for record in binaries} != required_binary_roles
    ):
        fail("PROTOCOL_CONTRACT_MISMATCH", "binary_role_mismatch")
    for binary in binaries:
        if not SHA256.fullmatch(str(binary.get("sha256", ""))):
            fail("PROTOCOL_CONTRACT_MISMATCH", "invalid_binary_sha256")

    accepts = records_named(records, "t32_protocol_sample_accept")
    samples = 1 if mode == "local" else 5
    if mode == "local":
        expected_sequence = [(0, index) for index in range(7)]
    else:
        expected_sequence = [
            (sample, (sample + position) % 7)
            for sample in range(samples)
            for position in range(7)
        ]
    expected_slots = set(expected_sequence)
    actual_slots: dict[tuple[int, int], dict[str, str | int]] = {}
    actual_sequence: list[tuple[int, int]] = []
    for accept in accepts:
        sample = integer(accept, "sample_index", "PROTOCOL_CONTRACT_MISMATCH")
        index = integer(accept, "fixture_index", "PROTOCOL_CONTRACT_MISMATCH")
        slot = (sample, index)
        if slot in actual_slots:
            fail("PROTOCOL_CONTRACT_MISMATCH", f"duplicate_slot={slot}")
        actual_slots[slot] = accept
        actual_sequence.append(slot)
    if set(actual_slots) != expected_slots:
        fail("PROTOCOL_CONTRACT_MISMATCH", "accepted_slot_roster_mismatch")
    if actual_sequence != expected_sequence:
        fail("PROTOCOL_CONTRACT_MISMATCH", "cyclic_rotation_order_mismatch")

    digests: dict[str, set[str]] = defaultdict(set)
    prepare_cells: dict[str, list[bool]] = defaultdict(list)
    for sample, index in sorted(expected_slots):
        expected = ROSTER[index]
        name = expected[0]
        accept = actual_slots[(sample, index)]
        require_values(accept, "PROTOCOL_CONTRACT_MISMATCH", mode=mode, fixture=name)
        sample_id = str(accept.get("sample_id", ""))
        prefix = "local" if mode == "local" else str(start["machine"])
        expected_sample_id = f"{prefix}_s{sample}_i{index}_a1"
        require_values(
            accept,
            "PROTOCOL_CONTRACT_MISMATCH",
            sample_id=expected_sample_id,
            attempt="1",
        )
        if mode == "cuda":
            phase_source = exactly_one(
                records,
                "t32_protocol_source_check",
                "SOURCE_DRIFT",
                sample_id=sample_id,
                surface="phase",
            )
            counter_source = exactly_one(
                records,
                "t32_protocol_source_check",
                "SOURCE_DRIFT",
                sample_id=sample_id,
                surface="counter",
            )
            for source in (phase_source, counter_source):
                require_values(
                    source,
                    "SOURCE_DRIFT",
                    status="PASS",
                    expected_commit=expected_commit,
                    actual_commit=expected_commit,
                    fixture_manifest="PASS",
                    worktree="clean",
                    binary_hashes="PASS",
                )
            phase_source_line = integer(phase_source, "_line", "SOURCE_DRIFT")
            counter_source_line = integer(counter_source, "_line", "SOURCE_DRIFT")
            phase_pre = validate_quiet(records, raw_blocks, sample_id, "phase", "pre", 3)
            phase_begin = exactly_one(
                records,
                "t32_protocol_sample_begin",
                "PROTOCOL_CONTRACT_MISMATCH",
                sample_id=sample_id,
                surface="phase",
            )
            require_values(
                phase_begin,
                "PROTOCOL_CONTRACT_MISMATCH",
                mode="cuda",
                sample_index=str(sample),
                fixture_index=str(index),
                fixture=name,
                attempt="1",
            )
            phase_identity = validate_identity(records, sample_id, "phase", expected)
            prepare_line, within = validate_prepare(records, sample_id, sample)
            phase_post = validate_quiet(records, raw_blocks, sample_id, "phase", "post", 1)
            counter_pre = validate_quiet(records, raw_blocks, sample_id, "counter", "pre", 3)
            counter_begin = exactly_one(
                records,
                "t32_protocol_sample_begin",
                "PROTOCOL_CONTRACT_MISMATCH",
                sample_id=sample_id,
                surface="counter",
            )
            require_values(
                counter_begin,
                "PROTOCOL_CONTRACT_MISMATCH",
                mode="cuda",
                sample_index=str(sample),
                fixture_index=str(index),
                fixture=name,
                attempt="1",
            )
            counter_identity = validate_identity(records, sample_id, "counter", expected)
            counter_post = validate_quiet(records, raw_blocks, sample_id, "counter", "post", 1)
            contract = exactly_one(
                records,
                "t32_protocol_contract_check",
                "PROTOCOL_CONTRACT_MISMATCH",
                sample_id=sample_id,
            )
            require_values(
                contract,
                "PROTOCOL_CONTRACT_MISMATCH",
                capacity_caps="PASS",
                dispatch_names="PASS",
                dispatch_counts="PASS",
                status="PASS",
            )
            if not (
                phase_source_line
                < phase_pre
                < integer(phase_begin, "_line", "PROTOCOL_CONTRACT_MISMATCH")
                < phase_post
                < phase_identity
                <= prepare_line
                < counter_source_line
                < counter_pre
                < integer(counter_begin, "_line", "PROTOCOL_CONTRACT_MISMATCH")
                < counter_post
                < counter_identity
                < integer(accept, "_line", "PROTOCOL_CONTRACT_MISMATCH")
            ):
                fail("PROTOCOL_CONTRACT_MISMATCH", f"sample_id={sample_id} record_order")
            prepare_cells[name].append(within)
        else:
            source = exactly_one(
                records,
                "t32_protocol_source_check",
                "SOURCE_DRIFT",
                sample_id=sample_id,
                surface="counter",
            )
            require_values(
                source,
                "SOURCE_DRIFT",
                status="PASS",
                expected_commit=expected_commit,
                actual_commit=expected_commit,
                fixture_manifest="PASS",
                worktree="clean",
                binary_hashes="PASS",
            )
            source_line = integer(source, "_line", "SOURCE_DRIFT")
            counter_pre = validate_quiet(records, raw_blocks, sample_id, "counter", "pre", 3)
            counter_begin = exactly_one(
                records,
                "t32_protocol_sample_begin",
                "PROTOCOL_CONTRACT_MISMATCH",
                sample_id=sample_id,
                surface="counter",
            )
            require_values(
                counter_begin,
                "PROTOCOL_CONTRACT_MISMATCH",
                mode="local",
                sample_index="0",
                fixture_index=str(index),
                fixture=name,
                attempt="1",
            )
            counter_identity = validate_identity(records, sample_id, "counter", expected)
            counter_post = validate_quiet(records, raw_blocks, sample_id, "counter", "post", 1)
            if name == "e1_10":
                scalar = exactly_one(
                    records,
                    "t32_protocol_scalar_reference",
                    "IDENTITY_MISMATCH",
                    sample_id=sample_id,
                )
                require_values(scalar, "IDENTITY_MISMATCH", fixture="e1_10", status="PASS")
            if not (
                source_line
                < counter_pre
                < integer(counter_begin, "_line", "PROTOCOL_CONTRACT_MISMATCH")
                < counter_post
                < counter_identity
                < integer(accept, "_line", "PROTOCOL_CONTRACT_MISMATCH")
            ):
                fail("PROTOCOL_CONTRACT_MISMATCH", f"sample_id={sample_id} record_order")

        digest = exactly_one(
            records,
            "t32_protocol_counter_digest",
            "COUNTER_DIGEST_MISMATCH",
            sample_id=sample_id,
        )
        require_values(digest, "COUNTER_DIGEST_MISMATCH", fixture=name, status="PASS")
        digest_sha = str(digest.get("sha256", ""))
        if not SHA256.fullmatch(digest_sha):
            fail("COUNTER_DIGEST_MISMATCH", f"sample_id={sample_id} invalid_sha256")
        digests[name].add(digest_sha)
        digest_line = integer(digest, "_line", "COUNTER_DIGEST_MISMATCH")
        accept_line = integer(accept, "_line", "PROTOCOL_CONTRACT_MISMATCH")
        if mode == "cuda":
            contract_line = integer(contract, "_line", "PROTOCOL_CONTRACT_MISMATCH")
            if not (
                prepare_line < contract_line < counter_source_line
                and counter_identity < digest_line < accept_line
            ):
                fail("PROTOCOL_CONTRACT_MISMATCH", f"sample_id={sample_id} derived_record_order")
        else:
            if not (counter_identity < digest_line < accept_line):
                fail("PROTOCOL_CONTRACT_MISMATCH", f"sample_id={sample_id} digest_record_order")
            if name == "e1_10":
                scalar_line = integer(scalar, "_line", "IDENTITY_MISMATCH")
                if not (counter_identity < scalar_line < accept_line):
                    fail("PROTOCOL_CONTRACT_MISMATCH", f"sample_id={sample_id} scalar_record_order")

    for fixture, values in digests.items():
        if len(values) != 1:
            fail("COUNTER_DIGEST_MISMATCH", f"fixture={fixture} distinct_digests={len(values)}")
    complete = exactly_one(records, "t32_protocol_complete", "PROTOCOL_CONTRACT_MISMATCH")
    expected_status = "PASS" if mode == "local" else "COLLECTED"
    require_values(
        complete,
        "PROTOCOL_CONTRACT_MISMATCH",
        mode=mode,
        accepted=str(samples * 7),
        status=expected_status,
    )
    if accepts and integer(complete, "_line", "PROTOCOL_CONTRACT_MISMATCH") <= max(
        integer(accept, "_line", "PROTOCOL_CONTRACT_MISMATCH") for accept in accepts
    ):
        fail("PROTOCOL_CONTRACT_MISMATCH", "completion_precedes_last_accept")

    if mode == "cuda":
        for fixture, verdicts in sorted(prepare_cells.items()):
            usable = len(verdicts) == 5 and all(verdicts)
            print(
                "record=t32_protocol_prepare_cell "
                f"fixture={fixture} accepted_pairs={len(verdicts)} "
                f"all_within_tolerance={str(usable).lower()} "
                f"attribution={'usable' if usable else 'perturbed_unusable'}"
            )
    analysis_status = "PASS" if mode == "local" else "COLLECTED"
    print(
        f"record=t32_protocol_analysis mode={mode} status={analysis_status} accepted={samples * 7}"
    )


if __name__ == "__main__":
    main()
