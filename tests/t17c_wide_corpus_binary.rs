use assert_cmd::cargo::cargo_bin_cmd;

#[test]
fn k48_wide_sizing_dry_run_is_host_only_and_reproduces_load30_arenas() {
    let fixture = "configs/benchmarks/width_via_load_k48_h16/\
                   fattree_k48_h16_load_30_sustained.toml";
    let output = cargo_bin_cmd!("t17c_wide_corpus")
        .args(["--sizing-dry-run", fixture])
        .output()
        .expect("sizing dry-run must launch");
    assert!(
        output.status.success(),
        "sizing dry-run failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("sizing report must be UTF-8");
    assert!(stdout.contains(
        "record=t17c_wide_sizing_protocol \
         mode=host_arithmetic_only allocates_device=false executes_simulation=false plane_count=28"
    ));
    assert_eq!(
        stdout
            .lines()
            .filter(|line| line.starts_with("record=t17c_wide_sizing_plane "))
            .count(),
        28
    );
    let plane_names = stdout
        .lines()
        .filter(|line| line.starts_with("record=t17c_wide_sizing_plane "))
        .map(|line| {
            line.split_whitespace()
                .find_map(|field| field.strip_prefix("name="))
                .expect("plane record must name the plane")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        plane_names,
        [
            "control",
            "params",
            "node_state",
            "generators",
            "flows",
            "routes",
            "links",
            "fel_meta",
            "fel_records",
            "queue_meta",
            "queue_records",
            "in_service",
            "outbox",
            "worklist",
            "summary",
            "observed",
            "departures",
            "arrivals",
            "lp_state",
            "remote_meta",
            "remote_staging",
            "observation_meta",
            "inbound_meta",
            "inbound_producers",
            "merge_cursors",
            "stream_state",
            "stream_records",
            "scheduler_state",
        ]
    );
    // T21 fix 2 appended the per-round FEL root cache to `stream_state`: two words per LP. O2.12
    // additionally sizes stream records at their exact 11/5/10-word physical widths. The literals
    // below exclude the root-cache region so that its size remains image-derived.
    const LOWERED_LPS: usize = 147_456;
    let round_scratch_bytes = days_executor::device_sizing::round_scratch_words(LOWERED_LPS)
        .expect("round scratch must size")
        * std::mem::size_of::<u64>();
    assert!(stdout.contains(&format!(
        "record=t17c_wide_sizing_arena \
         legacy_heap_event_slots=32775270 fallback_heap_event_slots=152986 \
         channel_stream_event_slots=2355300 service_stream_event_slots=294912 \
         generator_stream_event_slots=11060 heap_arena_bytes=21853024 \
         stream_arena_bytes={} total_event_arena_bytes={} \
         legacy_heap_arena_bytes=3675548832",
        490_621_544_usize + round_scratch_bytes,
        512_474_568_usize + round_scratch_bytes,
    )));
    assert!(stdout.contains("record=t17c_wide_sizing_total plane_count=28 total_device_bytes="));
}
