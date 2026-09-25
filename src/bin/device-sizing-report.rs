//! Host-only device sizing report.
//!
//! Lowers one fixture and prints the exact default device plan the Metal and CUDA planners would
//! allocate: every plane, the event arenas, and the total. It is host arithmetic only: it never
//! allocates device memory and never executes the simulation, so it runs on any machine.
//!
//! Usage: `device-sizing-report [FIXTURE]`, with `FIXTURE` relative to the repository root. The
//! default is the k48 width-via-load 30% sustained fixture.

const DEFAULT_FIXTURE: &str =
    "configs/benchmarks/width_via_load_k48_h16/fattree_k48_h16_load_30_sustained.toml";

fn main() {
    let mut fixture = None;
    for argument in std::env::args().skip(1) {
        match argument.as_str() {
            unknown if unknown.starts_with("--") => panic!("unknown argument {unknown}"),
            path if fixture.is_none() => fixture = Some(path.to_owned()),
            extra => panic!("unexpected second fixture path {extra}"),
        }
    }
    let fixture = fixture.unwrap_or_else(|| DEFAULT_FIXTURE.to_owned());
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&fixture);
    let image = days::scenario::compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let report = days_executor::size_default_device_plan(&image)
        .unwrap_or_else(|error| panic!("failed to size {}: {error}", path.display()));

    println!(
        "record=t17c_wide_sizing_protocol mode=host_arithmetic_only allocates_device=false \
         executes_simulation=false plane_count={} fixture={fixture}",
        report.planes.len()
    );
    for plane in &report.planes {
        println!(
            "record=t17c_wide_sizing_plane index={} name={} words={} bytes={} fixture={fixture}",
            plane.index, plane.name, plane.words, plane.bytes,
        );
    }
    let arenas = report.event_arenas;
    println!(
        "record=t17c_wide_sizing_arena legacy_heap_event_slots={} \
         fallback_heap_event_slots={} channel_stream_event_slots={} \
         service_stream_event_slots={} generator_stream_event_slots={} heap_arena_bytes={} \
         stream_arena_bytes={} total_event_arena_bytes={} legacy_heap_arena_bytes={} \
         fixture={fixture}",
        arenas.legacy_heap_event_slots,
        arenas.fallback_heap_event_slots,
        arenas.channel_stream_event_slots,
        arenas.service_stream_event_slots,
        arenas.generator_stream_event_slots,
        arenas.heap_arena_bytes,
        arenas.stream_arena_bytes,
        arenas.total_event_arena_bytes(),
        arenas.legacy_heap_arena_bytes,
    );
    println!(
        "record=t17c_wide_sizing_total plane_count={} total_device_bytes={} \
         total_device_mib={:.6} total_device_gib={:.9} fixture={fixture}",
        report.planes.len(),
        report.total_device_bytes,
        report.total_device_bytes as f64 / 1_048_576.0,
        report.total_device_bytes as f64 / 1_073_741_824.0,
    );
}
