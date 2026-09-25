#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn main() {
    use std::path::PathBuf;
    use std::time::Instant;

    use days::scenario::compile_config;
    use days_executor::{CpuConfig, MetalConfig, run_cpu, run_metal, run_scalar};

    let relative = std::env::args().nth(1).unwrap_or_else(|| {
        "configs/benchmarks/width_via_load_full/fattree_k32_load_10.toml".into()
    });
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&relative);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));

    let scalar_started = Instant::now();
    let scalar = run_scalar(&image, None)
        .unwrap_or_else(|error| panic!("scalar failed for {}: {error}", path.display()));
    let scalar_ns = scalar_started.elapsed().as_nanos();

    let cpu_started = Instant::now();
    let cpu = run_cpu(
        &image,
        None,
        CpuConfig {
            workers: 4,
            ..CpuConfig::default()
        },
    )
    .unwrap_or_else(|error| panic!("W4 CPU failed for {}: {error}", path.display()));
    let cpu_ns = cpu_started.elapsed().as_nanos();
    assert_eq!(cpu.result, scalar, "W4 differs from scalar");
    drop(cpu);

    let metal_started = Instant::now();
    let metal = run_metal(&image, None, MetalConfig::default())
        .unwrap_or_else(|error| panic!("Metal failed for {}: {error}", path.display()));
    let metal_end_to_end_ns = metal_started.elapsed().as_nanos();
    assert_eq!(metal.result, scalar, "Metal differs from scalar");
    if relative == "configs/benchmarks/width_via_load_full/fattree_k32_load_10.toml" {
        assert_eq!(metal.transitions, 10_604_109);
    }

    println!("fixture={}", path.display());
    println!(
        "nodes={} flows={} initial_events={}",
        image.nodes.len(),
        image.flows.len(),
        image.initial_events.len()
    );
    println!("scalar_s={:.6}", scalar_ns as f64 / 1_000_000_000.0);
    println!("w4_cpu_s={:.6}", cpu_ns as f64 / 1_000_000_000.0);
    println!(
        "metal_end_to_end_s={:.6}",
        metal_end_to_end_ns as f64 / 1_000_000_000.0
    );
    println!(
        "metal_encode_to_completion_s={:.6}",
        metal.wall_ns as f64 / 1_000_000_000.0
    );
    println!(
        "metal_device_s={:.6}",
        metal.device_ns as f64 / 1_000_000_000.0
    );
    println!(
        "metal_host_encode_submit_s={:.6}",
        metal.host_encode_submit_ns as f64 / 1_000_000_000.0
    );
    println!(
        "metal_rounds={} metal_transitions={}",
        metal.rounds, metal.transitions
    );
    println!(
        "metal_vs_scalar={:.3}x metal_vs_w4={:.3}x",
        metal_end_to_end_ns as f64 / scalar_ns as f64,
        metal_end_to_end_ns as f64 / cpu_ns as f64
    );
}

#[cfg(not(all(feature = "metal-spike", target_vendor = "apple")))]
fn main() {
    eprintln!("t14_metal_gate requires --features metal-spike on an Apple target");
    std::process::exit(2);
}
