use sim::{Time, SimContext, Facility, RandomVar, Process};
use rand::Rng;
use std::cell::RefCell;

// helper constants
const SEED_A: u64  = 100000;
const SEED_S: u64  = 200000;

/// Globally shared data.
struct Shared<R: Rng> {
	rng_a: RefCell<R>,
	rng_s: RefCell<R>,
	joe: Facility,
	wait_time: RandomVar
}

/// Customer process with access to the barber and a random processing delay.
struct Customer;

impl Customer {
	pub async fn actions(self, sim: SimContext<'_, Shared<impl Rng>>) {
		// access the barber and record the time for the report
		let arrival_time = sim.now();
		sim.shared().joe.seize().await;
		sim.shared().wait_time.tabulate(sim.now() - arrival_time);
		
		// spend time
		sim.advance(
			sim.shared().rng_s.borrow_mut().gen_range(12.0..18.0)
		).await;
		// release the barber
		sim.shared().joe.release();
	}
}

async fn barbershop<'b>(sim: SimContext<'b, Shared<impl Rng + 'b>>, duration: Time) {
	// activate a process to generate the customers
	sim.activate(async move {
		loop {
			sim.advance(
				sim.shared().rng_a.borrow_mut().gen_range(12.0..24.0)
			).await;
			if sim.now() >= duration { return; }
			sim.activate(Customer.actions(sim));
		}
	});
	
	// wait until the store closes
	sim.advance(duration).await;
	
	// finish processing the queue (no more customers arrive)
	sim.shared().joe.seize().await;
}

#[cfg(not(test))]
fn main() {
	use rand::{rngs::SmallRng, SeedableRng};
	
	let result = sim::simulation(
		// global data
		Shared {
			rng_a: RefCell::new(SmallRng::seed_from_u64(SEED_A)),
			rng_s: RefCell::new(SmallRng::seed_from_u64(SEED_S)),
			joe: Facility::new(),
			wait_time: RandomVar::new()
		},
		// simulation entry point
		|sim|
			Process::new(sim, barbershop(sim, 60.0*24.0*7.0*3.0))
	);
	
	// print statistics
	println!("wait_time: {:#.3}", result.wait_time);
}

#[cfg(test)]
criterion::criterion_main!(bench::benches);

#[cfg(all(test, windows))]
mod slx;

#[cfg(test)]
mod bench {
	use super::*;
	use criterion::{Criterion, BenchmarkId, BatchSize, PlotConfiguration,
	                AxisScale, criterion_group};
	
	#[cfg(feature = "odemx")]
	mod odemx {
		use std::os::raw::c_double;
		
		#[link(name = "odemx", kind = "static")]
		extern {
			pub fn barbershop(duration: c_double);
		}
	}
	#[cfg(windows)]
	const SLX_PATH: &'static str = "C:\\Wolverine\\SLX";
	const RANGE: u32 =   10;
	const STEP: Time = 1000.0;
	
	fn barbershop_bench(c: &mut Criterion) {
		use rand::{rngs::SmallRng, SeedableRng};
		
		let mut group = c.benchmark_group("Barbershop");
		
		// set-up the benchmark parameters
		group.confidence_level(0.99);
		group.plot_config(
			PlotConfiguration::default()
				.summary_scale(AxisScale::Logarithmic)
		);
		// group.sampling_mode(criterion::SamplingMode::Linear);
		// group.measurement_time(std::time::Duration::from_secs(300));
		
		#[cfg(windows)]
		let slx_path = {
			let path = slx::slx_version(SLX_PATH);
			
			if path.is_none() {
				println!("SLX not found, skipping SLX benchmarks!");
			} else {
				println!("Using SLX program at {:?}", path.as_ref().unwrap());
			}
			
			path
		};
		
		// vary in the length of the simulation run
		for sim_duration in (0..RANGE).map(|c| Time::from(1 << c)*STEP) {
			#[cfg(windows)]
			if let Some(path) = slx_path.as_ref() {
				let duration = sim_duration.to_string();
				let args = [
					"/silent",
					"/stdout",
					"/noicon",
					"/nowarn",
					"/noxwarn",
					"/#BENCH",
					"slx\\barbershop.slx",
					duration.as_str()
				];
				
				// benchmark the SLX implementation
				group.bench_function(
					BenchmarkId::new("SLX", sim_duration),
					|b| b.iter_custom(|iters|
						slx::slx_bench(
							path.as_os_str(),
							&args,
							iters as usize
						).expect("couldn't benchmark the SLX program")
					)
				);
			}
			
			// benchmark the C++ implementation
			#[cfg(feature = "odemx")]
			group.bench_function(
				BenchmarkId::new("ODEMx", sim_duration),
				|b| b.iter(
					|| unsafe { odemx::barbershop(sim_duration as _) }
				)
			);
			
			// benchmark the Rust implementation
			group.bench_function(
				BenchmarkId::new("Rust", sim_duration),
				|b| b.iter_batched(
					|| Shared {
						rng_a: RefCell::new(SmallRng::seed_from_u64(SEED_A)),
						rng_s: RefCell::new(SmallRng::seed_from_u64(SEED_S)),
						joe: Facility::new(),
						wait_time: RandomVar::new()
					},
					|shared| sim::simulation(
						shared,
						|sim| Process::new(sim, barbershop(sim, sim_duration))
					),
					BatchSize::SmallInput
				)
			);
		}
		
		group.finish();
	}
	
	criterion_group!(benches, barbershop_bench);
}
