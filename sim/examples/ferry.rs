use sim::{Time, SimContext, Sender, Receiver, RandomVar, select, channel, Process};
use rand::{rngs::SmallRng, SeedableRng, Rng};
use rand_distr::{Exp, Normal, Distribution};
use std::cell::RefCell;

const SEED: u64 = 100000;
const FERRY_COUNT: usize = 2;
const HARBOR_COUNT: usize = 4;
const HARBOR_DISTANCE: Time  = 10.0;
const FERRY_TIMEOUT  : Time  = 5.0;
const FERRY_CAPACITY : usize = 5;

struct Shared {
	master_rng: RefCell<SmallRng>,
	ferry_cargo_len: RandomVar,
	ferry_load_time: RandomVar,
	car_wait_time: RandomVar,
}

#[derive(Debug)]
struct Car {
	arrival_time: Time,
	load_duration: Time
}

struct Pier {
	rng: SmallRng,
	landing_site: Sender<Car>
}

struct Ferry {
	cargo: Vec<Car>,
	timeout: Time,
	travel_time: Time,
	piers: Vec<Receiver<Car>>
}

impl Pier {
	async fn actions(mut self, sim: SimContext<'_,Shared>) {
		let arrival_delay = Exp::new(0.1).unwrap();
		let loading_delay = Normal::new(0.5, 0.2).unwrap();
		
		loop {
			sim.advance(arrival_delay.sample(&mut self.rng)).await;
			self.landing_site.send(Car {
				arrival_time: sim.now(),
				load_duration: loading_delay.sample_iter(&mut self.rng).find(|&val| val >= 0.0).unwrap()
			}).await.expect("no ferries in the simulation");
		}
	}
}

impl Ferry {
	async fn actions(mut self, sim: SimContext<'_,Shared>) {
		loop {
			for pier in self.piers.iter() {
				// unload the cars
				for car in self.cargo.drain(..) {
					sim.advance(car.load_duration).await;
				}
				
				let begin_loading = sim.now();
				
				// wait until new cars arrive or a timeout occurs
				while self.cargo.len() < self.cargo.capacity() {
					match select(sim, pier.recv(), async {
						sim.advance(self.timeout).await;
						None
					}).await {
						// a car arrived in time
						Some(car) => {
							sim.shared().car_wait_time.tabulate(sim.now() - car.arrival_time);
							sim.advance(car.load_duration).await;
							self.cargo.push(car);
						}
						// the timeout has been triggered
						None => break
					}
				}
				
				sim.shared().ferry_load_time.tabulate(sim.now() - begin_loading);
				sim.shared().ferry_cargo_len.tabulate(self.cargo.len() as f64);
				
				// travel to the next harbor
				sim.advance(self.travel_time).await;
			}
		}
	}
}

async fn ferry(sim: SimContext<'_,Shared>, duration: Time, ferries: usize, harbors: usize) {
	let mut ports = Vec::with_capacity(harbors);
	
	// create all of the harbors
	for _ in 0..harbors {
		let (sx, rx) = channel();
		let harbor = Pier {
			rng: SmallRng::from_seed(sim.shared().master_rng.borrow_mut().gen()),
			landing_site: sx
		};
		sim.activate(harbor.actions(sim));
		ports.push(rx);
	}
	
	// create all of the ferries
	for i in 0..ferries {
		let ferry = Ferry {
			cargo: Vec::with_capacity(FERRY_CAPACITY),
			timeout: FERRY_TIMEOUT,
			travel_time: HARBOR_DISTANCE,
			piers: ports.iter().skip(i)
			            .chain(ports.iter().take(i)).cloned().collect()
		};
		sim.activate(ferry.actions(sim));
	}
	
	// await the end of the simulation
	sim.advance(duration).await;
	
	// take cars into account that weren't picked up by a ferry
	for port in ports {
		for _ in 0..port.len() {
			let car = port.recv().await.unwrap();
			sim.shared().car_wait_time.tabulate(sim.now() - car.arrival_time);
		}
	}
}

#[cfg(not(test))]
fn main() {
	let result = sim::simulation(
		// global data
		Shared {
			master_rng: RefCell::new(SmallRng::seed_from_u64(SEED)),
			ferry_cargo_len: RandomVar::new(),
			ferry_load_time: RandomVar::new(),
			car_wait_time: RandomVar::new(),
		},
		// simulation entry point
		|sim| Process::new(
			sim, ferry(sim, 24.0*60.0*7.0, FERRY_COUNT, HARBOR_COUNT)
		)
	);
	
	println!("Number of harbors: {}", HARBOR_COUNT);
	println!("Number of ferries: {}", FERRY_COUNT);
	println!("Car wait time: {:#.3}", result.car_wait_time);
	println!("Ferry cargo len: {:#.3}", result.ferry_cargo_len);
	println!("Ferry load time: {:#.3}", result.ferry_load_time);
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
		use std::os::raw::{c_double, c_uint};
		
		#[link(name = "odemx", kind = "static")]
		extern {
			pub fn ferry(duration: c_double, ferries: c_uint, harbors: c_uint);
		}
	}
	#[cfg(windows)]
	const SLX_PATH: &'static str = "C:\\Wolverine\\SLX";
	const RANGE: u32 =   10;
	const STEP: Time = 1000.0;
	
	fn ferry_bench(c: &mut Criterion) {
		let mut group = c.benchmark_group("Ferry");
		
		// set-up the benchmark parameters
		group.confidence_level(0.99);
		group.plot_config(
			PlotConfiguration::default()
				.summary_scale(AxisScale::Logarithmic)
		);
		// group.sampling_mode(criterion::SamplingMode::Linear);
		// group.measurement_time(std::time::Duration::from_secs(600));
		
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
		for (i, sim_duration) in (0..RANGE).map(|c| Time::from(1 << c)*STEP).enumerate() {
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
					"slx\\ferry.slx",
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
				|b| b.iter(|| unsafe {
					odemx::ferry(
						sim_duration as _, 
						FERRY_COUNT as _, 
						HARBOR_COUNT as _
					)
				})
			);
			
			// benchmark the Rust implementation
			group.bench_function(
				BenchmarkId::new("Rust", sim_duration),
				|b| b.iter_batched(
					|| Shared {
						master_rng: RefCell::new(SmallRng::seed_from_u64(SEED*((i+1) as u64))),
						ferry_cargo_len: RandomVar::new(),
						ferry_load_time: RandomVar::new(),
						car_wait_time: RandomVar::new(),
					},
					|shared| sim::simulation(
						shared,
						|sim| Process::new(
							sim,
							ferry(sim, sim_duration, FERRY_COUNT, HARBOR_COUNT)
						)
					),
					BatchSize::SmallInput
				)
			);
		}
		
		group.finish();
	}
	
	criterion_group!(benches, ferry_bench);
}
