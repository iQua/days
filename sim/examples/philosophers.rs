use sim::{SimContext, Time, RandomVar, Control, until, Process};
use std::{rc::Rc, cell::{RefCell, Cell}};
use rand::{rngs::SmallRng, SeedableRng, Rng};
use rand_distr::{Exp, Normal, Uniform, Distribution};
use rayon::prelude::*;

const PHILOSOPHER_COUNT: usize = 5;
const SEED: u64 = 100000;

struct Shared {
	master_rng: RefCell<SmallRng>,
	sim_duration: Cell<Time>
}

struct Table {
	forks: Vec<Control<bool>>,
	forks_held: Control<usize>,
	forks_awaited: Control<usize>
}

impl Table {
	fn new(forks: usize) -> Self {
		Table {
			forks: (0..forks).map(|_| Control::new(false)).collect(),
			forks_held: Control::default(),
			forks_awaited: Control::default()
		}
	}
	
	// blocks until the requested fork is available and acquires it
	async fn acquire_fork(&self, i: usize) {
		let num = i % self.forks.len();
		self.forks_awaited.set(self.forks_awaited.get() + 1);
		until(&self.forks[num], |fork| !fork.get()).await;
		self.forks[num].set(true);
		self.forks_awaited.set(self.forks_awaited.get() - 1);
		self.forks_held.set(self.forks_held.get() + 1);
	}
	
	// returns the requested fork to the table
	fn release_fork(&self, i: usize) {
		self.forks[i % self.forks.len()].set(false);
		self.forks_held.set(self.forks_held.get() - 1);
	}
}

struct Philosopher {
	table: Rc<Table>,
	seat: usize,
	rng: SmallRng
}

impl Philosopher {
	async fn actions(mut self, sim: SimContext<'_, Shared>) {
		let thinking_duration = Exp::new(1.0).unwrap();
		let artificial_delay = Uniform::new(0.1, 0.2);
		let eating_duration = Normal::new(0.5, 0.2).unwrap();
		
		loop {
			// spend some time pondering the nature of things
			sim.advance(thinking_duration.sample(&mut self.rng)).await;
			
			// acquire the first fork
			self.table.acquire_fork(self.seat).await;
			
			// introduce an artificial delay to leave room for deadlocks
			sim.advance(artificial_delay.sample(&mut self.rng)).await;
			
			// acquire the second fork
			self.table.acquire_fork(self.seat + 1).await;
			
			// spend some time eating
			sim.advance(
				eating_duration
					.sample_iter(&mut self.rng)
					.find(|&val| val >= 0.0)
					.unwrap()
			).await;
			
			// release the forks
			self.table.release_fork(self.seat + 1);
			self.table.release_fork(self.seat);
		}
	}
}

async fn run_once(sim: SimContext<'_, Shared>, count: usize) {
	let table = Rc::new(Table::new(count));
	
	// create the philosopher-processes and seat them
	for i in 0..count {
		sim.activate(Philosopher {
			table: table.clone(),
			seat: i,
			rng: SmallRng::from_seed(sim.shared().master_rng.borrow_mut().gen())
		}.actions(sim));
	}
	
	// wait for the precise configuration indicating a deadlock
	// (we technically don't have to do this, because deadlocks imply that no
	//  processes can advance, causing the simulation to end anyways;
	//  SLX doesn't allow us to record the time of that event, so we can't use
	//  it here either)
	until(
		(&table.forks_held, &table.forks_awaited), 
		|(held, awaited)|
			held.get() == count && awaited.get() == count
	).await;
	
	// tabulate the current system time
	sim.shared().sim_duration.set(sim.now());
}

fn philosophers(count: usize, reruns: usize) -> RandomVar {
	// use thread-based parallelism to concurrently run simulation models
	(1..=reruns)
		.into_par_iter()
		.map(|i|
			sim::simulation(
				// global data
				Shared {
					master_rng: RefCell::new(SmallRng::seed_from_u64(i as u64 * SEED)),
					sim_duration: Cell::default()
				},
				// simulation entry point
				|sim| Process::new(sim, run_once(sim, count))
			).sim_duration.get()
		)
		.fold(
			|| RandomVar::new(),
			|var, duration| { var.tabulate(duration); var }
		)
		.reduce(
			|| RandomVar::new(),
			|var_a, var_b| { var_a.merge(&var_b); var_a }
		)
}

#[cfg(not(test))]
fn main() {
	const EXPERIMENT_COUNT : usize = 500;
	
	let sim_duration = philosophers(PHILOSOPHER_COUNT, EXPERIMENT_COUNT);
	println!("sim_duration: {:#}", sim_duration);
}

#[cfg(test)]
criterion::criterion_main!(bench::benches);

#[cfg(all(test, windows))]
mod slx;

#[cfg(test)]
mod bench {
	use super::*;
	use criterion::{Criterion, BenchmarkId, PlotConfiguration, AxisScale,
	                criterion_group};
	
	#[cfg(feature = "odemx")]
	mod odemx {
		use std::os::raw::c_uint;
		
		#[link(name = "odemx", kind = "static")]
		extern {
			pub fn philosophers(count: c_uint, reruns: c_uint);
		}
	}
	#[cfg(windows)]
	const SLX_PATH: &'static str = "C:\\Wolverine\\SLX";
	const RANGE:   u32 = 10;
	const STEP:  usize =  3;
	
	fn philosopher_bench(c: &mut Criterion) {
		let mut group = c.benchmark_group("Philosophers");
		
		// set-up the benchmark parameters
		group.confidence_level(0.99);
		group.plot_config(
			PlotConfiguration::default()
				.summary_scale(AxisScale::Logarithmic)
		);
		// group.sampling_mode(SamplingMode::Linear);
		// group.measurement_time(std::time::Duration::from_secs(900));
		
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
		
		// vary the number of performed reruns in each experiment
		for experiment_count in (0..RANGE).map(|c| (1 << c)*STEP) {
			#[cfg(windows)]
			if let Some(path) = slx_path.as_ref() {
				let count = experiment_count.to_string();
				let args = [
					"/silent",
					"/stdout",
					"/noicon",
					"/nowarn",
					"/noxwarn",
					"/#BENCH",
					"slx\\philosophers.slx",
					count.as_str()
				];
				
				// benchmark the SLX implementation
				group.bench_function(
					BenchmarkId::new("SLX", experiment_count),
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
				BenchmarkId::new("ODEMx", experiment_count),
				|b| b.iter(|| unsafe {
					odemx::philosophers(
						PHILOSOPHER_COUNT as _,
						experiment_count as _
					)
				})
			);
			
			// benchmark the Rust implementation
			group.bench_function(
				BenchmarkId::new("Rust", experiment_count),
				|b| b.iter(||
					philosophers(PHILOSOPHER_COUNT, experiment_count)
				)
			);
		}
		
		group.finish();
	}
	
	criterion_group!(benches, philosopher_bench);
}
