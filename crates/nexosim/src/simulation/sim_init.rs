use std::fmt;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::channel::ChannelObserver;
use crate::executor::{Executor, SimulationContext};
use crate::model::ProtoModel;
use crate::time::{AtomicTime, Clock, MonotonicTime, NoClock, TearableAtomicTime};
use crate::util::priority_queue::PriorityQueue;
use crate::util::sync_cell::SyncCell;

use super::{
    ExecutionError, GlobalScheduler, Mailbox, Scheduler, SchedulerState, Signal, Simulation,
    add_model,
};

/// Builder for a multi-threaded, discrete-event simulation.
pub struct SimInit {
    executor: Executor,
    scheduler_state: Arc<SchedulerState>,
    time: AtomicTime,
    is_halted: Arc<AtomicBool>,
    clock: Box<dyn Clock + 'static>,
    clock_tolerance: Option<Duration>,
    timeout: Duration,
    max_groups_per_step_task: usize,
    observers: Vec<(String, Box<dyn ChannelObserver>)>,
    abort_signal: Signal,
    model_names: Vec<String>,
}

impl SimInit {
    /// Creates a builder for a multithreaded simulation running on all
    /// available logical threads.
    pub fn new() -> Self {
        Self::with_num_threads(num_cpus::get())
    }

    /// Creates a builder for a simulation running on the specified number of
    /// threads.
    ///
    /// Note that the number of worker threads is automatically constrained to
    /// be between 1 and `usize::BITS` (inclusive). It is always set to 1 on
    /// `wasm` targets.
    pub fn with_num_threads(num_threads: usize) -> Self {
        let num_threads = if cfg!(target_family = "wasm") {
            1
        } else {
            num_threads.clamp(1, usize::BITS as usize)
        };
        let time = SyncCell::new(TearableAtomicTime::new(MonotonicTime::EPOCH));
        let simulation_context = SimulationContext {
            #[cfg(feature = "tracing")]
            time_reader: time.reader(),
        };

        let abort_signal = Signal::new();
        let executor = if num_threads == 1 {
            Executor::new_single_threaded(simulation_context, abort_signal.clone())
        } else {
            Executor::new_multi_threaded(num_threads, simulation_context, abort_signal.clone())
        };

        let scheduler_queue = Arc::new(Mutex::new(PriorityQueue::new()));
        let scheduler_state = Arc::new(SchedulerState::new(
            scheduler_queue,
            executor.executor_id(),
            num_threads,
        ));

        Self {
            executor,
            scheduler_state,
            time,
            is_halted: Arc::new(AtomicBool::new(false)),
            clock: Box::new(NoClock::new()),
            clock_tolerance: None,
            timeout: Duration::ZERO,
            max_groups_per_step_task: 1,
            observers: Vec::new(),
            abort_signal,
            model_names: Vec::new(),
        }
    }

    /// Sets the simulation time quantization quantum in nanoseconds.
    ///
    /// A value of 0 disables quantization.
    pub fn set_time_quantum_ns(self, quantum_ns: u64) -> Self {
        self.scheduler_state.set_time_quantum_ns(quantum_ns);
        self
    }

    /// Sets the number of hot standby workers.
    pub fn set_hot_worker_count(mut self, hot_worker_count: usize) -> Self {
        self.executor.set_hot_worker_count(hot_worker_count);
        self
    }

    /// Sets the maximum number of action groups bundled into a single executor
    /// task per step.
    ///
    /// A value of 1 preserves the default behavior (one task per group).
    pub fn set_max_groups_per_step_task(mut self, max_groups: usize) -> Self {
        self.max_groups_per_step_task = max_groups.max(1);
        self
    }

    /// Adds a model and its mailbox to the simulation bench.
    ///
    /// The `name` argument needs not be unique. The use of the dot character in
    /// the name is possible but discouraged as it can cause confusion with the
    /// fully qualified name of a submodel. If an empty string is provided, it
    /// is replaced by the string `<unknown>`.
    pub fn add_model<P: ProtoModel>(
        mut self,
        model: P,
        mailbox: Mailbox<P::Model>,
        name: impl Into<String>,
    ) -> Self {
        let mut name = name.into();
        if name.is_empty() {
            name = String::from("<unknown>");
        };
        self.observers
            .push((name.clone(), Box::new(mailbox.0.observer())));
        let scheduler = GlobalScheduler::new(
            self.scheduler_state.clone(),
            self.time.reader(),
            self.is_halted.clone(),
        );

        add_model(
            model,
            mailbox,
            name,
            scheduler,
            &self.executor,
            &self.abort_signal,
            &mut self.model_names,
        );

        self
    }

    /// Synchronizes the simulation with the provided [`Clock`].
    ///
    /// If the clock isn't explicitly set then the default [`NoClock`] is used,
    /// resulting in the simulation running as fast as possible.
    pub fn set_clock(mut self, clock: impl Clock + 'static) -> Self {
        self.clock = Box::new(clock);

        self
    }

    /// Specifies a tolerance for clock synchronization.
    ///
    /// When a clock synchronization tolerance is set, then any report of
    /// synchronization loss by [`Clock::synchronize`] that exceeds the
    /// specified tolerance will trigger an [`ExecutionError::OutOfSync`] error.
    pub fn set_clock_tolerance(mut self, tolerance: Duration) -> Self {
        self.clock_tolerance = Some(tolerance);

        self
    }

    /// Sets a timeout for the call to [`SimInit::init`] and for any subsequent
    /// simulation step.
    ///
    /// The timeout corresponds to the maximum wall clock time allocated for the
    /// completion of a single simulation step before an
    /// [`ExecutionError::Timeout`] error is raised.
    ///
    /// A null duration disables the timeout, which is the default behavior.
    ///
    /// See also [`Simulation::set_timeout`].
    #[cfg(not(target_family = "wasm"))]
    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;

        self
    }

    /// Builds a simulation initialized at the specified simulation time,
    /// executing the [`Model::init`](crate::model::Model::init) method on all
    /// model initializers.
    ///
    /// The simulation object and its associated scheduler are returned upon
    /// success.
    pub fn init(
        self,
        start_time: MonotonicTime,
    ) -> Result<(Simulation, Scheduler), ExecutionError> {
        self.scheduler_state
            .init_origin_seqs(self.model_names.len().checked_add(1).unwrap());

        self.time.write(start_time);

        let scheduler = Scheduler::new(
            self.scheduler_state.clone(),
            self.time.reader(),
            self.is_halted.clone(),
        );
        let mut simulation = Simulation::new(
            self.executor,
            self.scheduler_state,
            self.time,
            self.clock,
            self.clock_tolerance,
            self.timeout,
            self.max_groups_per_step_task,
            self.observers,
            self.model_names,
            self.is_halted,
        );
        simulation.synchronize_clock(start_time)?;
        simulation.run()?;

        Ok((simulation, scheduler))
    }
}

impl Default for SimInit {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SimInit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimInit").finish_non_exhaustive()
    }
}
