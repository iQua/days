//! Scheduling functions and types.
use std::any::type_name;
use std::cell::UnsafeCell;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use crossbeam_utils::CachePadded;
use recycle_box::{RecycleBox, coerce_box};
use serde::Serialize;

use crate::channel::Sender;
use crate::model::Model;
use crate::ports::{InputFn, ReplyReader};
use crate::simulation::queue_items::{
    Event, EventId, EventIdErased, EventKey, FastEvent, FastScheduledEvent, Query, QueryId,
    QueueItem,
};
use crate::time::{AtomicTimeReader, ClockReader, Deadline, MonotonicTime};
use crate::util::priority_queue::PriorityQueue;
use crate::util::serialization::serialization_config;

#[cfg(all(test, not(nexosim_loom)))]
use crate::{time::AtomicTime, time::TearableAtomicTime, util::sync_cell::SyncCell};

use super::{Address, ExecutionError, GLOBAL_ORIGIN_ID, SaveError};

/// A scheduler for events and queries meant to be processed at specified
/// deadlines.
///
/// The `Scheduler` handle is `Clone`-able and can be shared or sent to other
/// threads.
///
/// When scheduling an event or query, it is important to consider that its
/// deadline must be in the future of the current simulation time and that
/// stepping method such as
/// [`Simulation::step`](crate::simulation::Simulation::step) or
/// [`Simulation::run`](crate::simulation::Simulation::run) eagerly advance the
/// simulation time to the deadline of the next scheduled event or simulation
/// tick. If a stepping method is executed concurrently, therefore, events or
/// queries can only be scheduled after the deadline associated with the next
/// scheduler event or simulation tick.
#[derive(Clone, Debug)]
pub struct Scheduler(GlobalScheduler);

impl Scheduler {
    /// Creates a new scheduler.
    pub(crate) fn new(
        state: Arc<SchedulerState>,
        time: AtomicTimeReader,
        is_halted: Arc<AtomicBool>,
    ) -> Self {
        Self(GlobalScheduler::new(state, time, is_halted))
    }

    /// Creates a dummy scheduler (for testing purposes only).
    #[cfg(all(test, not(nexosim_loom)))]
    #[allow(dead_code)]
    pub(crate) fn dummy() -> Self {
        let time = AtomicTime::new(TearableAtomicTime::new(MonotonicTime::EPOCH)).reader();
        let scheduler_queue = Arc::new(Mutex::new(SchedulerQueue::new()));
        let state = Arc::new(SchedulerState::new(scheduler_queue, 0, 1));
        let is_halted = Arc::new(AtomicBool::default());

        Self(GlobalScheduler::new(state, time, is_halted))
    }

    /// Returns the current simulation time.
    ///
    /// # Examples
    ///
    /// ```
    /// use nexosim::simulation::Scheduler;
    /// use nexosim::time::MonotonicTime;
    ///
    /// fn is_third_millennium(scheduler: &Scheduler) -> bool {
    ///     let time = scheduler.time();
    ///     time >= MonotonicTime::new(978307200, 0).unwrap()
    ///         && time < MonotonicTime::new(32535216000, 0).unwrap()
    /// }
    /// ```
    pub fn time(&self) -> MonotonicTime {
        self.0.time()
    }

    /// Schedules a readily-built event at a future time.
    #[cfg(feature = "server")]
    pub(crate) fn schedule(
        &self,
        deadline: impl Deadline,
        event: Event,
    ) -> Result<(), SchedulingError> {
        self.0.schedule_from(deadline, event, GLOBAL_ORIGIN_ID)
    }

    /// Schedules an event at a future time.
    ///
    /// An error is returned if the specified time is not in the future of the
    /// current simulation time.
    ///
    /// Events scheduled for the same time and targeting the same model are
    /// guaranteed to be processed according to the scheduling order.
    pub fn schedule_event<T>(
        &self,
        deadline: impl Deadline,
        event_id: &EventId<T>,
        arg: T,
    ) -> Result<(), SchedulingError>
    where
        T: Send + Clone + 'static,
    {
        self.0
            .schedule_event_from(deadline, event_id, arg, GLOBAL_ORIGIN_ID)
    }

    /// Schedules multiple events at future times.
    ///
    /// An error is returned if any of the specified deadlines is not in the
    /// future of the current simulation time. If an error is returned, no event
    /// is scheduled.
    pub fn schedule_event_batch<T, D>(
        &self,
        deadlines_and_args: Vec<(D, T)>,
        event_id: &EventId<T>,
    ) -> Result<(), SchedulingError>
    where
        T: Send + Clone + 'static,
        D: Deadline + Copy,
    {
        self.0
            .schedule_event_batch_from(deadlines_and_args, event_id, GLOBAL_ORIGIN_ID)
    }

    /// Schedules a cancellable event at a future time and returns an event key.
    ///
    /// An error is returned if the specified time is not in the future of the
    /// current simulation time.
    ///
    /// Events scheduled for the same time and targeting the same model are
    /// guaranteed to be processed according to the scheduling order.
    pub fn schedule_keyed_event<T>(
        &self,
        deadline: impl Deadline,
        event_id: &EventId<T>,
        arg: T,
    ) -> Result<EventKey, SchedulingError>
    where
        T: Send + Clone + 'static,
    {
        self.0
            .schedule_keyed_event_from(deadline, event_id, arg, GLOBAL_ORIGIN_ID)
    }

    /// Schedules a periodically recurring event at a future time.
    ///
    /// An error is returned if the specified time is not in the future of the
    /// current simulation time, or if the specified period is null.
    ///
    /// Events scheduled for the same time and targeting the same model are
    /// guaranteed to be processed according to the scheduling order.
    pub fn schedule_periodic_event<T>(
        &self,
        deadline: impl Deadline,
        period: Duration,
        event_id: &EventId<T>,
        arg: T,
    ) -> Result<(), SchedulingError>
    where
        T: Send + Clone + 'static,
    {
        self.0
            .schedule_periodic_event_from(deadline, period, event_id, arg, GLOBAL_ORIGIN_ID)
    }

    /// Schedules a cancellable, periodically recurring event at a future time
    /// and returns an event key.
    ///
    /// An error is returned if the specified time is not in the future of the
    /// current simulation time, or if the specified period is null.
    ///
    /// Events scheduled for the same time and targeting the same model are
    /// guaranteed to be processed according to the scheduling order.
    pub fn schedule_keyed_periodic_event<T>(
        &self,
        deadline: impl Deadline,
        period: Duration,
        event_id: &EventId<T>,
        arg: T,
    ) -> Result<EventKey, SchedulingError>
    where
        T: Send + Clone + 'static,
    {
        self.0
            .schedule_keyed_periodic_event_from(deadline, period, event_id, arg, GLOBAL_ORIGIN_ID)
    }

    /// Schedules a query at a future time.
    ///
    /// An error is returned if the specified time is not in the future of the
    /// current simulation.
    ///
    /// Queries scheduled for the same time and targeting the same model are
    /// guaranteed to be processed according to the scheduling order.
    pub fn schedule_query<T, R>(
        &self,
        deadline: impl Deadline,
        query_id: &QueryId<T, R>,
        arg: T,
    ) -> Result<ReplyReader<R>, SchedulingError>
    where
        T: Send + Clone + 'static,
        R: Send + 'static,
    {
        self.0
            .schedule_query_from(deadline, query_id, arg, GLOBAL_ORIGIN_ID)
    }

    /// Requests the simulation to be interrupted at the earliest opportunity.
    ///
    /// If a stepping method such as
    /// [`Simulation::step`](crate::simulation::Simulation::step) or
    /// [`Simulation::run`](crate::simulation::Simulation::run) is concurrently
    /// being executed, this will cause such method to return before it steps to
    /// next scheduler deadline or simulation tick (if any) with
    /// [`ExecutionError::Halted`](crate::simulation::ExecutionError::Halted).
    ///
    /// Otherwise, this will cause the next call to a `Simulation::step*` or
    /// `Simulation::process*` method to return immediately with
    /// [`ExecutionError::Halted`](crate::simulation::ExecutionError::Halted).
    ///
    /// In all cases, once
    /// [`ExecutionError::Halted`](crate::simulation::ExecutionError::Halted) is
    /// returned, the simulation can be resumed at any moment with another call
    /// to a stepping method or a
    /// [`Simulation::process_*`](crate::simulation::Simulation::process_event)
    /// methods.
    pub fn halt(&self) {
        self.0.halt()
    }
}

/// An error returned when the scheduled time or the repetition period are
/// invalid.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[non_exhaustive]
pub enum SchedulingError {
    /// The scheduled time does not lie in the future of the current simulation
    /// time.
    InvalidScheduledTime,
    /// The repetition period is zero.
    NullRepetitionPeriod,
}

impl fmt::Display for SchedulingError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidScheduledTime => write!(
                fmt,
                "the scheduled time should be in the future of the current simulation time"
            ),
            Self::NullRepetitionPeriod => write!(fmt, "the repetition period cannot be zero"),
        }
    }
}

impl Error for SchedulingError {}

/// Alias for the scheduler queue type.
///
/// Why use both time and origin as the key? The short answer is that this
/// allows the preservation of the relative ordering of events which have the
/// same origin (where the origin is either a model instance or the global
/// scheduler). The preservation of this ordering is implemented by the event
/// loop, which aggregate events with the same origin into single sequential
/// futures, thus ensuring that they are not executed concurrently.
pub(crate) type SchedulerQueue = PriorityQueue<SchedulerKey, QueueItem>;

pub(crate) type SchedulerKey = (MonotonicTime, usize);

/// Scheduler state shared by all scheduler handles and the simulation.
pub(crate) struct SchedulerState {
    pub(super) scheduler_queue: Arc<Mutex<SchedulerQueue>>,
    pub(super) local_buffers: LocalScheduleBuffers,
    executor_id: usize,
    prefer_prepared_fast_events: bool,
    origin_seqs: OnceLock<Box<[CachePadded<AtomicU64>]>>,
    global_origin_seq: CachePadded<AtomicU64>,
    time_quantum_ns: AtomicU64,
}

impl SchedulerState {
    pub(crate) fn new(
        scheduler_queue: Arc<Mutex<SchedulerQueue>>,
        executor_id: usize,
        num_workers: usize,
    ) -> Self {
        let num_workers = num_workers.max(1);
        let prefer_prepared_fast_events = num_workers > 1;
        let buffers = (0..num_workers)
            .map(|_| CachePadded::new(UnsafeCell::new(Vec::new())))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            scheduler_queue,
            local_buffers: LocalScheduleBuffers { buffers },
            executor_id,
            prefer_prepared_fast_events,
            origin_seqs: OnceLock::new(),
            global_origin_seq: CachePadded::new(AtomicU64::new(0)),
            time_quantum_ns: AtomicU64::new(0),
        }
    }

    pub(super) fn flush_local(&self, scheduler_queue: &mut SchedulerQueue) {
        for buf in self.local_buffers.buffers.iter() {
            let buf = unsafe { &mut *buf.get() };
            for item in buf.drain(..) {
                scheduler_queue.insert_with_epoch((item.time, item.origin_id), item.item, item.seq);
            }
        }
    }

    pub(crate) fn init_origin_seqs(&self, model_origin_count: usize) {
        let model_origin_count = model_origin_count.max(1);
        let origin_seqs = (0..model_origin_count)
            .map(|_| CachePadded::new(AtomicU64::new(0)))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        self.origin_seqs
            .set(origin_seqs)
            .expect("origin sequences already initialized");
    }

    pub(crate) fn reset_origin_seqs(&self) {
        self.global_origin_seq.store(0, Ordering::Relaxed);
        if let Some(origin_seqs) = self.origin_seqs.get() {
            for seq in origin_seqs.iter() {
                seq.store(0, Ordering::Relaxed);
            }
        }
    }

    pub(super) fn next_seq(&self, origin_id: usize) -> u64 {
        if origin_id == GLOBAL_ORIGIN_ID {
            let seq = self.global_origin_seq.fetch_add(1, Ordering::Relaxed);
            assert_ne!(seq, u64::MAX, "origin sequence counter overflow");
            return seq;
        }

        let origin_seqs = self
            .origin_seqs
            .get()
            .expect("origin sequences not initialized");

        debug_assert!(origin_id < origin_seqs.len());
        let seq = origin_seqs[origin_id].fetch_add(1, Ordering::Relaxed);
        assert_ne!(seq, u64::MAX, "origin sequence counter overflow");
        seq
    }

    pub(crate) fn set_time_quantum_ns(&self, quantum_ns: u64) {
        self.time_quantum_ns.store(quantum_ns, Ordering::Relaxed);
    }

    pub(super) fn quantize_time(&self, time: MonotonicTime) -> MonotonicTime {
        let quantum_ns = self.time_quantum_ns.load(Ordering::Relaxed);
        if quantum_ns == 0 {
            return time;
        }

        let since_epoch = time.duration_since(MonotonicTime::EPOCH);
        let q = quantum_ns as u128;
        let rem = (since_epoch.as_nanos() % q) as u64;
        if rem == 0 {
            return time;
        }

        time + Duration::from_nanos(quantum_ns - rem)
    }

    #[inline]
    fn prefers_prepared_fast_events(&self) -> bool {
        self.prefer_prepared_fast_events
    }

    fn local_worker_id_if_owned(&self) -> Option<usize> {
        if let (Some(worker_id), Some(executor_id)) =
            (crate::executor::worker_id(), crate::executor::executor_id())
        {
            if executor_id == self.executor_id {
                return Some(worker_id);
            }
        }

        None
    }
}

pub(super) struct LocalScheduleBuffers {
    pub(super) buffers: Box<[CachePadded<UnsafeCell<Vec<LocalScheduleItem>>>]>,
}

// Safety: each buffer is written to exclusively by its owning worker thread and
// drained only when the executor is quiescent.
unsafe impl Sync for LocalScheduleBuffers {}

impl LocalScheduleBuffers {
    fn push(&self, worker_id: usize, item: LocalScheduleItem) {
        debug_assert!(worker_id < self.buffers.len());

        unsafe { &mut *self.buffers[worker_id].get() }.push(item);
    }
}

pub(super) struct LocalScheduleItem {
    pub(super) time: MonotonicTime,
    pub(super) origin_id: usize,
    pub(super) seq: u64,
    pub(super) item: QueueItem,
}

struct FastBatchEvent<M, F, S, T>
where
    M: Model,
    F: for<'a> InputFn<'a, M, T, S> + Clone + Send + Sync + 'static,
    S: Send + Sync + 'static,
    T: Serialize + Send + 'static,
{
    event_id: EventIdErased,
    sender: Sender<M>,
    func: F,
    arg: Option<T>,
    _phantom: PhantomData<S>,
}

impl<M, F, S, T> FastBatchEvent<M, F, S, T>
where
    M: Model,
    F: for<'a> InputFn<'a, M, T, S> + Clone + Send + Sync + 'static,
    S: Send + Sync + 'static,
    T: Serialize + Send + 'static,
{
    fn new(event_id: EventIdErased, sender: Sender<M>, func: F, arg: T) -> Self {
        Self {
            event_id,
            sender,
            func,
            arg: Some(arg),
            _phantom: PhantomData,
        }
    }
}

impl<M, F, S, T> FastScheduledEvent for FastBatchEvent<M, F, S, T>
where
    M: Model,
    F: for<'a> InputFn<'a, M, T, S> + Clone + Send + Sync + 'static,
    S: Send + Sync + 'static,
    T: Serialize + Send + 'static,
{
    fn event_id(&self) -> EventIdErased {
        self.event_id
    }

    fn into_future(self: Box<Self>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let Self {
            sender,
            func,
            arg,
            _phantom: _,
            ..
        } = *self;

        let arg = arg.expect("fast scheduled event consumed more than once");
        let fut = async move {
            // Ignore send errors (e.g. no recipient), like the standard scheduler path.
            let _ = sender
                .send(
                    move |model: &mut M,
                          scheduler,
                          env,
                          recycle_box: RecycleBox<()>|
                          -> RecycleBox<dyn Future<Output = ()> + Send + '_> {
                        let fut = func.call(model, arg, scheduler, env);
                        coerce_box!(RecycleBox::recycle(recycle_box, fut))
                    },
                )
                .await;
        };

        Box::pin(fut)
    }

    fn to_serializable_parts(
        &self,
    ) -> Result<(EventIdErased, Vec<u8>, Option<Duration>, Option<EventKey>), ExecutionError> {
        let arg = self
            .arg
            .as_ref()
            .expect("fast scheduled event consumed before serialization");

        let data =
            bincode::serde::encode_to_vec(arg, serialization_config()).map_err(|e| {
                SaveError::ArgumentSerializationError {
                    type_name: type_name::<T>(),
                    cause: Box::new(e),
                }
            })?;

        Ok((self.event_id, data, None, None))
    }
}

impl<M, F, S, T> fmt::Debug for FastBatchEvent<M, F, S, T>
where
    M: Model,
    F: for<'a> InputFn<'a, M, T, S> + Clone + Send + Sync + 'static,
    S: Send + Sync + 'static,
    T: Serialize + Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FastBatchEvent")
            .field("event_id", &self.event_id.0)
            .field("arg_type", &type_name::<T>())
            .finish_non_exhaustive()
    }
}

struct FastBatchPreparedEvent<T>
where
    T: Serialize + Send + 'static,
{
    event_id: EventIdErased,
    arg: Option<T>,
    fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<T> FastBatchPreparedEvent<T>
where
    T: Serialize + Send + Clone + 'static,
{
    fn new<M, F, S>(event_id: EventIdErased, sender: Sender<M>, func: F, arg: T) -> Self
    where
        M: Model,
        F: for<'a> InputFn<'a, M, T, S> + Clone + Send + Sync + 'static,
        S: Send + Sync + 'static,
    {
        let serializable_arg = arg.clone();

        let fut = async move {
            // Ignore send errors (e.g. no recipient), like the standard scheduler path.
            let _ = sender
                .send(
                    move |model: &mut M,
                          scheduler,
                          env,
                          recycle_box: RecycleBox<()>|
                          -> RecycleBox<dyn Future<Output = ()> + Send + '_> {
                        let fut = func.call(model, arg, scheduler, env);
                        coerce_box!(RecycleBox::recycle(recycle_box, fut))
                    },
                )
                .await;
        };

        Self {
            event_id,
            arg: Some(serializable_arg),
            fut: Some(Box::pin(fut)),
        }
    }
}

impl<T> FastScheduledEvent for FastBatchPreparedEvent<T>
where
    T: Serialize + Send + 'static,
{
    fn event_id(&self) -> EventIdErased {
        self.event_id
    }

    fn into_future(mut self: Box<Self>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.fut
            .take()
            .expect("fast scheduled event consumed more than once")
    }

    fn to_serializable_parts(
        &self,
    ) -> Result<(EventIdErased, Vec<u8>, Option<Duration>, Option<EventKey>), ExecutionError> {
        let arg = self
            .arg
            .as_ref()
            .expect("fast scheduled event consumed before serialization");

        let data =
            bincode::serde::encode_to_vec(arg, serialization_config()).map_err(|e| {
                SaveError::ArgumentSerializationError {
                    type_name: type_name::<T>(),
                    cause: Box::new(e),
                }
            })?;

        Ok((self.event_id, data, None, None))
    }
}

impl<T> fmt::Debug for FastBatchPreparedEvent<T>
where
    T: Serialize + Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FastBatchPreparedEvent")
            .field("event_id", &self.event_id.0)
            .field("arg_type", &type_name::<T>())
            .finish_non_exhaustive()
    }
}

/// Internal implementation of the global scheduler.
#[derive(Clone)]
pub(crate) struct GlobalScheduler {
    state: Arc<SchedulerState>,
    time: AtomicTimeReader,
    is_halted: Arc<AtomicBool>,
}

impl GlobalScheduler {
    pub(crate) fn new(
        state: Arc<SchedulerState>,
        time: AtomicTimeReader,
        is_halted: Arc<AtomicBool>,
    ) -> Self {
        Self {
            state,
            time,
            is_halted,
        }
    }

    /// Returns the current simulation time.
    pub(crate) fn time(&self) -> MonotonicTime {
        // We use `read` rather than `try_read` because the scheduler can be
        // sent to another thread than the simulator's and could thus
        // potentially see a torn read if the simulator increments time
        // concurrently. The chances of this happening are very small since
        // simulation time is not changed frequently.
        self.time.read()
    }

    /// Returns a clock reader.
    pub(crate) fn clock_reader(&self) -> ClockReader {
        ClockReader::from_atomic_time_reader(&self.time)
    }

    /// Schedules a readily-built event identified by its origin at a future
    /// time.
    #[cfg(feature = "server")]
    pub(crate) fn schedule_from(
        &self,
        deadline: impl Deadline,
        event: Event,
        origin_id: usize,
    ) -> Result<(), SchedulingError> {
        self.schedule_item_from(deadline, QueueItem::Event(Box::new(event)), origin_id)
    }

    /// Schedules an event identified by its identifier and origin at a future
    /// time.
    pub(crate) fn schedule_event_from<T>(
        &self,
        deadline: impl Deadline,
        event_id: &EventId<T>,
        arg: T,
        origin_id: usize,
    ) -> Result<(), SchedulingError>
    where
        T: Send + Clone + 'static,
    {
        let event = Event::new(event_id, arg);
        self.schedule_item_from(deadline, QueueItem::Event(Box::new(event)), origin_id)
    }

    /// Schedules an event identified by its identifier and origin at a future
    /// time using a typed dispatch fast path.
    pub(crate) fn schedule_event_fast_from<M, F, T, S>(
        &self,
        deadline: impl Deadline,
        event_id: &EventId<T>,
        func: F,
        arg: T,
        address: impl Into<Address<M>>,
        origin_id: usize,
    ) -> Result<(), SchedulingError>
    where
        M: Model,
        F: for<'a> InputFn<'a, M, T, S> + Clone + Send + Sync + 'static,
        T: Serialize + Send + Clone + 'static,
        S: Send + Sync + 'static,
    {
        let sender = address.into().0;
        let event_id = EventIdErased::from(event_id);
        let event: Box<dyn FastScheduledEvent> = if self.state.prefers_prepared_fast_events() {
            Box::new(FastBatchPreparedEvent::new(event_id, sender, func, arg))
        } else {
            Box::new(FastBatchEvent::new(event_id, sender, func, arg))
        };
        let item = QueueItem::FastEvent(FastEvent::new(event));

        self.schedule_item_from(deadline, item, origin_id)
    }

    /// Schedules multiple events identified by their origin at future times.
    ///
    /// This method guarantees that if an error is returned, no event is
    /// scheduled.
    pub(crate) fn schedule_event_batch_from<T, D>(
        &self,
        deadlines_and_args: Vec<(D, T)>,
        event_id: &EventId<T>,
        origin_id: usize,
    ) -> Result<(), SchedulingError>
    where
        T: Send + Clone + 'static,
        D: Deadline + Copy,
    {
        if deadlines_and_args.is_empty() {
            return Ok(());
        }

        if let Some(worker_id) = self.state.local_worker_id_if_owned() {
            let now = self.time();
            for (deadline, _) in &deadlines_and_args {
                let time = (*deadline).into_time(now);
                if now >= time {
                    return Err(SchedulingError::InvalidScheduledTime);
                }
            }

            for (deadline, arg) in deadlines_and_args {
                let time = self.state.quantize_time(deadline.into_time(now));
                let seq = self.state.next_seq(origin_id);
                self.state.local_buffers.push(
                    worker_id,
                    LocalScheduleItem {
                        time,
                        origin_id,
                        seq,
                        item: QueueItem::Event(Box::new(Event::new(event_id, arg))),
                    },
                );
            }

            return Ok(());
        }

        // The scheduler queue must always be locked when reading the time (see
        // `schedule_from`).
        let mut scheduler_queue = self.state.scheduler_queue.lock().unwrap();
        let now = self.time();

        for (deadline, _) in &deadlines_and_args {
            let time = (*deadline).into_time(now);
            if now >= time {
                return Err(SchedulingError::InvalidScheduledTime);
            }
        }

        for (deadline, arg) in deadlines_and_args {
            let time = self.state.quantize_time(deadline.into_time(now));
            let seq = self.state.next_seq(origin_id);
            scheduler_queue.insert_with_epoch(
                (time, origin_id),
                QueueItem::Event(Box::new(Event::new(event_id, arg))),
                seq,
            );
        }

        Ok(())
    }

    /// Schedules multiple events at future times using a typed fast path.
    ///
    /// This method keeps a serializable representation for save/restore while
    /// bypassing type-erased dispatch during normal execution.
    pub(crate) fn schedule_event_batch_fast_from<M, F, T, S, D>(
        &self,
        deadlines_and_args: Vec<(D, T)>,
        event_id: &EventId<T>,
        func: F,
        address: impl Into<Address<M>>,
        origin_id: usize,
    ) -> Result<(), SchedulingError>
    where
        M: Model,
        F: for<'a> InputFn<'a, M, T, S> + Clone + Send + Sync + 'static,
        T: Serialize + Send + Clone + 'static,
        S: Send + Sync + 'static,
        D: Deadline + Copy,
    {
        if deadlines_and_args.is_empty() {
            return Ok(());
        }

        let sender = address.into().0;
        let event_id = EventIdErased::from(event_id);
        let use_prepared_fast_events = self.state.prefers_prepared_fast_events();

        if let Some(worker_id) = self.state.local_worker_id_if_owned() {
            let now = self.time();
            for (deadline, _) in &deadlines_and_args {
                let time = (*deadline).into_time(now);
                if now >= time {
                    return Err(SchedulingError::InvalidScheduledTime);
                }
            }

            if use_prepared_fast_events {
                for (deadline, arg) in deadlines_and_args {
                    let time = self.state.quantize_time(deadline.into_time(now));
                    let seq = self.state.next_seq(origin_id);
                    let item = QueueItem::FastEvent(FastEvent::new(Box::new(
                        FastBatchPreparedEvent::new(event_id, sender.clone(), func.clone(), arg),
                    )));

                    self.state.local_buffers.push(
                        worker_id,
                        LocalScheduleItem {
                            time,
                            origin_id,
                            seq,
                            item,
                        },
                    );
                }
            } else {
                for (deadline, arg) in deadlines_and_args {
                    let time = self.state.quantize_time(deadline.into_time(now));
                    let seq = self.state.next_seq(origin_id);
                    let item = QueueItem::FastEvent(FastEvent::new(Box::new(FastBatchEvent::new(
                        event_id,
                        sender.clone(),
                        func.clone(),
                        arg,
                    ))));

                    self.state.local_buffers.push(
                        worker_id,
                        LocalScheduleItem {
                            time,
                            origin_id,
                            seq,
                            item,
                        },
                    );
                }
            }

            return Ok(());
        }

        // The scheduler queue must always be locked when reading the time (see
        // `schedule_from`).
        let mut scheduler_queue = self.state.scheduler_queue.lock().unwrap();
        let now = self.time();

        for (deadline, _) in &deadlines_and_args {
            let time = (*deadline).into_time(now);
            if now >= time {
                return Err(SchedulingError::InvalidScheduledTime);
            }
        }

        if use_prepared_fast_events {
            for (deadline, arg) in deadlines_and_args {
                let time = self.state.quantize_time(deadline.into_time(now));
                let seq = self.state.next_seq(origin_id);
                let item = QueueItem::FastEvent(FastEvent::new(Box::new(
                    FastBatchPreparedEvent::new(event_id, sender.clone(), func.clone(), arg),
                )));
                scheduler_queue.insert_with_epoch((time, origin_id), item, seq);
            }
        } else {
            for (deadline, arg) in deadlines_and_args {
                let time = self.state.quantize_time(deadline.into_time(now));
                let seq = self.state.next_seq(origin_id);
                let item = QueueItem::FastEvent(FastEvent::new(Box::new(FastBatchEvent::new(
                    event_id,
                    sender.clone(),
                    func.clone(),
                    arg,
                ))));
                scheduler_queue.insert_with_epoch((time, origin_id), item, seq);
            }
        }

        Ok(())
    }

    /// Schedules a cancellable event identified by its identifier and origin at
    /// a future time and returns an event key.
    pub(crate) fn schedule_keyed_event_from<T>(
        &self,
        deadline: impl Deadline,
        event_id: &EventId<T>,
        arg: T,
        origin_id: usize,
    ) -> Result<EventKey, SchedulingError>
    where
        T: Send + Clone + 'static,
    {
        let event_key = EventKey::new();
        let event = Event::new(event_id, arg).with_key(event_key.clone());

        self.schedule_item_from(deadline, QueueItem::Event(Box::new(event)), origin_id)?;

        Ok(event_key)
    }

    /// Schedules a periodically recurring event identified by its id and origin
    /// at a future time.
    pub(crate) fn schedule_periodic_event_from<T>(
        &self,
        deadline: impl Deadline,
        period: Duration,
        event_id: &EventId<T>,
        arg: T,
        origin_id: usize,
    ) -> Result<(), SchedulingError>
    where
        T: Send + Clone + 'static,
    {
        if period.is_zero() {
            return Err(SchedulingError::NullRepetitionPeriod);
        }

        let event = Event::new(event_id, arg).with_period(period);
        self.schedule_item_from(deadline, QueueItem::Event(Box::new(event)), origin_id)
    }

    /// Schedules a cancellable, periodically recurring event identified by its
    /// id and origin at a future time and returns an event key.
    pub(crate) fn schedule_keyed_periodic_event_from<T>(
        &self,
        deadline: impl Deadline,
        period: Duration,
        event_id: &EventId<T>,
        arg: T,
        origin_id: usize,
    ) -> Result<EventKey, SchedulingError>
    where
        T: Send + Clone + 'static,
    {
        if period.is_zero() {
            return Err(SchedulingError::NullRepetitionPeriod);
        }
        let event_key = EventKey::new();

        let event = Event::new(event_id, arg)
            .with_period(period)
            .with_key(event_key.clone());

        self.schedule_item_from(deadline, QueueItem::Event(Box::new(event)), origin_id)?;

        Ok(event_key)
    }

    /// Schedules a query identified by its id and origin at a future time.
    pub(crate) fn schedule_query_from<T, R>(
        &self,
        deadline: impl Deadline,
        query_id: &QueryId<T, R>,
        arg: T,
        origin_id: usize,
    ) -> Result<ReplyReader<R>, SchedulingError>
    where
        T: Send + Clone + 'static,
        R: Send + 'static,
    {
        let (query, rx) = Query::new(query_id, arg);
        self.schedule_item_from(deadline, QueueItem::Query(Box::new(query)), origin_id)?;

        Ok(rx)
    }

    /// Requests the simulation to return as early as possible upon the
    /// completion of the current time step.
    pub(crate) fn halt(&self) {
        self.is_halted.store(true, Ordering::Relaxed);
    }

    fn schedule_item_from(
        &self,
        deadline: impl Deadline,
        item: QueueItem,
        origin_id: usize,
    ) -> Result<(), SchedulingError> {
        if let Some(worker_id) = self.state.local_worker_id_if_owned() {
            let now = self.time();
            let time = deadline.into_time(now);
            if now >= time {
                return Err(SchedulingError::InvalidScheduledTime);
            }
            let time = self.state.quantize_time(time);
            let seq = self.state.next_seq(origin_id);

            self.state.local_buffers.push(
                worker_id,
                LocalScheduleItem {
                    time,
                    origin_id,
                    seq,
                    item,
                },
            );
            return Ok(());
        }

        // The scheduler queue must always be locked when reading the time,
        // otherwise the following race could occur:
        // 1) this method reads the time and concludes that it is not too late to
        //    schedule the action,
        // 2) the `Simulation` object takes the lock, increments simulation time and
        //    runs the simulation step,
        // 3) this method takes the lock and schedules the now-outdated action.
        let mut scheduler_queue = self.state.scheduler_queue.lock().unwrap();

        let now = self.time();
        let time = deadline.into_time(now);
        if now >= time {
            return Err(SchedulingError::InvalidScheduledTime);
        }
        let time = self.state.quantize_time(time);

        let seq = self.state.next_seq(origin_id);
        scheduler_queue.insert_with_epoch((time, origin_id), item, seq);

        Ok(())
    }
}

impl fmt::Debug for GlobalScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlobalScheduler")
            .field("time", &self.time())
            .field("is_halted", &self.is_halted.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, not(nexosim_loom)))]
impl GlobalScheduler {
    /// Creates a dummy scheduler for testing purposes.
    pub(crate) fn new_dummy() -> Self {
        let dummy_priority_queue = Arc::new(Mutex::new(SchedulerQueue::new()));
        let dummy_state = Arc::new(SchedulerState::new(dummy_priority_queue, 0, 1));
        let dummy_time = SyncCell::new(TearableAtomicTime::new(MonotonicTime::EPOCH)).reader();
        let dummy_running = Arc::new(AtomicBool::new(false));
        GlobalScheduler::new(dummy_state, dummy_time, dummy_running)
    }
}
