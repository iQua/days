//! The core library for discrete-event simulation using stackless coroutines.

use std::{
    cell::{Cell, RefCell},
    cmp::Ordering,
    collections::BinaryHeap,
    fmt::{Display, Formatter},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{self, Context, Poll},
};

use log::warn;

// simple time type
pub type Time = f64;

/// Performs a single simulation run.
///
pub fn simulation<G, F>(shared: G, main: F) -> G
where
    F: FnOnce(SimContext<G>) -> Process<G>,
{
    // create a fresh scheduler and a handle to it
    let sched = Scheduler::new(shared);
    let sim = SimContext { handle: &sched };

    // construct a custom context to pass into the poll()-method
    let event_waker = StateEventWaker::new(sim);
    let waker = unsafe { event_waker.as_waker() };
    let mut cx = Context::from_waker(&waker);

    // evaluate the passed function for the main process and schedule it
    let root = main(sim);
    sched.schedule(root.clone());

    // pop processes until empty or the main process terminates
    while let Some(process) = sched.next_event() {
        warn!("while loop start - {:.3}", sched.now.get());
        if process.poll(&mut cx).is_ready() && process == root {
            break;
        }
        warn!("while loop end - {:.3}", sched.now.get());
    }

    // clear the scheduler before it is dropped to break the dependency
    // cycle between the processes and the scheduler that we made possible
    // by using a raw pointer for the simulation context
    sched.clear();

    // return the global data
    sched.shared
}

// priority queue of time-process-pairs using time as the key
type EventQ<'s, G> = BinaryHeap<NextEvent<'s, G>>;

/// The (private) scheduler for processes.
struct Scheduler<'s, G> {
    /// The current simulation time.
    now: Cell<Time>,

    /// The event-calendar organized chronologically.
    calendar: RefCell<EventQ<'s, G>>,

    /// The currently active process.
    active: RefCell<Process<'s, G>>,

    /// Globally-accessible data.
    shared: G,
}

impl<'s, G> Scheduler<'s, G> {
    /// Creates a new scheduler.
    #[inline]
    fn new(shared: G) -> Self {
        Self {
            now: Cell::default(),
            calendar: RefCell::default(),
            active: RefCell::default(),
            shared,
        }
    }

    /// Clears the scheduler, dropping all of the contained processes.
    fn clear(&self) {
        // this is a surprisingly delicate operation because any of the
        // processes that are still present in the event-queue may re- or
        // deschedule processes upon drop, with the consequence of writing
        // into the event-queue while it's being cleared (and therefore in an
        // inconsistent state); swapping out the event queue with an empty one
        // circumvents this issue completely, since clearing and dropping now
        // behave like atomic operations

        // atomically replace the event queue with an empty one
        let mut events = self.calendar.replace(EventQ::default());

        // now clear the old one; the calendar may not be empty after this
        // operation if any destructors scheduled new processes
        events.clear();

        // replace the contents of the calendar with the old (empty) event queue
        // and drop the temporary one as an optimization
        self.calendar.replace(events);

        // the user has to be actively malicious by scheduling new processes
        // upon dropping them to make it this far and we don't have all day
        assert!(
            self.calendar.borrow().is_empty(),
            "Please don't activate new processes on drop."
        );

        // replace the active process with an already terminated one
        self.active.replace(Process::default());
    }

    /// Schedules a process at the current simulation time.
    #[inline]
    fn schedule(&self, process: Process<'s, G>) {
        warn!("schedule - {:.3}", self.now.get());
        self.schedule_in(Time::default(), process);
    }

    /// Schedules a process at a later simulation time.
    #[inline]
    fn schedule_in(&self, dt: Time, process: Process<'s, G>) {
        self.calendar
            .borrow_mut()
            .push(NextEvent(self.now.get() + dt, process));
        warn!(
            "schedule_in: pushed back to EventQ with wake-up time = {:.3}; queue length = {:.3}",
            self.now.get() + dt,
            self.calendar.borrow().len()
        );
        for event in self.calendar.borrow().iter() {
            warn!("schedule_in: EventQ = {:.3}", event);
        }
    }

    /// Removes the process with the next event time from the calendar and
    /// activates it.
    #[inline]
    fn next_event(&self) -> Option<Process<'s, G>> {
        let NextEvent(now, process) = self.calendar.borrow_mut().pop()?;
        self.now.set(now);
        warn!(
            "next_event - popped out from EventQ with sim time = {:.3}; queue length = {:?}",
            now,
            self.calendar.borrow().len()
        );
        self.active.replace(process.clone());
        Some(process)
    }
}

/// A light-weight handle to the scheduler.
pub struct SimContext<'s, G = ()> {
    handle: *const Scheduler<'s, G>,
}

// this allows the creation of copies
impl<'s, G> Clone for SimContext<'s, G> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

// this allows moving the context without invalidating it (copy semantics)
impl<'s, G> Copy for SimContext<'s, G> {}

impl<'s, G> SimContext<'s, G> {
    /// Returns a (reference-counted) copy of the currently active process.
    #[inline]
    pub fn active(&self) -> Process<'s, G> {
        self.sched().active.borrow().clone()
    }

    /// Activates a new process with the given future.
    #[inline]
    pub fn activate<F>(&self, f: F)
    where
        F: Future<Output = ()> + 's,
    {
        warn!(
            "New coroutine called sim::activate() - current time = {:.3}",
            self.now()
        );
        self.reactivate(Process::new(*self, f));
    }

    /// Reactivates a process that has been suspended with wait().
    #[inline]
    pub fn reactivate(&self, process: Process<'s, G>) {
        warn!("reactivate - {:.3}", self.now());
        assert!(process.0.borrow().state.is_some());
        self.sched().schedule(process);
    }

    /// Reactivates the currently active process after some time has passed.
    #[inline]
    pub async fn advance(&self, dt: Time) {
        warn!("advance - {:.3}", dt);
        self.sched().schedule_in(dt, self.active());
        sleep().await
    }

    /// Returns the current simulation time.
    #[inline]
    pub fn now(&self) -> Time {
        self.sched().now.get()
    }

    /// Returns a shared reference to the global data.
    #[inline]
    pub fn shared(&self) -> &G {
        &self.sched().shared
    }

    /// Private function to get a safe reference to the scheduler.
    #[inline]
    fn sched(&self) -> &Scheduler<'s, G> {
        // This is safe if no simulation context escapes from the closure
        // passed to simulation() which is enforced through the use of
        // higher-order trait-bounds.
        unsafe { &*self.handle }
    }
}

/// A bare-bone process type that can also be used as a waker.
pub struct Process<'s, G>(Arc<RefCell<Inner<'s, G>>>);

/// The private details of the [`Process`](struct.Process.html) type.
struct Inner<'s, G> {
    /// The simulation context needed to implement the `Waker` interface.
    context: SimContext<'s, G>,
    /// `Some` [`Future`] associated with this process or `None` if it has been
    /// terminated externally.
    ///
    /// [`Future`]: https://doc.rust-lang.org/std/future/trait.Future.html
    state: Option<Pin<Box<dyn Future<Output = ()> + 's>>>,
}

impl<'s, G> Process<'s, G> {
    /// Combines a future and a simulation context to a process.
    #[inline]
    pub fn new(sim: SimContext<'s, G>, fut: impl Future<Output = ()> + 's) -> Self {
        Process(Arc::new(RefCell::new(Inner {
            context: sim,
            state: Some(Box::pin(fut)),
        })))
    }

    /// Releases the [`Future`] contained in this process.
    ///
    /// This will also execute all of the destructors for the local variables
    /// initialized by this process.
    ///
    /// [`Future`]: https://doc.rust-lang.org/std/future/trait.Future.html
    #[inline]
    pub fn terminate(&self) {
        self.0.borrow_mut().state.take();
    }

    /// Returns a `Waker` for this process.
    #[inline]
    pub fn waker(self) -> task::Waker {
        unsafe { task::Waker::from_raw(self.raw_waker()) }
    }

    /// Private function for polling the process.
    #[inline]
    fn poll(&self, cx: &mut Context) -> Poll<()> {
        warn!("poll - start at {}", self.0.borrow().context.now());
        if let Some(fut) = self.0.borrow_mut().state.as_mut() {
            fut.as_mut().poll(cx)
        } else {
            Poll::Ready(())
        }
    }
}

// Creates a terminated process.
impl<'s, G> Default for Process<'s, G> {
    #[inline]
    fn default() -> Self {
        Process(Arc::new(RefCell::new(Inner {
            context: SimContext {
                handle: std::ptr::null(),
            },
            state: None,
        })))
    }
}

// Increases the reference counter of this process.
impl<'s, G> Clone for Process<'s, G> {
    #[inline]
    fn clone(&self) -> Self {
        Process(self.0.clone())
    }
}

// allows processes to be compared for equality
impl<G> PartialEq for Process<'_, G> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

// marks the equality-relation as total
impl<G> Eq for Process<'_, G> {}

/// Time-process-pair that has a total order defined based on the time.
struct NextEvent<'p, G>(Time, Process<'p, G>);

impl<G> PartialEq for NextEvent<'_, G> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<'s, G> Display for NextEvent<'s, G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NextEvent").field("time", &self.0).finish()
    }
}

impl<G> Eq for NextEvent<'_, G> {}

impl<G> PartialOrd for NextEvent<'_, G> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<G> Ord for NextEvent<'_, G> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .partial_cmp(&other.0)
            .expect("illegal event time NaN")
            .reverse()
    }
}

/* **************************** specialized waker *************************** */

impl<'s, G> Process<'s, G> {
    /// Virtual function table for the waker.
    const VTABLE: task::RawWakerVTable =
        task::RawWakerVTable::new(Self::clone, Self::wake, Self::wake_by_ref, Self::drop);

    /// Constructs a raw waker from a simulation context and a process.
    #[inline]
    fn raw_waker(self) -> task::RawWaker {
        task::RawWaker::new(Arc::into_raw(self.0) as *const (), &Self::VTABLE)
    }

    unsafe fn clone(this: *const ()) -> task::RawWaker {
        let waker = Arc::from_raw(this as *const RefCell<Inner<G>>);

        // increase the reference counter once
        let _ = Arc::into_raw(waker.clone());

        // this is technically unsafe because Wakers are Send + Sync and so this
        // call might be executed from a different thread, creating a data race
        // hazard; we are using Arc<T> rather than Rc<T> to guard against this
        // hazard.
        task::RawWaker::new(Arc::into_raw(waker) as *const (), &Self::VTABLE)
    }

    unsafe fn wake(this: *const ()) {
        let waker = Arc::from_raw(this as *const RefCell<Inner<G>>);

        // this can happen if a synchronization structure forgets to clean
        // up registered Waker objects on destruct; this would lead to
        // hard-to-diagnose bugs if we were to ignore it
        assert!(
            waker.borrow().state.is_some(),
            "Attempted to wake a terminated process."
        );

        warn!("wake - {:.3}", waker.borrow().context.now());
        // this is technically unsafe because Wakers are Send + Sync and so this
        // call might be executed from a different thread, creating a data race
        // hazard; we are using Arc<T> rather than Rc<T> to guard against this
        // hazard.
        let sim = waker.borrow().context;
        sim.reactivate(Process(waker));
    }

    unsafe fn wake_by_ref(this: *const ()) {
        let waker = Arc::from_raw(this as *const RefCell<Inner<G>>);

        // keep the waker alive
        let _ = Arc::into_raw(waker.clone());

        // this can happen if a synchronization structure forgets to clean
        // up registered Waker objects on destruct; this would lead to
        // hard-to-diagnose bugs if we were to ignore it
        assert!(
            waker.borrow().state.is_some(),
            "Attempted to wake a terminated process."
        );

        // this is technically unsafe because Wakers are Send + Sync and so this
        // call might be executed from a different thread, creating a data race
        // hazard; we leave preventing this as an exercise to the reader!
        let sim = waker.borrow().context;
        sim.reactivate(Process(waker));
    }

    unsafe fn drop(this: *const ()) {
        // this is technically unsafe because Wakers are Send + Sync and so this
        // call might be executed from a different thread, creating a data race
        // hazard; we leave preventing this as an exercise to the reader!
        Arc::from_raw(this as *const RefCell<Inner<G>>);
    }
}

/// Complex waker that is used to implement state events.
///
/// This is the shallow version that is created on the stack of the function
/// running the event loop. It assumes that the stored process is the currently
/// active one and creates the deep version when it is cloned.
struct StateEventWaker<'s, G> {
    context: SimContext<'s, G>,
}

impl<'s, G> StateEventWaker<'s, G> {
    /// Virtual function table for the waker.
    const VTABLE: task::RawWakerVTable =
        task::RawWakerVTable::new(Self::clone, Self::wake, Self::wake_by_ref, Self::drop);

    /// Creates a new (shallow) waker using only the simulation context.
    #[inline]
    fn new(sim: SimContext<'s, G>) -> Self {
        StateEventWaker { context: sim }
    }

    /// Constructs a new waker using only a reference.
    ///
    /// This function is unsafe because it is up to the user to ensure that the
    /// waker doesn't outlive the reference.
    #[inline]
    unsafe fn as_waker(&self) -> task::Waker {
        task::Waker::from_raw(task::RawWaker::new(
            self as *const _ as *const (),
            &Self::VTABLE,
        ))
    }

    unsafe fn clone(this: *const ()) -> task::RawWaker {
        // return the currently active process as a raw waker
        (*(this as *const Self)).context.active().raw_waker()
    }

    unsafe fn wake(_this: *const ()) {
        // waking the active process can safely be ignored
    }

    unsafe fn wake_by_ref(_this: *const ()) {
        // waking the active process can safely be ignored
    }

    unsafe fn drop(_this: *const ()) {
        // memory is released in the main event loop
    }
}

// Specialized futures

/// Returns a future that unconditionally puts the calling process to sleep.
#[inline]
pub fn sleep() -> impl Future<Output = ()> {
    Sleep { ready: false }
}

/// Returns a future that can be awaited to produce the currently set waker.
#[inline]
pub fn waker() -> impl Future<Output = task::Waker> {
    Waker {}
}

/// Future that blocks on the first call and returns on the second one.
struct Sleep {
    ready: bool,
}

impl Future for Sleep {
    type Output = ();

    #[inline]
    fn poll(mut self: Pin<&mut Self>, _: &mut Context) -> Poll<Self::Output> {
        if self.ready {
            Poll::Ready(())
        } else {
            self.ready = true;
            Poll::Pending
        }
    }
}

/// Future that returns immediately with a cloned waker.
struct Waker;

impl Future for Waker {
    type Output = task::Waker;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        Poll::Ready(cx.waker().clone())
    }
}

// Statistical facilities

/// A simple collector for statistical data, inspired by SLX's random_variable.
#[derive(Clone, Debug)]
pub struct RandomVar {
    total: Cell<u32>,
    sum: Cell<f64>,
    sqr: Cell<f64>,
    min: Cell<f64>,
    max: Cell<f64>,
}

impl RandomVar {
    /// Creates a new random variable.
    #[inline]
    pub fn new() -> Self {
        RandomVar::default()
    }

    /// Resets all stored statistical data.
    pub fn clear(&self) {
        self.total.set(0);
        self.sum.set(0.0);
        self.sqr.set(0.0);
        self.min.set(f64::INFINITY);
        self.max.set(f64::NEG_INFINITY);
    }

    /// Adds another value to the statistical collection.
    pub fn tabulate<T: Into<f64>>(&self, val: T) {
        let val: f64 = val.into();

        self.total.set(self.total.get() + 1);
        self.sum.set(self.sum.get() + val);
        self.sqr.set(self.sqr.get() + val * val);

        if self.min.get() > val {
            self.min.set(val);
        }
        if self.max.get() < val {
            self.max.set(val);
        }
    }

    /// Combines the statistical collection of two random variables into one.
    pub fn merge(&self, other: &Self) {
        self.total.set(self.total.get() + other.total.get());
        self.sum.set(self.sum.get() + other.sum.get());
        self.sqr.set(self.sqr.get() + other.sqr.get());

        if self.min.get() > other.min.get() {
            self.min.set(other.min.get());
        }
        if self.max.get() < other.max.get() {
            self.max.set(other.max.get());
        }
    }
}

impl Default for RandomVar {
    fn default() -> Self {
        RandomVar {
            total: Cell::default(),
            sum: Cell::default(),
            sqr: Cell::default(),
            min: Cell::new(f64::INFINITY),
            max: Cell::new(f64::NEG_INFINITY),
        }
    }
}

impl Display for RandomVar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let total = self.total.get();
        let mean = self.sum.get() / f64::from(total);
        let variance = self.sqr.get() / f64::from(total) - mean * mean;
        let std_dev = variance.sqrt();

        f.debug_struct("RandomVar")
            .field("total", &total)
            .field("mean", &mean)
            .field("std_dev", &std_dev)
            .field("min", &self.min.get())
            .field("max", &self.max.get())
            .finish()
    }
}
