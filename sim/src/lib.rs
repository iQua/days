///! The core library for discrete-event simulation using stackless coroutines.
use std::{
    cell::{Cell, RefCell},
    cmp::Ordering,
    collections::{BinaryHeap, VecDeque},
    fmt::{Display, Formatter},
    future::Future,
    pin::Pin,
    rc::{Rc, Weak},
    sync::atomic::{AtomicBool, Ordering as AtomicOrdering},
    sync::Arc,
    task::{self, Context, Poll},
};

// simple time type
pub type Time = f64;

/// Performs a single simulation run.
///
/// Input is a function that takes a simulation context and returns the first
/// process. It would be better if the function only needed to return the future
/// needed to initialize the first process, but then the function signature gets
/// more complicated and is harder to explain in a paper.
///
/// But just in case you're wondering how to do it:
/// ```
/// # use sim::{SimContext, Process};
/// # use std::future::Future;
/// pub trait Active<'s,G> {
///   fn lifecycle(self, sim: SimContext<'s,G>) -> Process<'s,G>;
/// }
///
/// impl<'s,G,F,R> Active<'s,G> for F
/// where F: FnOnce(SimContext<'s,G>) -> R,
///       R: Future<Output = ()> + 's {
///   fn lifecycle(self, sim: SimContext<'s,G>) -> Process<'s,G> {
///     sim.process(self(sim))
///   }
/// }
/// ```
/// You may then change the signature to
/// ```
/// fn simulation<G>(shared: G, main: impl for<'s> Active<'s,G>) {}
/// ```
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
        if process.poll(&mut cx).is_ready() && process == root {
            break;
        }
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
        self.schedule_in(Time::default(), process);
    }

    /// Schedules a process at a later simulation time.
    #[inline]
    fn schedule_in(&self, dt: Time, process: Process<'s, G>) {
        self.calendar
            .borrow_mut()
            .push(NextEvent(self.now.get() + dt, process));
    }

    /// Removes the process with the next event time from the calendar and
    /// activates it.
    #[inline]
    fn next_event(&self) -> Option<Process<'s, G>> {
        let NextEvent(now, process) = self.calendar.borrow_mut().pop()?;
        self.now.set(now);
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
        self.reactivate(Process::new(*self, f));
    }

    /// Reactivates a process that has been suspended with wait().
    #[inline]
    pub fn reactivate(&self, process: Process<'s, G>) {
        assert!(process.0.borrow().state.is_some());
        self.sched().schedule(process);
    }

    /// Reactivates the currently active process after some time has passed.
    #[inline]
    pub async fn advance(&self, dt: Time) {
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
pub struct Process<'s, G>(Rc<RefCell<Inner<'s, G>>>);

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
        Process(Rc::new(RefCell::new(Inner {
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
        Process(Rc::new(RefCell::new(Inner {
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
        Rc::ptr_eq(&self.0, &other.0)
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
        task::RawWaker::new(Rc::into_raw(self.0) as *const (), &Self::VTABLE)
    }

    unsafe fn clone(this: *const ()) -> task::RawWaker {
        let waker = Rc::from_raw(this as *const RefCell<Inner<G>>);

        // increase the reference counter once
        Rc::into_raw(waker.clone());

        // this is technically unsafe because Wakers are Send + Sync and so this
        // call might be executed from a different thread, creating a data race
        // hazard; we leave preventing this as an exercise to the reader!
        task::RawWaker::new(Rc::into_raw(waker) as *const (), &Self::VTABLE)
    }

    unsafe fn wake(this: *const ()) {
        let waker = Rc::from_raw(this as *const RefCell<Inner<G>>);

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

    unsafe fn wake_by_ref(this: *const ()) {
        let waker = Rc::from_raw(this as *const RefCell<Inner<G>>);

        // keep the waker alive
        Rc::into_raw(waker.clone());

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
        Rc::from_raw(this as *const RefCell<Inner<G>>);
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
        (&*(this as *const Self)).context.active().raw_waker()
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

/* *************************** specialized futures ************************** */

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

// this is the end of the simulator core, the remainder is used in the examples

/* *********************** synchronization structures *********************** */

/// GPSS-inspired facility that houses one exclusive resource to be used by
/// arbitrary many processes.
pub struct Facility {
    /// A queue of waiting-to-be-activated processes.
    queue: RefCell<VecDeque<task::Waker>>,
    /// A boolean indicating whether the facility is in use or not.
    in_use: Cell<bool>,
}

impl Facility {
    /// Creates a new facility.
    pub fn new() -> Self {
        Facility {
            queue: RefCell::new(VecDeque::new()),
            in_use: Cell::new(false),
        }
    }

    /// Attempts to seize the facility, blocking until it's possible.
    pub async fn seize(&self) {
        if self.in_use.replace(true) {
            self.queue.borrow_mut().push_back(Waker.await);
            sleep().await;
        }
    }

    /// Releases the facility and activates the next waiting process.
    pub fn release(&self) {
        if let Some(process) = self.queue.borrow_mut().pop_front() {
            process.wake();
        } else {
            self.in_use.set(false);
        }
    }
}

/// A one-shot, writable container type that awakens a pre-registered process
/// on write.
pub struct Promise<T> {
    /// The waker of the process to awaken on write.
    caller: task::Waker,
    /// The written value to be extracted by the caller.
    result: Cell<Option<T>>,
}

impl<T> Promise<T> {
    /// Creates a new promise with an unwritten result.
    pub fn new(waker: task::Waker) -> Self {
        Self {
            caller: waker,
            result: Cell::new(None),
        }
    }

    /// Writes the result value and reawakens the caller.
    pub fn fulfill(&self, result: T) {
        if self.result.replace(Some(result)).is_none() {
            self.caller.wake_by_ref();
        }
    }

    /// Extracts the written value and resets the state of the promise.
    pub fn redeem(&self) -> Option<T> {
        self.result.replace(None)
    }
}

/// Returns a future that takes two other futures and completes as soon as
/// one of them returns, canceling the other future before returning.
///
/// This design guarantees that the two futures passed as input to this
/// function cannot outlive its returned future, enabling us to allow
/// references to variables in the local scope. The passed futures may
/// compute a value, as long as the return type is identical in both cases.
pub async fn select<'s, 'u, G, E, O, R>(
    sim: SimContext<'s, G>,
    either: E,
    or: O,
) -> (Option<R>, Option<R>)
where
    E: Future<Output = R> + 'u,
    O: Future<Output = R> + 'u,
    R: 'u,
{
    use std::mem::transmute;

    let either_completed = Arc::new(AtomicBool::new(false));
    let or_completed = Arc::new(AtomicBool::new(false));
    let either_completed_clone = either_completed.clone();
    let or_completed_clone = or_completed.clone();

    // create a one-shot channel that reactivates the caller on write
    let promise_either = &Promise::new(sim.active().waker());
    let promise_or = &Promise::new(sim.active().waker());

    // this unsafe block shortens the guaranteed lifetimes of the processes
    // contained in the scheduler; it is safe because the constructed future
    // ensures that both futures are terminated before the promise and
    // itself are terminated, thereby preventing references to the lesser
    // constrained processes to survive the call to select()
    let sim = unsafe { transmute::<SimContext<'s, G>, SimContext<'_, G>>(sim) };

    // create the two competing processes
    // a future optimization would be to keep them on the stack
    let p1 = Process::new(sim.clone(), async move {
        let result = either.await;
        either_completed_clone.store(true, AtomicOrdering::SeqCst);
        promise_either.fulfill(result);
    });
    let p2 = Process::new(sim, async move {
        let result = or.await;
        or_completed_clone.store(true, AtomicOrdering::SeqCst);
        promise_or.fulfill(result);
    });

    // activate them
    sim.reactivate(p1.clone());
    sim.reactivate(p2.clone());

    // wait for reactivation; the promise will wake us on fulfillment
    sleep().await;

    let result1 = if either_completed.load(AtomicOrdering::SeqCst) {
        match promise_either.redeem() {
            Some(result) => Some(result),
            None => None,
        }
    } else {
        None
    };

    let result2 = if or_completed.load(AtomicOrdering::SeqCst) {
        match promise_or.redeem() {
            Some(result) => Some(result),
            None => None,
        }
    } else {
        None
    };

    // terminate both processes
    // (this is redundant for one of them but doesn't hurt either)
    p1.terminate();
    p2.terminate();

    // extract the promised value
    (result1, result2)
}

/// Complex channel with space for infinitely many elements of arbitrary type.
///
/// This channel supports arbitrary many readers and writers and uses wakers
/// to reactivate suspended processes.
struct Channel<T> {
    /// A queue of messages.
    store: VecDeque<T>,
    /// A queue of processes waiting to receive a message.
    waiting: VecDeque<task::Waker>,
}

/// Creates a channel and returns a pair of read and write ends.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let channel = Rc::new(RefCell::new(Channel {
        store: VecDeque::new(),
        waiting: VecDeque::new(),
    }));
    (Sender(Rc::downgrade(&channel)), Receiver(channel))
}

/// Write-end of a channel.
pub struct Sender<T>(Weak<RefCell<Channel<T>>>);

/// Read-end of a channel.
pub struct Receiver<T>(Rc<RefCell<Channel<T>>>);

// Allow senders to be duplicated.
impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

// Allow receivers to be duplicated.
impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Unpin> Sender<T> {
    /// Returns a future that can be awaited to send a message.
    pub fn send(&self, elem: T) -> SendFuture<T> {
        SendFuture(&self.0, Some(elem))
    }
}

impl<T> Drop for Sender<T> {
    #[inline]
    fn drop(&mut self) {
        // check if there are still receivers
        if let Some(chan) = self.0.upgrade() {
            // check if we're the last sender to drop
            if Rc::weak_count(&chan) == 1 {
                // awake all of the waiting receivers so that they get to return
                for process in chan.borrow_mut().waiting.drain(..) {
                    process.wake();
                }
            }
        }
    }
}

impl<T: Unpin> Receiver<T> {
    /// Returns a future that can be awaited to receive a message.
    pub fn recv(&self) -> ReceiveFuture<T> {
        ReceiveFuture(&self.0, None)
    }

    /// Returns the number of elements that can be received before model time
    /// has to be consumed.
    pub fn len(&self) -> usize {
        self.0.borrow().len()
    }
}

impl<T> Channel<T> {
    /// Private method returning the number of elements left in the channel.
    fn len(&self) -> usize {
        self.store.len()
    }

    /// Private method enqueuing a process into the waiting list.
    fn enqueue(&mut self, process: task::Waker) {
        self.waiting.push_back(process);
    }

    /// Private method removing a process from the waiting list.
    fn dequeue(&mut self) -> Option<task::Waker> {
        self.waiting.pop_front()
    }

    /// Private method that unregisters a previously registered waker.
    fn unregister(&mut self, waker: task::Waker) {
        self.waiting.retain(|elem| !waker.will_wake(elem));
    }

    /// Private method inserting a message into the queue.
    fn send(&mut self, value: T) {
        self.store.push_back(value);
    }

    /// Private method extracting a message from the queue non-blocking.
    fn recv(&mut self) -> Option<T> {
        self.store.pop_front()
    }
}

/// Future for the [`send()`] operation on a channel sender.
///
/// [`send()`]: struct.Sender.html#method.send
pub struct SendFuture<'c, T>(&'c Weak<RefCell<Channel<T>>>, Option<T>);

/// Future for the [`recv()`] operation on a channel receiver.
///
/// [`recv()`]: struct.Receiver.html#method.recv
pub struct ReceiveFuture<'c, T>(&'c Rc<RefCell<Channel<T>>>, Option<task::Waker>);

impl<T: Unpin> Future for SendFuture<'_, T> {
    type Output = Result<(), T>;

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let elem = self.1.take().unwrap();

        // test if there are still readers left to listen
        if let Some(channel) = self.0.upgrade() {
            let mut channel = channel.borrow_mut();
            channel.send(elem);

            // awake a waiting process
            if let Some(process) = channel.dequeue() {
                process.wake();
            }

            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(elem))
        }
    }
}

impl<T: Unpin> Future for ReceiveFuture<'_, T> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut channel = self.0.borrow_mut();

        // clear the local copy of our waker to prevent unnecessary
        // de-registration attempts in our destructor
        self.1 = None;

        if let Some(c) = channel.recv() {
            // there are elements in the channel
            Poll::Ready(Some(c))
        } else if Rc::weak_count(&self.0) == 0 {
            // no elements in the channel and no potential senders exist
            Poll::Ready(None)
        } else {
            // no elements, but potential senders exist: check back later
            let waker = cx.waker().clone();
            self.1 = Some(waker.clone());
            channel.enqueue(waker);
            Poll::Pending
        }
    }
}

impl<T> Drop for ReceiveFuture<'_, T> {
    #[inline]
    fn drop(&mut self) {
        // take care to unregister our waker from the channel
        if let Some(waker) = self.1.take() {
            self.0.borrow_mut().unregister(waker);
        }
    }
}

/// A trait signifying that the type implementing it is able to broadcast state
/// changes to [wakers].
///
/// Types implementing it can participate in [`until()`] expressions, the most
/// commonly-used of them being [`Control`] variables.
///
/// [wakers]: https://doc.rust-lang.org/std/task/struct.Waker.html
/// [`until()`]: fn.until.html
/// [`Control`]: struct.Control.html
pub trait Controlled {
    /// Allows a [waker] to subscribe to the controlled expression to be
    /// informed about any and all state changes.
    ///
    /// # Safety
    /// This method is unsafe, because the controlled expression will call the
    /// [`wake_by_ref()`] method on the waker during every change in state. The caller
    /// is responsible to ensure the validity of all subscribed wakers at
    /// the time of the state change.
    ///
    /// The [`span()`] method provides a safe alternative.
    ///
    /// [waker]: https://doc.rust-lang.org/std/task/struct.Waker.html
    /// [`span()`]: trait.Controlled.html#method.span
    /// [`wake_by_ref()`]: https://doc.rust-lang.org/std/task/struct.Waker.html#method.wake_by_ref
    unsafe fn subscribe(&self, waker: &task::Waker);

    /// Unsubscribes a previously subscribed [waker] from the controlled
    /// expression.
    ///
    /// [waker]: https://doc.rust-lang.org/std/task/struct.Waker.html
    unsafe fn unsubscribe(&self, waker: &task::Waker);
}

/// Guarding structure that ensures that waker and controlled expression are
/// valid and fixed in space.
pub struct WakerSpan<'s, C: ?Sized + Controlled> {
    /// The controlled expression.
    cv: &'s C,
    /// The waker.
    waker: &'s task::Waker,
}

// implement a constructor for the guard
impl<'s, C: ?Sized + Controlled> WakerSpan<'s, C> {
    /// Subscribes a [waker] to a controlled expression for a certain duration.
    ///
    /// This method subscribes the waker to the controlled expression and
    /// returns an object that unsubscribes the waker from the same controlled
    /// expression upon drop. The borrow-checker secures the correct lifetimes
    /// of waker and controlled expression so that this method can be safe.
    ///
    /// [waker]: https://doc.rust-lang.org/std/task/struct.Waker.html
    #[inline]
    pub fn new(cv: &'s C, waker: &'s task::Waker) -> Self {
        // this is safe because we bind the lifetime of the waker to the guard
        unsafe {
            cv.subscribe(waker);
        }
        WakerSpan { cv, waker }
    }
}

// implement drop for the guard
impl<C: ?Sized + Controlled> Drop for WakerSpan<'_, C> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            self.cv.unsubscribe(self.waker);
        }
    }
}

// a pointer to a controlled expression is also a controlled expression
impl<T: Controlled> Controlled for &'_ T {
    #[inline]
    unsafe fn subscribe(&self, waker: &task::Waker) {
        Controlled::subscribe(*self, waker);
    }

    #[inline]
    unsafe fn unsubscribe(&self, waker: &task::Waker) {
        Controlled::unsubscribe(*self, waker);
    }
}

/// Marks a variable as a state variable which can be used in conjunction with
/// the [`until()`] function.
///
/// Whenever a control variable changes its value, the runtime system checks
/// whether any other parts of the model are currently waiting for the variable
/// to attain a certain value.
///
/// [`until()`]: fn.until.html
pub struct Control<T> {
    /// The inner value.
    value: Cell<T>,
    /// A list of waiting processes, identified by their [wakers].
    ///
    /// [wakers]: https://doc.rust-lang.org/std/task/struct.Waker.html
    waiting: RefCell<Vec<task::Waker>>,
}

impl<T> Control<T> {
    /// Creates a new control variable and initializes its value.
    #[inline]
    pub const fn new(value: T) -> Self {
        Self {
            value: Cell::new(value),
            waiting: RefCell::new(Vec::new()),
        }
    }

    /// Assigns a new value to the control variable.
    ///
    /// This can lead to potential activations of waiting processes.
    #[inline]
    pub fn set(&self, val: T) {
        self.notify();
        self.value.set(val);
    }

    /// Extracts the current value from the control variable.
    #[inline]
    pub fn get(&self) -> T
    where
        T: Copy,
    {
        self.value.get()
    }

    /// Notifies all of the waiting processes to re-check their state condition.
    ///
    /// This action is usually performed automatically when the value of the
    /// control variable is changed through the [`set()`]-method but may be
    /// triggered manually when this mechanism is bypassed somehow.
    ///
    /// [`set()`]: #method.set
    pub fn notify(&self) {
        for waker in self.waiting.borrow().iter() {
            waker.wake_by_ref();
        }
    }
}

// implement the trait marking control variables as controlled expressions
impl<T: Copy> Controlled for Control<T> {
    #[inline]
    unsafe fn subscribe(&self, waker: &task::Waker) {
        self.waiting.borrow_mut().push(waker.clone());
    }

    unsafe fn unsubscribe(&self, waker: &task::Waker) {
        let mut waiting = self.waiting.borrow_mut();
        let pos = waiting.iter().position(|w| w.will_wake(waker));

        if let Some(pos) = pos {
            waiting.remove(pos);
        } else {
            panic!("attempt to unsubscribe waker that isn't subscribed to");
        }
    }
}

impl<T: Default> Default for Control<T> {
    fn default() -> Self {
        Self {
            value: Cell::new(T::default()),
            waiting: RefCell::new(Vec::new()),
        }
    }
}

/// An internal macro to generate implementations of the [`Controlled`] trait
/// for tuples of [`Controlled`] expressions.
///
/// [`Controlled`]: trait.Controlled.html
macro_rules! controlled_tuple_impl {
	// base rule generating an implementation for a concrete tuple
	($($T:ident -> $ID:tt),* .) => {
		impl<$($T:Controlled),*> Controlled for ($($T,)*) {
			unsafe fn subscribe(&self, _waker: &task::Waker) {
				$(self.$ID.subscribe(_waker);)*
			}

			unsafe fn unsubscribe(&self, _waker: &task::Waker) {
				$(self.$ID.unsubscribe(_waker);)*
			}
		}
	};

	($($T:ident -> $ID:tt),* . $HEAD:ident -> $HID:tt $(, $TAIL:ident -> $TID:tt)*) => {
		controlled_tuple_impl!($($T -> $ID),* .);
		controlled_tuple_impl!($($T -> $ID,)* $HEAD -> $HID . $($TAIL -> $TID),*);
	};

	($HEAD:ident -> $HID:tt $(, $TAIL:ident -> $TID:tt)* $(,)?) => {
		controlled_tuple_impl!($HEAD -> $HID . $($TAIL -> $TID),*);
	};
}

controlled_tuple_impl! {
    A -> 0, B -> 1, C -> 2, D -> 3, E -> 4, F -> 5, G -> 6, H -> 7,
    I -> 8, J -> 9, K ->10, L ->11, M ->12, N ->13, O ->14, P ->15,
}

/// Returns a future that suspends until an arbitrary boolean condition
/// involving [`Controlled`] expressions evaluates to `true`.
///
/// [`Controlled`]: trait.Controlled.html
#[inline]
pub async fn until<C: Controlled>(cntl: C, cond: impl Fn(&C) -> bool) {
    if !cond(&cntl) {
        let waker = waker().await;
        let span = WakerSpan::new(&cntl, &waker);

        loop {
            sleep().await;
            if cond(&cntl) {
                break;
            }
        }

        drop(span);
    }
}

/* ************************* statistical facilities ************************* */

/// A simple collector for statistical data, inspired by SLX's random_variable.
#[derive(Clone)]
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
