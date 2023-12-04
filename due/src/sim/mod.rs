use std::collections::BinaryHeap;
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;

use log::{error, warn};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::{channel, Sender};
use tokio::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard, Semaphore};
use tokio::task::yield_now;

pub type Time = f64;
type SortQ = BinaryHeap<Event>;

pub struct Simulator<S: Send + Sync + 'static> {
    /// the current simulation clock
    now: Arc<RwLock<Time>>,
    /// a semaphore that represents the number of active coroutines
    semaphore: Arc<Semaphore>,
    /// a shared data structure across all coroutines
    shared: Arc<RwLock<S>>,
    /// a random number generator
    rng: Arc<Mutex<SmallRng>>,
    /// a sorted queue of events, each advances the simulation clock
    calendar: Arc<Mutex<SortQ>>,
    /// a channel to send a message to the process_event coroutine
    process_sender: Arc<RwLock<mpsc::Sender<usize>>>,
    process_receiver: Arc<RwLock<mpsc::Receiver<usize>>>,
}

impl<S: Clone + Send + Sync + 'static> Clone for Simulator<S> {
    #[inline]
    fn clone(&self) -> Self {
        Simulator {
            now: Arc::clone(&self.now),
            semaphore: Arc::clone(&self.semaphore),
            shared: Arc::clone(&self.shared),
            rng: Arc::clone(&self.rng),
            calendar: Arc::clone(&self.calendar),
            process_sender: Arc::clone(&self.process_sender),
            process_receiver: Arc::clone(&self.process_receiver),
        }
    }
}

impl<S: Send + Sync + 'static> Simulator<S> {
    pub fn new(shared: S, seed: u64) -> Arc<Self> {
        let (sender, receiver) = mpsc::channel(1);
        Arc::new(Self {
            now: Arc::new(RwLock::new(Time::default())),
            semaphore: Arc::new(Semaphore::new(0)),
            shared: Arc::new(RwLock::new(shared)),
            rng: Arc::new(Mutex::new(SmallRng::seed_from_u64(seed))),
            calendar: Arc::new(Mutex::new(SortQ::default())),
            process_sender: Arc::new(RwLock::new(sender)),
            process_receiver: Arc::new(RwLock::new(receiver)),
        })
    }

    async fn process(sim: Arc<Simulator<S>>) {
        loop {
            warn!(
                "process: waiting for a message at time {:.3}",
                sim.now().await
            );
            let mut receiver = sim.process_receiver.write().await;
            receiver.recv().await;

            // When all coroutines are blocked, the number of available
            // permits in the semaphore becomes zero. In this case, the
            // earliest advance event should be processed and the simulation
            // clock should be advanced.
            warn!("process: popping event at time {:.3}", sim.now().await);
            if !sim.calendar.lock().await.is_empty() {
                sim.pop_event().await;
            } else {
                warn!(
                    "process: no events in the calendar queue at time {:.3}",
                    sim.now().await
                );
                return;
            }

        }
    }

    pub async fn run<F>(&self, sim: Arc<Simulator<S>>, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        warn!(
            "run - initial coroutine called sim.run(): current time: {:?}",
            sim.now().await
        );

        tokio::spawn(f);

        tokio::spawn(Simulator::process(sim))
            .await
            .expect("The network simulation session failed.");
    }

    #[inline]
    pub async fn activate<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.add_permit();
        warn!(
            "activate - new coroutine called sim.activate(): current time: {:?}",
            self.now().await
        );

        tokio::spawn(f);
    }

    #[inline]
    pub async fn terminate(&self) {
        self.remove_permit().await;
        warn!(
            "terminate: remove one permit at time {:.3}",
            self.now().await
        );
    }

    #[inline]
    fn add_permit(&self) {
        self.semaphore.add_permits(1);
    }

    #[inline]
    async fn remove_permit(&self) {
        let permit = self
            .semaphore
            .acquire()
            .await
            .expect("Failed to acquire a permit.");

        permit.forget();
    }

    #[inline]
    pub async fn advance(&self, timeout: Time) {
        // yields to the other coroutines to have an opportunity to run
        yield_now().await;

        warn!("advance - advance for {} seconds.", timeout);
        let available_permits = self.semaphore.available_permits();
        warn!("available permits in advance start: {}", available_permits);

        let (tx, rx) = channel();
        let wakeup_time = self.now().await + timeout;
        let event = Event(wakeup_time, tx);

        // adds the event to the calendar
        self.push_event(event).await;

        let permit = self
            .semaphore
            .acquire()
            .await
            .expect("Failed to acquire a permit in advance().");

        // notifies the simulator coroutine to process events if needed
        let available_permits = self.semaphore.available_permits();
        warn!("available permits in advance: {}", available_permits);

        if available_permits == 0 {
            let sender = self.process_sender.read().await;
            let _ = sender.send(0).await;
        }

        match rx.await {
            Ok(_) => warn!(
                "advance: complete at time {:.3} after timeout: {:.3}",
                self.now().await,
                timeout
            ),
            Err(_) => warn!("advance: channel was closed before a message was received"),
        }

        drop(permit);
    }

    #[inline]
    pub async fn recv_with_permit<P>(&self, receiver: &mut UnboundedReceiver<P>) -> Option<P> {
        warn!("recv_with_permit: started at time {:.3}", self.now().await);
        let available_permits = self.semaphore.available_permits();
        warn!("available permits at start: {}", available_permits);
        let permit = self
            .semaphore
            .acquire()
            .await
            .expect("Failed to acquire a permit.");

        // notifies the simulator coroutine to process events if needed
        let available_permits = self.semaphore.available_permits();
        warn!(
            "available permits in recv_with_permit: {}",
            available_permits
        );

        if available_permits == 0 {
            let sender = self.process_sender.read().await;
            let _ = sender.send(0).await;
        }

        warn!(
            "recv_with_permit: waiting for packets at time {:.3}",
            self.now().await
        );
        let packet = receiver.recv().await;

        drop(permit);
        warn!("recv_with_permit: complete at time {:.3}", self.now().await);
        packet
    }

    #[inline]
    pub async fn now(&self) -> Time {
        let now = self.now.read().await;
        *now
    }

    #[inline]
    pub async fn set_time(&self, new_time: Time) {
        let mut now = self.now.write().await;
        *now = new_time;
        warn!("set_time: set sim time to {:?}", new_time);
    }

    #[inline]
    pub async fn push_event(&self, event: Event) {
        let mut calendar = self.calendar.lock().await;
        calendar.push(event);
        warn!(
            "push_event: push event to SortQ at time {:.3}, queue length = {:?}",
            self.now().await,
            calendar.len(),
        );
        for event in calendar.iter() {
            warn!("push_event: EventQ = {:.3}", event);
        }
    }

    /// Removes the next event from SortQ, sets the new simulation time,
    /// and sends a message to the coroutine that is waiting for advancing
    /// the simulation clock.
    pub async fn pop_event(&self) {
        let mut calendar = self.calendar.lock().await;

        if let Some(Event(now, sender)) = calendar.pop() {
            self.set_time(now).await;
            warn!(
                "pop_event: pop out from SortQ at time {:.3}, queue length = {:?}",
                self.now().await,
                calendar.len(),
            );
            for event in calendar.iter() {
                warn!("pop_event: EventQ = {:.3}", event);
            }

            let _ = sender.send(usize::default());
        } else {
            error!("There are no events in the calendar queue.")
        }
    }

    pub async fn get_rng(&self) -> SmallRng {
        let seed: u64 = {
            let mut rng = self.rng.lock().await;
            rng.gen()
        };
        SmallRng::seed_from_u64(seed)
    }

    pub async fn read_shared(&self) -> RwLockReadGuard<'_, S> {
        self.shared.read().await
    }

    pub async fn write_shared(&self) -> RwLockWriteGuard<'_, S> {
        self.shared.write().await
    }
}

pub struct Event(pub Time, pub Sender<usize>);

impl PartialEq for Event {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for Event {}

impl PartialOrd for Event {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Event {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .expect("invalid event wakeup time NaN")
            .reverse()
    }
}

impl Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Event").field("time", &self.0).finish()
    }
}

// Statistical facilities

/// A simple collector for statistical data
#[derive(Clone, Debug)]
pub struct RandomVar {
    total: Arc<RwLock<u32>>,
    sum: Arc<RwLock<f64>>,
    sqr: Arc<RwLock<f64>>,
    min: Arc<RwLock<f64>>,
    max: Arc<RwLock<f64>>,
}

impl RandomVar {
    /// Creates a new random variable.
    #[inline]
    pub fn new() -> Self {
        RandomVar::default()
    }

    /// Resets all stored statistical data.
    pub async fn clear(&self) {
        *self.total.write().await = 0;
        *self.sum.write().await = 0.0;
        *self.sqr.write().await = 0.0;
        *self.min.write().await = f64::INFINITY;
        *self.max.write().await = f64::NEG_INFINITY;
    }

    /// Adds another value to the statistical collection.
    pub async fn tabulate<T: Into<f64>>(&self, val: T) {
        let val: f64 = val.into();

        let mut total = self.total.write().await;
        let mut sum = self.sum.write().await;
        let mut sqr = self.sqr.write().await;
        let mut min = self.min.write().await;
        let mut max = self.max.write().await;

        *total += 1;
        *sum += val;
        *sqr += val.powi(2);
        *min = (*min).min(val);
        *max = (*max).max(val);
    }

    /// Combines the statistical collection of two random variables into one.
    pub async fn merge(&self, other: &Self) {
        let mut total = self.total.write().await;
        let mut sum = self.sum.write().await;
        let mut sqr = self.sqr.write().await;
        let mut min = self.min.write().await;
        let mut max = self.max.write().await;

        let other_total = *other.total.read().await;
        let other_sum = *other.sum.read().await;
        let other_sqr = *other.sqr.read().await;
        let other_min = *other.min.read().await;
        let other_max = *other.max.read().await;

        *total += other_total;
        *sum += other_sum;
        *sqr += other_sqr;
        *min = (*min).min(other_min);
        *max = (*max).max(other_max);
    }

    /// Displays the statistics
    pub async fn display_stats(&self) {
        let total = *self.total.read().await;
        let sum = *self.sum.read().await;
        let sqr = *self.sqr.read().await;
        let min = *self.min.read().await;
        let max = *self.max.read().await;

        let mean = sum / f64::from(total);
        let variance = sqr / f64::from(total) - mean.powi(2);
        let std_dev = variance.sqrt();

        println!(
            "{}",
            format_args!(
                "RandomVar - total: {}, mean: {:.3}, std_dev: {:.3}, min: {:.3}, max: {:.3}",
                total, mean, std_dev, min, max
            )
        );
    }
}

impl Default for RandomVar {
    fn default() -> Self {
        RandomVar {
            total: Arc::new(RwLock::new(0)),
            sum: Arc::new(RwLock::new(0.0)),
            sqr: Arc::new(RwLock::new(0.0)),
            min: Arc::new(RwLock::new(f64::INFINITY)),
            max: Arc::new(RwLock::new(f64::NEG_INFINITY)),
        }
    }
}
