use std::collections::BinaryHeap;
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;

use log::warn;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::sync::Semaphore;
use tokio::sync::{mpsc::UnboundedSender, Mutex, RwLock};

pub type Time = f64;
type SortQ = BinaryHeap<Event>;

pub struct SimContext<S: Send + Sync + 'static> {
    now: Arc<RwLock<Time>>,
    semaphore: Arc<Semaphore>,
    shared: Arc<RwLock<S>>,
    calendar: Arc<Mutex<SortQ>>,
}

impl<S: Clone + Send + Sync + 'static> Clone for SimContext<S> {
    #[inline]
    fn clone(&self) -> Self {
        SimContext {
            now: Arc::clone(&self.now),
            semaphore: Arc::clone(&self.semaphore),
            shared: Arc::clone(&self.shared),
            calendar: Arc::clone(&self.calendar),
        }
    }
}

impl<S: Send + Sync + 'static> SimContext<S> {
    pub fn new(shared: S) -> Arc<Self> {
        Arc::new(Self {
            now: Arc::new(RwLock::new(Time::default())),
            semaphore: Arc::new(Semaphore::new(0)),
            calendar: Arc::new(Mutex::new(SortQ::default())),
            shared: Arc::new(RwLock::new(shared)),
        })
    }

    #[inline]
    pub async fn activate<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.semaphore.add_permits(1);
        warn!(
            "New coroutine called sim.activate(): current time: {:?}",
            self.get_time().await
        );

        tokio::spawn(f);
    }

    #[inline]
    pub async fn terminate<F>(&self) {
        let permit = self
            .semaphore
            .acquire()
            .await
            .expect("Failed to acquire a permit.");
        permit.forget();
        warn!(
            "terminate: remove one permit at time {:3}",
            self.get_time().await
        );
    }

    #[inline]
    pub async fn advance(&self, wait_time: Time) {
        warn!("advance: {}", wait_time);
        let (tx, mut rx) = unbounded_channel::<usize>();
        let wake_time = self.get_time().await + wait_time;
        let event = Event(wake_time, tx);

        // adds the event to the calendar
        {
            let mut calendar = self.calendar.lock().await;
            calendar.push(event);
        }

        let permit = self
            .semaphore
            .acquire()
            .await
            .expect("Failed to acquire a permit.");

        rx.recv().await;

        drop(permit);
        warn!(
            "advance: complete at time {} after wait_time: {}",
            self.get_time().await,
            wait_time
        );
    }

    #[inline]
    pub async fn recv_with_permit<P>(&self, receiver: &mut UnboundedReceiver<P>) -> Option<P> {
        let permit = self
            .semaphore
            .acquire()
            .await
            .expect("Failed to acquire a permit.");

        let packet = receiver.recv().await;

        drop(permit);
        warn!(
            "recv_with_permit: complete at time {}",
            self.get_time().await
        );
        packet
    }

    #[inline]
    async fn get_time(&self) -> Time {
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
            "push_event: push event to SortQ at time {:3}, queue length = {:?}",
            self.get_time().await,
            calendar.len()
        );
    }

    /// Removes the next event from the SortQ, sets the new time and return the
    /// sender to the coroutine
    pub async fn pop_event(&self) -> Option<UnboundedSender<usize>> {
        let mut calendar = self.calendar.lock().await;
        let Event(now, sender) = calendar.pop()?;
        self.set_time(now).await;
        warn!(
            "pop_event: pop out from SortQ at time {:3}, queue length = {:?}",
            self.get_time().await,
            calendar.len()
        );
        Some(sender)
    }
}

pub struct Event(pub Time, pub UnboundedSender<usize>);

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
