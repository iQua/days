use std::collections::BinaryHeap;
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;

use log::warn;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::Semaphore;
use tokio::sync::{mpsc::UnboundedSender, Mutex, RwLock};

pub type Time = f64;
type SortQ = BinaryHeap<NextEvent>;

pub struct SimContext<G: Send + Sync + 'static> {
    now: Arc<RwLock<Time>>,
    semaphore: Arc<Semaphore>,
    shared: Arc<RwLock<G>>,
    calendar: Arc<Mutex<SortQ>>,
}

impl<G: Clone + Send + Sync + 'static> Clone for SimContext<G> {
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

impl<G: Send + Sync + 'static> SimContext<G> {
    pub fn new(shared: G) -> Arc<Self> {
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
    pub async fn terminate<F>(&self)
    {
        let permit = self.semaphore.acquire().await.expect("Failed to acquire a permit.");
        permit.forget();
        warn!("terminate: remove one permit at time {:3}", self.get_time().await);
    }


    #[inline]
    pub async fn advance(&self, wait_time: Time) {
        warn!("advance: {}", wait_time);
        let (tx, mut rx) = unbounded_channel::<usize>();
        let wake_time = self.get_time().await + wait_time;
        let event = NextEvent(wake_time, tx);

        // adds the event to the calendar
        {
            let mut calendar = self.calendar.lock().await;
            calendar.push(event);
        }

        rx.recv().await;
    }

    async fn get_time(&self) -> Time {
        let now = self.now.read().await;
        *now
    }

    pub async fn set_time(&self, new_time: Time) {
        let mut now = self.now.write().await;
        *now = new_time;
        warn!("set_time: set sim time to {:?}", new_time);
    }

    /// Removes the next event from the SortQ, sets the new time and return the
    /// sender to the coroutine
    pub async fn next_event(&self) -> Option<UnboundedSender<usize>> {
        let mut calendar = self.calendar.lock().await;
        let NextEvent(now, sender) = calendar.pop()?;
        self.set_time(now).await;
        warn!(
            "next_event: pop out from SortQ, queue length = {:?}",
            calendar.len()
        );
        Some(sender)
    }
}

pub struct NextEvent(pub Time, pub UnboundedSender<usize>);

impl PartialEq for NextEvent {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for NextEvent {}

impl PartialOrd for NextEvent {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NextEvent {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .expect("invalid event wakeup time NaN")
            .reverse()
    }
}

impl Display for NextEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NextEvent").field("time", &self.0).finish()
    }
}
