use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{collections::BinaryHeap, pin::Pin};

use log::warn;
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

        tokio::spawn(Process::new(
            Arc::clone(&self.now),
            Arc::clone(&self.semaphore),
            Arc::clone(&self.shared),
            f,
        ));
    }

    #[inline]
    pub async fn advance(&self, wait_time: Time, sender: UnboundedSender<usize>) {
        // todo
    }

    fn clear(&self) {
        // TODO
    }

    async fn get_time(&self) -> Time {
        let now = self.now.read().await;
        *now
    }

    async fn set_time(&self, new_time: Time) {
        let mut now = self.now.write().await;
        *now = new_time;
        warn!("set_time: set sim time to {:?}", new_time);
    }

    /// Removes the next event from the SortQ, sets the new time and return the
    /// sender to the coroutine
    async fn next_event(&self) -> Option<UnboundedSender<usize>> {
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

pub struct Process<G: Send + Sync + 'static>(Arc<Mutex<Inner<G>>>);

struct Inner<G: Send + Sync + 'static> {
    now: Arc<RwLock<Time>>,
    semaphore: Arc<Semaphore>,
    shared: Arc<RwLock<G>>,
    state: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<G: Send + Sync + 'static> Process<G> {
    #[inline]
    pub fn new(
        now: Arc<RwLock<Time>>,
        semaphore: Arc<Semaphore>,
        shared: Arc<RwLock<G>>,
        fut: impl Future<Output = ()> + Send + 'static,
    ) -> Self {
        Process(Arc::new(Mutex::new(Inner {
            now,
            semaphore,
            shared,
            state: Some(Box::pin(fut)),
        })))
    }

    async fn now(&self) -> Time {
        let inner = self.0.lock().await;
        let now = inner.now.read().await;
        *now
    }

    #[inline]
    pub fn terminate(&self) {
        // TODO
        // delete a permit permanently in the semaphore
    }
}

impl<G: Send + Sync + 'static> Future for Process<G> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.0.try_lock();
        match inner {
            Ok(mut guard) => {
                if let Some(ref mut state) = guard.state {
                    state.as_mut().poll(cx)
                } else {
                    Poll::Ready(())
                }
            }
            Err(_) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
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
