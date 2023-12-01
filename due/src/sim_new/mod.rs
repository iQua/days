use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use std::task::{Poll, Context};
use std::{collections::BinaryHeap, pin::Pin};

use log::warn;
use tokio::sync::{mpsc::UnboundedSender, Mutex, RwLock};

pub type Time = f64;

pub struct SimContext<G> {
    pub handle: *const Scheduler<G>,
}

impl<G> Clone for SimContext<G> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<G> Copy for SimContext<G> {}

impl<G> SimContext<G> {
    pub async fn activate(&self, f: F)
    where
        F: Future<Output = ()> + Send,
    {
        warn!("New coroutine called sim.activate(): current time: {:?}", self.now().await);
        tokio::spawn(Process::new(*self, f));
    }

    #[inline]
    pub async fn now(&self) -> Time {
        let now = self.sched().now.read().await;
        *now
    }

    /// Private function to get a safe reference to the scheduler.
    #[inline]
    fn sched(&self) -> &Scheduler<G> {
        unsafe { &*self.handle}
    }

}

type SortQ = BinaryHeap<NextEvent>;

pub struct Scheduler<G> {
    now: Arc<RwLock<Time>>,
    calendar: Arc<Mutex<SortQ>>,
    shared: G,
}

impl<G> Scheduler<G> {
    #[inline]
    fn new(shared: G) -> Self {
        Self {
            now: Arc::new(RwLock::new(Time::default())),
            calendar: Arc::new(Mutex::new(SortQ::default())),
            shared,
        }
    }

    fn clear(&self) {
        // TODO
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
        self.set_time(now);
        warn!(
            "next_event: pop out from SortQ, queue length = {:?}",
            calendar.len()
        );
        Some(sender)
    }
}

pub struct Process<G>(Arc<Mutex<Inner<G>>>);

struct Inner<G> {
    context: SimContext<G>,
    state: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<G> Process<G> {
    #[inline]
    pub fn new(sim: SimContext<G>, fut: impl Future<Output = ()> + Send) -> Self {
        Process(Arc::new(Mutex::new(Inner {
            context: sim,
            state: Some(Box::pin(fut)),
        })))
    }

    async fn now(&self) -> Time {
        let inner = self.0.lock().await;
        inner.context.now().await
    }

}

impl<G> Future for Process<G> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.0.try_lock();
        match inner {
            Ok(mut guard) => {
                if let Some(ref mut state) = guard.state{
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
        f.debug_struct("NextEvent")
            .field("time", &self.0)
            .finish()
    }
}
