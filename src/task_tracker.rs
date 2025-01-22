use scopeguard;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

pub static TASK_COUNT: AtomicUsize = AtomicUsize::new(0);

pub async fn instrumented_task<F, T>(task: F) -> T
where
    F: Future<Output = T>,
{
    // Increment on task start
    TASK_COUNT.fetch_add(1, Ordering::Relaxed);

    // Decrement on task completion (even if it panics)
    let guard = scopeguard::guard((), |_| {
        TASK_COUNT.fetch_sub(1, Ordering::Relaxed);
    });

    let result = task.await;
    std::mem::forget(guard); // Explicitly drop guard if needed
    result
}

pub fn current_tasks() -> usize {
    TASK_COUNT.load(Ordering::Relaxed)
}
