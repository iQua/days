use self::Token::*;
use std::rc::Rc;

async fn decompress<I>(mut inp: I, out: Sender<char>)
where I: Iterator<Item = u8> {
	while let Some(c) = inp.next() {
		if c == 0xFF {
			let len = inp.next().unwrap();
			let c = inp.next().unwrap();
			
			for _ in 0..len {
				out.send(c as char).await;
			}
		} else {
			out.send(c as char).await;
		}
	}
}

#[derive(Clone, Debug)]
enum Token { WORD(String), PUNCT(char) }

async fn tokenize(inp: Receiver<char>, out: Sender<Token>) {
	while let Some(mut c) = inp.recv().await {
		if c.is_alphabetic() {
			let mut text = c.to_string();
			while let Some(new_c) = inp.recv().await {
				if new_c.is_alphabetic() {
					text.push(new_c);
				} else {
					c = new_c; break;
				}
			}
			out.send(WORD(text)).await;
		}
		out.send(PUNCT(c)).await;
	}
}

fn main() {
	let exec = Executor::new();
	let spawner = exec.spawner();
	let input = b"He\xff\x02lo IST!".iter().cloned();
	
	exec.run(async {
		let (s1, r1) = channel::<char>();
		let (s2, r2) = channel::<Token>();
		
		spawner.spawn(decompress(input, s1));
		spawner.spawn(tokenize(r1, s2));
		
		while let Some(token) = r2.recv().await {
			println!("{:?}", token);
		}
	});
}

/* **************************** library contents **************************** */
// the following code belongs into a library but is shown here for clarity

// additional imports for the necessary execution environment
use std::{
	pin::Pin,
	cell::RefCell,
	future::Future,
	task::{self, Poll, Context},
	mem::swap
};

/* ************************** executor environment ************************** */

// heterogeneous, pinned list of futures
type FutureQ = Vec<Pin<Box<dyn Future<Output=()>>>>;

/// A simple executor that drives the event loop.
struct Executor {
	sched: RefCell<FutureQ>
}

impl Executor {
	/// Constructs a new executor.
	pub fn new() -> Self {
		Executor {
			sched: RefCell::new(vec![])
		}
	}
	
	/// Creates and returns a Spawner that can be used to insert new futures
	/// into the executors future queue.
	pub fn spawner(&self) -> Spawner {
		Spawner { sched: &self.sched }
	}
	
	/// Runs a future to completion.
	pub fn run<F: Future>(&self, mut future: F) -> F::Output {
		// construct a custom context to pass into the poll()-method
		let waker = unsafe {
			task::Waker::from_raw(task::RawWaker::new(
				std::ptr::null(),
				&EXECUTOR_WAKER_VTABLE
			))
		};
		let mut cx = task::Context::from_waker(&waker);
		
		// pin the passed future for the duration of this function;
		// note: this is safe since we're not moving it around
		let mut future = unsafe {
			Pin::new_unchecked(&mut future)
		};
		let mut sched = FutureQ::new();
		
		// implement a busy-wait event loop for simplicity
		loop {
			// poll the primary future, allowing secondary futures to spawn
			if let Poll::Ready(val) = future.as_mut().poll(&mut cx) {
				break val;
			}
			
			// swap the scheduled futures with an empty queue
			swap(&mut sched, &mut self.sched.borrow_mut());
			
			// iterate over all secondary futures presently scheduled
			for mut future in sched.drain(..) {
				// if they are not completed, reschedule
				if future.as_mut().poll(&mut cx).is_pending() {
					self.sched.borrow_mut().push(future);
				}
			}
		}
	}
}

/// A handle to the executors future queue that can be used to insert new tasks.
struct Spawner<'a> { sched: &'a RefCell<FutureQ> }

impl<'a> Spawner<'a> {
	pub fn spawn(&self, future: impl Future<Output=()> + 'static) {
		self.sched.borrow_mut().push(Box::pin(future));
	}
}

/* **************************** specialized waker *************************** */

/// Simple waker that isn't really used (yet), except to show the mechanism.
struct TrivialWaker;

impl TrivialWaker {
	unsafe fn clone(_this: *const ()) -> task::RawWaker {
		unimplemented!()
	}
	
	unsafe fn wake(_this: *const ()) {
		unimplemented!()
	}
	
	unsafe fn wake_by_ref(_this: *const ()) {
		unimplemented!()
	}
	
	unsafe fn drop(_this: *const ()) {}
}

/// Virtual function table for the simple waker.
static EXECUTOR_WAKER_VTABLE: task::RawWakerVTable = task::RawWakerVTable::new(
	TrivialWaker::clone,
	TrivialWaker::wake,
	TrivialWaker::wake_by_ref,
	TrivialWaker::drop
);

/* ************************** asynchronous wrappers ************************* */

/// Simple channel with space for only one element.
struct Channel<T> {
	slot: RefCell<Option<T>>
}

/// Creates a channel and returns a pair of read and write ends.
fn channel<T>() -> (Sender<T>, Receiver<T>) {
	let channel = Rc::new(Channel {
		slot: RefCell::new(None)
	});
	(Sender { channel: channel.clone() }, Receiver { channel })
}

/// Write-end of a channel.
struct Sender<T> { channel: Rc<Channel<T>> }

/// Read-end of a channel.
struct Receiver<T> { channel: Rc<Channel<T>> }

impl<T: Clone> Sender<T> {
	/// Method used to push an element into the channel.
	/// Blocks until the previous element has been consumed.
	pub fn send<'s>(&'s self, elem: T) -> impl Future<Output = ()> + 's {
		SendFuture { channel: &self.channel, elem }
	}
}

impl<T: Clone> Receiver<T> {
	/// Method used to consume an element from the channel.
	/// Blocks until an element can be consumed.
	pub fn recv<'r>(&'r self) -> impl Future<Output = Option<T>> + 'r {
		ReceiveFuture { channel: &self.channel }
	}
}

/// A future that pushes an element into a channel.
struct SendFuture<'c,T>    { channel: &'c Rc<Channel<T>>, elem: T }

/// A future that consumes an element from a channel.
struct ReceiveFuture<'c,T> { channel: &'c Rc<Channel<T>> }

impl<T: Clone> Future for SendFuture<'_,T> {
	type Output = ();
	
	fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
		// check if there is space for the element
		if self.channel.slot.borrow().is_none() {
			// replace the empty element with ours
			self.channel.slot.replace(Some(self.elem.clone()));
			Poll::Ready(())
		} else {
			// try again at a later time
			Poll::Pending
		}
	}
}

impl<T> Future for ReceiveFuture<'_,T> {
	type Output = Option<T>;
	
	fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
		// check if there is an element in the channel
		if let Some(c) = self.channel.slot.borrow_mut().take() {
			// return it
			Poll::Ready(Some(c))
		} else if Rc::strong_count(self.channel) == 1 {
			// check if the sender disconnected
			Poll::Ready(None)
		} else {
			// try again at a later time
			Poll::Pending
		}
	}
}
