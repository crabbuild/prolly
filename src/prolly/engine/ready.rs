use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

pub(crate) mod sealed {
    pub trait ReadyFuture {}
}

/// A future whose construction is restricted to ready-only engine paths.
pub(crate) trait ReadyFuture: Future + sealed::ReadyFuture {}

impl<F> ReadyFuture for F where F: Future + sealed::ReadyFuture {}

/// Crate-private marker wrapper used only for operations over synchronous stores.
pub(crate) struct ReadyOnly<F> {
    inner: F,
}

impl<F: Future> Future for ReadyOnly<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `inner` is structurally pinned with `self` and is never moved
        // while this `ReadyOnly` value is pinned.
        unsafe { self.map_unchecked_mut(|ready| &mut ready.inner) }.poll(context)
    }
}

impl<F> sealed::ReadyFuture for ReadyOnly<F> {}

/// Mark a crate-controlled operation over `SyncStoreAsAsync` as ready-only.
pub(crate) fn ready_only<F: Future>(future: F) -> ReadyOnly<F> {
    ReadyOnly { inner: future }
}

/// Poll a ready-only future exactly once without a runtime or thread parking.
pub(crate) fn run_ready<F: ReadyFuture>(future: F) -> F::Output {
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("ready-only future returned Pending"),
    }
}

fn noop_waker() -> Waker {
    // SAFETY: the vtable never dereferences the null data pointer and all
    // operations are valid no-ops for the duration of this local poll.
    unsafe { Waker::from_raw(noop_raw_waker()) }
}

fn noop_raw_waker() -> RawWaker {
    RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)
}

unsafe fn clone_noop_waker(_data: *const ()) -> RawWaker {
    noop_raw_waker()
}

unsafe fn noop_wake(_data: *const ()) {}

static NOOP_WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_noop_waker, noop_wake, noop_wake, noop_wake);

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use super::{ready_only, run_ready};

    struct CountedReady {
        polls: Arc<AtomicUsize>,
        output: usize,
    }

    impl Future for CountedReady {
        type Output = usize;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::Relaxed);
            Poll::Ready(self.output)
        }
    }

    struct UnexpectedPending;

    impl Future for UnexpectedPending {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    #[test]
    fn ready_execution_returns_values_and_errors_after_one_poll() {
        let polls = Arc::new(AtomicUsize::new(0));
        let value = run_ready(ready_only(CountedReady {
            polls: polls.clone(),
            output: 42,
        }));
        assert_eq!(value, 42);
        assert_eq!(polls.load(Ordering::Relaxed), 1);

        let error = run_ready(ready_only(async { Err::<(), _>("store error") }));
        assert_eq!(error, Err("store error"));
    }

    #[test]
    fn ready_execution_is_reentrant() {
        let value = run_ready(ready_only(async {
            run_ready(ready_only(async { 41 })) + 1
        }));
        assert_eq!(value, 42);
    }

    #[test]
    #[should_panic(expected = "ready-only future returned Pending")]
    fn ready_execution_rejects_pending() {
        run_ready(ready_only(UnexpectedPending));
    }
}
