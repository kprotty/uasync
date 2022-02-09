use super::parker::Parker;
use core::{
    cell::UnsafeCell,
    marker::PhantomData,
    mem::replace,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

pub unsafe fn from_parker<P: Parker>(parker: &P) -> Waker {
    struct ParkerWaker<P: Parker>(PhantomData<P>);

    impl<P: Parker> ParkerWaker<P> {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |ptr| RawWaker::new(ptr, &Self::VTABLE),
            Self::wake_by_ref,
            Self::wake_by_ref,
            |_ptr| {},
        );

        unsafe fn wake_by_ref(ptr: *const ()) {
            (*(ptr as *const P)).unpark();
        }
    }

    let ptr = parker as *const P as *const ();
    let raw_waker = RawWaker::new(ptr, &ParkerWaker::<P>::VTABLE);
    Waker::from_raw(raw_waker)
}

const EMPTY: usize = 0;
const UPDATING: usize = 1;
const WAITING: usize = 2;
const NOTIFIED: usize = 3;

#[derive(Default)]
pub struct AtomicWaker {
    state: AtomicUsize,
    waker: UnsafeCell<Option<Waker>>,
}

impl AtomicWaker {
    pub fn poll(&self, ctx: &mut Context<'_>) -> Poll<()> {
        let state = self.state.load(Ordering::Acquire);
        if state == NOTIFIED {
            return Poll::Ready(());
        }

        assert!(state == EMPTY || state == WAITING);
        if let Err(state) =
            self.state
                .compare_exchange(state, UPDATING, Ordering::Acquire, Ordering::Acquire)
        {
            assert_eq!(state, NOTIFIED);
            return Poll::Ready(());
        }

        {
            let waker = unsafe { &mut *self.waker.get() };
            let will_wake = waker
                .as_ref()
                .map(|w| ctx.waker().will_wake(w))
                .unwrap_or(false);

            if !will_wake {
                *waker = Some(ctx.waker().clone());
            }
        }

        match self
            .state
            .compare_exchange(UPDATING, WAITING, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Poll::Pending,
            Err(NOTIFIED) => Poll::Ready(()),
            Err(_) => unreachable!(),
        }
    }

    pub fn wake(&self) {
        let state = self.state.swap(NOTIFIED, Ordering::AcqRel);
        if state != WAITING {
            assert!(state == EMPTY || state == UPDATING);
            return;
        }

        let waker = replace(unsafe { &mut *self.waker.get() }, None);
        waker.unwrap().wake();
    }
}
