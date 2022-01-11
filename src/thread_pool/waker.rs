use std::{
    mem::replace,
    sync::atomic::{AtomicU8, Ordering},
    sync::Mutex,
    task::{Context, Poll, Waker},
};

#[derive(Default)]
pub(super) struct AtomicWaker {
    state: AtomicU8,
    waker: Mutex<Option<Waker>>,
}

impl AtomicWaker {
    const EMPTY: u8 = 0;
    const UPDATING: u8 = 1;
    const WAITING: u8 = 2;
    const NOTIFIED: u8 = 3;

    pub(super) fn poll(&self, ctx: &mut Context<'_>) -> Poll<()> {
        let state = self.state.load(Ordering::Acquire);
        match state {
            Self::EMPTY | Self::WAITING => {}
            Self::NOTIFIED => return Poll::Ready(()),
            _ => unreachable!(),
        }

        if let Err(state) =
            self.state
                .compare_exchange(state, Self::UPDATING, Ordering::Acquire, Ordering::Acquire)
        {
            assert_eq!(state, Self::NOTIFIED);
            return Poll::Ready(());
        }

        {
            let mut waker = self.waker.try_lock().unwrap();
            let will_wake = waker
                .as_ref()
                .map(|w| ctx.waker().will_wake(w))
                .unwrap_or(false);

            if !will_wake {
                *waker = Some(ctx.waker().clone());
            }
        }

        match self.state.compare_exchange(
            Self::UPDATING,
            Self::WAITING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Poll::Pending,
            Err(Self::NOTIFIED) => Poll::Ready(()),
            Err(_) => unreachable!(),
        }
    }

    pub(super) fn wake(&self) {
        match self.state.swap(Self::NOTIFIED, Ordering::AcqRel) {
            Self::EMPTY | Self::UPDATING => return,
            Self::WAITING => {}
            _ => unreachable!(),
        }

        let mut waker = self.waker.try_lock().unwrap();
        replace(&mut *waker, None).unwrap().wake();
    }
}
