use crate::{
    parking::{park_async, queue::try_block_on, unpark},
    parking::{FilterOp, ParkToken, Parker, UnparkResult, UnparkToken},
};
use core::{
    hint::spin_loop,
    mem::replace,
    sync::atomic::{AtomicUsize, Ordering},
};

const UNLOCKED: usize = 0;
const WRITER_BIT: usize = 1;
const PARKED_BIT: usize = 2;
const WRITER_PARKED_BIT: usize = 3;

const ONE_READER: usize = 4;
const READER_MASK: usize = !(ONE_READER - 1);

const TOKEN_PARKED: ParkToken = ParkToken(0);
const TOKEN_RETRY: UnparkToken = UnparkToken(0);
const TOKEN_HANDOFF: UnparkToken = UnparkToken(1);

#[derive(Default)]
pub struct RawRwLock {
    state: AtomicUsize,
}

impl RawRwLock {
    pub const fn new() -> Self {
        Self {
            state: AtomicUsize::new(UNLOCKED),
        }
    }

    #[inline]
    pub fn is_writing(&self) -> bool {
        let state = self.state.load(Ordering::Relaxed);
        state & WRITER_BIT != 0
    }

    #[inline]
    pub fn readers(&self) -> usize {
        let state = self.state.load(Ordering::Relaxed);
        let shift = READER_MASK.trailing_zeros();
        state >> shift
    }

    #[inline]
    pub fn try_write(&self) -> bool {
        self.try_write_poll().is_ok()
    }

    fn try_write_poll(&self) -> Result<usize, usize> {
        self.state
            .fetch_update(Ordering::Acquire, Ordering::Relaxed, |state| {
                match state & LOCKED_BIT {
                    0 => Some(state | LOCKED_BIT),
                    _ => None,
                }
            })
    }

    fn try_write_fast(&self) -> bool {
        self.state
            .compare_exchange_weak(UNLOCKED, WRITER_BIT, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    #[inline]
    pub fn try_write_for<P: Parker>(&self, timeout: Duration) -> bool {
        self.try_write_fast() || self.write_slow::<P>(Some(P::now() + timeout))
    }

    #[inline]
    pub fn try_write_until<P: Parker>(&self, deadline: P::Instant) -> bool {
        self.try_write_fast() || self.write_slow::<P>(Some(deadline))
    }

    #[inline]
    pub fn write<P: Parker>(&self) {
        if !self.try_write_fast() {
            assert!(self.write_slow::<P>(None));
        }
    }

    #[inline]
    pub async fn write_async<P: Parker>(&self) {
        if !self.try_write_fast() {
            self.write_slow_async::<P>().await;
        }
    }

    #[cold]
    fn write_slow<P: Parker>(&self, deadline: Option<P::Instant>) -> bool {
        try_block_on::<P, _>(deadline, self.write_slow_async::<P>()).is_some()
    }

    #[cold]
    async fn write_slow_async<P: Parker>(&self) {
        self.acquire_writer_bit::<P>().await;
        self.wait_for_readers::<P>().await;
    }

    async fn acquire_writer_bit<P: Parker>(&self) {
        let try_acquire = || self.try_write_poll().map(|| {});
        let validate = |state: usize| {
            let writing_and_parked = WRITER_BIT | PARKED_BIT;
            state & writing_and_parked == writing_and_parked
        };

        let on_cancel_drop = || {};
        let on_cancel = |has_more: bool| {
            if !has_more {
                self.state.fetch_and(!PARKED_BIT, Ordering::Relaxed);
            }
        };

        self.acquire::<P, _, _, _, _>(
            ParkToken(WRITER_BIT),
            PARKED_BIT,
            try_acquire,
            validate,
            on_cancel,
            on_cancel_drop,
        )
        .await;
    }

    async fn wait_for_readers<P: Parker>(&self) {
        let try_acquire = || {
            let state = self.state.load(Ordering::Acquire);
            match state & READER_MASK {
                0 => Ok(()),
                _ => Err(state),
            }
        };
        let validate = |state: usize| {
            assert_ne!(state & WRITER_BIT, 0);
            (state & READER_MASK != 0) && (state & WRITER_PARKED_BIT != 0)
        };

        let on_cancel = |_has_more: bool| {};
        let on_cancel_drop = || {
            let remove = WRITER_BIT | WRITER_PARKED_BIT;
            let state = self.state.fetch_sub(remove, Ordering::Relaxed);
            assert_eq!(state & remove, remove);

            if state & PARKED_BIT != 0 {
                self.unpark_others(TOKEN_RETRY, |_new_state, result| {
                    if !result.has_more {
                        self.state.fetch_and(!PARKED_BIT, Ordering::Relaxed);
                    }
                });
            }
        };

        self.acquire::<P, _, _, _, _>(
            ParkToken(WRITER_BIT),
            WRITER_PARKED_BIT,
            try_acquire,
            on_validate,
            on_cancel,
            on_cancel_drop,
        )
        .await;
    }

    #[inline]
    pub fn try_read(&self) -> bool {
        self.try_read_fast() || self.try_read_slow()
    }

    #[inline]
    fn try_read_fast(&self) -> bool {
        let state = self.state.load(Ordering::Relaxed);
        self.try_read_with(state).is_ok()
    }

    #[cold]
    fn try_read_slow(&self) -> bool {
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            state = match self.try_read_with(state) {
                Ok(_) => return true,                  // acquired reader
                Err(None) => return false,             // read count overflowed
                Err(Some(Err(_))) => return false,     // has writer
                Err(Some(Ok(new_state))) => new_state, // cas failed
            };
        }
    }

    #[inline]
    fn try_read_with(&self, state: usize) -> Result<(), Option<Result<usize, usize>>> {
        let state = self.state.load(Ordering::Relaxed);
        if state & WRITER_BIT != 0 {
            return Err(Some(Err(state)));
        }

        let new_state = match state.checked_add(ONE_READER) {
            Some(new_state) => new_state,
            None => return Err(None),
        };

        self.state
            .compare_exchange_weak(state, new_state, Ordering::Acquire, Ordering::Relaxed)
            .map(|_| {})
            .map_err(|state| Some(Ok(state)))
    }

    #[inline]
    pub fn try_read_for<P: Parker>(&self, timeout: Duration) -> bool {
        self.try_read_fast() || self.read_slow::<P>(Some(P::now() + timeout))
    }

    #[inline]
    pub fn try_read_until<P: Parker>(&self, deadline: P::Instant) -> bool {
        self.try_read_fast() || self.read_slow::<P>(Some(deadline))
    }

    #[inline]
    pub fn read<P: Parker>(&self) {
        if !self.try_read_fast() {
            assert!(self.read_slow::<P>(None));
        }
    }

    #[inline]
    pub async fn read_async<P: Parker>(&self) {
        if !self.try_read_fast() {
            self.read_slow_async::<P>().await;
        }
    }

    #[cold]
    fn read_slow<P: Parker>(&self, deadline: Option<P::Instant>) -> bool {
        try_block_on::<P, _>(deadline, self.read_slow_async::<P>()).is_some()
    }

    #[cold]
    async fn read_slow_async<P: Parker>(&self) {
        let try_acquire = || {
            let mut state = self.state.load(Ordering::Relaxed);
            loop {
                state = match self.try_read_with(state) {
                    Ok(_) => return Ok(()), // acquired reader
                    Err(None) => unreachable!("reader count overflowed"),
                    Err(Some(Err(_))) => return Err(state), // has writer
                    Err(Some(Ok(new_state))) => new_state,  // cas failed
                };
            }
        };
        let validate = |state: usize| {
            let writing_and_parked = WRITER_BIT | PARKED_BIT;
            state & writing_and_parked == writing_and_parked
        };

        let on_cancel_drop = || {};
        let on_cancel = |has_more: bool| {
            if !has_more {
                self.state.fetch_and(!PARKED_BIT, Ordering::Relaxed);
            }
        };

        self.acquire::<P, _, _, _, _>(
            ParkToken(ONE_READER),
            PARKED_BIT,
            try_acquire,
            validate,
            on_cancel,
            on_cancel_drop,
        )
        .await;
    }

    fn key_for(&self, park_bit: usize) -> usize {
        (self as *const Self as usize)
            + match park_bit {
                PARKED_BIT => 0,
                WRITER_PARKED_BIT => 1,
                _ => unreachable!("invalid RwLock park bit"),
            }
    }

    async fn acquire<P, TryAcquireFn, ValidateFn, OnCancelFn, OnCancelDropFn>(
        &self,
        park_token: ParkToken,
        park_bit: usize,
        mut try_acquire: TryAcquireFn,
        validate_fn: ValidateFn,
        on_cancel: OnCancelFn,
        on_cancel_drop: OnCancelDropFn,
    ) where
        P: Parker,
        TryAcquireFn: FnMut() -> Result<(), usize>,
        ValidateFn: FnMut(usize) -> bool,
        OnCancelFn: FnOnce(bool),
        OnCancelDropFn: FnOnce(),
    {
        struct CallOnDrop<F: FnOnce()>(Option<F>);

        impl<F: FnOnce()> Drop for CallOnDrop {
            fn drop(&mut self) {
                if let Some(f) = self.0.take() {
                    f();
                }
            }
        }

        let mut call_on_drop = CallOnDrop(Some(on_cancel_drop));
        let complete = || call_on_drop.0 = None;

        loop {
            let mut spin = 0;
            loop {
                let state = match try_acquire() {
                    Ok(_) => return complete(),
                    Err(e) => e,
                };

                if state & park_bit == 0 {
                    if P::yield_now(spin) {
                        spin += 1;
                        continue;
                    }

                    if let Err(_) = self.state.compare_exchange_weak(
                        state,
                        state | park_bit,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        spin_loop();
                        continue;
                    }
                }

                let key = self.key_for(park_bit);
                let before_sleep = || {};
                let validate = || {
                    let state = self.state.load(Ordering::Relaxed);
                    on_validate(validate)
                };

                break match unsafe {
                    park_async(key, validate, before_sleep, on_cancel, park_token).await
                } {
                    None => {}
                    Some(TOKEN_RETRY) => {}
                    Some(TOKEN_HANDOFF) => return complete(),
                };
            }
        }
    }

    #[inline]
    pub unsafe fn unlock_write<P: Parker>(&self) {
        let state = self.state.fetch_sub(WRITER_BIT, Ordering::Release);
        debug_assert_ne!(state & WRITER_BIT, 0);

        if state != WRITER_BIT {
            debug_assert_eq!(state, PARKED_BIT);
            self.unlock_write_slow::<P>();
        }
    }

    #[cold]
    fn unlock_write_slow<P: Parker>(&self) {
        self.unpark_others::<P, _>(TOKEN_RETRY, |_new_state, result| {
            if !result.has_more {
                self.state.fetch_and(!PARKED_BIT, Ordering::Relaxed);
            }
        })
    }

    #[inline]
    pub unsafe fn unlock_write_fair<P: Parker>(&self) {
        if self
            .state
            .compare_exchange(WRITER_BIT, UNLOCKED, Ordering::Release, Ordering::Relaxed)
            .is_err()
        {
            self.unlock_write_fair_slow::<P>();
        }
    }

    #[cold]
    fn unlock_write_fair_slow<P: Parker>(&self) {
        self.unpark_others::<P, _>(TOKEN_HANDOFF, |new_state, result| {
            if result.unparked == 0 {
                new_state = UNLOCKED;
            }
            if result.has_more {
                new_state |= PARKED_BIT;
            }
            self.state.store(new_state, Ordering::Release);
        });
    }

    #[inline]
    pub unsafe fn unlock_read<P: Parker>(&self) {
        self.unlock_read_fair::<P>()
    }

    #[inline]
    pub unsafe fn unlock_read_fair<P: Parker>(&self) {
        let state = self.state.fetch_sub(ONE_READER, Ordering::Release);
        debug_assert_ne!(state & READER_MASK, 0);

        if state & (READER_MASK | WRITER_PARKED_BIT) == (ONE_READER | WRITER_PARKED_BIT) {
            self.unlock_read_fair_slow::<P>();
        }
    }

    #[cold]
    fn unlock_read_fair_slow<P: Parker>(&self) {
        let mut unparked_writer = false;
        let filter = |park_token: ParkToken| {
            assert_eq!(park_token.0, WRITER_BIT);
            assert!(
                !unparked_writer,
                "multiple writers waiting for readers to exit"
            );
            unparked_writer = true;
            FilterOp::Unpark(TOKEN_HANDOFF)
        };

        let key = self.key_for(WRITER_PARKED_BIT);
        let before_unpark = |result: UnparkResult| {
            assert!(
                !result.has_more,
                "multiple writers waiting for readers to exit"
            );
            self.state.fetch_and(!WRITER_PARKED_BIT, Ordering::Relaxed);
        };

        unsafe {
            let _ = unpark::<P, _, _>(key, filter, before_unpark);
        }
    }

    fn unpark_others<P, BeforeUnpark>(&self, unpark_token: UnparkToken, before_wake: BeforeUnpark)
    where
        P: Parker,
        BeforeUnpark: FnOnce(usize, UnparkResult),
    {
        let new_state = core::Cell::new(0);
        let on_filter = |park_token| {
            match park_token.0 {
                WRITER_BIT => {}
                ONE_READER => {}
                _ => unreachable!("invalid waiter on RwLock"),
            }

            let new_s = new_state.get();
            if new_s & WRITER_BIT != 0 {
                return FilterOp::Stop;
            }

            new_state.set(new_s + park_token.0);
            FilterOp::Unpark(unpark_token)
        };

        let key = self as *const Self as usize;
        let on_before_unpark = |result: UnparkResult| {
            before_wake(new_state.get(), result);
        };

        unsafe {
            let _ = unpark::<P, _, _>(key, on_filter, on_before_unpark);
        }
    }
}
