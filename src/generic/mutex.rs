use crate::{
    parking::{FilterOp, ParkToken, Parker, UnparkToken, UnparkResult},
    parking::{
    park_async, queue::try_block_on, unpark,
};
use core::{
    mem::replace,
    hint::spin_loop,
    sync::atomic::{AtomicU8, Ordering},
};

const UNLOCKED: u8 = 0;
const LOCKED_BIT: u8 = 1;
const PARKED_BIT: u8 = 2;

const TOKEN_PARKED: ParkToken = ParkToken(0);
const TOKEN_RETRY: UnparkToken = UnparkToken(0);
const TOKEN_HANDOFF: UnparkToken = UnparkToken(1);

#[derive(Default)]
pub struct RawMutex {
    state: AtomicU8,
}

impl RawMutex {
    pub const fn new() -> Self {
        Self {
            state: AtomicU8::new(UNLOCKED),
        }
    }

    pub fn is_locked(&self) -> bool {
        let state = self.state.load(Ordering::Relaxed);
        state & LOCKED_BIT != 0
    }

    pub fn try_lock(&self) -> bool {
        self.try_lock_update().is_ok()
    }

    fn try_lock_update(&self) -> Result<usize, usize> {
        self.state
            .fetch_update(Ordering::Acquire, Ordering::Relaxed, |state| {
                match state & LOCKED_BIT {
                    0 => Some(state | LOCKED_BIT),
                    _ => None,
                }
            })
    }

    fn try_lock_fast(&self) -> bool {
        self.state
            .compare_exchange_weak(UNLOCKED, LOCKED_BIT, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    #[inline]
    pub fn try_lock_for<P: Parker>(&self, timeout: Duration) -> bool {
        self.try_lock_fast() || self.lock_slow::<P>(Some(P::now() + timeout))
    }

    #[inline]
    pub fn try_lock_until<P: Parker>(&self, deadline: Option<P::Instant>) -> bool {
        self.try_lock_fast() || self.lock_slow::<P>(Some(deadline))
    }

    #[inline]
    pub fn lock<P: Parker>(&self) {
        if !self.try_lock_fast() {
            assert!(self.lock_slow::<P>(None));
        }
    }

    #[inline]
    pub async fn lock_async<P: Parker>(&self) {
        if !self.try_lock_fast() {
            self.lock_slow_async::<P>();
        }
    }

    #[cold]
    fn lock_slow<P: Parker>(&self, deadline: Option<P::Instant>) -> bool {
        try_block_on(deadline, self.lock_slow_async::<P>()).is_some()
    }

    #[cold]
    fn lock_slow_async<P: Parker>(&self) {
        loop {
            let mut spin = 0;
            loop {
                let state = match self.try_lock_update() {
                    Ok(_) => return,
                    Err(e) => e,
                };

                if state & PARKED_BIT == 0 {
                    if P::yield_now(spin) {
                        spin += 1;
                        continue;
                    }

                    if let Err(_) = self.state.compare_exchange_weak(
                        state,
                        state | PARKED_BIT,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        spin_loop();
                        continue;
                    }
                }

                let key = self as *const Self as usize;
                let validate = || self.state.load(Ordering::Relaxed) == LOCKED_BIT | PARKED_BIT;
                let before_sleep = || {};
                let cancelled = |has_more: bool| {
                    if !has_more {
                        self.state.fetch_and(!PARKED_BIT, Ordering::Relaxed);
                    }
                };

                break match unsafe {
                    park_async(key, validate, before_sleep, cancelled, TOKEN_PARKED).await
                } {
                    None => {}
                    Some(TOKEN_RETRY) => {}
                    Some(TOKEN_HANDOFF) => return,
                };
            }
        }
    }

    #[inline]
    pub unsafe fn unlock<P: Parker>(&self) {
        let state = self.state.fetch_sub(LOCKED_BIT, Ordering::Release);
        debug_assert_ne!(state & LOCKED_BIT, 0);

        if state != LOCKED_BIT {
            debug_assert_eq!(state, PARKED_BIT);
            self.unlock_slow::<P>(TOKEN_RETRY, |result| {
                if !result.has_more {
                    self.state.fetch_and(!PARKED_BIT, Ordering::Relaxed);
                }
            });
        }
    }

    #[inline]
    pub unsafe fn unlock_fair<P: Parker>(&self) {
        if let Err(_) = self.state.compare_exchange(
            LOCKED_BIT,
            UNLOCKED,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            self.unlock_slow::<P>(TOKEN_HANDOFF, |result| {
                if result.unparked == 0 {
                    self.state.store(UNLOCKED, Ordering::Release);
                } else if !result.has_more {
                    self.state.store(LOCKED_BIT, Ordering::Relaxed);
                }
            });
        }
    }

    #[cold]
    fn unlock_slow<P: Parker>(&self, unpark_token: UnparkToken, before_unpark: impl FnOnce(UnparkResult)) {
        let mut unparked = false;
        let filter = |park_token| {
            assert_eq!(park_token, TOKEN_PARKED);
            if replace(&mut unparked, true) {
                FilterOp::Stop
            } else {
                FilterOp::Unpark(unpark_token)
            }
        };
        
        let key = self as *const Self as usize;
        let _ = unpark(key, filter, before_unpark);
    }
}
