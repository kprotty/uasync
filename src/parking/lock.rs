use super::{parker::Parker, waker::from_parker};
use core::{
    cell::{Cell, UnsafeCell},
    hint::spin_loop,
    marker::PhantomPinned,
    ops::{Deref, DerefMut},
    pin::Pin,
    ptr::NonNull,
    sync::atomic::{fence, AtomicUsize, Ordering},
    task::Waker,
};

pub struct Lock<T> {
    raw_lock: RawLock,
    value: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for Lock<T> {}
unsafe impl<T: Send> Sync for Lock<T> {}

impl<T> Lock<T> {
    pub const fn new(value: T) -> Self {
        Self {
            raw_lock: RawLock::new(),
            value: UnsafeCell::new(value),
        }
    }

    pub fn lock<P: Parker>(&self) -> LockGuard<'_, T> {
        self.raw_lock.lock::<P>();
        LockGuard { lock: self }
    }
}

pub struct LockGuard<'a, T> {
    lock: &'a Lock<T>,
}

impl<'a, T> Drop for LockGuard<'a, T> {
    fn drop(&mut self) {
        unsafe { self.lock.raw_lock.unlock() }
    }
}

impl<'a, T> Deref for LockGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.lock.value.get() }
    }
}

impl<'a, T> DerefMut for LockGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.value.get() }
    }
}

const UNLOCKED: usize = 0;
const LOCKED_BIT: usize = 1;
const WAKING_BIT: usize = 2;
const WAITER_MASK: usize = !3;

#[repr(align(4))]
struct Waiter {
    prev: Cell<Option<NonNull<Self>>>,
    next: Cell<Option<NonNull<Self>>>,
    tail: Cell<Option<NonNull<Self>>>,
    waker: Waker,
    _pinned: PhantomPinned,
}

struct RawLock {
    state: AtomicUsize,
}

impl RawLock {
    const fn new() -> Self {
        Self {
            state: AtomicUsize::new(UNLOCKED),
        }
    }

    #[inline]
    fn lock<P: Parker>(&self) {
        if let Err(_) = self.state.compare_exchange_weak(
            UNLOCKED,
            LOCKED_BIT,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            self.lock_slow::<P>();
        }
    }

    #[cold]
    fn lock_slow<P: Parker>(&self) {
        loop {
            let mut spin = 0;
            loop {
                let state =
                    match self
                        .state
                        .fetch_update(Ordering::Acquire, Ordering::Relaxed, |state| {
                            match state & LOCKED_BIT {
                                0 => Some(state | LOCKED_BIT),
                                _ => None,
                            }
                        }) {
                        Ok(_) => return,
                        Err(e) => e,
                    };

                let head = NonNull::new((state & WAITER_MASK) as *mut Waiter);
                if head.is_none() && P::yield_now(spin) {
                    spin += 1;
                    continue;
                }

                break P::with(|parker| unsafe {
                    let waiter = Waiter {
                        prev: Cell::new(None),
                        next: Cell::new(head),
                        tail: Cell::new(None),
                        waker: from_parker(parker),
                        _pinned: PhantomPinned,
                    };

                    let waiter = Pin::new_unchecked(&waiter);
                    if head.is_none() {
                        waiter.tail.set(Some(NonNull::from(&*waiter)));
                    }

                    let waiter_ptr = &*waiter as *const Waiter as usize;
                    assert_eq!(waiter_ptr & !WAITER_MASK, 0);

                    match self.state.compare_exchange_weak(
                        state,
                        (state & !WAITER_MASK) | waiter_ptr,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => assert!(parker.park(None)),
                        Err(_) => spin_loop(),
                    }
                });
            }
        }
    }

    fn should_wake(state: usize) -> bool {
        let is_not_waking = state & WAKING_BIT == 0;
        let has_waiters = state & WAITER_MASK != 0;
        has_waiters && is_not_waking
    }

    #[inline]
    unsafe fn unlock(&self) {
        let state = self.state.fetch_sub(LOCKED_BIT, Ordering::Release);
        debug_assert_ne!(state & LOCKED_BIT, 0);

        if Self::should_wake(state) {
            self.unlock_slow();
        }
    }

    #[cold]
    unsafe fn unlock_slow(&self) {
        let mut state =
            match self
                .state
                .fetch_update(Ordering::Acquire, Ordering::Relaxed, |state| {
                    if Self::should_wake(state) {
                        Some(state | WAKING_BIT)
                    } else {
                        None
                    }
                }) {
                Ok(state) => state | WAKING_BIT,
                Err(_) => return,
            };

        'dequeue: loop {
            let head = NonNull::new((state & WAITER_MASK) as *mut Waiter).unwrap();
            let tail = head.as_ref().tail.get().unwrap_or_else(|| {
                let mut current = head;
                loop {
                    let next = current.as_ref().next.get().unwrap();
                    next.as_ref().prev.set(Some(current));
                    current = next;

                    if let Some(tail) = current.as_ref().tail.get() {
                        head.as_ref().tail.set(Some(tail));
                        break tail;
                    }
                }
            });

            if state & LOCKED_BIT != 0 {
                match self.state.compare_exchange_weak(
                    state,
                    state & !WAKING_BIT,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return,
                    Err(e) => state = e,
                }

                fence(Ordering::Acquire);
                continue;
            }

            match tail.as_ref().prev.get() {
                Some(new_tail) => {
                    head.as_ref().tail.set(Some(new_tail));
                    self.state.fetch_and(!WAKING_BIT, Ordering::Release);
                }
                None => loop {
                    match self.state.compare_exchange_weak(
                        state,
                        state & LOCKED_BIT,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(e) => state = e,
                    }

                    if state & WAITER_MASK != 0 {
                        fence(Ordering::Acquire);
                        continue 'dequeue;
                    }
                },
            }

            tail.as_ref().waker.wake_by_ref();
            return;
        }
    }
}
