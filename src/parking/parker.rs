use core::{ops::Add, time::Duration};

pub unsafe trait Parker: Sync {
    type Instant: Add<Duration> + Clone;

    fn now() -> Self::Instant;

    fn yield_now(iteration: usize) -> bool;

    fn with<F>(f: impl FnOnce(&Self) -> F) -> F;

    fn park(&self, deadline: Option<Self::Instant>) -> bool;

    fn unpark(&self);
}

#[cfg(feature = "std")]
pub use std_impl::Parker as StdParker;

#[cfg(feature = "std")]
mod std_impl {
    use std::{
        hint::spin_loop,
        sync::atomic::{AtomicBool, Ordering},
        thread,
    };

    pub struct Parker {
        unparked: AtomicBool,
        thread: thread::Thread,
    }

    impl super::Parker for Parker {
        type Instant = std::time::Instant;

        fn now() -> Self::Instant {
            Self::Instant::now()
        }

        fn yield_now(iteration: usize) -> bool {
            if iteration < 100 {
                spin_loop();
                true
            } else {
                false
            }
        }

        fn with<F>(f: impl FnOnce(&Self) -> F) -> F {
            f(&Self {
                unparked: AtomicBool::new(false),
                thread: thread::current(),
            })
        }

        fn park(&self, deadline: Option<Self::Instant>) -> bool {
            let mut park_deadline = None;
            loop {
                if self.unparked.swap(false, Ordering::Acquire) {
                    return true;
                }

                match deadline {
                    None => thread::park(),
                    Some(deadline) => {
                        let current = Self::now();
                        let target = park_deadline.unwrap_or_else(|| current + deadline);
                        park_deadline = Some(target);

                        match target.checked_duration_since(current) {
                            Some(timeout) => thread::park_timeout(timeout),
                            None => return false,
                        }
                    }
                }
            }
        }

        fn unpark(&self) {
            if !self.unparked.swap(true, Ordering::Release) {
                let thread = self.thread.clone();
                std::mem::drop(self);
                return thread.unpark();
            }
        }
    }
}
