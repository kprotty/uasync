use std::{
    sync::atomic::{AtomicIsize, Ordering},
    sync::{Condvar, Mutex},
    time::Duration,
};

#[derive(Default)]
pub(super) struct Semaphore {
    value: AtomicIsize,
    inner: InnerSemaphore,
}

impl Semaphore {
    pub(super) fn wait(&self, timeout: Option<Duration>) {
        let value = self.value.fetch_sub(1, Ordering::Acquire);
        if value > 0 {
            return;
        }

        if self.inner.wait(timeout) {
            return;
        }

        let _ = self
            .value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |state| {
                if state < 0 {
                    Some(state + 1)
                } else {
                    assert!(self.inner.wait(None));
                    None
                }
            });
    }

    pub(super) fn post(&self, n: usize) {
        let inc: isize = n.try_into().unwrap();
        let value = self.value.fetch_add(inc, Ordering::Release);
        if value >= 0 {
            return;
        }

        let wake: usize = inc.min(-value).try_into().unwrap();
        self.inner.post(wake)
    }
}

#[derive(Default)]
struct InnerSemaphore {
    count: Mutex<usize>,
    cond: Condvar,
}

impl InnerSemaphore {
    #[cold]
    fn wait(&self, timeout: Option<Duration>) -> bool {
        let mut count = self.count.lock().unwrap();
        let cond = |count: &mut usize| *count == 0;
        count = match timeout {
            None => self.cond.wait_while(count, cond).unwrap(),
            Some(dur) => self.cond.wait_timeout_while(count, dur, cond).unwrap().0,
        };

        *count > 0 && {
            *count -= 1;
            true
        }
    }

    #[cold]
    fn post(&self, wake: usize) {
        *self.count.lock().unwrap() += wake;
        match wake {
            1 => self.cond.notify_one(),
            _ => self.cond.notify_all(),
        }
    }
}
