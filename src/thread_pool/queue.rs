use super::task::Runnable;
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, Mutex},
};

#[derive(Default)]
pub(super) struct Queue {
    pending: AtomicBool,
    deque: Mutex<VecDeque<Arc<dyn Runnable>>>,
}

impl Queue {
    pub(super) fn is_empty(&self) -> bool {
        !self.pending.load(Ordering::Acquire)
    }

    pub(super) fn push(&self, runnable: Arc<dyn Runnable>) {
        let mut deque = self.deque.lock().unwrap();
        deque.push_back(runnable);
        self.pending.store(true, Ordering::Relaxed);
    }

    pub(super) fn pop(&self) -> Option<Arc<dyn Runnable>> {
        if self.is_empty() {
            return None;
        }

        let mut deque = self.deque.lock().unwrap();
        let runnable = deque.pop_front()?;

        self.pending.store(deque.len() > 0, Ordering::Relaxed);
        Some(runnable)
    }

    pub(super) fn steal(&self) -> Result<Option<Arc<dyn Runnable>>, ()> {
        if self.is_empty() {
            return Ok(None);
        }

        let mut deque = match self.deque.try_lock() {
            Ok(guard) => guard,
            Err(_) => return Err(()),
        };

        let runnable = match deque.pop_front() {
            Some(runnable) => runnable,
            None => return Ok(None),
        };

        self.pending.store(deque.len() > 0, Ordering::Relaxed);
        Ok(Some(runnable))
    }
}
