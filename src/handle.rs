use super::{scheduler::Scheduler, enter::EnterGuard, thread::{Thread, ThreadContext}, task::{Task, JoinHandle}};
use std::{
    fmt,
    sync::Arc,
    future::Future,
};

pub struct Handle {
    pub(super) scheduler: Arc<Scheduler>,
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handle").finish()
    }
}

impl Clone for Handle {
    fn clone(&self) -> Self {
        Self {
            scheduler: self.scheduler.clone(),
        }
    }
}

impl Handle {
    pub fn current() -> Self {
        Self {
            scheduler: Thread::current().scheduler.clone(),
        }
    }

    pub fn try_current() -> Option<Self> {
        Thread::try_current().map(|thread| Self {
            scheduler: thread.scheduler.clone(),
        })
    }

    pub fn enter(&self) -> EnterGuard<'_> {
        EnterGuard::from(&self.scheduler)
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let future = Box::pin(future);
        Task::block_on(&self.scheduler, future)
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let future = Box::pin(future);
        let thread = Thread::enter(&self.scheduler, None);
        Task::spawn(&self.scheduler, Some(&thread), future)
    }
}