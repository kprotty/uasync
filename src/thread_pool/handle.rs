use super::{
    enter::EnterGuard,
    scheduler::Scheduler,
    task::{JoinHandle, Task},
    thread::{Thread, ThreadContext},
};
use std::{fmt, future::Future, sync::Arc};

pub struct TryCurrentError {
    missing: bool,
}

impl TryCurrentError {
    pub fn is_missing_context(&self) -> bool {
        self.missing
    }

    pub fn is_thread_local_destroyed(&self) -> bool {
        !self.missing
    }
}

impl std::error::Error for TryCurrentError {}

impl fmt::Debug for TryCurrentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.missing {
            true => write!(f, "NoContext"),
            false => write!(f, "ThreadLocalDestroyed"),
        }
    }
}

impl fmt::Display for TryCurrentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.missing {
            true => write!(f, Thread::CONTEXT_MISSING_ERROR),
            false => write!(f, Thread::CONTEXT_DESTROYED_ERROR),
        }
    }
}

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

    pub fn try_current() -> Result<Self, TryCurrentError> {
        match Thread::try_current() {
            Ok(Some(thread)) => Ok(Self {
                scheduler: thread.scheduler.clone(),
            }),
            Ok(None) => Err(TryCurrentError { missing: true }),
            Err(_) => Err(TryCurrentError { missing: false }),
        }
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
