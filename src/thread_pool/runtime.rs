use super::{handle::Handle, enter::EnterGuard, task::{Task, JoinHandle}, thread::{Thread, ThreadContext}};
use std::{
    fmt,
    io,
    time::Duration,
    future::Future,
};

pub struct Runtime {
    handle: Handle,
    shutdown: bool,
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime").finish()
    }
}

impl Runtime {
    pub(crate) fn from(handle: Handle) -> Self {
        Self {
            handle,
            shutdown: true,
        }
    }

    pub fn new() -> io::Result<Self> {
        Builder::new_multi_thread().build()
    }

    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    pub fn enter(&self) -> EnterGuard<'_> {
        self.handle.enter()
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let future = Box::pin(future);
        Task::block_on(&self.handle.scheduler, future)
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let future = Box::pin(future);
        Task::spawn(&self.handle.scheduler, None, future)
    }

    pub fn shutdown_background(self) {
        let mut mut_self = self;
        mut_self.shutdown_and_join(None);
    }

    pub fn shutdown_timeout(self, timeout: Duration) {
        let mut mut_self = self;
        mut_self.shutdown_and_join(Some(Some(timeout)));
    }

    fn shutdown_and_join(&mut self, join_timeout: Option<Option<Duration>>) {
        if replace(&mut self.shutdown, false) {
            self.handle.scheduler.shutdown();

            if let Some(timeout) = join_timeout {
                self.handle.scheduler.join(timeout);
            }
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.shutdown_and_join(Some(None))
    }
}