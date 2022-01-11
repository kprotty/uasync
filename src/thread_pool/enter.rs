use super::{scheduler::Scheduler, thread::Thread};
use std::{fmt, sync::Arc};

pub struct EnterGuard<'a> {
    _scheduler: &'a Scheduler,
    _thread: Thread,
}

impl<'a> fmt::Debug for EnterGuard<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnterGuard").finish()
    }
}

impl<'a> EnterGuard<'a> {
    pub(super) fn from(scheduler: &'a Arc<Scheduler>) -> Self {
        Self {
            _scheduler: &*scheduler,
            _thread: Thread::enter(scheduler, None),
        }
    }
}
