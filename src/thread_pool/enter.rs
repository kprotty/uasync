use super::{scheduler::Scheduler, thread::Thread};

pub struct EnterGuard<'a> {
    _scheduler: &'a Scheduler,
    _thread: Thread,
}

impl<'a> EnterGuard<'a> {
    pub(super) fn from(_scheduler: &'a Scheduler) -> Self {
        Self {
            _scheduler,
            _thread: Thread::enter(_scheduler, None),
        }
    }
}