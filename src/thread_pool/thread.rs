use super::{
    scheduler::Scheduler,
    task::Runnable,
};
use std::{
    rc::Rc,
    cell::{Cell, RefCell},
    ops::Deref,
    sync::Arc,
};

pub(super) struct ThreadContext {
    pub(super) scheduler: Arc<Scheduler>,
    pub(super) queue_index: Option<usize>,
    pub(super) be_fair: Cell<bool>,
    pub(super) lifo_slot: Cell<Option<Arc<dyn Runnable>>>,
}

impl ThreadContext {
    fn try_with<F>(f: impl FnOnce(&mut Option<Rc<Self>>) -> F) -> Result<F, ()> {
        thread_local!(static TLS: RefCell<Option<Rc<Thread>>> = RefCell::new(None));
        TLS.try_with(|ref_cell| f(&mut *ref_cell.borrow_mut())).ok()
    }
}

pub(super) struct Thread {
    context: Rc<ThreadContext>,
}

impl Thread {
    pub(super) const CONTEXT_MISSING_ERROR: &'static str = "Thread not running in the context of a Runtime";
    pub(super) const CONTEXT_DESTROYED_ERROR: &'static str = "ThreadLocal runtime context was destroyed";

    pub(super) fn try_current() -> Result<Option<Self>, ()> {
        let context = ThreadContext::try_with(|tls| tls.as_ref().map(Rc::clone))?;
        Ok(context.map(|context| Self { context }))
    }

    pub(super) fn current() -> Self {
        match Self::try_current() {
            Ok(Some(thread)) => thread,
            Ok(None) => unreachable!(Self::CONTEXT_MISSING_ERROR),
            Err(_) => unreachable!(Self::CONTEXT_DESTROYED_ERROR),
        }
    }

    pub(super) fn enter(scheduler: &Arc<Scheduler>, queue_index: Option<usize>) -> Self {
        ThreadContext::try_with(|tls| {
            if let Some(context) = tls {
                return Self { context: context.clone() };
            }

            let context = Rc::new(ThreadContext {
                scheduler: scheduler.clone(),
                queue_index,
                be_fair: Cell::new(false),
                lifo_slot: Cell::new(None),
            });

            *tls = Some(context.clone());
            Self { context }
        }).unwrap()
    }
}

impl Deref for Thread {
    type Target = ThreadContext;

    fn deref(&self) -> &Self::Target {
        &*self.context
    }
}

impl Drop for Thread {
    fn drop(&mut self) {
        assert!(Rc::strong_count(&self.context) == 2);
        let context = ThreadContext::with_tls(|tls| replace(tls, None)).unwrap();
        assert!(Rc::ptr_eq(&context, &self.context));
    }
}