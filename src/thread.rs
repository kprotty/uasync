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
    fn with_tls<F>(f: impl FnOnce(&mut Option<Rc<Self>>) -> F) -> F {
        thread_local!(static TLS: RefCell<Option<Rc<Thread>>> = RefCell::new(None));
        TLS.with(|ref_cell| f(&mut *ref_cell.borrow_mut()))
    }
}

pub(super) struct Thread {
    context: Rc<ThreadContext>,
}

impl Thread {
    pub(super) fn try_current() -> Option<Self> {
        ThreadContext::with_tls(|tls| tls.as_ref().map(|context| Self { context: context.clone() }))
    }

    pub(super) fn current() -> Self {
        Self::try_current().expect("Thread not running in the context of an Runtime")
    }

    pub(super) fn enter(scheduler: &Arc<Scheduler>, queue_index: Option<usize>) -> Self {
        ThreadContext::with_tls(|tls| {
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
        })
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