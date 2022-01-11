use super::{scheduler::Scheduler, thread::Thread, waker::AtomicWaker};
use std::{
    any::Any,
    fmt,
    future::Future,
    io,
    mem::{drop, replace},
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicU8, Ordering},
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
    thread,
};

pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let future = Box::pin(future);
    let thread = Thread::current();
    Task::spawn(&thread.scheduler, Some(&thread), future)
}

pub struct JoinHandle<T> {
    joinable: Option<Arc<dyn Joinable<T>>>,
}

impl<T> JoinHandle<T> {
    pub fn abort(&self) {
        if let Some(joinable) = self.joinable.as_ref() {
            if joinable.poll_abort() {
                joinable.clone().abort();
            }
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle").finish()
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let joinable = self
            .joinable
            .take()
            .expect("JoinHandle polled after completion");

        if let Poll::Ready(result) = joinable.poll_join(ctx) {
            return Poll::Ready(result);
        }

        self.joinable = Some(joinable);
        Poll::Pending
    }
}

pub struct JoinError {
    panic: Option<Box<dyn Any + Send + 'static>>,
}

impl std::error::Error for JoinError {}

impl From<JoinError> for io::Error {
    fn from(this: JoinError) -> io::Error {
        io::Error::new(
            io::ErrorKind::Other,
            match &this.panic {
                Some(_) => "task panicked",
                None => "task was cancelled",
            },
        )
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.panic {
            Some(_) => write!(f, "panic"),
            None => write!(f, "cancelled"),
        }
    }
}

impl fmt::Debug for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.panic {
            Some(_) => write!(f, "JoinError::Panic(_)"),
            None => write!(f, "JoinError::Cancelled"),
        }
    }
}

impl JoinError {
    pub fn is_cancelled(&self) -> bool {
        self.panic.is_none()
    }

    pub fn is_panic(&self) -> bool {
        self.panic.is_some()
    }

    pub fn try_into_panic(self) -> Result<Box<dyn Any + Send + 'static>, JoinError> {
        match self.panic {
            Some(panic) => Ok(panic),
            None => Err(self),
        }
    }

    pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
        self.try_into_panic().unwrap()
    }
}

pub async fn yield_now() {
    struct YieldFuture {
        yielded: bool,
    }

    impl Future for YieldFuture {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.yielded {
                return Poll::Ready(());
            }

            ctx.waker().wake_by_ref();
            self.yielded = true;
            return Poll::Pending;
        }
    }

    YieldFuture { yielded: false }.await
}

#[derive(Default)]
struct TaskState {
    state: AtomicU8,
}

impl TaskState {
    const IDLE: u8 = 0;
    const SCHEDULED: u8 = 1;
    const RUNNING: u8 = 2;
    const NOTIFIED: u8 = 3;
    const ABORTED: u8 = 4;

    fn transition_to_scheduled(&self) -> bool {
        self.state
            .fetch_update(Ordering::Release, Ordering::Relaxed, |state| match state {
                Self::RUNNING => Some(Self::NOTIFIED),
                Self::IDLE => Some(Self::SCHEDULED),
                _ => None,
            })
            .map(|state| state == Self::IDLE)
            .unwrap_or(false)
    }

    fn transition_to_running(&self) -> bool {
        match self.state.swap(Self::RUNNING, Ordering::Acquire) {
            Self::SCHEDULED => true,
            Self::ABORTED => false,
            _ => unreachable!(),
        }
    }

    fn transition_to_idle(&self) -> bool {
        self.state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| match state {
                Self::RUNNING => Some(Self::IDLE),
                Self::NOTIFIED => Some(Self::SCHEDULED),
                Self::ABORTED => None,
                _ => unreachable!(),
            })
            .map(|state| state == Self::RUNNING)
            .unwrap_or(false)
    }

    fn transition_to_aborted(&self) -> bool {
        match self.state.swap(Self::ABORTED, Ordering::Release) {
            Self::IDLE => true,
            _ => false,
        }
    }
}

enum TaskData<F: Future> {
    Empty,
    Polling(Pin<Box<F>>, Waker),
    Ready(F::Output),
    Panic(Box<dyn Any + Send + 'static>),
    Aborted,
}

pub(super) struct Task<F: Future> {
    state: TaskState,
    waker: AtomicWaker,
    data: Mutex<TaskData<F>>,
    scheduler: Arc<Scheduler>,
}

impl<F: Future> Task<F> {
    pub(super) fn block_on(scheduler: &Arc<Scheduler>, mut future: Pin<Box<F>>) -> F::Output {
        struct Parker {
            thread: thread::Thread,
            unparked: AtomicBool,
        }

        impl Parker {
            fn new() -> Self {
                Self {
                    thread: thread::current(),
                    unparked: AtomicBool::new(false),
                }
            }

            fn park(&self) {
                while !self.unparked.swap(false, Ordering::Acquire) {
                    thread::park();
                }
            }
        }

        impl Wake for Parker {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref()
            }

            fn wake_by_ref(self: &Arc<Self>) {
                if !self.unparked.swap(true, Ordering::Release) {
                    self.thread.unpark();
                }
            }
        }

        let result = {
            let parker = Arc::new(Parker::new());
            let waker = Waker::from(parker.clone());
            let mut ctx = Context::from_waker(&waker);

            let thread = Thread::enter(scheduler, None);
            if !Arc::ptr_eq(&thread.scheduler, scheduler) {
                unreachable!("nested block_on() is not supported");
            }

            scheduler.on_task_begin();
            catch_unwind(AssertUnwindSafe(|| loop {
                match future.as_mut().poll(&mut ctx) {
                    Poll::Ready(output) => break output,
                    Poll::Pending => parker.park(),
                }
            }))
        };

        scheduler.on_task_complete();
        match result {
            Ok(output) => output,
            Err(error) => resume_unwind(error),
        }
    }
}

impl<F> Task<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    pub(super) fn spawn(
        scheduler: &Arc<Scheduler>,
        thread: Option<&Thread>,
        future: Pin<Box<F>>,
    ) -> JoinHandle<F::Output> {
        let task = Arc::new(Task {
            state: TaskState::default(),
            waker: AtomicWaker::default(),
            data: Mutex::new(TaskData::Empty),
            scheduler: scheduler.clone(),
        });

        let waker = Waker::from(task.clone());
        *task.data.try_lock().unwrap() = TaskData::Polling(future, waker);

        scheduler.on_task_begin();
        assert!(task.state.transition_to_scheduled());
        scheduler.schedule(task.clone(), thread, false);

        JoinHandle {
            joinable: Some(task),
        }
    }
}

impl<F> Wake for Task<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn wake(self: Arc<Self>) {
        if self.state.transition_to_scheduled() {
            match Thread::try_current().ok().flatten() {
                Some(thread) => thread.scheduler.schedule(self, Some(&thread), false),
                None => self.scheduler.schedule(self.clone(), None, false),
            }
        }
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.state.transition_to_scheduled() {
            let task = self.clone();
            match Thread::try_current().ok().flatten() {
                Some(thread) => thread.scheduler.schedule(task, Some(&thread), false),
                None => self.scheduler.schedule(task, None, false),
            }
        }
    }
}

pub(super) trait Runnable: Send + Sync {
    fn run(self: Arc<Self>, thread: &Thread);
}

impl<F> Runnable for Task<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn run(self: Arc<Self>, thread: &Thread) {
        let mut data = self.data.try_lock().unwrap();
        *data = match self.state.transition_to_running() {
            false => TaskData::Aborted,
            _ => {
                let poll_result = match &mut *data {
                    TaskData::Polling(future, waker) => {
                        let future = future.as_mut();
                        let mut ctx = Context::from_waker(waker);
                        catch_unwind(AssertUnwindSafe(|| future.poll(&mut ctx)))
                    }
                    _ => unreachable!(),
                };

                match poll_result {
                    Err(error) => TaskData::Panic(error),
                    Ok(Poll::Ready(output)) => TaskData::Ready(output),
                    Ok(Poll::Pending) => {
                        drop(data);
                        return match self.state.transition_to_idle() {
                            false => thread.scheduler.schedule(self, Some(thread), true),
                            _ => {}
                        };
                    }
                }
            }
        };

        drop(data);
        self.waker.wake();
        thread.scheduler.on_task_complete();
    }
}

trait Joinable<T>: Send + Sync {
    fn poll_join(&self, ctx: &mut Context<'_>) -> Poll<Result<T, JoinError>>;
    fn poll_abort(&self) -> bool;
    fn abort(self: Arc<Self>);
}

impl<F> Joinable<F::Output> for Task<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn poll_join(&self, ctx: &mut Context<'_>) -> Poll<Result<F::Output, JoinError>> {
        if let Poll::Pending = self.waker.poll(ctx) {
            return Poll::Pending;
        }

        match replace(&mut *self.data.try_lock().unwrap(), TaskData::Empty) {
            TaskData::Ready(output) => Poll::Ready(Ok(output)),
            TaskData::Aborted => Poll::Ready(Err(JoinError { panic: None })),
            TaskData::Panic(error) => Poll::Ready(Err(JoinError { panic: Some(error) })),
            _ => unreachable!(),
        }
    }

    fn poll_abort(&self) -> bool {
        self.state.transition_to_aborted()
    }

    fn abort(self: Arc<Self>) {
        self.wake()
    }
}
