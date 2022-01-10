#![forbid(unsafe_code)]
#![warn(
    rust_2018_idioms,
    unreachable_pub,
    // missing_docs,
    // missing_debug_implementations,
)]

use std::{
    any::Any,
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::Future,
    mem::replace,
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    pin::Pin,
    rc::Rc,
    sync::atomic::{fence, AtomicBool, AtomicIsize, AtomicU8, AtomicUsize, Ordering},
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

pub struct Builder {
    stack_size: Option<NonZeroUsize>,
    worker_threads: Option<NonZeroUsize>,
    on_thread_park: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
    on_thread_unpark: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
    on_thread_start: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
    on_thread_stop: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
    on_thread_name: Option<Arc<dyn Fn() -> String + Send + Sync + 'static>>,
}

impl Builder {
    pub fn new_multi_thread() -> Self {
        Self {
            stack_size: None,
            worker_threads: None,
            on_thread_park: None,
            on_thread_unpark: None,
            on_thread_start: None,
            on_thread_stop: None,
            on_thread_name: None,
        }
    }

    pub fn worker_threads(&mut self, worker_threads: usize) -> &mut Self {
        self.worker_threads = NonZeroUsize::new(worker_threads);
        self
    }

    pub fn thread_stack_size(&mut self, stack_size: usize) -> &mut Self {
        self.stack_size = NonZeroUsize::new(stack_size);
        self
    }

    pub fn thread_name(&mut self, name: impl Into<String>) -> &mut Self {
        let name: String = name.into();
        self.thread_name_fn(move || name.clone())
    }

    pub fn thread_name_fn(
        &mut self,
        callback: impl Fn() -> String + Send + Sync + 'static,
    ) -> &mut Self {
        self.on_thread_name = Some(Arc::new(callback));
        self
    }

    pub fn on_thread_park(&mut self, callback: impl Fn() + Send + Sync + 'static) -> &mut Self {
        self.on_thread_park = Some(Arc::new(callback));
        self
    }

    pub fn on_thread_unpark(&mut self, callback: impl Fn() + Send + Sync + 'static) -> &mut Self {
        self.on_thread_unpark = Some(Arc::new(callback));
        self
    }

    pub fn on_thread_start(&mut self, callback: impl Fn() + Send + Sync + 'static) -> &mut Self {
        self.on_thread_start = Some(Arc::new(callback));
        self
    }

    pub fn on_thread_stop(&mut self, callback: impl Fn() + Send + Sync + 'static) -> &mut Self {
        self.on_thread_stop = Some(Arc::new(callback));
        self
    }

    pub fn build(&mut self) -> std::io::Result<Executor> {
        let scheduler = Arc::new(Scheduler::new(
            self.worker_threads.or(NonZeroUsize::new(1)).unwrap(),
            self.on_thread_park.as_ref().map(Arc::clone),
            self.on_thread_unpark.as_ref().map(Arc::clone),
        ));

        for queue_index in 0..scheduler.run_queues.len() {
            let mut builder = thread::Builder::new();
            if let Some(on_thread_name) = self.on_thread_name.as_ref() {
                builder = builder.name((on_thread_name)());
            }
            if let Some(stack_size) = self.stack_size {
                builder = builder.stack_size(stack_size.get());
            }

            let spawned = {
                let scheduler = scheduler.clone();
                let on_thread_start = self.on_thread_start.as_ref().map(Arc::clone);
                let on_thread_stop = self.on_thread_stop.as_ref().map(Arc::clone);

                builder.spawn(move || {
                    if let Some(on_thread_start) = on_thread_start {
                        (on_thread_start)();
                    }

                    let _thread_enter = Thread::enter(&scheduler, Some(queue_index));
                    scheduler.run(queue_index);

                    if let Some(on_thread_stop) = on_thread_stop {
                        (on_thread_stop)();
                    }
                })
            };

            if let Err(error) = spawned {
                scheduler.shutdown();
                return Err(error);
            }
        }

        Ok(Executor {
            handle: Handle { scheduler },
            shutdown: true,
        })
    }
}

pub struct Executor {
    handle: Handle,
    shutdown: bool,
}

impl Executor {
    pub fn new() -> std::io::Result<Self> {
        Builder::new_multi_thread().build()
    }

    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    pub fn enter(&self) -> EnterGuard<'_> {
        self.handle.enter()
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.handle.scheduler.block_on(Box::pin(future))
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
        mut_self.shutdown_and_join(None)
    }

    pub fn shutdown_timeout(self, timeout: Duration) {
        let mut mut_self = self;
        mut_self.shutdown_and_join(Some(Some(timeout)))
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

impl Drop for Executor {
    fn drop(&mut self) {
        self.shutdown_and_join(Some(None))
    }
}

pub struct Handle {
    scheduler: Arc<Scheduler>,
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
        EnterGuard {
            _handle: self,
            _thread_enter: Thread::enter(&self.scheduler, None),
        }
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.scheduler.block_on(Box::pin(future))
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let future = Box::pin(future);
        let thread = Thread::current();
        Task::spawn(&self.scheduler, Some(&thread), future)
    }
}

pub struct EnterGuard<'a> {
    _handle: &'a Handle,
    _thread_enter: ThreadEnter,
}

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

impl<T> Future for JoinHandle<T> {
    type Output = T;

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
struct TaskWaker {
    state: AtomicU8,
    waker: Mutex<Option<Waker>>,
}

impl TaskWaker {
    const EMPTY: u8 = 0;
    const UPDATING: u8 = 1;
    const WAITING: u8 = 2;
    const NOTIFIED: u8 = 3;

    fn poll(&self, ctx: &mut Context<'_>) -> Poll<()> {
        let state = self.state.load(Ordering::Acquire);
        match state {
            Self::EMPTY | Self::WAITING => {}
            Self::NOTIFIED => return Poll::Ready(()),
            _ => unreachable!(),
        }

        if let Err(state) =
            self.state
                .compare_exchange(state, Self::UPDATING, Ordering::Acquire, Ordering::Acquire)
        {
            assert_eq!(state, Self::NOTIFIED);
            return Poll::Ready(());
        }

        {
            let mut waker = self.waker.try_lock().unwrap();
            let will_wake = waker
                .as_ref()
                .map(|w| ctx.waker().will_wake(w))
                .unwrap_or(false);

            if !will_wake {
                *waker = Some(ctx.waker().clone());
            }
        }

        match self.state.compare_exchange(
            Self::UPDATING,
            Self::WAITING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Poll::Pending,
            Err(Self::NOTIFIED) => Poll::Ready(()),
            Err(_) => unreachable!(),
        }
    }

    fn wake(&self) {
        match self.state.swap(Self::NOTIFIED, Ordering::AcqRel) {
            Self::EMPTY | Self::UPDATING => return,
            Self::WAITING => {}
            _ => unreachable!(),
        }

        let mut waker = self.waker.try_lock().unwrap();
        replace(&mut *waker, None).unwrap().wake();
    }
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
        self.state.load(Ordering::Acquire) == Self::SCHEDULED && {
            self.state.store(Self::RUNNING, Ordering::Relaxed);
            true
        }
    }

    fn transition_to_idle(&self) -> bool {
        self.state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| match state {
                Self::RUNNING => Some(Self::IDLE),
                Self::NOTIFIED => Some(Self::SCHEDULED),
                _ => unreachable!(),
            })
            .map(|state| state == Self::RUNNING)
            .unwrap_or(false)
    }
}

enum TaskData<F: Future> {
    Empty,
    Polling(Pin<Box<F>>, Waker),
    Ready(F::Output),
    Panic(Box<dyn Any + Send + 'static>),
}

struct Task<F: Future> {
    state: TaskState,
    waker: TaskWaker,
    data: Mutex<TaskData<F>>,
    scheduler: Arc<Scheduler>,
}

impl<F> Task<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn spawn(
        scheduler: &Arc<Scheduler>,
        thread: Option<&Rc<Thread>>,
        future: Pin<Box<F>>,
    ) -> JoinHandle<F::Output> {
        let task = Arc::new(Task {
            state: TaskState::default(),
            waker: TaskWaker::default(),
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
            match Thread::try_current() {
                Some(thread) => thread.scheduler.schedule(self, Some(&thread), false),
                None => self.scheduler.schedule(self.clone(), None, false),
            }
        }
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.state.transition_to_scheduled() {
            let task = self.clone();
            match Thread::try_current() {
                Some(thread) => thread.scheduler.schedule(task, Some(&thread), false),
                None => self.scheduler.schedule(task, None, false),
            }
        }
    }
}

trait Runnable: Send + Sync {
    fn run(self: Arc<Self>, thread: &Rc<Thread>);
}

impl<F> Runnable for Task<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn run(self: Arc<Self>, thread: &Rc<Thread>) {
        let mut data = self.data.try_lock().unwrap();
        assert!(self.state.transition_to_running());

        *data = {
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
                        true => {}
                        false => thread.scheduler.schedule(self, Some(thread), true),
                    };
                }
            }
        };

        drop(data);
        self.waker.wake();
        thread.scheduler.on_task_complete();
    }
}

trait Joinable<T>: Send + Sync {
    fn poll_join(&self, ctx: &mut Context<'_>) -> Poll<T>;
}

impl<F> Joinable<F::Output> for Task<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn poll_join(&self, ctx: &mut Context<'_>) -> Poll<F::Output> {
        if let Poll::Pending = self.waker.poll(ctx) {
            return Poll::Pending;
        }

        match replace(&mut *self.data.try_lock().unwrap(), TaskData::Empty) {
            TaskData::Ready(output) => Poll::Ready(output),
            TaskData::Panic(error) => resume_unwind(error),
            _ => unreachable!(),
        }
    }
}

struct Thread {
    scheduler: Arc<Scheduler>,
    queue_index: Option<usize>,
    be_fair: Cell<bool>,
    lifo_slot: Cell<Option<Arc<dyn Runnable>>>,
}

impl Thread {
    fn with_tls<F>(f: impl FnOnce(&mut Option<Rc<Self>>) -> F) -> F {
        thread_local!(static TLS: RefCell<Option<Rc<Thread>>> = RefCell::new(None));
        TLS.with(|ref_cell| f(&mut *ref_cell.borrow_mut()))
    }

    fn try_current() -> Option<Rc<Self>> {
        Self::with_tls(|tls| tls.as_ref().map(Rc::clone))
    }

    fn current() -> Rc<Self> {
        Self::try_current().expect("Thread not running in the context of an Executor")
    }

    fn enter(scheduler: &Arc<Scheduler>, queue_index: Option<usize>) -> ThreadEnter {
        Self::with_tls(|tls| {
            if let Some(thread) = tls {
                return ThreadEnter {
                    thread: thread.clone(),
                };
            }

            let thread = Rc::new(Self {
                scheduler: scheduler.clone(),
                queue_index,
                be_fair: Cell::new(false),
                lifo_slot: Cell::new(None),
            });

            *tls = Some(thread.clone());
            ThreadEnter { thread }
        })
    }
}

struct ThreadEnter {
    thread: Rc<Thread>,
}

impl Drop for ThreadEnter {
    fn drop(&mut self) {
        assert!(Rc::strong_count(&self.thread) == 2);
        let thread = Thread::with_tls(|tls| replace(tls, None)).unwrap();
        assert!(Rc::ptr_eq(&thread, &self.thread));
    }
}

struct Scheduler {
    join_semaphore: Semaphore,
    idle_semaphore: Semaphore,
    co_prime: NonZeroUsize,
    tasks: AtomicUsize,
    state: AtomicUsize,
    injector: Queue,
    run_queues: Box<[Queue]>,
    on_thread_park: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
    on_thread_unpark: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
}

impl Scheduler {
    fn new(
        mut num_workers: NonZeroUsize,
        on_thread_park: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
        on_thread_unpark: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
    ) -> Self {
        let gcd = |mut a: usize, mut b: usize| loop {
            match () {
                _ if a == b => break a,
                _ if a > b => a -= b,
                _ => b -= a,
            }
        };

        num_workers = NonZeroUsize::new(num_workers.get().min(Self::STATE_MASK)).unwrap();
        let co_prime = (num_workers.get() / 2..num_workers.get())
            .filter(|&n| gcd(n, num_workers.get()) == 1)
            .next()
            .unwrap_or(1);

        Self {
            join_semaphore: Semaphore::default(),
            idle_semaphore: Semaphore::default(),
            co_prime: NonZeroUsize::new(co_prime).unwrap(),
            tasks: AtomicUsize::new(0),
            state: AtomicUsize::new(0),
            injector: Queue::default(),
            run_queues: (0..num_workers.get()).map(|_| Queue::default()).collect(),
            on_thread_park,
            on_thread_unpark,
        }
    }

    fn block_on<F: Future, FutRef: Deref<Target = F> + DerefMut>(
        self: &Arc<Self>,
        mut future: Pin<FutRef>,
    ) -> F::Output {
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

        let parker = Arc::new(Parker::new());
        let waker = Waker::from(parker.clone());
        let mut ctx = Context::from_waker(&waker);

        let thread_enter = Thread::enter(self, None);
        if !Arc::ptr_eq(&thread_enter.thread.scheduler, self) {
            unreachable!("nested block_on() is not supported");
        }

        loop {
            match future.as_mut().poll(&mut ctx) {
                Poll::Ready(output) => return output,
                Poll::Pending => parker.park(),
            }
        }
    }

    fn schedule(&self, runnable: Arc<dyn Runnable>, thread: Option<&Rc<Thread>>, be_fair: bool) {
        if let Some(thread) = thread {
            if let Some(queue_index) = thread.queue_index {
                let mut runnable = Some(runnable);
                if be_fair {
                    thread.be_fair.set(true);
                } else {
                    runnable = thread.lifo_slot.replace(runnable);
                }

                if let Some(runnable) = runnable {
                    self.run_queues[queue_index].push(runnable);
                    self.unpark();
                }

                return;
            }
        }

        self.injector.push(runnable);
        fence(Ordering::SeqCst);
        self.unpark()
    }

    const STATE_BITS: u32 = (usize::BITS - 1) / 2;
    const STATE_MASK: usize = (1 << Self::STATE_BITS) - 1;
    const SHUTDOWN_MASK: usize = 1 << (usize::BITS - 1);

    const IDLE_SHIFT: u32 = Self::STATE_BITS * 0;
    const SEARCHING_SHIFT: u32 = Self::STATE_BITS * 1;

    fn unpark(&self) {
        self.state
            .fetch_update(Ordering::Release, Ordering::Relaxed, |state| {
                if state & Self::SHUTDOWN_MASK != 0 {
                    return None;
                }

                let searching = (state >> Self::SEARCHING_SHIFT) & Self::STATE_MASK;
                debug_assert!(searching <= self.run_queues.len());
                if searching > 0 {
                    return None;
                }

                let mut idle = (state >> Self::IDLE_SHIFT) & Self::STATE_MASK;
                debug_assert!(idle <= self.run_queues.len());
                idle = idle.checked_sub(1)?;

                Some((1 << Self::SEARCHING_SHIFT) | (idle << Self::IDLE_SHIFT))
            })
            .map(|_| self.idle_semaphore.post(1))
            .unwrap_or(())
    }

    fn search(&self, was_searching: bool) -> bool {
        if was_searching {
            return true;
        }

        let state = self.state.load(Ordering::Relaxed);
        let searching = (state >> Self::SEARCHING_SHIFT) & Self::STATE_MASK;
        debug_assert!(searching <= self.run_queues.len());
        if (2 * searching) >= self.run_queues.len() {
            return false;
        }

        let state = self
            .state
            .fetch_add(1 << Self::SEARCHING_SHIFT, Ordering::Acquire);

        let searching = (state >> Self::SEARCHING_SHIFT) & Self::STATE_MASK;
        debug_assert!(searching < self.run_queues.len());
        return true;
    }

    fn discovered(&self, was_searching: bool) -> bool {
        if was_searching {
            let state = self
                .state
                .fetch_sub(1 << Self::SEARCHING_SHIFT, Ordering::Release);

            let searching = (state >> Self::SEARCHING_SHIFT) & Self::STATE_MASK;
            debug_assert!(searching <= self.run_queues.len());
            debug_assert!(searching > 0);

            if searching == 1 {
                self.unpark();
            }
        }

        return false;
    }

    fn park(&self, was_searching: bool) -> Option<bool> {
        let mut update: usize = 1 << Self::IDLE_SHIFT;
        if was_searching {
            update = update.wrapping_sub(1 << Self::SEARCHING_SHIFT);
        }

        let state = self.state.fetch_add(update, Ordering::SeqCst);
        let idle = (state >> Self::IDLE_SHIFT) & Self::STATE_MASK;
        debug_assert!(idle < self.run_queues.len());

        let searching = (state >> Self::SEARCHING_SHIFT) & Self::STATE_MASK;
        debug_assert!(searching <= self.run_queues.len());
        debug_assert!(searching >= was_searching as usize);

        if state & Self::SHUTDOWN_MASK != 0 {
            let state = self
                .state
                .fetch_sub(1 << Self::IDLE_SHIFT, Ordering::Relaxed);

            let idle = (state >> Self::IDLE_SHIFT) & Self::STATE_MASK;
            debug_assert!(idle <= self.run_queues.len());
            debug_assert!(idle > 0);
            return None;
        }

        if was_searching && searching == 1 && !self.is_empty() {
            self.unpark();
        }

        if let Some(on_thread_park) = self.on_thread_park.as_ref() {
            (on_thread_park)();
        }

        self.idle_semaphore.wait(None);

        if let Some(on_thread_unpark) = self.on_thread_unpark.as_ref() {
            (on_thread_unpark)();
        }

        Some(true)
    }

    fn is_empty(&self) -> bool {
        self.run_queues
            .iter()
            .map(|queue| queue.is_empty())
            .find(|&is_empty| !is_empty)
            .unwrap_or_else(|| self.injector.is_empty())
    }

    fn shutdown(&self) {
        self.state
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |mut state| {
                let idle = (state >> Self::IDLE_SHIFT) & Self::STATE_MASK;
                state -= idle << Self::IDLE_SHIFT;
                state += idle << Self::SEARCHING_SHIFT;

                let searching = (state >> Self::SEARCHING_SHIFT) & Self::STATE_MASK;
                debug_assert!(searching <= self.run_queues.len());

                state |= Self::SHUTDOWN_MASK;
                Some(state)
            })
            .map(|state| {
                let idle = (state >> Self::IDLE_SHIFT) & Self::STATE_MASK;
                if idle > 0 {
                    self.idle_semaphore.post(idle);
                }
            })
            .unwrap_or(())
    }

    fn on_task_begin(&self) {
        let tasks = self.tasks.fetch_add(1, Ordering::Relaxed);
        assert_ne!(tasks, usize::MAX);
    }

    fn on_task_complete(&self) {
        let tasks = self.tasks.fetch_sub(1, Ordering::AcqRel);
        assert_ne!(tasks, usize::MAX);

        if tasks == 1 {
            let state = self.state.load(Ordering::Relaxed);
            if state & Self::SHUTDOWN_MASK != 0 {
                self.join_semaphore.post(1);
            }
        }
    }

    fn join(&self, timeout: Option<Duration>) {
        let mut tasks = self.tasks.load(Ordering::Acquire);

        if tasks == 0 {
            self.join_semaphore.wait(timeout);
            tasks = self.tasks.load(Ordering::Acquire);
        }

        if timeout.is_none() {
            assert_eq!(tasks, 0);
        }
    }

    fn run(self: &Arc<Self>, queue_index: usize) {
        let thread_enter = Thread::enter(self, Some(queue_index));
        let thread: &Rc<Thread> = &thread_enter.thread;

        let mut tick = queue_index;
        let mut is_searching = false;
        let mut xorshift = NonZeroUsize::new(queue_index)
            .or(NonZeroUsize::new(0xdeadbeef))
            .unwrap();

        let run_queue = &self.run_queues[queue_index];
        loop {
            let fairness = match thread.be_fair.take() || (tick % 61 == 0) {
                true => self.injector.steal().unwrap_or(None),
                false => None,
            };

            let polled = fairness.or_else(|| {
                thread.lifo_slot.take().or_else(|| {
                    run_queue.pop().or_else(|| {
                        is_searching = self.search(is_searching);
                        if is_searching {
                            for _ in 0..32 {
                                let shifts = match usize::BITS {
                                    64 => (13, 7, 17),
                                    32 => (13, 17, 5),
                                    _ => unreachable!(),
                                };

                                let mut xs = xorshift.get();
                                xs ^= xs >> shifts.0;
                                xs ^= xs << shifts.1;
                                xs ^= xs >> shifts.2;
                                xorshift = NonZeroUsize::new(xs).unwrap();

                                let range = self.run_queues.len();
                                let prime = self.co_prime.get();
                                let mut index = xs % range;
                                let mut is_empty = true;

                                for _ in 0..range {
                                    if index != queue_index {
                                        match self.run_queues[index].steal() {
                                            Ok(Some(runnable)) => return Some(runnable),
                                            Ok(None) => {}
                                            Err(_) => is_empty = false,
                                        }
                                    }

                                    index += prime;
                                    if index >= range {
                                        index -= range;
                                    }
                                }

                                match self.injector.steal() {
                                    Ok(Some(runnable)) => return Some(runnable),
                                    Ok(None) => {}
                                    Err(_) => is_empty = false,
                                }

                                if is_empty {
                                    break;
                                }
                            }
                        }

                        None
                    })
                })
            });

            if let Some(runnable) = polled {
                is_searching = self.discovered(is_searching);
                tick = tick.wrapping_add(1);
                runnable.run(thread);
                continue;
            }

            is_searching = match self.park(is_searching) {
                Some(searching) => searching,
                None => break,
            };
        }
    }
}

#[derive(Default)]
struct Semaphore {
    value: AtomicIsize,
    count: Mutex<usize>,
    cond: Condvar,
}

impl Semaphore {
    #[cold]
    fn sem_wait(&self, timeout: Option<Duration>) -> bool {
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
    fn sem_post(&self, wake: usize) {
        *self.count.lock().unwrap() += wake;
        match wake {
            1 => self.cond.notify_one(),
            _ => self.cond.notify_all(),
        }
    }

    fn wait(&self, timeout: Option<Duration>) {
        let value = self.value.fetch_sub(1, Ordering::Acquire);
        if value > 0 {
            return;
        }

        if self.sem_wait(timeout) {
            return;
        }

        let _ = self
            .value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |state| {
                if state < 0 {
                    Some(state + 1)
                } else {
                    self.sem_wait(None);
                    None
                }
            });
    }

    fn post(&self, n: usize) {
        let inc: isize = n.try_into().unwrap();
        let value = self.value.fetch_add(inc, Ordering::Release);
        if value >= 0 {
            return;
        }

        let wake: usize = inc.min(-value).try_into().unwrap();
        self.sem_post(wake)
    }
}

#[derive(Default)]
struct Queue {
    pending: AtomicBool,
    deque: Mutex<VecDeque<Arc<dyn Runnable>>>,
}

impl Queue {
    fn is_empty(&self) -> bool {
        !self.pending.load(Ordering::Acquire)
    }

    fn push(&self, runnable: Arc<dyn Runnable>) {
        let mut deque = self.deque.lock().unwrap();
        deque.push_back(runnable);
        self.pending.store(true, Ordering::Relaxed);
    }

    fn pop(&self) -> Option<Arc<dyn Runnable>> {
        if self.is_empty() {
            return None;
        }

        let mut deque = self.deque.lock().unwrap();
        let runnable = deque.pop_front()?;

        self.pending.store(deque.len() > 0, Ordering::Relaxed);
        Some(runnable)
    }

    fn steal(&self) -> Result<Option<Arc<dyn Runnable>>, ()> {
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
