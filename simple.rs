#![forbid(unsafe_code)]
#![warn(
    rust_2018_idioms,
    unreachable_pub,
    missing_docs,
    missing_debug_implementations,
)]

use std::{
    any::Any,
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::Future,
    mem::replace,
    num::NonZeroUsize,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    pin::Pin,
    rc::Rc,
    sync::atomic::{fence, AtomicBool, AtomicIsize, AtomicU8, AtomicUsize, Ordering},
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Wake, Waker},
    thread,
};

//! An Executor represents a thread pool used to poll async tasks.
pub struct Executor {
    scheduler: Arc<Scheduler>,
}

impl Executor {
    //! Creates an executor that spawns **at most** `max_workers` threads for polling async tasks.
    pub fn with_workers(max_workers: usize) -> Self {
        Self {
            scheduler: Scheduler::new(max_workers),
        }
    }

    
}

    //! 
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
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

        let mut future = Box::pin(future);
        let parker = Arc::new(Parker::new());

        let waker = Waker::from(parker.clone());
        let mut ctx = Context::from_waker(&waker);

        let _thread_enter = Thread::enter(self.scheduler.clone(), None);
        loop {
            match future.as_mut().poll(&mut ctx) {
                Poll::Ready(output) => return output,
                Poll::Pending => parker.park(),
            }
        }
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        self.scheduler.shutdown();
    }
}

pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let future = Box::pin(future);
    let thread = Thread::current().expect("Not running inside executor context");

    let task = Arc::new(Task {
        state: TaskState::default(),
        waker: TaskWaker::default(),
        data: Mutex::new(TaskData::Empty),
        scheduler: thread.scheduler.clone(),
    });

    let waker = Waker::from(task.clone());
    *task.data.try_lock().unwrap() = TaskData::Polling(future, waker);

    assert!(task.state.transition_to_scheduled());
    thread.scheduler.schedule(task.clone(), Some(&thread), false);

    JoinHandle {
        joinable: Some(task),
    }
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

impl<F> Wake for Task<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn wake(self: Arc<Self>) {
        if self.state.transition_to_scheduled() {
            match Thread::current() {
                Some(thread) => thread.scheduler.schedule(self, Some(&thread), false),
                None => self.scheduler.schedule(self.clone(), None, false),
            }
        }
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.state.transition_to_scheduled() {
            match Thread::current() {
                Some(thread) => thread.scheduler.schedule(self.clone(), Some(&thread), false),
                None => self.scheduler.schedule(self.clone(), None, false),
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

    fn current() -> Option<Rc<Self>> {
        Self::with_tls(|tls| tls.as_ref().map(Rc::clone))
    }

    fn enter(scheduler: Arc<Scheduler>, queue_index: Option<usize>) -> ThreadEnter {
        Self::with_tls(|tls| {
            assert!(tls.is_none(), "nested block_on() is not supported");

            let thread = Rc::new(Self {
                scheduler,
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
    semaphore: Semaphore,
    co_prime: NonZeroUsize,
    state: AtomicUsize,
    injector: Queue,
    run_queues: Box<[Queue]>,
}

impl Scheduler {
    fn new(max_workers: usize) -> Arc<Self> {
        let gcd = |mut a: usize, mut b: usize| loop {
            match () {
                _ if a == b => break a,
                _ if a > b => a -= b,
                _ => b -= a,
            }
        };

        let num_workers = max_workers.max(1).min(Self::STATE_MASK);
        let co_prime = (num_workers / 2..num_workers)
            .filter(|&n| gcd(n, num_workers) == 1)
            .next()
            .unwrap_or(1);

        let scheduler = Arc::new(Self {
            semaphore: Semaphore::default(),
            co_prime: NonZeroUsize::new(co_prime).unwrap(),
            state: AtomicUsize::new(0),
            injector: Queue::default(),
            run_queues: (0..num_workers).map(|_| Queue::default()).collect(),
        });

        for queue_index in 0..num_workers {
            let scheduler = scheduler.clone();
            thread::spawn(move || scheduler.run(queue_index));
        }

        return scheduler;
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
            .map(|_| self.semaphore.post(1))
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

        self.semaphore.wait();
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
                    self.semaphore.post(idle);
                }
            })
            .unwrap_or(())
    }

    fn run(self: Arc<Self>, queue_index: usize) {
        let thread_enter = Thread::enter(self, Some(queue_index));
        let thread: &Rc<Thread> = &thread_enter.thread;

        let mut tick = queue_index;
        let mut is_searching = false;
        let mut xorshift = NonZeroUsize::new(queue_index)
            .or(NonZeroUsize::new(0xdeadbeef))
            .unwrap();

        let run_queue = &thread.scheduler.run_queues[queue_index];
        loop {
            let fairness = match thread.be_fair.take() || (tick % 61 == 0) {
                true => thread.scheduler.injector.steal().unwrap_or(None),
                false => None,
            };

            let polled = fairness.or_else(|| thread.lifo_slot.take().or_else(|| {
                run_queue.pop().or_else(|| {
                    is_searching = thread.scheduler.search(is_searching);
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

                            let range = thread.scheduler.run_queues.len();
                            let prime = thread.scheduler.co_prime.get();
                            let mut index = xs % range;
                            let mut is_empty = true;

                            for _ in 0..range {
                                if index != queue_index {
                                    match thread.scheduler.run_queues[index].steal() {
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

                            match thread.scheduler.injector.steal() {
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
            }));

            if let Some(runnable) = polled {
                is_searching = thread.scheduler.discovered(is_searching);
                tick = tick.wrapping_add(1);
                runnable.run(thread);
                continue;
            }

            is_searching = match thread.scheduler.park(is_searching) {
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
    fn wait(&self) {
        let value = self.value.fetch_sub(1, Ordering::Acquire);
        if value > 0 {
            return;
        }

        let mut count = self.count.lock().unwrap();
        count = self.cond.wait_while(count, |c| *c == 0).unwrap();
        *count -= 1;
    }

    fn post(&self, n: usize) {
        let inc: isize = n.try_into().unwrap();
        let value = self.value.fetch_add(inc, Ordering::Release);
        if value >= 0 {
            return;
        }

        let wake: usize = inc.min(-value).try_into().unwrap();
        *self.count.lock().unwrap() += wake;
        match wake {
            1 => self.cond.notify_one(),
            _ => self.cond.notify_all(),
        }
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
