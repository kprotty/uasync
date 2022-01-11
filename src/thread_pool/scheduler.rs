use super::{
    queue::Queue,
    random::{RandomSequence, Rng},
    semaphore::Semaphore,
    task::Runnable,
    thread::Thread,
};
use std::{
    num::NonZeroUsize,
    sync::atomic::{fence, AtomicUsize, Ordering},
    sync::Arc,
    time::Duration,
};

pub(super) struct Scheduler {
    join_semaphore: Semaphore,
    idle_semaphore: Semaphore,
    rand_seq: RandomSequence,
    tasks: AtomicUsize,
    state: AtomicUsize,
    injector: Queue,
    run_queues: Box<[Queue]>,
    on_thread_park: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
    on_thread_unpark: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
}

impl Scheduler {
    pub(super) fn new(
        worker_threads: NonZeroUsize,
        on_thread_park: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
        on_thread_unpark: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
    ) -> Self {
        let worker_threads = worker_threads.get().min(Self::STATE_MASK);
        let worker_threads = NonZeroUsize::new(worker_threads).unwrap();

        Self {
            join_semaphore: Semaphore::default(),
            idle_semaphore: Semaphore::default(),
            rand_seq: RandomSequence::new(worker_threads),
            tasks: AtomicUsize::new(0),
            state: AtomicUsize::new(0),
            injector: Queue::default(),
            run_queues: (0..worker_threads.get())
                .map(|_| Queue::default())
                .collect(),
            on_thread_park,
            on_thread_unpark,
        }
    }

    pub(super) fn worker_threads(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.run_queues.len()).unwrap()
    }

    pub(super) fn schedule(
        &self,
        runnable: Arc<dyn Runnable>,
        thread: Option<&Thread>,
        be_fair: bool,
    ) {
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

    #[allow(clippy::erasing_op)]
    const IDLE_SHIFT: u32 = Self::STATE_BITS * 0;

    #[allow(clippy::identity_op)]
    const SEARCHING_SHIFT: u32 = Self::STATE_BITS * 1;

    fn unpark(&self) {
        self.state
            .fetch_update(Ordering::Release, Ordering::Relaxed, |state| {
                if state & Self::SHUTDOWN_MASK != 0 {
                    return None;
                }

                let searching = (state >> Self::SEARCHING_SHIFT) & Self::STATE_MASK;
                assert!(searching <= self.run_queues.len());
                if searching > 0 {
                    return None;
                }

                let mut idle = (state >> Self::IDLE_SHIFT) & Self::STATE_MASK;
                assert!(idle <= self.run_queues.len());
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
        assert!(searching <= self.run_queues.len());
        if (2 * searching) >= self.run_queues.len() {
            return false;
        }

        let state = self
            .state
            .fetch_add(1 << Self::SEARCHING_SHIFT, Ordering::Acquire);

        let searching = (state >> Self::SEARCHING_SHIFT) & Self::STATE_MASK;
        assert!(searching < self.run_queues.len());
        true
    }

    fn discovered(&self, was_searching: bool) -> bool {
        if was_searching {
            let state = self
                .state
                .fetch_sub(1 << Self::SEARCHING_SHIFT, Ordering::Release);

            let searching = (state >> Self::SEARCHING_SHIFT) & Self::STATE_MASK;
            assert!(searching <= self.run_queues.len());
            assert!(searching > 0);

            if searching == 1 {
                self.unpark();
            }
        }

        false
    }

    fn park(&self, was_searching: bool) -> Option<bool> {
        let mut update: usize = 1 << Self::IDLE_SHIFT;
        if was_searching {
            update = update.wrapping_sub(1 << Self::SEARCHING_SHIFT);
        }

        let state = self.state.fetch_add(update, Ordering::SeqCst);
        let idle = (state >> Self::IDLE_SHIFT) & Self::STATE_MASK;
        assert!(idle < self.run_queues.len());

        let searching = (state >> Self::SEARCHING_SHIFT) & Self::STATE_MASK;
        assert!(searching <= self.run_queues.len());
        assert!(searching >= was_searching as usize);

        if state & Self::SHUTDOWN_MASK != 0 {
            let state = self
                .state
                .fetch_sub(1 << Self::IDLE_SHIFT, Ordering::Relaxed);

            let idle = (state >> Self::IDLE_SHIFT) & Self::STATE_MASK;
            assert!(idle <= self.run_queues.len());
            assert!(idle > 0);
            return None;
        }

        if was_searching && searching == 1 && !self.is_empty() {
            self.unpark();
        }

        if let Some(callback) = self.on_thread_park.as_ref() {
            (callback)();
        }

        self.idle_semaphore.wait(None);

        if let Some(callback) = self.on_thread_unpark.as_ref() {
            (callback)();
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

    pub(super) fn shutdown(&self) {
        self.state
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |mut state| {
                let idle = (state >> Self::IDLE_SHIFT) & Self::STATE_MASK;
                state -= idle << Self::IDLE_SHIFT;
                state += idle << Self::SEARCHING_SHIFT;

                let searching = (state >> Self::SEARCHING_SHIFT) & Self::STATE_MASK;
                assert!(searching <= self.run_queues.len());

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

    pub(super) fn on_task_begin(&self) {
        let tasks = self.tasks.fetch_add(1, Ordering::Relaxed);
        assert_ne!(tasks, usize::MAX);
    }

    pub(super) fn on_task_complete(&self) {
        let tasks = self.tasks.fetch_sub(1, Ordering::AcqRel);
        assert_ne!(tasks, 0);

        if tasks == 1 {
            let state = self.state.load(Ordering::Relaxed);
            if state & Self::SHUTDOWN_MASK != 0 {
                self.join_semaphore.post(1);
            }
        }
    }

    pub(super) fn join(&self, timeout: Option<Duration>) {
        let mut tasks = self.tasks.load(Ordering::Acquire);
        if tasks > 0 {
            self.join_semaphore.wait(timeout);
            tasks = self.tasks.load(Ordering::Acquire);
        }

        if timeout.is_none() {
            assert_eq!(tasks, 0);
        }
    }

    pub(super) fn run_worker(self: &Arc<Self>, queue_index: usize) {
        let thread = Thread::enter(self, Some(queue_index));

        let mut tick = queue_index;
        let mut is_searching = false;
        let mut rng = Rng::new(queue_index);

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
                                let mut is_empty = false;
                                let rand_seed = rng.next().unwrap().get();

                                for target_index in self.rand_seq.iter(rand_seed) {
                                    if target_index != queue_index {
                                        match self.run_queues[target_index].steal() {
                                            Ok(Some(runnable)) => return Some(runnable),
                                            Ok(None) => {}
                                            Err(_) => is_empty = false,
                                        }
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
                runnable.run(&thread);
                continue;
            }

            is_searching = match self.park(is_searching) {
                Some(searching) => searching,
                None => break,
            };
        }
    }
}
