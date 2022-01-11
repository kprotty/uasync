use super::{runtime::Runtime, scheduler::Scheduler}; 
use std::{
    io,
    thread,
    num::NonZeroUsize,
    sync::Arc,
    fmt,
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

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder").finish()
    }
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

    pub fn build(&mut self) -> io::Result<Runtime> {
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

                    scheduler.run_worker(queue_index);

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

        Ok(Runtime::from(scheduler))
    }
}