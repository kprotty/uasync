use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    thread,
};

#[macro_use]
extern crate bencher;
use bencher::Bencher;

struct WaitGroup(Arc<(AtomicUsize, thread::Thread)>);

impl WaitGroup {
    fn new() -> Self {
        Self(Arc::new((AtomicUsize::new(0), thread::current())))
    }

    fn add(&self) -> Self {
        self.0 .0.fetch_add(1, Ordering::Relaxed);
        Self(self.0.clone())
    }

    fn wait(&self) {
        while self.0 .0.load(Ordering::Acquire) != 0 {
            thread::park();
        }
    }

    fn done(&self) {
        if self.0 .0.fetch_sub(1, Ordering::Release) == 1 {
            self.0 .1.unpark();
        }
    }
}

trait BenchExecutor {
    type JoinHandle: Future<Output = ()> + Send;
    fn block_on<F: Future<Output = ()>>(future: F);
    fn spawn<F: Future<Output = ()> + Send + 'static>(future: F) -> Self::JoinHandle;
}

struct TokioJoinHandle<T>(tokio::task::JoinHandle<T>);

impl<T> Future for TokioJoinHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(ctx) {
            Poll::Ready(result) => Poll::Ready(result.unwrap()),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct TokioExecutor;

impl BenchExecutor for TokioExecutor {
    type JoinHandle = TokioJoinHandle<()>;

    fn block_on<F: Future<Output = ()>>(future: F) {
        tokio::runtime::Builder::new_multi_thread()
            .build()
            .unwrap()
            .block_on(future)
    }

    fn spawn<F: Future<Output = ()> + Send + 'static>(future: F) -> Self::JoinHandle {
        TokioJoinHandle(tokio::spawn(future))
    }
}

struct AsyncExecutor;

struct AsyncTask<T>(Option<async_task::Task<T>>);

impl<T> Future for AsyncTask<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new((&mut self.0).as_mut().unwrap()).poll(ctx)
    }
}

impl<T> Drop for AsyncTask<T> {
    fn drop(&mut self) {
        self.0.take().unwrap().detach();
    }
}

static GLOBAL_ASYNC_EXECUTOR: async_executor::Executor = async_executor::Executor::new();

impl BenchExecutor for AsyncExecutor {
    type JoinHandle = AsyncTask<()>;

    fn block_on<F: Future<Output = ()>>(future: F) {
        let (signal, shutdown) = async_channel::unbounded::<()>();
        easy_parallel::Parallel::new()
            .each(0..num_cpus::get(), |_| {
                futures_lite::future::block_on(GLOBAL_ASYNC_EXECUTOR.run(shutdown.recv()))
            })
            .finish(|| {
                futures_lite::future::block_on(async {
                    future.await;
                    std::mem::drop(signal);
                })
            });
    }

    fn spawn<F: Future<Output = ()> + Send + 'static>(future: F) -> Self::JoinHandle {
        AsyncTask(Some(GLOBAL_ASYNC_EXECUTOR.spawn(future)))
    }
}

struct UasyncExecutor;

struct UasyncJoinHandle<T>(uasync::task::JoinHandle<T>);

impl<T> Future for UasyncJoinHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(ctx) {
            Poll::Ready(result) => Poll::Ready(result.unwrap()),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl BenchExecutor for UasyncExecutor {
    type JoinHandle = UasyncJoinHandle<()>;

    fn block_on<F: Future<Output = ()>>(future: F) {
        uasync::runtime::Builder::new_multi_thread()
            .worker_threads(num_cpus::get())
            .build()
            .unwrap()
            .block_on(future)
    }

    fn spawn<F: Future<Output = ()> + Send + 'static>(future: F) -> Self::JoinHandle {
        UasyncJoinHandle(uasync::spawn(future))
    }
}

fn bench_spawn_inject_uasync(b: &mut Bencher) {
    bench_spawn_inject::<UasyncExecutor>(b)
}

fn bench_spawn_inject_tokio(b: &mut Bencher) {
    bench_spawn_inject::<TokioExecutor>(b)
}

fn bench_spawn_inject_async_executor(b: &mut Bencher) {
    bench_spawn_inject::<AsyncExecutor>(b)
}

fn bench_spawn_inject<E: BenchExecutor>(b: &mut Bencher) {
    E::block_on(async move {
        b.iter(|| {
            E::spawn(async move {});
        });
    });
}

benchmark_group!(
    spawn_inject,
    bench_spawn_inject_uasync,
    bench_spawn_inject_tokio,
    bench_spawn_inject_async_executor
);
benchmark_main!(spawn_inject);

// #[bench]
// fn bench_spawn_in_worker_uasync(b: &mut Bencher) {
//     bench_spawn_in_worker::<UasyncExecutor>(b)
// }

// #[bench]
// fn bench_spawn_in_worker_tokio(b: &mut Bencher) {
//     bench_spawn_in_worker::<TokioExecutor>(b)
// }

// #[bench]
// fn bench_spawn_in_worker_async_executor(b: &mut Bencher) {
//     bench_spawn_in_worker::<AsyncExecutor>(b)
// }

// fn bench_spawn_in_worker<E: BenchExecutor>(b: &mut Bencher) {
//     E::block_on(async move {
//         let b_ptr = b as *mut Bencher as usize;
//         E::spawn(async move {
//             let b = unsafe { &mut *(b_ptr as *mut Bencher) };
//             b.iter(|| {
//                 E::spawn(async move {});
//             })
//         })
//         .await;
//     });
// }

// #[bench]
// fn bench_multi_spawner_uasync(b: &mut Bencher) {
//     bench_multi_spawner::<UasyncExecutor>(b)
// }

// #[bench]
// fn bench_multi_spawner_tokio(b: &mut Bencher) {
//     bench_multi_spawner::<TokioExecutor>(b)
// }

// #[bench]
// fn bench_multi_spawner_async_executor(b: &mut Bencher) {
//     bench_multi_spawner::<AsyncExecutor>(b)
// }

// fn bench_multi_spawner<E: BenchExecutor>(b: &mut Bencher) {
//     E::block_on(async move {
//         let wg = WaitGroup::new();
//         b.iter(|| {
//             for _ in 0..10 {
//                 let wg = wg.add();
//                 E::spawn(async move {
//                     let handles = (0..1000).map(|_| E::spawn(async move {}));
//                     for handle in handles.collect::<Vec<_>>() {
//                         handle.await;
//                     }
//                     wg.done();
//                 });
//             }
//             wg.wait();
//         });
//     });
// }

// #[bench]
// fn bench_ping_pong_uasync(b: &mut Bencher) {
//     bench_ping_pong::<UasyncExecutor>(b)
// }

// #[bench]
// fn bench_ping_pong_tokio(b: &mut Bencher) {
//     bench_ping_pong::<TokioExecutor>(b)
// }

// #[bench]
// fn bench_ping_pong_async_executor(b: &mut Bencher) {
//     bench_ping_pong::<AsyncExecutor>(b)
// }

// fn bench_ping_pong<E: BenchExecutor>(b: &mut Bencher) {
//     E::block_on(async move {
//         let wg = WaitGroup::new();
//         b.iter(|| {
//             for _ in 0..1000 {
//                 let wg = wg.add();
//                 E::spawn(async move {
//                     let (tx1, rx1) = tokio::sync::oneshot::channel();
//                     let (tx2, rx2) = tokio::sync::oneshot::channel();

//                     E::spawn(async move {
//                         rx1.await.unwrap();
//                         tx2.send(()).unwrap();
//                     });

//                     tx1.send(()).unwrap();
//                     rx2.await.unwrap();
//                     wg.done();
//                 });
//             }
//             wg.wait();
//         });
//     });
// }

// #[bench]
// fn bench_chain_uasync(b: &mut Bencher) {
//     bench_chain::<UasyncExecutor>(b)
// }

// #[bench]
// fn bench_chain_tokio(b: &mut Bencher) {
//     bench_chain::<TokioExecutor>(b)
// }

// #[bench]
// fn bench_chain_async_executor(b: &mut Bencher) {
//     bench_chain::<AsyncExecutor>(b)
// }

// fn bench_chain<E: BenchExecutor>(b: &mut Bencher) {
//     E::block_on(async move {
//         let wg = WaitGroup::new();
//         b.iter(|| {
//             fn chain_iter<E: BenchExecutor>(iter: usize, wg: WaitGroup) {
//                 match iter {
//                     0 => wg.done(),
//                     n => std::mem::drop(E::spawn(async move {
//                         chain_iter::<E>(n - 1, wg)
//                     })),
//                 }
//             }

//             chain_iter::<E>(1000, wg.add());
//             wg.wait();
//         });
//     });
// }

// #[bench]
// fn bench_yield_uasync(b: &mut Bencher) {
//     bench_yield::<UasyncExecutor>(b)
// }

// #[bench]
// fn bench_yield_tokio(b: &mut Bencher) {
//     bench_yield::<TokioExecutor>(b)
// }

// #[bench]
// fn bench_yield_async_executor(b: &mut Bencher) {
//     bench_yield::<AsyncExecutor>(b)
// }

// fn bench_yield<E: BenchExecutor>(b: &mut Bencher) {
//     E::block_on(async move {
//         let wg = WaitGroup::new();
//         b.iter(|| {
//             for _ in 0..1000 {
//                 let wg = wg.add();
//                 E::spawn(async move {
//                     for _ in 0..200 {
//                         tokio::task::yield_now().await;
//                     }
//                     wg.done();
//                 });
//             }
//             wg.wait();
//         });
//     });
// }
