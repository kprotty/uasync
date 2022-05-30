use std::{thread, time::{Duration}, sync::mpsc::*};
use uasync::{runtime::Runtime, runtime::Builder};

#[test]
fn test_create_builder() {
    let _ = Builder::new_multi_thread();
}

#[test]
fn test_create_runtime() {
    let _: Runtime = Builder::new_multi_thread()
    .worker_threads(2)
    .build()
    .unwrap();

    let _: Runtime = Runtime::new_with_thread_count(15).unwrap();
    let _: Runtime = Runtime::new_with_thread_count(1).unwrap();
}

#[test]
fn test_hello_world_blocking() {
    async fn hello_world() {
        println!("HELLO WORLD")
    }

    let runtime: Runtime = Builder::new_multi_thread()
    .worker_threads(2)
    .build()
    .unwrap();

    runtime.block_on(hello_world())
}

#[test]
fn test_hello_world_async() {
    async fn hello() {
        print!("HELLO ");
    }
    async fn world() {
        println!("WORLD");
    }
    async fn hello_world() {
        let _h = hello().await;
        let _w = world().await;
    }
    {
        let runtime: Runtime = Runtime::new_with_thread_count(1).unwrap();

        runtime.spawn(hello());
        runtime.spawn(world());
        runtime.shutdown_timeout(Duration::from_secs(1));
    }
    {
        let runtime: Runtime = Runtime::new_with_thread_count(1).unwrap();
        runtime.block_on(hello_world())
    }
}

#[test]
fn test_timeouts() {
    async fn sleep(dur: Duration, sender: SyncSender<bool>) {
        println!("Sleeping...");
        thread::sleep(dur);
        println!("Slept {} seconds", dur.as_secs_f64());
        sender.send(true).unwrap();
    }

    {
        let runtime: Runtime = Runtime::new_with_thread_count(1).unwrap();
        let sec = Duration::from_secs(1);
        let (sender, receiver) = sync_channel(1);
        let _ = runtime.spawn(sleep(sec.clone(), sender));
        runtime.shutdown_background();
        match receiver.recv() {
            Ok(val) => assert!(val==true),
            Err(_) => assert!(false),
        }
    }

    {
        let runtime: Runtime = Runtime::new_with_thread_count(1).unwrap();
        let sleep_dur = Duration::from_secs(5);
        let half = Duration::from_millis(500);

        let (sender, receiver) = sync_channel(1);
        let _ = runtime.spawn(sleep(sleep_dur, sender));
        runtime.shutdown_timeout(half);
        match receiver.recv() {
            Ok(val) => assert!(val==true),
            Err(_) => assert!(false),
        }
    }
}