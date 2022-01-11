#![forbid(unsafe_code)]
#![warn(
    rust_2018_idioms,
    unreachable_pub,
    // missing_docs,
    missing_debug_implementations,
)]

mod thread_pool;

pub mod runtime {
    pub use crate::thread_pool::{
        builder::Builder,
        enter::Enter,
        handle::{Handle, TryCurrentError},
        runtime::Runtime,
    };
}

pub mod task {
    pub use crate::thread_pool::task::{
        spawn,
        yield_now,
        JoinHandle,
        JoinError,
    };
}

pub use task::spawn;










