use super::{
    lock::Lock,
    node::{Node, Stack, Tree},
    parker::Parker,
    waker::{from_parker, AtomicWaker},
};
use core::{
    cell::Cell,
    future::Future,
    marker::{PhantomData, PhantomPinned},
    mem::drop,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

macro_rules! offset_of {
    ($type:ty, $field:tt) => {{
        let stub = core::mem::MaybeUninit::<$type>::uninit();
        let field = core::ptr::addr_of!((*stub.as_ptr()).$field);
        (field as usize) - (stub.as_ptr() as usize)
    }};
}

macro_rules! container_of {
    ($type:ty, $field:tt, $ptr:expr) => {{
        let offset = offset_of!($type, $field);
        let base = (($ptr) as usize) - offset;
        base as *const $type
    }};
}

struct Waiter {
    node: Node,
    waker: AtomicWaker,
    token: Cell<usize>,
    _pinned: PhantomPinned,
}

struct Bucket {
    tree: Lock<Tree>,
}

impl Bucket {
    fn from(key: usize) -> &'static Bucket {
        const NUM_BUCKETS: usize = 256;
        const BUCKET: Bucket = Bucket {
            tree: Lock::new(Tree::new()),
        };

        #[cfg(target_pointer_width = "64")]
        const FIB_HASH_MULT: usize = 0x9E3779B97F4A7C15;
        #[cfg(target_pointer_width = "32")]
        const FIB_HASH_MULT: usize = 0x9E3779B9;

        static BUCKETS: [Bucket; NUM_BUCKETS] = [BUCKET; NUM_BUCKETS];
        let index = key.wrapping_mul(FIB_HASH_MULT) % NUM_BUCKETS;
        &BUCKETS[index]
    }
}

pub async unsafe fn wait<P, ValidateFn, BeforeWaitFn, CancelFn>(
    key: usize,
    on_validate: ValidateFn,
    on_before_wait: BeforeWaitFn,
    on_cancel: CancelFn,
) -> Option<usize>
where
    P: Parker,
    ValidateFn: FnOnce() -> Option<usize>,
    BeforeWaitFn: FnOnce(),
    CancelFn: FnOnce(bool),
{
    let bucket = Bucket::from(key);
    let mut tree = bucket.tree.lock::<P>();

    let token = match on_validate() {
        Some(token) => token,
        None => return None,
    };

    let waiter = Waiter {
        node: Node::default(),
        waker: AtomicWaker::default(),
        token: Cell::new(token),
        _pinned: PhantomPinned,
    };

    let waiter = Pin::new_unchecked(&waiter);
    let node = Pin::map_unchecked(waiter.as_ref(), |w| &w.node);
    tree.find(key).insert(node);
    drop(tree);

    struct WaitFuture<'a, P: Parker, C: FnOnce(bool)> {
        key: usize,
        on_cancel: Option<C>,
        waiter: Pin<&'a Waiter>,
        bucket: &'static Bucket,
        _parker: PhantomData<P>,
    }

    impl<'a, P: Parker, C: FnOnce(bool)> Future for WaitFuture<'a, P, C> {
        type Output = usize;

        fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut_self = unsafe { Pin::get_unchecked_mut(self) };
            if mut_self.on_cancel.is_none() {
                unreachable!("WaitFuture polled after completion");
            }

            if mut_self.waiter.waker.poll(ctx).is_ready() {
                mut_self.on_cancel = None;
                let wake_token = mut_self.waiter.token.get();
                return Poll::Ready(wake_token);
            }

            Poll::Pending
        }
    }

    impl<'a, P: Parker, C: FnOnce(bool)> Drop for WaitFuture<'a, P, C> {
        fn drop(&mut self) {
            if let Some(on_cancel) = self.on_cancel.take() {
                self.drop_slow(on_cancel);
            }
        }
    }

    impl<'a, P: Parker, C: FnOnce(bool)> WaitFuture<'a, P, C> {
        #[cold]
        fn drop_slow(&self, on_cancel: C) {
            unsafe {
                let mut tree = self.bucket.tree.lock::<P>();
                let mut list = tree.find(self.key);

                // try to remove our node from the list and cancel appropriately
                let node = Pin::map_unchecked(self.waiter.as_ref(), |w| &w.node);
                if list.try_remove(node) {
                    let is_empty = list.iter().next().is_none();
                    on_cancel(is_empty);
                    return;
                }

                // Our node was already removed and it will be notified "soon".
                // We need to block until said notification comes through unless
                // we risk returning early and invalidating the AtomicWaker reference
                // used by the thread about to wake us up due to intrusive memory usage.
                let unparked = try_block_on::<P, _>(None, {
                    struct WakerFuture<'a> {
                        waker: &'a AtomicWaker,
                    }

                    impl<'a> Future for WakerFuture<'a> {
                        type Output = ();

                        fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<()> {
                            self.waker.poll(ctx)
                        }
                    }

                    WakerFuture {
                        waker: &self.waiter.waker,
                    }
                });

                assert!(unparked.is_some());
            }
        }
    }

    let wait_future: WaitFuture<'_, P, _> = WaitFuture {
        key,
        on_cancel: Some(on_cancel),
        waiter,
        bucket,
        _parker: PhantomData,
    };

    on_before_wait();
    Some(wait_future.await)
}

pub unsafe fn wake<P, FilterFn, BeforeWakeFn>(
    key: usize,
    mut on_filter: FilterFn,
    on_before_wake: BeforeWakeFn,
) where
    P: Parker,
    FilterFn: FnMut(usize, bool) -> Option<Option<usize>>,
    BeforeWakeFn: FnOnce(bool),
{
    let bucket = Bucket::from(key);
    let mut tree = bucket.tree.lock::<P>();

    let mut list = tree.find(key);
    let mut unparked = Stack::default();
    let mut nodes = list.iter().peekable();

    while let Some(node) = nodes.next() {
        let waiter = container_of!(Waiter, node, &*node as *const Node);
        let wait_token = (*waiter).token.get();
        let is_last = nodes.peek().is_none();

        let wake_token = match on_filter(wait_token, is_last) {
            None => break,
            Some(None) => continue,
            Some(Some(wake_token)) => wake_token,
        };

        let node_ptr = NonNull::from(&*node);
        assert!(list.try_remove(node));

        (*waiter).token.set(wake_token);
        unparked.push(node_ptr);
    }

    let is_empty = list.iter().next().is_none();
    drop(nodes);
    drop(list);

    on_before_wake(is_empty);
    drop(tree);

    while let Some(node) = unparked.pop() {
        let waiter = container_of!(Waiter, node, node.as_ptr());
        (*waiter).waker.wake();
    }
}

pub unsafe fn try_block_on<P: Parker, F: Future>(
    deadline: Option<P::Instant>,
    mut future: F,
) -> Option<F::Output> {
    let result = P::with(|parker| {
        let waker = from_parker(parker);
        let mut ctx = Context::from_waker(&waker);

        let mut timed_out = false;
        let mut future = Pin::new_unchecked(&mut future);

        while !timed_out {
            match future.as_mut().poll(&mut ctx) {
                Poll::Ready(output) => return Some(output),
                Poll::Pending => timed_out = !parker.park(deadline.clone()),
            }
        }

        None
    });

    drop(future); // may use P::with itself
    result
}
