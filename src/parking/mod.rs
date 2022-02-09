pub(crate) mod lock;
mod node;
mod parker;
pub(crate) mod queue;
mod waker;

pub use parker::Parker;

#[cfg(feature = "std")]
pub use parker::StdParker;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct ParkToken(pub usize);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct UnparkToken(pub usize);

pub async unsafe fn park_async<P, ValidateFn, BeforeSleepFn, CancelledFn>(
    key: usize,
    validate: ValidateFn,
    before_sleep: BeforeSleepFn,
    cancelled: CancelledFn,
    park_token: ParkToken,
) -> Option<UnparkToken>
where
    P: Parker,
    ValidateFn: FnOnce() -> bool,
    BeforeSleepFn: FnOnce(),
    CancelledFn: FnOnce(bool),
{
    let on_validate = || match validate() {
        true => Some(park_token.0),
        _ => None,
    };

    let on_cancel = |is_empty: bool| {
        let has_more = !is_empty;
        cancelled(has_more);
    };

    let wake_token = queue::wait::<P, _, _, _>(key, on_validate, before_sleep, on_cancel).await?;

    Some(UnparkToken(wake_token))
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ParkResult {
    Unparked(UnparkToken),
    Invalid,
    TimedOut,
}

pub unsafe fn park<P, ValidateFn, BeforeSleepFn, TimedOutFn>(
    key: usize,
    validate: impl FnOnce() -> bool,
    before_sleep: impl FnOnce(),
    timed_out: impl FnOnce(bool),
    park_token: ParkToken,
    deadline: Option<P::Instant>,
) -> ParkResult
where
    P: Parker,
    ValidateFn: FnOnce() -> bool,
    BeforeSleepFn: FnOnce(),
    TimedOutFn: FnOnce(bool),
{
    let future = park_async::<P, _, _, _>(key, validate, before_sleep, timed_out, park_token);

    match queue::try_block_on::<P, _>(deadline, future) {
        None => ParkResult::TimedOut,
        Some(None) => ParkResult::Invalid,
        Some(Some(unpark_token)) => ParkResult::Unparked(unpark_token),
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct UnparkResult {
    pub unparked: usize,
    pub has_more: bool,
    _sealed: (),
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum FilterOp {
    Unpark(UnparkToken),
    Skip,
    Stop,
}

pub unsafe fn unpark<P, FilterFn, BeforeUnparkFn>(
    key: usize,
    filter: FilterFn,
    before_unpark: BeforeUnparkFn,
) -> UnparkResult
where
    P: Parker,
    FilterFn: FnMut(ParkToken) -> FilterOp,
    BeforeUnparkFn: FnOnce(UnparkResult),
{
    let mut filter = filter;
    let mut before_unpark = Some(before_unpark);

    let unparked = core::cell::Cell::new(0);
    let on_filter = |wait_token, _is_empty| match filter(ParkToken(wait_token)) {
        FilterOp::Unpark(unpark_token) => {
            unparked.set(unparked.get() + 1);
            Some(Some(unpark_token.0))
        }
        FilterOp::Skip => Some(None),
        FilterOp::Stop => None,
    };

    let mut result = UnparkResult::default();
    let on_before_wake = |is_empty: bool| {
        result = UnparkResult {
            unparked: unparked.get(),
            has_more: !is_empty,
            _sealed: (),
        };

        let before_unpark = before_unpark.take().unwrap();
        before_unpark(result);
    };

    queue::wake::<P, _, _>(key, on_filter, on_before_wake);
    if let Some(before_unpark) = before_unpark.take() {
        before_unpark(result);
    }

    result
}
