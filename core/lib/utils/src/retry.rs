use rand::Rng;
use std::{
    fmt::Debug,
    future::Future,
    pin::Pin,
    time::Duration,
};
/// Retry an async RPC call with exponential backoff and jitter.
/// `operation` is the async closure to retry. `max_retries` controls how many attempts will be made.

/// With state (&mut S). Works with `&mut self` methods.
pub async fn retry_with_backoff<S, F, T, E>(
    state: &mut S,
    mut op: F,
    max_retries: usize,
) -> Result<T, E>
where
    // For any borrow lifetime of `&mut S`, the op returns a boxed Future tied to that lifetime
    for<'a> F: FnMut(&'a mut S) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>,
    E: Debug,
{
    let mut attempt = 0usize;
    loop {
        match op(state).await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < max_retries => {
                attempt += 1;
                let exp = 1u64 << (attempt.saturating_sub(1).min(6));
                let delay_ms = exp * 200 + rand::thread_rng().gen_range(0..200);
                tracing::debug!(?e, attempt, delay_ms, "retry_with_backoff: transient error");
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// No-state convenience: zero-arg closure returning a boxed Future.
pub async fn retry_with_backoff_no_state<F, Fut, T, E>(
    mut op: F,
    max_retries: usize,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Debug,
{
    let mut attempt = 0usize;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < max_retries => {
                attempt += 1;
                let exp = 1u64 << (attempt.saturating_sub(1).min(6));
                let delay_ms = exp * 200 + rand::thread_rng().gen_range(0..200);
                tracing::debug!(?e, attempt, delay_ms, "retry_with_backoff_no_state: transient error");
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
