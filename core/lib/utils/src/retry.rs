use std::time::Duration;
use tokio_retry::{strategy::{ExponentialBackoff, jitter}, Retry};

/// Retry an async RPC call with exponential backoff and jitter.
/// `operation` is the async closure to retry. `max_retries` controls how many attempts will be made.
pub async fn retry_rpc_call<F, Fut, T, E>(operation: F, max_retries: usize) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    let retry_strategy = ExponentialBackoff::from_millis(200)
        .max_delay(Duration::from_secs(10))
        .map(jitter)
        .take(max_retries);

    Retry::spawn(retry_strategy, || async {
        let res = operation().await;
        if res.is_err() {
            tracing::debug!("Retrying RPC due to error: {:?}", res);
        }
        res
    })
    .await
}
