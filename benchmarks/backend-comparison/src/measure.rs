use std::fmt::Display;
use std::future::Future;
use std::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Measured<T> {
    pub value: T,
    pub elapsed_ns: u128,
}

pub async fn measure<T, F, E>(future: F) -> Result<Measured<T>, String>
where
    F: Future<Output = Result<T, E>>,
    E: Display,
{
    let started = Instant::now();
    let value = future.await.map_err(|error| error.to_string())?;
    let elapsed_ns = started.elapsed().as_nanos().max(1);
    Ok(Measured { value, elapsed_ns })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn validation_runs_after_the_timed_operation() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let measured = measure({
            let events = events.clone();
            async move {
                events.lock().unwrap().push("operation");
                Ok::<_, String>(7)
            }
        })
        .await
        .unwrap();
        events.lock().unwrap().push("validation");

        assert_eq!(*events.lock().unwrap(), ["operation", "validation"]);
        assert_eq!(measured.value, 7);
        assert!(measured.elapsed_ns > 0);
    }
}
