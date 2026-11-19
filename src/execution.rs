//! A cancel is tied to the execution running when it was requested. A retry keeps that
//! execution's permit until the server has acknowledged the last CancelRequest.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use spelaion_driver_api::{DriverError, Result};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tokio_postgres::Client;

#[derive(Default)]
pub struct Order {
    gate: Arc<AsyncMutex<()>>,
    state: Mutex<State>,
    failed: AtomicBool,
}

#[derive(Default)]
struct State {
    submitted: u64,
    cancelled_up_to: u64,
    active: Option<Arc<Active>>,
}

pub struct Active {
    _permit: OwnedMutexGuard<()>,
    cancelled: AtomicBool,
    finished: AtomicBool,
}

impl Active {
    pub fn finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }
}

pub struct Execution {
    order: Arc<Order>,
    active: Arc<Active>,
}

impl Execution {
    /// If preparation or catalog lookup was in flight, no user statement needs to run.
    pub fn check(&self) -> Result<()> {
        if self.active.cancelled.load(Ordering::SeqCst) {
            Err(DriverError::Cancelled)
        } else {
            Ok(())
        }
    }
}

impl Drop for Execution {
    fn drop(&mut self) {
        let mut state = self.order.state.lock().expect("execution lock poisoned");
        self.active.finished.store(true, Ordering::SeqCst);
        state.active = None;
    }
}

impl Order {
    pub fn fail(&self) {
        self.failed.store(true, Ordering::SeqCst);
    }

    pub fn failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }

    /// Every command that sends SQL takes the same permit. In particular, catalog work
    /// must wait if a cancel for an earlier execution is still on its way to the server.
    pub async fn lock(&self, client: &Client) -> Result<OwnedMutexGuard<()>> {
        let permit = self.gate.clone().lock_owned().await;
        loop {
            if self.failed() || client.is_closed() {
                return Err(DriverError::Connection("the connection is closed".into()));
            }
            // A dropped stream or preparation can leave responses to drain. Do not make
            // a new execution cancellable while the previous one is still on the backend.
            if client.transaction_status().is_some() {
                return Ok(permit);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub async fn start(self: &Arc<Self>, client: &Client) -> Result<Execution> {
        let id = {
            let mut state = self.state.lock().expect("execution lock poisoned");
            state.submitted += 1;
            state.submitted
        };
        let permit = self.lock(client).await?;
        let mut state = self.state.lock().expect("execution lock poisoned");
        if id <= state.cancelled_up_to {
            return Err(DriverError::Cancelled);
        }
        let active = Arc::new(Active {
            _permit: permit,
            cancelled: AtomicBool::new(false),
            finished: AtomicBool::new(false),
        });
        state.active = Some(active.clone());
        Ok(Execution {
            order: self.clone(),
            active,
        })
    }

    pub fn cancel(&self) -> Option<Arc<Active>> {
        let mut state = self.state.lock().expect("execution lock poisoned");
        state.cancelled_up_to = state.submitted;
        let active = state.active.as_ref()?;
        if active.cancelled.swap(true, Ordering::SeqCst) {
            return None;
        }
        Some(active.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_cancel_keeps_the_permit_after_the_execution_has_ended() {
        let order = Arc::new(Order::default());
        let permit = order.gate.clone().lock_owned().await;
        let active = Arc::new(Active {
            _permit: permit,
            cancelled: AtomicBool::new(false),
            finished: AtomicBool::new(false),
        });
        order.state.lock().unwrap().active = Some(active.clone());
        let execution = Execution {
            order: order.clone(),
            active,
        };
        let cancelling = order.cancel().unwrap();
        assert!(matches!(execution.check(), Err(DriverError::Cancelled)));
        drop(execution);
        assert!(cancelling.finished());
        assert!(order.cancel().is_none());
        assert!(order.gate.try_lock().is_err());
        drop(cancelling);
        assert!(order.gate.try_lock().is_ok());
    }
}
