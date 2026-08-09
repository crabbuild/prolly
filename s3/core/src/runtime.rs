use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::{Error, ErrorCode, OperationId, Result, UploadId, WorkspaceId};

pub trait Clock: Send + Sync + 'static {
    fn now_millis(&self) -> Result<u64>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> Result<u64> {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "system clock precedes epoch"))?;
        u64::try_from(elapsed.as_millis())
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "timestamp overflow"))
    }
}

#[derive(Debug)]
pub struct FixedClock {
    millis: AtomicU64,
}

impl FixedClock {
    pub fn new(millis: u64) -> Self {
        Self {
            millis: AtomicU64::new(millis),
        }
    }

    pub fn set(&self, millis: u64) {
        self.millis.store(millis, Ordering::SeqCst);
    }

    pub fn advance(&self, millis: u64) -> Result<u64> {
        self.millis
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(millis)
            })
            .map(|previous| previous + millis)
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "fixed clock overflow"))
    }
}

impl Clock for FixedClock {
    fn now_millis(&self) -> Result<u64> {
        Ok(self.millis.load(Ordering::SeqCst))
    }
}

pub trait IdSource: Send + Sync + 'static {
    fn operation(&self) -> OperationId;
    fn workspace(&self) -> WorkspaceId;
    fn upload(&self) -> UploadId;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RandomIdSource;

impl IdSource for RandomIdSource {
    fn operation(&self) -> OperationId {
        OperationId::new()
    }

    fn workspace(&self) -> WorkspaceId {
        WorkspaceId::new()
    }

    fn upload(&self) -> UploadId {
        UploadId::new()
    }
}

#[derive(Debug)]
pub struct SequenceIdSource {
    prefix: u64,
    next: AtomicU64,
}

impl SequenceIdSource {
    pub fn new(prefix: u64, first_sequence: u64) -> Self {
        Self {
            prefix,
            next: AtomicU64::new(first_sequence),
        }
    }

    fn next_uuid(&self) -> Uuid {
        let sequence = self.next.fetch_add(1, Ordering::SeqCst);
        Uuid::from_u128(((self.prefix as u128) << 64) | sequence as u128)
    }
}

impl IdSource for SequenceIdSource {
    fn operation(&self) -> OperationId {
        OperationId(self.next_uuid())
    }

    fn workspace(&self) -> WorkspaceId {
        WorkspaceId(self.next_uuid())
    }

    fn upload(&self) -> UploadId {
        UploadId(self.next_uuid())
    }
}
