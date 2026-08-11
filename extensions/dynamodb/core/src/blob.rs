use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use prolly::{BlobRef, LargeValueConfig, ValueRef};

use crate::{Error, Result};

/// Sendable future returned by [`BlobStorage`].
pub type BlobFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Provider-neutral content-addressed storage used for large logical items.
///
/// The object-safe boundary keeps the logical core independent of AWS while
/// ensuring client operation futures remain safe to spawn on Tokio runtimes.
pub trait BlobStorage: Send + Sync {
    fn get_blob<'a>(&'a self, reference: &'a BlobRef) -> BlobFuture<'a, Option<Vec<u8>>>;
    fn put_blob<'a>(&'a self, bytes: &'a [u8]) -> BlobFuture<'a, BlobRef>;
}

/// Inline-only policy used by provider-independent tests and small stores.
#[derive(Debug, Default)]
pub struct InlineBlobStorage;

impl BlobStorage for InlineBlobStorage {
    fn get_blob<'a>(&'a self, reference: &'a BlobRef) -> BlobFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            Err(Error::Blob(format!(
                "blob {:?} cannot be read without configured blob storage",
                reference.cid
            )))
        })
    }

    fn put_blob<'a>(&'a self, _bytes: &'a [u8]) -> BlobFuture<'a, BlobRef> {
        Box::pin(async {
            Err(Error::Blob(
                "large item requires configured blob storage".into(),
            ))
        })
    }
}

pub(crate) struct BlobLayer {
    storage: Arc<dyn BlobStorage>,
    config: LargeValueConfig,
}

impl BlobLayer {
    pub(crate) fn inline_only() -> Self {
        Self {
            storage: Arc::new(InlineBlobStorage),
            config: LargeValueConfig::new(usize::MAX),
        }
    }

    pub(crate) fn new(storage: Arc<dyn BlobStorage>, config: LargeValueConfig) -> Result<Self> {
        if config.inline_threshold == 0 {
            return Err(Error::Validation(
                "large-value inline threshold must be nonzero".into(),
            ));
        }
        Ok(Self { storage, config })
    }

    pub(crate) fn inline_threshold(&self) -> usize {
        self.config.inline_threshold
    }

    pub(crate) async fn prepare(&self, value: Vec<u8>) -> Result<Vec<u8>> {
        self.prepare_with_inline_threshold(value, self.config.inline_threshold)
            .await
    }

    pub(crate) async fn prepare_with_inline_threshold(
        &self,
        value: Vec<u8>,
        inline_threshold: usize,
    ) -> Result<Vec<u8>> {
        if inline_threshold == 0 {
            return Err(Error::Validation(
                "large-value inline threshold must be nonzero".into(),
            ));
        }
        if value.len() > inline_threshold {
            let expected = BlobRef::from_bytes(&value);
            let reference = self.storage.put_blob(&value).await?;
            reference.validate_bytes(&value)?;
            if reference != expected {
                return Err(Error::Blob(
                    "blob provider returned a non-canonical content reference".into(),
                ));
            }
            return Ok(ValueRef::Blob(reference).to_bytes());
        }
        if ValueRef::inline_requires_escape(&value) {
            Ok(ValueRef::Inline(value).to_bytes())
        } else {
            Ok(value)
        }
    }

    pub(crate) async fn resolve(&self, stored: &[u8]) -> Result<Vec<u8>> {
        match ValueRef::from_stored_bytes(stored)? {
            ValueRef::Inline(value) => Ok(value),
            ValueRef::Blob(reference) => {
                let bytes = self.storage.get_blob(&reference).await?.ok_or_else(|| {
                    Error::CorruptData(format!("referenced blob {:?} is missing", reference.cid))
                })?;
                reference.validate_bytes(&bytes)?;
                Ok(bytes)
            }
        }
    }

    pub(crate) async fn get_verified(&self, reference: &BlobRef) -> Result<Vec<u8>> {
        let bytes = self.storage.get_blob(reference).await?.ok_or_else(|| {
            Error::CorruptData(format!("referenced blob {:?} is missing", reference.cid))
        })?;
        reference.validate_bytes(&bytes)?;
        Ok(bytes)
    }

    pub(crate) async fn put_verified(&self, reference: &BlobRef, bytes: &[u8]) -> Result<()> {
        reference.validate_bytes(bytes)?;
        let stored = self.storage.put_blob(bytes).await?;
        if stored != *reference {
            return Err(Error::Blob(
                "blob provider returned a non-canonical content reference".into(),
            ));
        }
        Ok(())
    }
}
