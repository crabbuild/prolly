use std::sync::Arc;

use prolly::{AsyncBlobStore, BlobRef};
use prolly_dynamodb_core::{BlobFuture, BlobStorage, Error as CoreError};
use prolly_store_dynamodb::DynamoDbBlobStore;

/// Adapter that keeps the logical core independent of the DynamoDB provider.
pub(crate) struct DynamoBlobStorage(pub(crate) DynamoDbBlobStore);

impl BlobStorage for DynamoBlobStorage {
    fn get_blob<'a>(&'a self, reference: &'a BlobRef) -> BlobFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            self.0
                .get_blob(reference)
                .await
                .map_err(|error| CoreError::Blob(error.to_string()))
        })
    }

    fn put_blob<'a>(&'a self, bytes: &'a [u8]) -> BlobFuture<'a, BlobRef> {
        Box::pin(async move {
            self.0
                .put_blob(bytes)
                .await
                .map_err(|error| CoreError::Blob(error.to_string()))
        })
    }
}

pub(crate) fn dynamo_blob_storage(
    store: &prolly_store_dynamodb::DynamoDbStore,
) -> Arc<dyn BlobStorage> {
    Arc::new(DynamoBlobStorage(DynamoDbBlobStore::new(
        store.backend().clone(),
    )))
}
