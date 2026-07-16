#[cfg(feature = "async-store")]
mod r#async;
pub mod catalog;
pub mod composite;
pub mod hnsw;
pub mod pq;
pub(crate) mod sq8;

use self::composite::CompositeAccelerator;
use self::hnsw::HnswIndex;
use self::pq::ProductQuantizer;
use super::ProximityTree;
use crate::prolly::error::Error;
use crate::prolly::store::Store;

#[cfg(feature = "async-store")]
pub(crate) use r#async::AsyncCompositeBase;
#[cfg(feature = "async-store")]
pub use r#async::{
    AsyncAcceleratorCatalog, AsyncAcceleratorSet, AsyncCompositeAccelerator, AsyncHnswIndex,
    AsyncProductQuantizer,
};

/// Validated source-bound derived accelerators available to one search.
pub struct AcceleratorSet<S: Store> {
    hnsw: Option<HnswIndex<S>>,
    pq: Option<ProductQuantizer<S>>,
    composite: Option<CompositeAccelerator<S>>,
}

impl<S: Store> Default for AcceleratorSet<S> {
    fn default() -> Self {
        Self {
            hnsw: None,
            pq: None,
            composite: None,
        }
    }
}

impl<S: Store> AcceleratorSet<S> {
    pub fn try_new(
        source: &ProximityTree,
        hnsw: Option<HnswIndex<S>>,
        pq: Option<ProductQuantizer<S>>,
    ) -> Result<Self, Error> {
        if let Some(index) = &hnsw {
            validate_binding(
                source,
                &index.source,
                index.dimensions,
                index.metric,
                index.count,
                "HNSW",
            )?;
        }
        if let Some(index) = &pq {
            validate_binding(
                source,
                &index.source,
                index.dimensions,
                index.metric,
                index.count,
                "product quantization",
            )?;
        }
        Ok(Self {
            hnsw,
            pq,
            composite: None,
        })
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn with_hnsw(mut self, source: &ProximityTree, index: HnswIndex<S>) -> Result<Self, Error> {
        if self.hnsw.is_some() {
            return Err(invalid("duplicate HNSW accelerator"));
        }
        validate_binding(
            source,
            &index.source,
            index.dimensions,
            index.metric,
            index.count,
            "HNSW",
        )?;
        self.hnsw = Some(index);
        Ok(self)
    }

    pub fn with_pq(
        mut self,
        source: &ProximityTree,
        index: ProductQuantizer<S>,
    ) -> Result<Self, Error> {
        if self.pq.is_some() {
            return Err(invalid("duplicate product-quantization accelerator"));
        }
        validate_binding(
            source,
            &index.source,
            index.dimensions,
            index.metric,
            index.count,
            "product quantization",
        )?;
        self.pq = Some(index);
        Ok(self)
    }

    pub fn with_composite(
        mut self,
        source: &ProximityTree,
        accelerator: CompositeAccelerator<S>,
    ) -> Result<Self, Error> {
        if self.composite.is_some() {
            return Err(invalid("duplicate composite accelerator"));
        }
        if accelerator.current_source != source.descriptor
            || accelerator.dimensions != source.config.dimensions
            || accelerator.metric != source.config.metric
            || accelerator.current_count != source.count
        {
            return Err(invalid(
                "composite accelerator is bound to a different source snapshot",
            ));
        }
        self.composite = Some(accelerator);
        Ok(self)
    }

    pub(crate) fn hnsw(&self) -> Option<&HnswIndex<S>> {
        self.hnsw.as_ref()
    }

    pub(crate) fn pq(&self) -> Option<&ProductQuantizer<S>> {
        self.pq.as_ref()
    }

    pub(crate) fn composite(&self) -> Option<&CompositeAccelerator<S>> {
        self.composite.as_ref()
    }
}

fn validate_binding(
    source: &ProximityTree,
    descriptor: &crate::prolly::cid::Cid,
    dimensions: u32,
    metric: super::DistanceMetric,
    count: u64,
    kind: &'static str,
) -> Result<(), Error> {
    if descriptor != &source.descriptor
        || dimensions != source.config.dimensions
        || metric != source.config.metric
        || count != source.count
    {
        return Err(invalid(format!(
            "{kind} accelerator is bound to a different source snapshot"
        )));
    }
    Ok(())
}

fn invalid(reason: impl Into<String>) -> Error {
    Error::InvalidProximitySearch {
        reason: reason.into(),
    }
}
