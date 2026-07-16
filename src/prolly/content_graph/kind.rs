use crate::prolly::cid::Cid;

/// Authenticated content codec used for one graph object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ContentObjectKind {
    OrderedNode,
    ProximityDescriptor,
    ProximityNode,
    OverflowDirectory,
    OverflowPage,
    ExternalVector,
    ScalarQuantization,
    ProductQuantization,
    HnswManifest,
    HnswPage,
    CompositeAccelerator,
    AcceleratorCatalog,
}

impl ContentObjectKind {
    pub(crate) const fn id(self) -> u8 {
        match self {
            Self::OrderedNode => 1,
            Self::ProximityDescriptor => 2,
            Self::ProximityNode => 3,
            Self::OverflowDirectory => 4,
            Self::OverflowPage => 5,
            Self::ExternalVector => 6,
            Self::ScalarQuantization => 7,
            Self::ProductQuantization => 8,
            Self::HnswManifest => 9,
            Self::HnswPage => 10,
            Self::CompositeAccelerator => 11,
            Self::AcceleratorCatalog => 12,
        }
    }

    pub(crate) fn from_id(id: u8) -> Option<Self> {
        Some(match id {
            1 => Self::OrderedNode,
            2 => Self::ProximityDescriptor,
            3 => Self::ProximityNode,
            4 => Self::OverflowDirectory,
            5 => Self::OverflowPage,
            6 => Self::ExternalVector,
            7 => Self::ScalarQuantization,
            8 => Self::ProductQuantization,
            9 => Self::HnswManifest,
            10 => Self::HnswPage,
            11 => Self::CompositeAccelerator,
            12 => Self::AcceleratorCatalog,
            _ => return None,
        })
    }
}

/// Typed content-addressed root plus decode context required by PRXN objects.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TypedContentRoot {
    pub kind: ContentObjectKind,
    pub cid: Cid,
    pub dimensions: Option<u32>,
}

impl TypedContentRoot {
    pub fn new(kind: ContentObjectKind, cid: Cid) -> Self {
        Self {
            kind,
            cid,
            dimensions: None,
        }
    }

    pub fn proximity_descriptor(cid: Cid) -> Self {
        Self::new(ContentObjectKind::ProximityDescriptor, cid)
    }

    pub fn with_dimensions(mut self, dimensions: u32) -> Self {
        self.dimensions = Some(dimensions);
        self
    }
}

/// One authenticated object returned in descendant-first order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypedContentObject {
    pub root: TypedContentRoot,
    pub bytes: Vec<u8>,
    pub depth: usize,
}
