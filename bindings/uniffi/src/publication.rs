use prolly::{NodePublication, PublicationOrigin};

pub const GENERAL: u32 = 0;
pub const POINT_UPSERT: u32 = 1;
pub const POINT_DELETE: u32 = 2;
pub const BATCH_MUTATION: u32 = 3;
pub const TREE_BUILD: u32 = 4;
pub const MERGE: u32 = 5;
pub const RANGE_DELETE: u32 = 6;
pub const REPLICATION: u32 = 7;
pub const MAINTENANCE: u32 = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Record)]
pub struct PublicationOriginRecord {
    pub code: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NodeEntryRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NodePublicationHintRecord {
    pub namespace: Vec<u8>,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NodePublicationRecord {
    pub nodes: Vec<NodeEntryRecord>,
    pub hint: Option<NodePublicationHintRecord>,
    pub origin: PublicationOriginRecord,
}

impl From<PublicationOrigin> for PublicationOriginRecord {
    fn from(origin: PublicationOrigin) -> Self {
        let code = match origin {
            PublicationOrigin::General => GENERAL,
            PublicationOrigin::PointUpsert => POINT_UPSERT,
            PublicationOrigin::PointDelete => POINT_DELETE,
            PublicationOrigin::BatchMutation => BATCH_MUTATION,
            PublicationOrigin::TreeBuild => TREE_BUILD,
            PublicationOrigin::Merge => MERGE,
            PublicationOrigin::RangeDelete => RANGE_DELETE,
            PublicationOrigin::Replication => REPLICATION,
            PublicationOrigin::Maintenance => MAINTENANCE,
            _ => GENERAL,
        };
        Self { code }
    }
}

impl From<NodePublication<'_>> for NodePublicationRecord {
    fn from(publication: NodePublication<'_>) -> Self {
        Self {
            nodes: publication
                .entries()
                .iter()
                .map(|(key, value)| NodeEntryRecord {
                    key: key.to_vec(),
                    value: value.to_vec(),
                })
                .collect(),
            hint: publication.hint().map(|hint| NodePublicationHintRecord {
                namespace: hint.namespace().to_vec(),
                key: hint.key().to_vec(),
                value: hint.value().to_vec(),
            }),
            origin: publication.origin().into(),
        }
    }
}

#[uniffi::export]
pub fn normalize_publication_origin_code(code: u32) -> u32 {
    match code {
        GENERAL | POINT_UPSERT | POINT_DELETE | BATCH_MUTATION | TREE_BUILD | MERGE
        | RANGE_DELETE | REPLICATION | MAINTENANCE => code,
        _ => GENERAL,
    }
}
