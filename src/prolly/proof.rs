//! Verifiable key path proofs for prolly trees.
//!
//! A key proof contains the root-to-leaf node path needed to verify either the
//! value at a key or the absence of that key under a root CID. Verification is
//! store-independent: it recomputes node CIDs from the supplied path, checks
//! child links, follows internal separator keys, and derives the value from the
//! terminal leaf.

use super::cid::Cid;
use super::diff::DiffPage;
use super::error::{Diff, Error};
use super::key;
use super::node::Node;
use super::range::{RangeCursor, RangePage};
#[cfg(feature = "async-store")]
use super::store::AsyncStore;
use super::store::Store;
use super::tree::Tree;
#[cfg(feature = "async-store")]
use super::AsyncProlly;
use super::Prolly;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

const PROOF_BUNDLE_VERSION: u64 = 1;
const PROOF_BUNDLE_KIND_KEY: u8 = 1;
const PROOF_BUNDLE_KIND_MULTI_KEY: u8 = 2;
const PROOF_BUNDLE_KIND_RANGE: u8 = 3;
const PROOF_BUNDLE_KIND_RANGE_PAGE: u8 = 4;
const PROOF_BUNDLE_KIND_DIFF_PAGE: u8 = 5;
const AUTHENTICATED_PROOF_ENVELOPE_VERSION: u64 = 1;
const AUTHENTICATED_PROOF_ENVELOPE_ALGORITHM_HMAC_SHA256: &str = "hmac-sha256";
const AUTHENTICATED_PROOF_ENVELOPE_DOMAIN: &[u8] = b"crabdb.prolly.authenticated-proof-envelope.v1";

#[derive(Serialize, Deserialize)]
struct ProofBundleWire {
    version: u64,
    kind: u8,
    root: Option<Vec<u8>>,
    keys: Vec<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    start: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    end: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    after: Option<Vec<u8>>,
    path_node_bytes: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
struct AuthenticatedProofEnvelopeSigningWire {
    version: u64,
    algorithm: String,
    key_id: Vec<u8>,
    proof_bundle: Vec<u8>,
    context: Vec<u8>,
    issued_at_millis: Option<u64>,
    expires_at_millis: Option<u64>,
    nonce: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct AuthenticatedProofEnvelopeWire {
    version: u64,
    algorithm: String,
    key_id: Vec<u8>,
    proof_bundle: Vec<u8>,
    context: Vec<u8>,
    issued_at_millis: Option<u64>,
    expires_at_millis: Option<u64>,
    nonce: Vec<u8>,
    signature: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct DiffPageProofBundleWire {
    version: u64,
    kind: u8,
    requested_end: Option<Vec<u8>>,
    limit: u64,
    base_range_page_proof: Vec<u8>,
    other_range_page_proof: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lookahead_base_key_proof: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lookahead_other_key_proof: Option<Vec<u8>>,
}

/// Root-to-leaf proof for one key.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyProof {
    /// Root CID the path claims to prove against, or `None` for an empty tree.
    pub root: Option<Cid>,
    /// Key being proven.
    pub key: Vec<u8>,
    /// Nodes from root to leaf. Empty only when `root` is `None`.
    pub path: Vec<Node>,
}

/// Store-independent verification result for a [`KeyProof`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyProofVerification {
    /// Whether the proof path is internally consistent for `root` and `key`.
    pub valid: bool,
    /// Root CID from the proof.
    pub root: Option<Cid>,
    /// Key from the proof.
    pub key: Vec<u8>,
    /// Verified value when the key is present. `None` means verified absence
    /// when `valid` is true.
    pub value: Option<Vec<u8>>,
}

/// Shared root proof for multiple keys.
///
/// The `path` vector stores each proof node at most once. Verification follows
/// the root-to-leaf route for every key through this node set and preserves the
/// original key order in [`MultiKeyProofVerification::results`].
#[derive(Clone, Debug, PartialEq)]
pub struct MultiKeyProof {
    /// Root CID the node set claims to prove against, or `None` for an empty
    /// tree.
    pub root: Option<Cid>,
    /// Keys being proven, in caller-requested order.
    pub keys: Vec<Vec<u8>>,
    /// De-duplicated nodes needed to prove all keys. Empty when `root` is
    /// `None` or `keys` is empty.
    pub path: Vec<Node>,
}

/// Store-independent verification result for a [`MultiKeyProof`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiKeyProofVerification {
    /// Whether every requested key proof is valid.
    pub valid: bool,
    /// Root CID from the proof.
    pub root: Option<Cid>,
    /// Per-key verification results, in the same order as the proof keys.
    pub results: Vec<KeyProofVerification>,
}

/// Complete proof for all entries in a key range.
///
/// The `path` vector stores every node needed to verify that all keys in
/// `[start, end)` have been included and that no other matching keys are
/// omitted.
#[derive(Clone, Debug, PartialEq)]
pub struct RangeProof {
    /// Root CID the node set claims to prove against, or `None` for an empty
    /// tree.
    pub root: Option<Cid>,
    /// Inclusive range start.
    pub start: Vec<u8>,
    /// Exclusive range end. `None` means unbounded.
    pub end: Option<Vec<u8>>,
    /// De-duplicated nodes needed to prove the full range.
    pub path: Vec<Node>,
}

/// Proof for a resumable range page.
///
/// Unlike [`RangeProof`], the lower bound is an optional exclusive cursor
/// boundary. That lets the proof model exactly match [`RangeCursor`] semantics:
/// a page proves every entry with `key > after` and `key < end`.
#[derive(Clone, Debug, PartialEq)]
pub struct RangePageProof {
    /// Root CID the node set claims to prove against, or `None` for an empty
    /// tree.
    pub root: Option<Cid>,
    /// Exclusive cursor lower bound. `None` means start at the beginning of
    /// the keyspace.
    pub after: Option<Vec<u8>>,
    /// Exclusive page upper bound. `None` means unbounded.
    pub end: Option<Vec<u8>>,
    /// De-duplicated nodes needed to prove the page window.
    pub path: Vec<Node>,
}

/// A bounded range page paired with a store-independent proof for that page.
#[derive(Clone, Debug, PartialEq)]
pub struct ProvedRangePage {
    /// Page entries plus the cursor that should be used to request the next
    /// page, if more entries were observed while constructing this proof.
    pub page: RangePage,
    /// Proof for the page window. Verifying this proof yields exactly
    /// `page.entries`.
    pub proof: RangePageProof,
}

/// Proof for a resumable diff page.
///
/// The proof verifies the base and other entries for the page key window, then
/// recomputes the diff offline. When another diff exists after the page, the
/// optional lookahead key proofs prove the first omitted diff key so the
/// verifier can derive the same continuation cursor as [`Prolly::diff_page`].
#[derive(Clone, Debug, PartialEq)]
pub struct DiffPageProof {
    /// Range-page proof over the base tree for the verified key window.
    pub base: RangePageProof,
    /// Range-page proof over the other tree for the verified key window.
    pub other: RangePageProof,
    /// Base-tree key proof for the first omitted diff key when there is another
    /// page.
    pub lookahead_base: Option<KeyProof>,
    /// Other-tree key proof for the first omitted diff key when there is another
    /// page.
    pub lookahead_other: Option<KeyProof>,
    /// Original exclusive upper bound requested by the caller.
    pub requested_end: Option<Vec<u8>>,
    /// Original page limit requested by the caller.
    pub limit: usize,
}

/// A bounded diff page paired with a store-independent proof for that page.
#[derive(Clone, Debug, PartialEq)]
pub struct ProvedDiffPage {
    /// Diff entries plus the cursor that should be used to request the next
    /// page, if more diffs were observed while constructing this proof.
    pub page: DiffPage,
    /// Proof whose verification recomputes exactly `page.diffs` and
    /// `page.next_cursor`.
    pub proof: DiffPageProof,
}

/// HMAC-authenticated envelope for any proof bundle.
///
/// The envelope does not change proof semantics: `proof_bundle` is still decoded
/// by [`KeyProof::from_bundle_bytes`], [`MultiKeyProof::from_bundle_bytes`],
/// [`RangeProof::from_bundle_bytes`], or [`RangePageProof::from_bundle_bytes`].
/// The envelope adds provenance fields and an HMAC-SHA256 signature so a peer
/// can reject tampered bundles before decoding them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedProofEnvelope {
    /// Signature algorithm. Version 1 supports `hmac-sha256`.
    pub algorithm: String,
    /// Caller-defined identifier used to select the shared secret.
    pub key_id: Vec<u8>,
    /// Canonical proof bundle bytes from one of the proof `to_bundle_bytes`
    /// methods.
    pub proof_bundle: Vec<u8>,
    /// Application-defined domain bytes, such as tenant, snapshot, endpoint, or
    /// authorization scope.
    pub context: Vec<u8>,
    /// Optional issue time in Unix milliseconds.
    pub issued_at_millis: Option<u64>,
    /// Optional expiration time in Unix milliseconds.
    pub expires_at_millis: Option<u64>,
    /// Caller-provided nonce to make envelopes unique across repeated proofs.
    pub nonce: Vec<u8>,
    /// HMAC-SHA256 over the canonical signing payload.
    pub signature: Vec<u8>,
}

/// Verification result for an [`AuthenticatedProofEnvelope`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedProofEnvelopeVerification {
    /// True when the algorithm is supported, the signature matches, and optional
    /// time bounds are valid for the supplied verification time.
    pub valid: bool,
    /// True when the algorithm is supported and the HMAC matches the envelope
    /// signing payload.
    pub signature_valid: bool,
    /// True when the optional issue/expiration bounds are valid or time checking
    /// was skipped.
    pub time_valid: bool,
    /// True when `issued_at_millis` is later than the supplied verification time.
    pub not_yet_valid: bool,
    /// True when `expires_at_millis` is less than or equal to the supplied
    /// verification time.
    pub expired: bool,
    /// Signature algorithm from the envelope.
    pub algorithm: String,
    /// Caller-defined key identifier from the envelope.
    pub key_id: Vec<u8>,
    /// Authenticated proof bundle bytes.
    pub proof_bundle: Vec<u8>,
    /// Authenticated application context bytes.
    pub context: Vec<u8>,
    /// Issue time from the envelope.
    pub issued_at_millis: Option<u64>,
    /// Expiration time from the envelope.
    pub expires_at_millis: Option<u64>,
    /// Nonce from the envelope.
    pub nonce: Vec<u8>,
}

/// Canonical proof bundle family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProofBundleKind {
    /// Bundle created by [`KeyProof::to_bundle_bytes`].
    Key,
    /// Bundle created by [`MultiKeyProof::to_bundle_bytes`].
    MultiKey,
    /// Bundle created by [`RangeProof::to_bundle_bytes`].
    Range,
    /// Bundle created by [`RangePageProof::to_bundle_bytes`].
    RangePage,
    /// Bundle created by [`DiffPageProof::to_bundle_bytes`].
    DiffPage,
}

impl ProofBundleKind {
    /// Stable lowercase identifier for language bindings and logs.
    pub fn as_str(self) -> &'static str {
        match self {
            ProofBundleKind::Key => "key",
            ProofBundleKind::MultiKey => "multi_key",
            ProofBundleKind::Range => "range",
            ProofBundleKind::RangePage => "range_page",
            ProofBundleKind::DiffPage => "diff_page",
        }
    }
}

/// Lightweight metadata decoded from canonical proof bundle bytes.
///
/// This summary is intended for routing opaque proof bundles before a caller
/// chooses the typed decoder. It validates bundle framing and root CID lengths,
/// but it does not replace proof verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProofBundleSummary {
    /// Bundle format version.
    pub version: u64,
    /// Proof bundle family.
    pub kind: ProofBundleKind,
    /// Root CID for single-root proofs, or the base root for diff-page proofs.
    pub root: Option<Cid>,
    /// Target/root CID for diff-page proofs. `None` for single-root proofs.
    pub other_root: Option<Cid>,
    /// Number of requested keys encoded directly in the top-level proof.
    pub key_count: usize,
    /// Number of encoded proof nodes carried by the bundle, including nested
    /// range/lookahead proof nodes for diff-page proofs.
    pub path_node_count: usize,
    /// Inclusive start bound for range proofs.
    pub start: Option<Vec<u8>>,
    /// Exclusive upper bound for range and range-page proofs.
    pub end: Option<Vec<u8>>,
    /// Exclusive lower cursor bound for range-page and diff-page proofs.
    pub after: Option<Vec<u8>>,
    /// Original requested upper bound for diff-page proofs.
    pub requested_end: Option<Vec<u8>>,
    /// Original page limit for diff-page proofs.
    pub limit: Option<usize>,
    /// Whether a diff-page proof carries continuation lookahead key proofs.
    pub has_lookahead: bool,
}

/// Store-independent verification result for opaque canonical proof bundle bytes.
///
/// This result is intentionally aggregate-level: it proves whether the decoded
/// bundle verifies and reports counts that are useful for routing, logging, and
/// sync protocols. Call the typed verifier after routing when callers need full
/// values, range entries, or diff payloads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProofBundleVerification {
    /// Lightweight decoded bundle metadata.
    pub summary: ProofBundleSummary,
    /// Whether the decoded typed proof verifies.
    pub valid: bool,
    /// Number of verified existing keys for key and multi-key proofs.
    pub exists_count: usize,
    /// Number of verified absent keys for key and multi-key proofs.
    pub absence_count: usize,
    /// Number of verified entries for range and range-page proofs.
    pub entry_count: usize,
    /// Number of verified diffs for diff-page proofs.
    pub diff_count: usize,
    /// Continuation cursor proved by a diff-page proof, when present.
    pub next_cursor: Option<RangeCursor>,
}

/// Verification result for serialized authenticated proof envelope bytes.
///
/// `valid` is true only when the envelope signature/time checks pass and the
/// authenticated proof bundle verifies. When the envelope verifies but the proof
/// bundle cannot be decoded, `proof` is `None` and `proof_error` describes the
/// authenticated proof failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedProofBundleVerification {
    /// True when both the envelope and contained proof bundle verify.
    pub valid: bool,
    /// HMAC/time verification for the authenticated envelope.
    pub envelope: AuthenticatedProofEnvelopeVerification,
    /// Store-independent verification of the authenticated proof bundle.
    pub proof: Option<ProofBundleVerification>,
    /// Proof decode/verification error for an otherwise valid envelope.
    pub proof_error: Option<String>,
}

/// Store-independent verification result for a [`RangeProof`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeProofVerification {
    /// Whether the proof is internally consistent and complete for the range.
    pub valid: bool,
    /// Root CID from the proof.
    pub root: Option<Cid>,
    /// Inclusive range start.
    pub start: Vec<u8>,
    /// Exclusive range end. `None` means unbounded.
    pub end: Option<Vec<u8>>,
    /// Verified entries in lexicographic key order.
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Store-independent verification result for a [`RangePageProof`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangePageProofVerification {
    /// Whether the proof is internally consistent and complete for the page
    /// window.
    pub valid: bool,
    /// Root CID from the proof.
    pub root: Option<Cid>,
    /// Exclusive cursor lower bound.
    pub after: Option<Vec<u8>>,
    /// Exclusive page upper bound. `None` means unbounded.
    pub end: Option<Vec<u8>>,
    /// Verified entries in lexicographic key order.
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Store-independent verification result for a [`DiffPageProof`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiffPageProofVerification {
    /// True when both range proofs are valid, bounds match, optional lookahead
    /// proofs are valid, and the recomputed diff count matches the page limit.
    pub valid: bool,
    /// Whether the base range-page proof verified.
    pub base_valid: bool,
    /// Whether the other range-page proof verified.
    pub other_valid: bool,
    /// Whether the lookahead proofs were absent for a final page or valid for a
    /// continued page.
    pub lookahead_valid: bool,
    /// Base tree root from the proof.
    pub base_root: Option<Cid>,
    /// Other tree root from the proof.
    pub other_root: Option<Cid>,
    /// Exclusive cursor lower bound.
    pub after: Option<Vec<u8>>,
    /// Original exclusive upper bound requested by the caller.
    pub requested_end: Option<Vec<u8>>,
    /// Exclusive upper bound covered by the range proofs. For continued pages
    /// this is the first omitted diff key; for final pages it equals
    /// `requested_end`.
    pub proof_end: Option<Vec<u8>>,
    /// Original page limit requested by the caller.
    pub limit: usize,
    /// Diffs recomputed from verified base/other entries.
    pub diffs: Vec<Diff>,
    /// Continuation cursor derived from the recomputed page and lookahead.
    pub next_cursor: Option<RangeCursor>,
}

impl KeyProofVerification {
    /// Whether the proof is valid and proves that the key exists.
    pub fn exists(&self) -> bool {
        self.valid && self.value.is_some()
    }

    /// Whether the proof is valid and proves that the key is absent.
    pub fn is_absence(&self) -> bool {
        self.valid && self.value.is_none()
    }
}

impl MultiKeyProofVerification {
    /// Whether all requested keys were verified.
    pub fn all_valid(&self) -> bool {
        self.valid && self.results.iter().all(|result| result.valid)
    }
}

impl RangeProofVerification {
    /// Whether the verified range contains no entries.
    pub fn is_empty(&self) -> bool {
        self.valid && self.entries.is_empty()
    }
}

impl RangePageProofVerification {
    /// Whether the verified page window contains no entries.
    pub fn is_empty(&self) -> bool {
        self.valid && self.entries.is_empty()
    }
}

impl DiffPageProofVerification {
    /// Whether the verified diff page contains no diffs.
    pub fn is_empty(&self) -> bool {
        self.valid && self.diffs.is_empty()
    }
}

impl AuthenticatedProofEnvelope {
    /// Serialize this envelope as deterministic, versioned binary bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        authenticated_proof_envelope_to_bytes(self)
    }

    /// Decode an envelope from bytes produced by
    /// [`AuthenticatedProofEnvelope::to_bytes`].
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        authenticated_proof_envelope_from_bytes(bytes)
    }

    /// Verify this envelope with the shared secret. Passing `None` for
    /// `now_millis` skips issue/expiration checks.
    pub fn verify(
        &self,
        secret: &[u8],
        now_millis: Option<u64>,
    ) -> AuthenticatedProofEnvelopeVerification {
        verify_authenticated_proof_envelope(self, secret, now_millis)
    }
}

impl ProofBundleSummary {
    /// Stable lowercase identifier for language bindings and logs.
    pub fn kind_name(&self) -> &'static str {
        self.kind.as_str()
    }
}

impl ProofBundleVerification {
    /// Stable lowercase identifier for language bindings and logs.
    pub fn kind_name(&self) -> &'static str {
        self.summary.kind_name()
    }
}

impl KeyProof {
    /// Verify this proof without consulting a store.
    pub fn verify(&self) -> KeyProofVerification {
        verify_key_proof(self)
    }

    /// Return the nodes in this proof as deterministic encoded bytes.
    pub fn path_node_bytes(&self) -> Vec<Vec<u8>> {
        self.path.iter().map(Node::to_bytes).collect()
    }

    /// Rebuild a typed proof from encoded path nodes.
    pub fn from_node_bytes(
        root: Option<Cid>,
        key: impl Into<Vec<u8>>,
        path_node_bytes: Vec<Vec<u8>>,
    ) -> Result<Self, Error> {
        let path = path_node_bytes
            .iter()
            .map(|bytes| Node::from_bytes(bytes))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            root,
            key: key.into(),
            path,
        })
    }

    /// Serialize this proof as a versioned, deterministic binary bundle.
    pub fn to_bundle_bytes(&self) -> Result<Vec<u8>, Error> {
        proof_bundle_to_bytes(ProofBundleWire {
            version: PROOF_BUNDLE_VERSION,
            kind: PROOF_BUNDLE_KIND_KEY,
            root: self.root.as_ref().map(|cid| cid.as_bytes().to_vec()),
            keys: vec![self.key.clone()],
            start: None,
            end: None,
            after: None,
            path_node_bytes: self.path_node_bytes(),
        })
    }

    /// Decode a proof from bytes produced by [`KeyProof::to_bundle_bytes`].
    pub fn from_bundle_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let wire = proof_bundle_from_bytes(bytes)?;
        if wire.kind != PROOF_BUNDLE_KIND_KEY {
            return Err(proof_bundle_deserialize("proof bundle is not a key proof"));
        }
        if wire.keys.len() != 1 {
            return Err(proof_bundle_deserialize(
                "key proof bundle must contain exactly one key",
            ));
        }
        Self::from_node_bytes(
            cid_from_bundle_root(wire.root)?,
            wire.keys.into_iter().next().unwrap(),
            wire.path_node_bytes,
        )
    }
}

impl MultiKeyProof {
    /// Verify this proof without consulting a store.
    pub fn verify(&self) -> MultiKeyProofVerification {
        verify_multi_key_proof(self)
    }

    /// Return the de-duplicated proof nodes as deterministic encoded bytes.
    pub fn path_node_bytes(&self) -> Vec<Vec<u8>> {
        self.path.iter().map(Node::to_bytes).collect()
    }

    /// Rebuild a typed proof from encoded path nodes.
    pub fn from_node_bytes(
        root: Option<Cid>,
        keys: Vec<Vec<u8>>,
        path_node_bytes: Vec<Vec<u8>>,
    ) -> Result<Self, Error> {
        let path = path_node_bytes
            .iter()
            .map(|bytes| Node::from_bytes(bytes))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { root, keys, path })
    }

    /// Serialize this proof as a versioned, deterministic binary bundle.
    pub fn to_bundle_bytes(&self) -> Result<Vec<u8>, Error> {
        proof_bundle_to_bytes(ProofBundleWire {
            version: PROOF_BUNDLE_VERSION,
            kind: PROOF_BUNDLE_KIND_MULTI_KEY,
            root: self.root.as_ref().map(|cid| cid.as_bytes().to_vec()),
            keys: self.keys.clone(),
            start: None,
            end: None,
            after: None,
            path_node_bytes: self.path_node_bytes(),
        })
    }

    /// Decode a proof from bytes produced by [`MultiKeyProof::to_bundle_bytes`].
    pub fn from_bundle_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let wire = proof_bundle_from_bytes(bytes)?;
        if wire.kind != PROOF_BUNDLE_KIND_MULTI_KEY {
            return Err(proof_bundle_deserialize(
                "proof bundle is not a multi-key proof",
            ));
        }
        Self::from_node_bytes(
            cid_from_bundle_root(wire.root)?,
            wire.keys,
            wire.path_node_bytes,
        )
    }
}

impl RangeProof {
    /// Verify this proof without consulting a store.
    pub fn verify(&self) -> RangeProofVerification {
        verify_range_proof(self)
    }

    /// Return the de-duplicated proof nodes as deterministic encoded bytes.
    pub fn path_node_bytes(&self) -> Vec<Vec<u8>> {
        self.path.iter().map(Node::to_bytes).collect()
    }

    /// Rebuild a typed proof from encoded path nodes.
    pub fn from_node_bytes(
        root: Option<Cid>,
        start: impl Into<Vec<u8>>,
        end: Option<Vec<u8>>,
        path_node_bytes: Vec<Vec<u8>>,
    ) -> Result<Self, Error> {
        let path = path_node_bytes
            .iter()
            .map(|bytes| Node::from_bytes(bytes))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            root,
            start: start.into(),
            end,
            path,
        })
    }

    /// Serialize this proof as a versioned, deterministic binary bundle.
    pub fn to_bundle_bytes(&self) -> Result<Vec<u8>, Error> {
        proof_bundle_to_bytes(ProofBundleWire {
            version: PROOF_BUNDLE_VERSION,
            kind: PROOF_BUNDLE_KIND_RANGE,
            root: self.root.as_ref().map(|cid| cid.as_bytes().to_vec()),
            keys: Vec::new(),
            start: Some(self.start.clone()),
            end: self.end.clone(),
            after: None,
            path_node_bytes: self.path_node_bytes(),
        })
    }

    /// Decode a proof from bytes produced by [`RangeProof::to_bundle_bytes`].
    pub fn from_bundle_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let wire = proof_bundle_from_bytes(bytes)?;
        if wire.kind != PROOF_BUNDLE_KIND_RANGE {
            return Err(proof_bundle_deserialize(
                "proof bundle is not a range proof",
            ));
        }
        let Some(start) = wire.start else {
            return Err(proof_bundle_deserialize(
                "range proof bundle must contain a start key",
            ));
        };
        Self::from_node_bytes(
            cid_from_bundle_root(wire.root)?,
            start,
            wire.end,
            wire.path_node_bytes,
        )
    }
}

impl RangePageProof {
    /// Verify this page proof without consulting a store.
    pub fn verify(&self) -> RangePageProofVerification {
        verify_range_page_proof(self)
    }

    /// Return the de-duplicated proof nodes as deterministic encoded bytes.
    pub fn path_node_bytes(&self) -> Vec<Vec<u8>> {
        self.path.iter().map(Node::to_bytes).collect()
    }

    /// Rebuild a typed proof from encoded path nodes.
    pub fn from_node_bytes(
        root: Option<Cid>,
        after: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        path_node_bytes: Vec<Vec<u8>>,
    ) -> Result<Self, Error> {
        let path = path_node_bytes
            .iter()
            .map(|bytes| Node::from_bytes(bytes))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            root,
            after,
            end,
            path,
        })
    }

    /// Serialize this page proof as a versioned, deterministic binary bundle.
    pub fn to_bundle_bytes(&self) -> Result<Vec<u8>, Error> {
        proof_bundle_to_bytes(ProofBundleWire {
            version: PROOF_BUNDLE_VERSION,
            kind: PROOF_BUNDLE_KIND_RANGE_PAGE,
            root: self.root.as_ref().map(|cid| cid.as_bytes().to_vec()),
            keys: Vec::new(),
            start: None,
            end: self.end.clone(),
            after: self.after.clone(),
            path_node_bytes: self.path_node_bytes(),
        })
    }

    /// Decode a proof from bytes produced by [`RangePageProof::to_bundle_bytes`].
    pub fn from_bundle_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let wire = proof_bundle_from_bytes(bytes)?;
        if wire.kind != PROOF_BUNDLE_KIND_RANGE_PAGE {
            return Err(proof_bundle_deserialize(
                "proof bundle is not a range page proof",
            ));
        }
        Self::from_node_bytes(
            cid_from_bundle_root(wire.root)?,
            wire.after,
            wire.end,
            wire.path_node_bytes,
        )
    }
}

impl DiffPageProof {
    /// Verify this diff-page proof without consulting a store.
    pub fn verify(&self) -> DiffPageProofVerification {
        verify_diff_page_proof(self)
    }

    /// Serialize this diff-page proof as a versioned, deterministic binary
    /// bundle.
    pub fn to_bundle_bytes(&self) -> Result<Vec<u8>, Error> {
        let limit = u64::try_from(self.limit)
            .map_err(|_| Error::Serialize("diff page proof limit is too large".to_string()))?;
        let base_range_page_proof = self.base.to_bundle_bytes()?;
        let other_range_page_proof = self.other.to_bundle_bytes()?;
        let lookahead_base_key_proof = self
            .lookahead_base
            .as_ref()
            .map(KeyProof::to_bundle_bytes)
            .transpose()?;
        let lookahead_other_key_proof = self
            .lookahead_other
            .as_ref()
            .map(KeyProof::to_bundle_bytes)
            .transpose()?;

        serde_cbor::ser::to_vec_packed(&DiffPageProofBundleWire {
            version: PROOF_BUNDLE_VERSION,
            kind: PROOF_BUNDLE_KIND_DIFF_PAGE,
            requested_end: self.requested_end.clone(),
            limit,
            base_range_page_proof,
            other_range_page_proof,
            lookahead_base_key_proof,
            lookahead_other_key_proof,
        })
        .map_err(|err| Error::Serialize(err.to_string()))
    }

    /// Decode a proof from bytes produced by [`DiffPageProof::to_bundle_bytes`].
    pub fn from_bundle_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let wire = diff_page_proof_bundle_from_bytes(bytes)?;
        let limit = usize::try_from(wire.limit)
            .map_err(|_| proof_bundle_deserialize("diff page proof bundle limit is too large"))?;

        Ok(Self {
            base: RangePageProof::from_bundle_bytes(&wire.base_range_page_proof)?,
            other: RangePageProof::from_bundle_bytes(&wire.other_range_page_proof)?,
            lookahead_base: wire
                .lookahead_base_key_proof
                .map(|proof| KeyProof::from_bundle_bytes(&proof))
                .transpose()?,
            lookahead_other: wire
                .lookahead_other_key_proof
                .map(|proof| KeyProof::from_bundle_bytes(&proof))
                .transpose()?,
            requested_end: wire.requested_end,
            limit,
        })
    }
}

impl<S: Store> Prolly<S> {
    /// Build a root-to-leaf proof for `key`.
    ///
    /// The returned proof is self-contained and can be verified without access
    /// to this store. A valid proof may prove either key presence or absence.
    pub fn prove_key(&self, tree: &Tree, key: &[u8]) -> Result<KeyProof, Error> {
        let mut path = Vec::new();

        let Some(root_cid) = &tree.root else {
            return Ok(KeyProof {
                root: None,
                key: key.to_vec(),
                path,
            });
        };

        let mut cid = root_cid.clone();
        loop {
            let node = self.load(&cid)?;
            let is_leaf = node.leaf;
            let child_index = path_child_index(&node, key);
            path.push(node.clone());

            if is_leaf {
                break;
            }

            let Some(child_bytes) = node.vals.get(child_index) else {
                return Err(Error::InvalidNode);
            };
            cid = cid_from_child_bytes(child_bytes).ok_or(Error::InvalidNode)?;
        }

        Ok(KeyProof {
            root: Some(root_cid.clone()),
            key: key.to_vec(),
            path,
        })
    }

    /// Build one shared proof for multiple keys.
    ///
    /// The returned proof de-duplicates shared path nodes while preserving the
    /// input key order. A valid proof may prove a mix of key presence and
    /// absence.
    pub fn prove_keys<K: AsRef<[u8]>>(
        &self,
        tree: &Tree,
        keys: &[K],
    ) -> Result<MultiKeyProof, Error> {
        let keys = keys
            .iter()
            .map(|key| key.as_ref().to_vec())
            .collect::<Vec<_>>();
        let mut path = Vec::new();

        let Some(root_cid) = &tree.root else {
            return Ok(MultiKeyProof {
                root: None,
                keys,
                path,
            });
        };

        if keys.is_empty() {
            return Ok(MultiKeyProof {
                root: Some(root_cid.clone()),
                keys,
                path,
            });
        }

        let mut seen = HashSet::new();
        for key in &keys {
            let key_proof = self.prove_key(tree, key)?;
            for node in key_proof.path {
                let cid = node.cid();
                if seen.insert(cid) {
                    path.push(node);
                }
            }
        }

        Ok(MultiKeyProof {
            root: Some(root_cid.clone()),
            keys,
            path,
        })
    }

    /// Build a complete proof for every entry in `[start, end)`.
    ///
    /// The returned proof contains all overlapping child subtrees needed to
    /// verify range completeness without access to this store.
    pub fn prove_range(
        &self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<RangeProof, Error> {
        let mut path = Vec::new();

        let Some(root_cid) = &tree.root else {
            return Ok(RangeProof {
                root: None,
                start: start.to_vec(),
                end: end.map(<[u8]>::to_vec),
                path,
            });
        };

        if range_is_empty_by_bounds(start, end) {
            return Ok(RangeProof {
                root: Some(root_cid.clone()),
                start: start.to_vec(),
                end: end.map(<[u8]>::to_vec),
                path,
            });
        }

        let mut seen = HashSet::new();
        self.collect_range_proof_nodes(root_cid, start, end, &mut seen, &mut path)?;

        Ok(RangeProof {
            root: Some(root_cid.clone()),
            start: start.to_vec(),
            end: end.map(<[u8]>::to_vec),
            path,
        })
    }

    /// Build a complete proof for every entry whose key starts with `prefix`.
    ///
    /// This is equivalent to calling [`Prolly::prove_range`] with bounds from
    /// [`crate::prefix_range`], but makes prefix/namespace proofs explicit at
    /// API boundaries.
    pub fn prove_prefix(&self, tree: &Tree, prefix: &[u8]) -> Result<RangeProof, Error> {
        let (start, end) = key::prefix_range(prefix);
        self.prove_range(tree, &start, end.as_deref())
    }

    /// Read a bounded range page and build a proof for exactly that page window.
    ///
    /// The proof uses the cursor's exclusive `after` bound instead of converting
    /// it into an inclusive key. This preserves raw byte-key semantics even when
    /// the cursor key is a prefix of later keys.
    pub fn prove_range_page(
        &self,
        tree: &Tree,
        cursor: &RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<ProvedRangePage, Error> {
        let after = cursor.after().map(<[u8]>::to_vec);

        if limit == 0 {
            let proof_end = after.clone().or_else(|| Some(Vec::new()));
            return Ok(ProvedRangePage {
                page: RangePage {
                    entries: Vec::new(),
                    next_cursor: Some(cursor.clone()),
                },
                proof: RangePageProof {
                    root: tree.root.clone(),
                    after,
                    end: proof_end,
                    path: Vec::new(),
                },
            });
        }

        let mut iter = self.range_from_cursor(tree, cursor, end)?;
        let mut entries = Vec::with_capacity(limit);

        for _ in 0..limit {
            let Some(item) = iter.next() else {
                let proof = self.prove_range_page_window(tree, after.as_deref(), end)?;
                return Ok(ProvedRangePage {
                    page: RangePage {
                        entries,
                        next_cursor: None,
                    },
                    proof,
                });
            };
            entries.push(item?);
        }

        let lookahead = iter.next().transpose()?;
        let proof_end = lookahead
            .as_ref()
            .map(|(key, _)| key.clone())
            .or_else(|| end.map(<[u8]>::to_vec));
        let proof = self.prove_range_page_window(tree, after.as_deref(), proof_end.as_deref())?;
        let next_cursor = lookahead.as_ref().and_then(|_| {
            entries
                .last()
                .map(|(key, _)| RangeCursor::after_key(key.clone()))
        });

        Ok(ProvedRangePage {
            page: RangePage {
                entries,
                next_cursor,
            },
            proof,
        })
    }

    /// Read a bounded diff page and build a proof for exactly that page.
    ///
    /// Verification recomputes the page from two range-page proofs and, when
    /// the result has a continuation cursor, two key proofs for the first
    /// omitted diff key.
    pub fn prove_diff_page(
        &self,
        base: &Tree,
        other: &Tree,
        cursor: &RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<ProvedDiffPage, Error> {
        let after = cursor.after().map(<[u8]>::to_vec);

        if limit == 0 {
            let proof_end = after.clone().or_else(|| Some(Vec::new()));
            return Ok(ProvedDiffPage {
                page: DiffPage {
                    diffs: Vec::new(),
                    next_cursor: Some(cursor.clone()),
                },
                proof: DiffPageProof {
                    base: RangePageProof {
                        root: base.root.clone(),
                        after: after.clone(),
                        end: proof_end.clone(),
                        path: Vec::new(),
                    },
                    other: RangePageProof {
                        root: other.root.clone(),
                        after,
                        end: proof_end,
                        path: Vec::new(),
                    },
                    lookahead_base: None,
                    lookahead_other: None,
                    requested_end: end.map(<[u8]>::to_vec),
                    limit,
                },
            });
        }

        let mut all_diffs = self.diff_from_cursor(base, other, cursor, end)?;
        let has_more = all_diffs.len() > limit;
        let lookahead_key = has_more.then(|| all_diffs[limit].key().to_vec());
        if has_more {
            all_diffs.truncate(limit);
        }

        let next_cursor = if has_more {
            all_diffs
                .last()
                .map(|diff| RangeCursor::after_key(diff.key().to_vec()))
        } else {
            None
        };
        let proof_end = lookahead_key.clone().or_else(|| end.map(<[u8]>::to_vec));
        let lookahead_base = lookahead_key
            .as_ref()
            .map(|key| self.prove_key(base, key))
            .transpose()?;
        let lookahead_other = lookahead_key
            .as_ref()
            .map(|key| self.prove_key(other, key))
            .transpose()?;

        Ok(ProvedDiffPage {
            page: DiffPage {
                diffs: all_diffs,
                next_cursor,
            },
            proof: DiffPageProof {
                base: self.prove_range_page_window(base, after.as_deref(), proof_end.as_deref())?,
                other: self.prove_range_page_window(
                    other,
                    after.as_deref(),
                    proof_end.as_deref(),
                )?,
                lookahead_base,
                lookahead_other,
                requested_end: end.map(<[u8]>::to_vec),
                limit,
            },
        })
    }

    fn prove_range_page_window(
        &self,
        tree: &Tree,
        after: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<RangePageProof, Error> {
        let Some(root_cid) = &tree.root else {
            return Ok(RangePageProof {
                root: None,
                after: after.map(<[u8]>::to_vec),
                end: end.map(<[u8]>::to_vec),
                path: Vec::new(),
            });
        };

        if page_range_is_empty_by_bounds(after, end) {
            return Ok(RangePageProof {
                root: Some(root_cid.clone()),
                after: after.map(<[u8]>::to_vec),
                end: end.map(<[u8]>::to_vec),
                path: Vec::new(),
            });
        }

        let mut seen = HashSet::new();
        let mut path = Vec::new();
        self.collect_range_page_proof_nodes(root_cid, after, end, &mut seen, &mut path)?;

        Ok(RangePageProof {
            root: Some(root_cid.clone()),
            after: after.map(<[u8]>::to_vec),
            end: end.map(<[u8]>::to_vec),
            path,
        })
    }

    fn collect_range_proof_nodes(
        &self,
        cid: &Cid,
        start: &[u8],
        end: Option<&[u8]>,
        seen: &mut HashSet<Cid>,
        path: &mut Vec<Node>,
    ) -> Result<(), Error> {
        let node = self.load(cid)?;
        if !seen.insert(cid.clone()) {
            return Ok(());
        }
        path.push(node.clone());

        if node.leaf {
            return Ok(());
        }

        for idx in overlapping_child_index_range(&node, start, end) {
            let child_start = node.keys[idx].as_slice();
            let child_end = child_span_end(&node, idx, None);
            if !span_overlaps_range(child_start, child_end, start, end) {
                if range_ends_before_or_at(end, child_start) {
                    break;
                }
                continue;
            }
            let child_cid = cid_from_child_bytes(node.vals.get(idx).ok_or(Error::InvalidNode)?)
                .ok_or(Error::InvalidNode)?;
            self.collect_range_proof_nodes(&child_cid, start, end, seen, path)?;
        }

        Ok(())
    }

    fn collect_range_page_proof_nodes(
        &self,
        cid: &Cid,
        after: Option<&[u8]>,
        end: Option<&[u8]>,
        seen: &mut HashSet<Cid>,
        path: &mut Vec<Node>,
    ) -> Result<(), Error> {
        let node = self.load(cid)?;
        if !seen.insert(cid.clone()) {
            return Ok(());
        }
        path.push(node.clone());

        if node.leaf {
            return Ok(());
        }

        let traversal_start = after.unwrap_or(&[]);
        for idx in overlapping_child_index_range(&node, traversal_start, end) {
            let child_start = node.keys[idx].as_slice();
            let child_end = child_span_end(&node, idx, None);
            if !span_overlaps_page_range(child_start, child_end, after, end) {
                if range_ends_before_or_at(end, child_start) {
                    break;
                }
                continue;
            }
            let child_cid = cid_from_child_bytes(node.vals.get(idx).ok_or(Error::InvalidNode)?)
                .ok_or(Error::InvalidNode)?;
            self.collect_range_page_proof_nodes(&child_cid, after, end, seen, path)?;
        }

        Ok(())
    }
}

#[cfg(feature = "async-store")]
impl<S: AsyncStore> AsyncProlly<S> {
    /// Build a root-to-leaf proof for `key`.
    ///
    /// The returned proof is self-contained and can be verified without access
    /// to this store. A valid proof may prove either key presence or absence.
    pub async fn prove_key(&self, tree: &Tree, key: &[u8]) -> Result<KeyProof, Error> {
        let mut path = Vec::new();

        let Some(root_cid) = &tree.root else {
            return Ok(KeyProof {
                root: None,
                key: key.to_vec(),
                path,
            });
        };

        let mut cid = root_cid.clone();
        loop {
            let node = self.load_arc(&cid).await?;
            let is_leaf = node.leaf;
            let child_index = path_child_index(&node, key);
            path.push((*node).clone());

            if is_leaf {
                break;
            }

            let Some(child_bytes) = node.vals.get(child_index) else {
                return Err(Error::InvalidNode);
            };
            cid = cid_from_child_bytes(child_bytes).ok_or(Error::InvalidNode)?;
        }

        Ok(KeyProof {
            root: Some(root_cid.clone()),
            key: key.to_vec(),
            path,
        })
    }

    /// Build one shared proof for multiple keys.
    ///
    /// The returned proof de-duplicates shared path nodes while preserving the
    /// input key order. A valid proof may prove a mix of key presence and
    /// absence.
    pub async fn prove_keys<K: AsRef<[u8]>>(
        &self,
        tree: &Tree,
        keys: &[K],
    ) -> Result<MultiKeyProof, Error> {
        let keys = keys
            .iter()
            .map(|key| key.as_ref().to_vec())
            .collect::<Vec<_>>();
        let mut path = Vec::new();

        let Some(root_cid) = &tree.root else {
            return Ok(MultiKeyProof {
                root: None,
                keys,
                path,
            });
        };

        if keys.is_empty() {
            return Ok(MultiKeyProof {
                root: Some(root_cid.clone()),
                keys,
                path,
            });
        }

        let mut seen = HashSet::new();
        for key in &keys {
            let key_proof = self.prove_key(tree, key).await?;
            for node in key_proof.path {
                let cid = node.cid();
                if seen.insert(cid) {
                    path.push(node);
                }
            }
        }

        Ok(MultiKeyProof {
            root: Some(root_cid.clone()),
            keys,
            path,
        })
    }

    /// Build a complete proof for every entry in `[start, end)`.
    ///
    /// The returned proof contains all overlapping child subtrees needed to
    /// verify range completeness without access to this store.
    pub async fn prove_range(
        &self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<RangeProof, Error> {
        let mut path = Vec::new();

        let Some(root_cid) = &tree.root else {
            return Ok(RangeProof {
                root: None,
                start: start.to_vec(),
                end: end.map(<[u8]>::to_vec),
                path,
            });
        };

        if range_is_empty_by_bounds(start, end) {
            return Ok(RangeProof {
                root: Some(root_cid.clone()),
                start: start.to_vec(),
                end: end.map(<[u8]>::to_vec),
                path,
            });
        }

        let mut seen = HashSet::new();
        self.collect_range_proof_nodes(root_cid, start, end, &mut seen, &mut path)
            .await?;

        Ok(RangeProof {
            root: Some(root_cid.clone()),
            start: start.to_vec(),
            end: end.map(<[u8]>::to_vec),
            path,
        })
    }

    /// Build a complete proof for every entry whose key starts with `prefix`.
    pub async fn prove_prefix(&self, tree: &Tree, prefix: &[u8]) -> Result<RangeProof, Error> {
        let (start, end) = key::prefix_range(prefix);
        self.prove_range(tree, &start, end.as_deref()).await
    }

    /// Read a bounded range page and build a proof for exactly that page window.
    pub async fn prove_range_page(
        &self,
        tree: &Tree,
        cursor: &RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<ProvedRangePage, Error> {
        let after = cursor.after().map(<[u8]>::to_vec);

        if limit == 0 {
            let proof_end = after.clone().or_else(|| Some(Vec::new()));
            return Ok(ProvedRangePage {
                page: RangePage {
                    entries: Vec::new(),
                    next_cursor: Some(cursor.clone()),
                },
                proof: RangePageProof {
                    root: tree.root.clone(),
                    after,
                    end: proof_end,
                    path: Vec::new(),
                },
            });
        }

        let mut iter = self.range_from_cursor(tree, cursor, end).await?;
        let mut entries = Vec::with_capacity(limit);

        for _ in 0..limit {
            let Some(item) = iter.next().await else {
                let proof = self
                    .prove_range_page_window(tree, after.as_deref(), end)
                    .await?;
                return Ok(ProvedRangePage {
                    page: RangePage {
                        entries,
                        next_cursor: None,
                    },
                    proof,
                });
            };
            entries.push(item?);
        }

        let lookahead = iter.next().await.transpose()?;
        let proof_end = lookahead
            .as_ref()
            .map(|(key, _)| key.clone())
            .or_else(|| end.map(<[u8]>::to_vec));
        let proof = self
            .prove_range_page_window(tree, after.as_deref(), proof_end.as_deref())
            .await?;
        let next_cursor = lookahead.as_ref().and_then(|_| {
            entries
                .last()
                .map(|(key, _)| RangeCursor::after_key(key.clone()))
        });

        Ok(ProvedRangePage {
            page: RangePage {
                entries,
                next_cursor,
            },
            proof,
        })
    }

    /// Read a bounded diff page through the async store and build a proof for
    /// exactly that page.
    pub async fn prove_diff_page(
        &self,
        base: &Tree,
        other: &Tree,
        cursor: &RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<ProvedDiffPage, Error> {
        let after = cursor.after().map(<[u8]>::to_vec);

        if limit == 0 {
            let proof_end = after.clone().or_else(|| Some(Vec::new()));
            return Ok(ProvedDiffPage {
                page: DiffPage {
                    diffs: Vec::new(),
                    next_cursor: Some(cursor.clone()),
                },
                proof: DiffPageProof {
                    base: RangePageProof {
                        root: base.root.clone(),
                        after: after.clone(),
                        end: proof_end.clone(),
                        path: Vec::new(),
                    },
                    other: RangePageProof {
                        root: other.root.clone(),
                        after,
                        end: proof_end,
                        path: Vec::new(),
                    },
                    lookahead_base: None,
                    lookahead_other: None,
                    requested_end: end.map(<[u8]>::to_vec),
                    limit,
                },
            });
        }

        let mut all_diffs = self.diff_from_cursor(base, other, cursor, end).await?;
        let has_more = all_diffs.len() > limit;
        let lookahead_key = has_more.then(|| all_diffs[limit].key().to_vec());
        if has_more {
            all_diffs.truncate(limit);
        }

        let next_cursor = if has_more {
            all_diffs
                .last()
                .map(|diff| RangeCursor::after_key(diff.key().to_vec()))
        } else {
            None
        };
        let proof_end = lookahead_key.clone().or_else(|| end.map(<[u8]>::to_vec));
        let lookahead_base = match &lookahead_key {
            Some(key) => Some(self.prove_key(base, key).await?),
            None => None,
        };
        let lookahead_other = match &lookahead_key {
            Some(key) => Some(self.prove_key(other, key).await?),
            None => None,
        };

        Ok(ProvedDiffPage {
            page: DiffPage {
                diffs: all_diffs,
                next_cursor,
            },
            proof: DiffPageProof {
                base: self
                    .prove_range_page_window(base, after.as_deref(), proof_end.as_deref())
                    .await?,
                other: self
                    .prove_range_page_window(other, after.as_deref(), proof_end.as_deref())
                    .await?,
                lookahead_base,
                lookahead_other,
                requested_end: end.map(<[u8]>::to_vec),
                limit,
            },
        })
    }

    async fn prove_range_page_window(
        &self,
        tree: &Tree,
        after: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<RangePageProof, Error> {
        let Some(root_cid) = &tree.root else {
            return Ok(RangePageProof {
                root: None,
                after: after.map(<[u8]>::to_vec),
                end: end.map(<[u8]>::to_vec),
                path: Vec::new(),
            });
        };

        if page_range_is_empty_by_bounds(after, end) {
            return Ok(RangePageProof {
                root: Some(root_cid.clone()),
                after: after.map(<[u8]>::to_vec),
                end: end.map(<[u8]>::to_vec),
                path: Vec::new(),
            });
        }

        let mut seen = HashSet::new();
        let mut path = Vec::new();
        self.collect_range_page_proof_nodes(root_cid, after, end, &mut seen, &mut path)
            .await?;

        Ok(RangePageProof {
            root: Some(root_cid.clone()),
            after: after.map(<[u8]>::to_vec),
            end: end.map(<[u8]>::to_vec),
            path,
        })
    }

    async fn collect_range_proof_nodes(
        &self,
        cid: &Cid,
        start: &[u8],
        end: Option<&[u8]>,
        seen: &mut HashSet<Cid>,
        path: &mut Vec<Node>,
    ) -> Result<(), Error> {
        let mut stack = vec![cid.clone()];
        while let Some(cid) = stack.pop() {
            if !seen.insert(cid.clone()) {
                continue;
            }
            let node = self.load_arc(&cid).await?;
            path.push((*node).clone());

            if node.leaf {
                continue;
            }

            let mut child_cids = Vec::new();
            for idx in overlapping_child_index_range(&node, start, end) {
                let child_start = node.keys[idx].as_slice();
                let child_end = child_span_end(&node, idx, None);
                if !span_overlaps_range(child_start, child_end, start, end) {
                    if range_ends_before_or_at(end, child_start) {
                        break;
                    }
                    continue;
                }
                child_cids.push(
                    cid_from_child_bytes(node.vals.get(idx).ok_or(Error::InvalidNode)?)
                        .ok_or(Error::InvalidNode)?,
                );
            }
            child_cids.reverse();
            stack.extend(child_cids);
        }

        Ok(())
    }

    async fn collect_range_page_proof_nodes(
        &self,
        cid: &Cid,
        after: Option<&[u8]>,
        end: Option<&[u8]>,
        seen: &mut HashSet<Cid>,
        path: &mut Vec<Node>,
    ) -> Result<(), Error> {
        let mut stack = vec![cid.clone()];
        while let Some(cid) = stack.pop() {
            if !seen.insert(cid.clone()) {
                continue;
            }
            let node = self.load_arc(&cid).await?;
            path.push((*node).clone());

            if node.leaf {
                continue;
            }

            let traversal_start = after.unwrap_or(&[]);
            let mut child_cids = Vec::new();
            for idx in overlapping_child_index_range(&node, traversal_start, end) {
                let child_start = node.keys[idx].as_slice();
                let child_end = child_span_end(&node, idx, None);
                if !span_overlaps_page_range(child_start, child_end, after, end) {
                    if range_ends_before_or_at(end, child_start) {
                        break;
                    }
                    continue;
                }
                child_cids.push(
                    cid_from_child_bytes(node.vals.get(idx).ok_or(Error::InvalidNode)?)
                        .ok_or(Error::InvalidNode)?,
                );
            }
            child_cids.reverse();
            stack.extend(child_cids);
        }

        Ok(())
    }
}

/// Verify a key proof without consulting a store.
pub fn verify_key_proof(proof: &KeyProof) -> KeyProofVerification {
    let valid = proof_is_consistent(proof);
    let value = if valid {
        verified_leaf_value(proof.path.last(), &proof.key)
    } else {
        None
    };

    KeyProofVerification {
        valid,
        root: proof.root.clone(),
        key: proof.key.clone(),
        value,
    }
}

/// Verify a shared multi-key proof without consulting a store.
pub fn verify_multi_key_proof(proof: &MultiKeyProof) -> MultiKeyProofVerification {
    let mut results = proof
        .keys
        .iter()
        .map(|key| KeyProofVerification {
            valid: false,
            root: proof.root.clone(),
            key: key.clone(),
            value: None,
        })
        .collect::<Vec<_>>();

    let node_map = match proof_node_map(proof) {
        Some(node_map) => node_map,
        None => {
            return MultiKeyProofVerification {
                valid: false,
                root: proof.root.clone(),
                results,
            };
        }
    };

    match &proof.root {
        None => {
            let valid = proof.path.is_empty();
            for result in &mut results {
                result.valid = valid;
            }
            MultiKeyProofVerification {
                valid,
                root: proof.root.clone(),
                results,
            }
        }
        Some(root) if proof.keys.is_empty() => {
            let valid = proof.path.is_empty() || node_map.contains_key(root);
            MultiKeyProofVerification {
                valid,
                root: proof.root.clone(),
                results,
            }
        }
        Some(root) => {
            let mut all_valid = true;
            for result in &mut results {
                match verified_value_from_node_set(root, &result.key, &node_map) {
                    Some(value) => {
                        result.valid = true;
                        result.value = value;
                    }
                    None => {
                        all_valid = false;
                    }
                }
            }
            MultiKeyProofVerification {
                valid: all_valid,
                root: proof.root.clone(),
                results,
            }
        }
    }
}

/// Verify a range proof without consulting a store.
pub fn verify_range_proof(proof: &RangeProof) -> RangeProofVerification {
    let mut entries = Vec::new();

    if range_is_empty_by_bounds(&proof.start, proof.end.as_deref()) {
        return RangeProofVerification {
            valid: proof.path.is_empty(),
            root: proof.root.clone(),
            start: proof.start.clone(),
            end: proof.end.clone(),
            entries,
        };
    }

    let node_map = match range_proof_node_map(proof) {
        Some(node_map) => node_map,
        None => {
            return RangeProofVerification {
                valid: false,
                root: proof.root.clone(),
                start: proof.start.clone(),
                end: proof.end.clone(),
                entries,
            };
        }
    };

    let valid = match &proof.root {
        None => proof.path.is_empty(),
        Some(root) => {
            let mut stack = HashSet::new();
            verify_range_node(
                root,
                &proof.start,
                proof.end.as_deref(),
                &node_map,
                &mut stack,
                &mut entries,
            )
        }
    };

    if !valid {
        entries.clear();
    }

    RangeProofVerification {
        valid,
        root: proof.root.clone(),
        start: proof.start.clone(),
        end: proof.end.clone(),
        entries,
    }
}

/// Verify a resumable range-page proof without consulting a store.
pub fn verify_range_page_proof(proof: &RangePageProof) -> RangePageProofVerification {
    let mut entries = Vec::new();

    if page_range_is_empty_by_bounds(proof.after.as_deref(), proof.end.as_deref()) {
        return RangePageProofVerification {
            valid: proof.path.is_empty(),
            root: proof.root.clone(),
            after: proof.after.clone(),
            end: proof.end.clone(),
            entries,
        };
    }

    let node_map = match range_page_proof_node_map(proof) {
        Some(node_map) => node_map,
        None => {
            return RangePageProofVerification {
                valid: false,
                root: proof.root.clone(),
                after: proof.after.clone(),
                end: proof.end.clone(),
                entries,
            };
        }
    };

    let valid = match &proof.root {
        None => proof.path.is_empty(),
        Some(root) => {
            let mut stack = HashSet::new();
            verify_range_page_node(
                root,
                proof.after.as_deref(),
                proof.end.as_deref(),
                &node_map,
                &mut stack,
                &mut entries,
            )
        }
    };

    if !valid {
        entries.clear();
    }

    RangePageProofVerification {
        valid,
        root: proof.root.clone(),
        after: proof.after.clone(),
        end: proof.end.clone(),
        entries,
    }
}

/// Verify a diff-page proof without consulting a store.
pub fn verify_diff_page_proof(proof: &DiffPageProof) -> DiffPageProofVerification {
    let base = verify_range_page_proof(&proof.base);
    let other = verify_range_page_proof(&proof.other);
    let same_bounds = proof.base.after == proof.other.after && proof.base.end == proof.other.end;
    let after = proof.base.after.clone();
    let proof_end = proof.base.end.clone();
    let mut diffs = Vec::new();

    let mut lookahead_valid = false;
    let mut next_cursor = None;
    let mut valid = base.valid && other.valid && same_bounds;

    if valid {
        diffs = diff_verified_entries(&base.entries, &other.entries);
        match (&proof.lookahead_base, &proof.lookahead_other) {
            (Some(base_lookahead), Some(other_lookahead)) => {
                let base_lookahead = verify_key_proof(base_lookahead);
                let other_lookahead = verify_key_proof(other_lookahead);
                lookahead_valid = verify_diff_page_lookahead(
                    &base_lookahead,
                    &other_lookahead,
                    after.as_deref(),
                    proof.requested_end.as_deref(),
                    proof_end.as_deref(),
                );
                valid = valid && lookahead_valid && proof.limit > 0 && diffs.len() == proof.limit;
                if valid {
                    next_cursor = diffs
                        .last()
                        .map(|diff| RangeCursor::after_key(diff.key().to_vec()));
                }
            }
            (None, None) => {
                lookahead_valid = true;
                if proof.limit == 0 {
                    valid = valid && diffs.is_empty();
                    if valid {
                        next_cursor = Some(
                            after
                                .clone()
                                .map(RangeCursor::after_key)
                                .unwrap_or_else(RangeCursor::start),
                        );
                    }
                } else {
                    valid = valid && proof_end == proof.requested_end && diffs.len() <= proof.limit;
                }
            }
            _ => {
                valid = false;
            }
        }
    }

    if !valid {
        diffs.clear();
        next_cursor = None;
    }

    DiffPageProofVerification {
        valid,
        base_valid: base.valid,
        other_valid: other.valid,
        lookahead_valid,
        base_root: base.root,
        other_root: other.root,
        after,
        requested_end: proof.requested_end.clone(),
        proof_end,
        limit: proof.limit,
        diffs,
        next_cursor,
    }
}

/// Decode lightweight metadata from canonical proof bundle bytes.
///
/// This function lets callers route an opaque bundle to the right typed decoder
/// without consulting a store. It validates version/kind framing and root CID
/// lengths, but it does not prove membership, absence, range completeness, or
/// diff-page continuation correctness.
pub fn inspect_proof_bundle(bytes: &[u8]) -> Result<ProofBundleSummary, Error> {
    match proof_bundle_from_bytes(bytes) {
        Ok(wire) => proof_bundle_summary_from_wire(wire),
        Err(primary_error) => match diff_page_proof_bundle_from_bytes(bytes) {
            Ok(wire) => diff_page_proof_bundle_summary_from_wire(wire),
            Err(_) => Err(primary_error),
        },
    }
}

/// Decode and verify opaque canonical proof bundle bytes.
///
/// This is the verifying counterpart to [`inspect_proof_bundle`]. It chooses
/// the typed proof decoder from the bundle kind, runs the matching verifier,
/// and returns an aggregate record that is convenient across FFI boundaries.
pub fn verify_proof_bundle(bytes: &[u8]) -> Result<ProofBundleVerification, Error> {
    let summary = inspect_proof_bundle(bytes)?;
    match summary.kind {
        ProofBundleKind::Key => {
            let verified = KeyProof::from_bundle_bytes(bytes)?.verify();
            Ok(ProofBundleVerification {
                summary,
                valid: verified.valid,
                exists_count: usize::from(verified.exists()),
                absence_count: usize::from(verified.is_absence()),
                entry_count: 0,
                diff_count: 0,
                next_cursor: None,
            })
        }
        ProofBundleKind::MultiKey => {
            let verified = MultiKeyProof::from_bundle_bytes(bytes)?.verify();
            let (exists_count, absence_count) = if verified.valid {
                (
                    verified
                        .results
                        .iter()
                        .filter(|result| result.exists())
                        .count(),
                    verified
                        .results
                        .iter()
                        .filter(|result| result.is_absence())
                        .count(),
                )
            } else {
                (0, 0)
            };
            Ok(ProofBundleVerification {
                summary,
                valid: verified.valid,
                exists_count,
                absence_count,
                entry_count: 0,
                diff_count: 0,
                next_cursor: None,
            })
        }
        ProofBundleKind::Range => {
            let verified = RangeProof::from_bundle_bytes(bytes)?.verify();
            Ok(ProofBundleVerification {
                summary,
                valid: verified.valid,
                exists_count: 0,
                absence_count: 0,
                entry_count: if verified.valid {
                    verified.entries.len()
                } else {
                    0
                },
                diff_count: 0,
                next_cursor: None,
            })
        }
        ProofBundleKind::RangePage => {
            let verified = RangePageProof::from_bundle_bytes(bytes)?.verify();
            Ok(ProofBundleVerification {
                summary,
                valid: verified.valid,
                exists_count: 0,
                absence_count: 0,
                entry_count: if verified.valid {
                    verified.entries.len()
                } else {
                    0
                },
                diff_count: 0,
                next_cursor: None,
            })
        }
        ProofBundleKind::DiffPage => {
            let verified = DiffPageProof::from_bundle_bytes(bytes)?.verify();
            Ok(ProofBundleVerification {
                summary,
                valid: verified.valid,
                exists_count: 0,
                absence_count: 0,
                entry_count: 0,
                diff_count: if verified.valid {
                    verified.diffs.len()
                } else {
                    0
                },
                next_cursor: verified.next_cursor,
            })
        }
    }
}

/// Build an HMAC-SHA256 authenticated envelope for canonical proof bundle bytes.
pub fn sign_proof_bundle_hmac_sha256(
    proof_bundle: impl Into<Vec<u8>>,
    key_id: impl Into<Vec<u8>>,
    secret: &[u8],
    context: impl Into<Vec<u8>>,
    issued_at_millis: Option<u64>,
    expires_at_millis: Option<u64>,
    nonce: impl Into<Vec<u8>>,
) -> Result<AuthenticatedProofEnvelope, Error> {
    let envelope = AuthenticatedProofEnvelope {
        algorithm: AUTHENTICATED_PROOF_ENVELOPE_ALGORITHM_HMAC_SHA256.to_string(),
        key_id: key_id.into(),
        proof_bundle: proof_bundle.into(),
        context: context.into(),
        issued_at_millis,
        expires_at_millis,
        nonce: nonce.into(),
        signature: Vec::new(),
    };
    let signature = hmac_sha256(
        secret,
        &authenticated_proof_envelope_signing_bytes(&envelope)?,
    );
    Ok(AuthenticatedProofEnvelope {
        signature: signature.to_vec(),
        ..envelope
    })
}

/// Verify an authenticated proof envelope with the shared secret.
///
/// Passing `None` for `now_millis` skips issue and expiration checks while still
/// authenticating the envelope bytes.
pub fn verify_authenticated_proof_envelope(
    envelope: &AuthenticatedProofEnvelope,
    secret: &[u8],
    now_millis: Option<u64>,
) -> AuthenticatedProofEnvelopeVerification {
    let algorithm_supported =
        envelope.algorithm == AUTHENTICATED_PROOF_ENVELOPE_ALGORITHM_HMAC_SHA256;
    let signature_valid = algorithm_supported
        && authenticated_proof_envelope_signing_bytes(envelope)
            .map(|bytes| {
                let expected = hmac_sha256(secret, &bytes);
                constant_time_eq(&expected, &envelope.signature)
            })
            .unwrap_or(false);
    let (not_yet_valid, expired) = match now_millis {
        Some(now) => (
            envelope
                .issued_at_millis
                .is_some_and(|issued_at| issued_at > now),
            envelope
                .expires_at_millis
                .is_some_and(|expires_at| expires_at <= now),
        ),
        None => (false, false),
    };
    let time_valid = !not_yet_valid && !expired;

    AuthenticatedProofEnvelopeVerification {
        valid: signature_valid && time_valid,
        signature_valid,
        time_valid,
        not_yet_valid,
        expired,
        algorithm: envelope.algorithm.clone(),
        key_id: envelope.key_id.clone(),
        proof_bundle: envelope.proof_bundle.clone(),
        context: envelope.context.clone(),
        issued_at_millis: envelope.issued_at_millis,
        expires_at_millis: envelope.expires_at_millis,
        nonce: envelope.nonce.clone(),
    }
}

/// Decode, authenticate, and verify serialized proof envelope bytes.
///
/// This is the one-shot verifier for transport payloads created by
/// [`AuthenticatedProofEnvelope::to_bytes`]. A malformed envelope is returned as
/// an error. An envelope with a bad signature or invalid time bounds returns a
/// verification record with `valid = false` and does not attempt proof
/// verification. An authenticated envelope carrying a malformed proof bundle
/// returns `valid = false` with `proof_error` populated.
pub fn verify_authenticated_proof_bundle(
    envelope_bytes: &[u8],
    secret: &[u8],
    now_millis: Option<u64>,
) -> Result<AuthenticatedProofBundleVerification, Error> {
    let envelope = AuthenticatedProofEnvelope::from_bytes(envelope_bytes)?;
    let envelope = verify_authenticated_proof_envelope(&envelope, secret, now_millis);

    if !envelope.valid {
        return Ok(AuthenticatedProofBundleVerification {
            valid: false,
            envelope,
            proof: None,
            proof_error: None,
        });
    }

    match verify_proof_bundle(&envelope.proof_bundle) {
        Ok(proof) => Ok(AuthenticatedProofBundleVerification {
            valid: proof.valid,
            envelope,
            proof: Some(proof),
            proof_error: None,
        }),
        Err(err) => Ok(AuthenticatedProofBundleVerification {
            valid: false,
            envelope,
            proof: None,
            proof_error: Some(err.to_string()),
        }),
    }
}

fn proof_node_map(proof: &MultiKeyProof) -> Option<HashMap<Cid, &Node>> {
    let mut node_map = HashMap::with_capacity(proof.path.len());
    for node in &proof.path {
        if !node_shape_is_valid(node) {
            return None;
        }
        node_map.insert(node.cid(), node);
    }
    Some(node_map)
}

fn range_proof_node_map(proof: &RangeProof) -> Option<HashMap<Cid, &Node>> {
    let mut node_map = HashMap::with_capacity(proof.path.len());
    for node in &proof.path {
        if !node_shape_is_valid(node) {
            return None;
        }
        node_map.insert(node.cid(), node);
    }
    Some(node_map)
}

fn range_page_proof_node_map(proof: &RangePageProof) -> Option<HashMap<Cid, &Node>> {
    let mut node_map = HashMap::with_capacity(proof.path.len());
    for node in &proof.path {
        if !node_shape_is_valid(node) {
            return None;
        }
        node_map.insert(node.cid(), node);
    }
    Some(node_map)
}

fn diff_verified_entries(base: &[(Vec<u8>, Vec<u8>)], other: &[(Vec<u8>, Vec<u8>)]) -> Vec<Diff> {
    let mut diffs = Vec::new();
    let mut base_idx = 0;
    let mut other_idx = 0;

    while base_idx < base.len() && other_idx < other.len() {
        let (base_key, base_value) = &base[base_idx];
        let (other_key, other_value) = &other[other_idx];
        match base_key.cmp(other_key) {
            std::cmp::Ordering::Less => {
                diffs.push(Diff::Removed {
                    key: base_key.clone(),
                    val: base_value.clone(),
                });
                base_idx += 1;
            }
            std::cmp::Ordering::Greater => {
                diffs.push(Diff::Added {
                    key: other_key.clone(),
                    val: other_value.clone(),
                });
                other_idx += 1;
            }
            std::cmp::Ordering::Equal => {
                if base_value != other_value {
                    diffs.push(Diff::Changed {
                        key: base_key.clone(),
                        old: base_value.clone(),
                        new: other_value.clone(),
                    });
                }
                base_idx += 1;
                other_idx += 1;
            }
        }
    }

    for (key, value) in &base[base_idx..] {
        diffs.push(Diff::Removed {
            key: key.clone(),
            val: value.clone(),
        });
    }
    for (key, value) in &other[other_idx..] {
        diffs.push(Diff::Added {
            key: key.clone(),
            val: value.clone(),
        });
    }

    diffs
}

fn verify_diff_page_lookahead(
    base: &KeyProofVerification,
    other: &KeyProofVerification,
    after: Option<&[u8]>,
    requested_end: Option<&[u8]>,
    proof_end: Option<&[u8]>,
) -> bool {
    if !base.valid || !other.valid || base.key != other.key {
        return false;
    }
    let key = base.key.as_slice();
    if proof_end != Some(key) || !key_in_page_range(key, after, requested_end) {
        return false;
    }
    match (&base.value, &other.value) {
        (None, None) => false,
        (Some(left), Some(right)) => left != right,
        _ => true,
    }
}

fn verify_range_node(
    cid: &Cid,
    start: &[u8],
    end: Option<&[u8]>,
    node_map: &HashMap<Cid, &Node>,
    stack: &mut HashSet<Cid>,
    entries: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> bool {
    if !stack.insert(cid.clone()) {
        return false;
    }

    let Some(node) = node_map.get(cid).copied() else {
        stack.remove(cid);
        return false;
    };

    if node.leaf {
        for (key, value) in node.keys.iter().zip(&node.vals) {
            if key_in_range(key, start, end) {
                entries.push((key.clone(), value.clone()));
            }
        }
        stack.remove(cid);
        return true;
    }

    for idx in overlapping_child_index_range(node, start, end) {
        let child_start = node.keys[idx].as_slice();
        let child_end = child_span_end(node, idx, None);
        if !span_overlaps_range(child_start, child_end, start, end) {
            if range_ends_before_or_at(end, child_start) {
                break;
            }
            continue;
        }

        let Some(child_cid) = node
            .vals
            .get(idx)
            .and_then(|bytes| cid_from_child_bytes(bytes))
        else {
            stack.remove(cid);
            return false;
        };
        let Some(child) = node_map.get(&child_cid).copied() else {
            stack.remove(cid);
            return false;
        };
        if node.level != child.level.saturating_add(1) {
            stack.remove(cid);
            return false;
        }
        if !verify_range_node(&child_cid, start, end, node_map, stack, entries) {
            stack.remove(cid);
            return false;
        }
    }

    stack.remove(cid);
    true
}

fn verify_range_page_node(
    cid: &Cid,
    after: Option<&[u8]>,
    end: Option<&[u8]>,
    node_map: &HashMap<Cid, &Node>,
    stack: &mut HashSet<Cid>,
    entries: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> bool {
    if !stack.insert(cid.clone()) {
        return false;
    }

    let Some(node) = node_map.get(cid).copied() else {
        stack.remove(cid);
        return false;
    };

    if node.leaf {
        for (key, value) in node.keys.iter().zip(&node.vals) {
            if key_in_page_range(key, after, end) {
                entries.push((key.clone(), value.clone()));
            }
        }
        stack.remove(cid);
        return true;
    }

    let traversal_start = after.unwrap_or(&[]);
    for idx in overlapping_child_index_range(node, traversal_start, end) {
        let child_start = node.keys[idx].as_slice();
        let child_end = child_span_end(node, idx, None);
        if !span_overlaps_page_range(child_start, child_end, after, end) {
            if range_ends_before_or_at(end, child_start) {
                break;
            }
            continue;
        }

        let Some(child_cid) = node
            .vals
            .get(idx)
            .and_then(|bytes| cid_from_child_bytes(bytes))
        else {
            stack.remove(cid);
            return false;
        };
        let Some(child) = node_map.get(&child_cid).copied() else {
            stack.remove(cid);
            return false;
        };
        if node.level != child.level.saturating_add(1) {
            stack.remove(cid);
            return false;
        }
        if !verify_range_page_node(&child_cid, after, end, node_map, stack, entries) {
            stack.remove(cid);
            return false;
        }
    }

    stack.remove(cid);
    true
}

fn verified_value_from_node_set(
    root: &Cid,
    key: &[u8],
    node_map: &HashMap<Cid, &Node>,
) -> Option<Option<Vec<u8>>> {
    let mut cid = root.clone();
    let mut visited = HashSet::new();

    for _ in 0..=node_map.len() {
        if !visited.insert(cid.clone()) {
            return None;
        }

        let node = *node_map.get(&cid)?;
        if node.leaf {
            return Some(verified_leaf_value(Some(node), key));
        }

        let child_index = path_child_index(node, key);
        let child_cid = cid_from_child_bytes(node.vals.get(child_index)?)?;
        let child = *node_map.get(&child_cid)?;
        if node.level != child.level.saturating_add(1) {
            return None;
        }
        cid = child_cid;
    }

    None
}

fn proof_is_consistent(proof: &KeyProof) -> bool {
    match (&proof.root, proof.path.as_slice()) {
        (None, []) => return true,
        (None, _) | (Some(_), []) => return false,
        (Some(root), [first, ..]) if &first.cid() != root => return false,
        _ => {}
    }

    for (depth, node) in proof.path.iter().enumerate() {
        if !node_shape_is_valid(node) {
            return false;
        }

        let is_last = depth + 1 == proof.path.len();
        if is_last {
            return node.leaf;
        }

        if node.leaf {
            return false;
        }

        let next = &proof.path[depth + 1];
        if node.level != next.level.saturating_add(1) {
            return false;
        }

        let child_index = path_child_index(node, &proof.key);
        let Some(child_bytes) = node.vals.get(child_index) else {
            return false;
        };
        let Some(child_cid) = cid_from_child_bytes(child_bytes) else {
            return false;
        };
        if next.cid() != child_cid {
            return false;
        }
    }

    false
}

fn verified_leaf_value(leaf: Option<&Node>, key: &[u8]) -> Option<Vec<u8>> {
    let leaf = leaf?;
    if !leaf.leaf {
        return None;
    }
    match leaf.search(key) {
        Ok(index) => leaf.vals.get(index).cloned(),
        Err(_) => None,
    }
}

fn node_shape_is_valid(node: &Node) -> bool {
    if node.keys.is_empty() || node.keys.len() != node.vals.len() {
        return false;
    }

    if !node.keys.windows(2).all(|window| window[0] < window[1]) {
        return false;
    }

    node.leaf || node.vals.iter().all(|value| value.len() == 32)
}

fn path_child_index(node: &Node, key: &[u8]) -> usize {
    node.keys
        .partition_point(|candidate| candidate.as_slice() <= key)
        .saturating_sub(1)
}

fn overlapping_child_index_range(
    node: &Node,
    range_start: &[u8],
    range_end: Option<&[u8]>,
) -> std::ops::Range<usize> {
    let start = node
        .keys
        .partition_point(|candidate| candidate.as_slice() < range_start)
        .saturating_sub(1);
    let end = range_end.map_or(node.len(), |end| {
        node.keys
            .partition_point(|candidate| candidate.as_slice() < end)
    });
    start..end.max(start).min(node.len())
}

fn child_span_end<'a>(node: &'a Node, idx: usize, span_end: Option<&'a [u8]>) -> Option<&'a [u8]> {
    node.keys.get(idx + 1).map(Vec::as_slice).or(span_end)
}

fn span_overlaps_range(
    span_start: &[u8],
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
) -> bool {
    !span_ends_before_or_at(span_end, range_start)
        && !range_ends_before_or_at(range_end, span_start)
}

fn span_ends_before_or_at(end: Option<&[u8]>, start: &[u8]) -> bool {
    end.is_some_and(|end| end <= start)
}

fn range_ends_before_or_at(end: Option<&[u8]>, start: &[u8]) -> bool {
    end.is_some_and(|end| end <= start)
}

fn range_is_empty_by_bounds(start: &[u8], end: Option<&[u8]>) -> bool {
    end.is_some_and(|end| end <= start)
}

fn page_range_is_empty_by_bounds(after: Option<&[u8]>, end: Option<&[u8]>) -> bool {
    match (after, end) {
        (Some(after), Some(end)) => end <= after,
        (None, Some(end)) => end.is_empty(),
        _ => false,
    }
}

fn key_in_range(key: &[u8], start: &[u8], end: Option<&[u8]>) -> bool {
    key >= start
        && match end {
            Some(end) => key < end,
            None => true,
        }
}

fn key_in_page_range(key: &[u8], after: Option<&[u8]>, end: Option<&[u8]>) -> bool {
    after.map_or(true, |after| key > after)
        && match end {
            Some(end) => key < end,
            None => true,
        }
}

fn span_overlaps_page_range(
    span_start: &[u8],
    span_end: Option<&[u8]>,
    after: Option<&[u8]>,
    end: Option<&[u8]>,
) -> bool {
    !after.is_some_and(|after| span_ends_before_or_at(span_end, after))
        && !range_ends_before_or_at(end, span_start)
}

fn cid_from_child_bytes(bytes: &[u8]) -> Option<Cid> {
    bytes.try_into().ok().map(Cid)
}

fn proof_bundle_to_bytes(wire: ProofBundleWire) -> Result<Vec<u8>, Error> {
    serde_cbor::ser::to_vec_packed(&wire).map_err(|err| Error::Serialize(err.to_string()))
}

fn proof_bundle_from_bytes(bytes: &[u8]) -> Result<ProofBundleWire, Error> {
    let wire: ProofBundleWire =
        serde_cbor::from_slice(bytes).map_err(|err| Error::Deserialize(err.to_string()))?;
    if wire.version != PROOF_BUNDLE_VERSION {
        return Err(proof_bundle_deserialize(format!(
            "unsupported proof bundle version {}",
            wire.version
        )));
    }
    match wire.kind {
        PROOF_BUNDLE_KIND_KEY
        | PROOF_BUNDLE_KIND_MULTI_KEY
        | PROOF_BUNDLE_KIND_RANGE
        | PROOF_BUNDLE_KIND_RANGE_PAGE => Ok(wire),
        other => Err(proof_bundle_deserialize(format!(
            "unsupported proof bundle kind {other}"
        ))),
    }
}

fn diff_page_proof_bundle_from_bytes(bytes: &[u8]) -> Result<DiffPageProofBundleWire, Error> {
    let wire: DiffPageProofBundleWire =
        serde_cbor::from_slice(bytes).map_err(|err| Error::Deserialize(err.to_string()))?;
    if wire.version != PROOF_BUNDLE_VERSION {
        return Err(proof_bundle_deserialize(format!(
            "unsupported diff page proof bundle version {}",
            wire.version
        )));
    }
    if wire.kind != PROOF_BUNDLE_KIND_DIFF_PAGE {
        return Err(proof_bundle_deserialize(
            "proof bundle is not a diff page proof",
        ));
    }
    Ok(wire)
}

fn proof_bundle_summary_from_wire(wire: ProofBundleWire) -> Result<ProofBundleSummary, Error> {
    Ok(ProofBundleSummary {
        version: wire.version,
        kind: proof_bundle_kind_from_u8(wire.kind)?,
        root: cid_from_bundle_root(wire.root)?,
        other_root: None,
        key_count: wire.keys.len(),
        path_node_count: wire.path_node_bytes.len(),
        start: wire.start,
        end: wire.end,
        after: wire.after,
        requested_end: None,
        limit: None,
        has_lookahead: false,
    })
}

fn diff_page_proof_bundle_summary_from_wire(
    wire: DiffPageProofBundleWire,
) -> Result<ProofBundleSummary, Error> {
    let limit = usize::try_from(wire.limit)
        .map_err(|_| proof_bundle_deserialize("diff page proof bundle limit is too large"))?;
    let base = proof_bundle_from_bytes(&wire.base_range_page_proof)?;
    if base.kind != PROOF_BUNDLE_KIND_RANGE_PAGE {
        return Err(proof_bundle_deserialize(
            "diff page proof base proof must be a range page proof",
        ));
    }
    let other = proof_bundle_from_bytes(&wire.other_range_page_proof)?;
    if other.kind != PROOF_BUNDLE_KIND_RANGE_PAGE {
        return Err(proof_bundle_deserialize(
            "diff page proof other proof must be a range page proof",
        ));
    }

    let mut path_node_count = base.path_node_bytes.len() + other.path_node_bytes.len();
    let mut has_lookahead = false;
    if let Some(lookahead) = &wire.lookahead_base_key_proof {
        let lookahead = proof_bundle_from_bytes(lookahead)?;
        if lookahead.kind != PROOF_BUNDLE_KIND_KEY {
            return Err(proof_bundle_deserialize(
                "diff page proof base lookahead must be a key proof",
            ));
        }
        path_node_count += lookahead.path_node_bytes.len();
        has_lookahead = true;
    }
    if let Some(lookahead) = &wire.lookahead_other_key_proof {
        let lookahead = proof_bundle_from_bytes(lookahead)?;
        if lookahead.kind != PROOF_BUNDLE_KIND_KEY {
            return Err(proof_bundle_deserialize(
                "diff page proof other lookahead must be a key proof",
            ));
        }
        path_node_count += lookahead.path_node_bytes.len();
        has_lookahead = true;
    }

    Ok(ProofBundleSummary {
        version: wire.version,
        kind: ProofBundleKind::DiffPage,
        root: cid_from_bundle_root(base.root)?,
        other_root: cid_from_bundle_root(other.root)?,
        key_count: 0,
        path_node_count,
        start: None,
        end: base.end,
        after: base.after,
        requested_end: wire.requested_end,
        limit: Some(limit),
        has_lookahead,
    })
}

fn proof_bundle_kind_from_u8(kind: u8) -> Result<ProofBundleKind, Error> {
    match kind {
        PROOF_BUNDLE_KIND_KEY => Ok(ProofBundleKind::Key),
        PROOF_BUNDLE_KIND_MULTI_KEY => Ok(ProofBundleKind::MultiKey),
        PROOF_BUNDLE_KIND_RANGE => Ok(ProofBundleKind::Range),
        PROOF_BUNDLE_KIND_RANGE_PAGE => Ok(ProofBundleKind::RangePage),
        PROOF_BUNDLE_KIND_DIFF_PAGE => Ok(ProofBundleKind::DiffPage),
        other => Err(proof_bundle_deserialize(format!(
            "unsupported proof bundle kind {other}"
        ))),
    }
}

fn cid_from_bundle_root(root: Option<Vec<u8>>) -> Result<Option<Cid>, Error> {
    root.map(|bytes| {
        bytes
            .try_into()
            .map(Cid)
            .map_err(|_| proof_bundle_deserialize("proof bundle root CID must be 32 bytes"))
    })
    .transpose()
}

fn proof_bundle_deserialize(message: impl Into<String>) -> Error {
    Error::Deserialize(format!("invalid proof bundle: {}", message.into()))
}

fn authenticated_proof_envelope_to_bytes(
    envelope: &AuthenticatedProofEnvelope,
) -> Result<Vec<u8>, Error> {
    serde_cbor::ser::to_vec_packed(&AuthenticatedProofEnvelopeWire {
        version: AUTHENTICATED_PROOF_ENVELOPE_VERSION,
        algorithm: envelope.algorithm.clone(),
        key_id: envelope.key_id.clone(),
        proof_bundle: envelope.proof_bundle.clone(),
        context: envelope.context.clone(),
        issued_at_millis: envelope.issued_at_millis,
        expires_at_millis: envelope.expires_at_millis,
        nonce: envelope.nonce.clone(),
        signature: envelope.signature.clone(),
    })
    .map_err(|err| Error::Serialize(err.to_string()))
}

fn authenticated_proof_envelope_from_bytes(
    bytes: &[u8],
) -> Result<AuthenticatedProofEnvelope, Error> {
    let wire: AuthenticatedProofEnvelopeWire =
        serde_cbor::from_slice(bytes).map_err(|err| Error::Deserialize(err.to_string()))?;
    if wire.version != AUTHENTICATED_PROOF_ENVELOPE_VERSION {
        return Err(authenticated_proof_envelope_deserialize(format!(
            "unsupported envelope version {}",
            wire.version
        )));
    }
    if wire.algorithm != AUTHENTICATED_PROOF_ENVELOPE_ALGORITHM_HMAC_SHA256 {
        return Err(authenticated_proof_envelope_deserialize(format!(
            "unsupported envelope algorithm {}",
            wire.algorithm
        )));
    }
    if wire.signature.len() != 32 {
        return Err(authenticated_proof_envelope_deserialize(
            "HMAC-SHA256 signature must be 32 bytes",
        ));
    }
    Ok(AuthenticatedProofEnvelope {
        algorithm: wire.algorithm,
        key_id: wire.key_id,
        proof_bundle: wire.proof_bundle,
        context: wire.context,
        issued_at_millis: wire.issued_at_millis,
        expires_at_millis: wire.expires_at_millis,
        nonce: wire.nonce,
        signature: wire.signature,
    })
}

fn authenticated_proof_envelope_signing_bytes(
    envelope: &AuthenticatedProofEnvelope,
) -> Result<Vec<u8>, Error> {
    let mut bytes = AUTHENTICATED_PROOF_ENVELOPE_DOMAIN.to_vec();
    bytes.push(0);
    let payload = serde_cbor::ser::to_vec_packed(&AuthenticatedProofEnvelopeSigningWire {
        version: AUTHENTICATED_PROOF_ENVELOPE_VERSION,
        algorithm: envelope.algorithm.clone(),
        key_id: envelope.key_id.clone(),
        proof_bundle: envelope.proof_bundle.clone(),
        context: envelope.context.clone(),
        issued_at_millis: envelope.issued_at_millis,
        expires_at_millis: envelope.expires_at_millis,
        nonce: envelope.nonce.clone(),
    })
    .map_err(|err| Error::Serialize(err.to_string()))?;
    bytes.extend(payload);
    Ok(bytes)
}

fn authenticated_proof_envelope_deserialize(message: impl Into<String>) -> Error {
    Error::Deserialize(format!(
        "invalid authenticated proof envelope: {}",
        message.into()
    ))
}

fn hmac_sha256(secret: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK_SIZE: usize = 64;

    let mut key_block = [0u8; BLOCK_SIZE];
    if secret.len() > BLOCK_SIZE {
        let digest = Sha256::digest(secret);
        key_block[..digest.len()].copy_from_slice(&digest);
    } else {
        key_block[..secret.len()].copy_from_slice(secret);
    }

    let mut inner_pad = [0x36u8; BLOCK_SIZE];
    let mut outer_pad = [0x5cu8; BLOCK_SIZE];
    for idx in 0..BLOCK_SIZE {
        inner_pad[idx] ^= key_block[idx];
        outer_pad[idx] ^= key_block[idx];
    }

    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(message);
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_digest);
    outer.finalize().into()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&left_byte, &right_byte) in left.iter().zip(right) {
        diff |= left_byte ^ right_byte;
    }
    diff == 0
}
