use prolly::{
    inspect_proof_bundle, sign_proof_bundle_hmac_sha256, verify_authenticated_proof_bundle,
    verify_authenticated_proof_envelope, verify_diff_page_proof, verify_key_proof,
    verify_multi_key_proof, verify_proof_bundle, verify_range_page_proof, verify_range_proof,
    AuthenticatedProofEnvelope, Config, Diff, DiffPageProof, KeyProof, MemStore, MultiKeyProof,
    Prolly, ProofBundleKind, RangeCursor, RangePageProof, RangeProof,
};
use std::collections::HashSet;
use std::sync::Arc;

fn proof_config() -> Config {
    Config::builder()
        .min_chunk_size(1)
        .max_chunk_size(2)
        .chunking_factor(16)
        .build()
}

fn populated_tree() -> (Prolly<Arc<MemStore>>, prolly::Tree) {
    let prolly = Prolly::new(Arc::new(MemStore::new()), proof_config());
    let entries = (0..12)
        .map(|idx| {
            (
                format!("k{idx:02}").into_bytes(),
                format!("v{idx:02}").into_bytes(),
            )
        })
        .collect::<Vec<_>>();
    let tree = prolly.build_from_sorted_entries(entries).unwrap();
    (prolly, tree)
}

#[test]
fn key_proof_verifies_present_key_without_store() {
    let (prolly, tree) = populated_tree();

    let proof = prolly.prove_key(&tree, b"k05").unwrap();
    assert_eq!(proof.root, tree.root);
    assert!(
        proof.path.len() > 1,
        "test should exercise an internal path"
    );

    let verification = verify_key_proof(&proof);
    assert!(verification.valid);
    assert!(verification.exists());
    assert_eq!(verification.key, b"k05");
    assert_eq!(verification.value, Some(b"v05".to_vec()));
}

#[test]
fn key_proof_verifies_absent_key_without_store() {
    let (prolly, tree) = populated_tree();

    let proof = prolly.prove_key(&tree, b"k05a").unwrap();
    assert_eq!(prolly.get(&tree, b"k05a").unwrap(), None);

    let verification = proof.verify();
    assert!(verification.valid);
    assert!(verification.is_absence());
    assert_eq!(verification.value, None);
}

#[test]
fn empty_tree_proof_verifies_absence() {
    let prolly = Prolly::new(MemStore::new(), proof_config());
    let tree = prolly.create();

    let proof = prolly.prove_key(&tree, b"missing").unwrap();
    assert_eq!(proof.root, None);
    assert!(proof.path.is_empty());

    let verification = proof.verify();
    assert!(verification.valid);
    assert!(verification.is_absence());
}

#[test]
fn tampered_proofs_do_not_verify() {
    let (prolly, tree) = populated_tree();
    let proof = prolly.prove_key(&tree, b"k05").unwrap();

    let mut wrong_root = proof.clone();
    wrong_root.root = Some(prolly::Cid::from_bytes(b"not this root"));
    assert!(!wrong_root.verify().valid);

    let mut wrong_leaf = proof.clone();
    let leaf = wrong_leaf.path.last_mut().unwrap();
    let value_index = leaf.search(b"k05").unwrap();
    leaf.vals[value_index] = b"tampered".to_vec();
    assert!(!wrong_leaf.verify().valid);
}

#[test]
fn proof_round_trips_through_node_bytes() {
    let (prolly, tree) = populated_tree();
    let proof = prolly.prove_key(&tree, b"k03").unwrap();

    let encoded = proof.path_node_bytes();
    let decoded =
        KeyProof::from_node_bytes(proof.root.clone(), proof.key.clone(), encoded).unwrap();

    assert_eq!(decoded.verify().value, Some(b"v03".to_vec()));
}

#[test]
fn key_proof_round_trips_through_bundle_bytes() {
    let (prolly, tree) = populated_tree();
    let proof = prolly.prove_key(&tree, b"k03").unwrap();

    let bytes = proof.to_bundle_bytes().unwrap();
    assert_eq!(bytes, proof.to_bundle_bytes().unwrap());
    let decoded = KeyProof::from_bundle_bytes(&bytes).unwrap();

    assert_eq!(decoded, proof);
    assert_eq!(decoded.verify().value, Some(b"v03".to_vec()));
}

#[test]
fn multi_key_proof_verifies_present_and_absent_keys_without_store() {
    let (prolly, tree) = populated_tree();

    let proof = prolly
        .prove_keys(
            &tree,
            &[b"k01".as_slice(), b"k05a".as_slice(), b"k10".as_slice()],
        )
        .unwrap();
    assert_eq!(proof.root, tree.root);
    assert_eq!(
        proof.keys,
        vec![b"k01".to_vec(), b"k05a".to_vec(), b"k10".to_vec()]
    );

    let verification = verify_multi_key_proof(&proof);
    assert!(verification.valid);
    assert!(verification.all_valid());
    assert_eq!(verification.results.len(), 3);
    assert_eq!(verification.results[0].value, Some(b"v01".to_vec()));
    assert!(verification.results[0].exists());
    assert_eq!(verification.results[1].value, None);
    assert!(verification.results[1].is_absence());
    assert_eq!(verification.results[2].value, Some(b"v10".to_vec()));
}

#[test]
fn multi_key_proof_deduplicates_shared_nodes() {
    let (prolly, tree) = populated_tree();
    let keys = [b"k01".as_slice(), b"k02".as_slice(), b"k03".as_slice()];

    let individual_node_count = keys
        .iter()
        .map(|key| prolly.prove_key(&tree, key).unwrap().path.len())
        .sum::<usize>();
    let proof = prolly.prove_keys(&tree, &keys).unwrap();
    let unique_cids = proof
        .path
        .iter()
        .map(|node| node.cid())
        .collect::<HashSet<_>>();

    assert_eq!(proof.path.len(), unique_cids.len());
    assert!(proof.path.len() < individual_node_count);
    assert!(proof.verify().valid);
}

#[test]
fn empty_tree_multi_key_proof_verifies_absence() {
    let prolly = Prolly::new(MemStore::new(), proof_config());
    let tree = prolly.create();

    let proof = prolly
        .prove_keys(&tree, &[b"a".as_slice(), b"b".as_slice()])
        .unwrap();
    assert_eq!(proof.root, None);
    assert!(proof.path.is_empty());

    let verification = proof.verify();
    assert!(verification.valid);
    assert_eq!(verification.results.len(), 2);
    assert!(verification
        .results
        .iter()
        .all(|result| result.is_absence()));
}

#[test]
fn multi_key_proof_round_trips_through_node_bytes() {
    let (prolly, tree) = populated_tree();
    let proof = prolly
        .prove_keys(&tree, &[b"k03".as_slice(), b"k07".as_slice()])
        .unwrap();

    let decoded = MultiKeyProof::from_node_bytes(
        proof.root.clone(),
        proof.keys.clone(),
        proof.path_node_bytes(),
    )
    .unwrap();
    let verification = decoded.verify();

    assert!(verification.valid);
    assert_eq!(verification.results[0].value, Some(b"v03".to_vec()));
    assert_eq!(verification.results[1].value, Some(b"v07".to_vec()));
}

#[test]
fn multi_key_proof_round_trips_through_bundle_bytes() {
    let (prolly, tree) = populated_tree();
    let proof = prolly
        .prove_keys(&tree, &[b"k03".as_slice(), b"k05a".as_slice()])
        .unwrap();

    let bytes = proof.to_bundle_bytes().unwrap();
    assert_eq!(bytes, proof.to_bundle_bytes().unwrap());
    let decoded = MultiKeyProof::from_bundle_bytes(&bytes).unwrap();
    let verification = decoded.verify();

    assert_eq!(decoded, proof);
    assert!(verification.valid);
    assert_eq!(verification.results[0].value, Some(b"v03".to_vec()));
    assert!(verification.results[1].is_absence());
}

#[test]
fn proof_bundle_decoding_rejects_wrong_kind_and_bad_root() {
    #[derive(serde::Serialize)]
    struct BadBundleWire {
        version: u64,
        kind: u8,
        root: Option<Vec<u8>>,
        keys: Vec<Vec<u8>>,
        path_node_bytes: Vec<Vec<u8>>,
    }

    let (prolly, tree) = populated_tree();
    let key_proof = prolly.prove_key(&tree, b"k03").unwrap();
    let multi_proof = prolly
        .prove_keys(&tree, &[b"k03".as_slice(), b"k04".as_slice()])
        .unwrap();

    assert!(MultiKeyProof::from_bundle_bytes(&key_proof.to_bundle_bytes().unwrap()).is_err());
    assert!(KeyProof::from_bundle_bytes(&multi_proof.to_bundle_bytes().unwrap()).is_err());

    let bad_root = serde_cbor::ser::to_vec_packed(&BadBundleWire {
        version: 1,
        kind: 1,
        root: Some(vec![1, 2, 3]),
        keys: vec![b"k03".to_vec()],
        path_node_bytes: key_proof.path_node_bytes(),
    })
    .unwrap();
    assert!(KeyProof::from_bundle_bytes(&bad_root).is_err());
}

#[test]
fn proof_bundle_summary_identifies_bundle_kinds_and_bounds() {
    let (prolly, tree) = populated_tree();

    let key_proof = prolly.prove_key(&tree, b"k03").unwrap();
    let key_summary = inspect_proof_bundle(&key_proof.to_bundle_bytes().unwrap()).unwrap();
    assert_eq!(key_summary.kind, ProofBundleKind::Key);
    assert_eq!(key_summary.kind_name(), "key");
    assert_eq!(key_summary.root, tree.root);
    assert_eq!(key_summary.other_root, None);
    assert_eq!(key_summary.key_count, 1);
    assert_eq!(key_summary.path_node_count, key_proof.path.len());
    assert_eq!(key_summary.limit, None);

    let multi_proof = prolly
        .prove_keys(&tree, &[b"k03".as_slice(), b"k05a".as_slice()])
        .unwrap();
    let multi_summary = inspect_proof_bundle(&multi_proof.to_bundle_bytes().unwrap()).unwrap();
    assert_eq!(multi_summary.kind, ProofBundleKind::MultiKey);
    assert_eq!(multi_summary.kind_name(), "multi_key");
    assert_eq!(multi_summary.root, tree.root);
    assert_eq!(multi_summary.key_count, 2);
    assert_eq!(multi_summary.path_node_count, multi_proof.path.len());

    let range_proof = prolly.prove_range(&tree, b"k02", Some(b"k06")).unwrap();
    let range_summary = inspect_proof_bundle(&range_proof.to_bundle_bytes().unwrap()).unwrap();
    assert_eq!(range_summary.kind, ProofBundleKind::Range);
    assert_eq!(range_summary.root, tree.root);
    assert_eq!(range_summary.start, Some(b"k02".to_vec()));
    assert_eq!(range_summary.end, Some(b"k06".to_vec()));
    assert_eq!(range_summary.after, None);
    assert_eq!(range_summary.path_node_count, range_proof.path.len());

    let cursor = RangeCursor::after_key(b"k03".to_vec());
    let proved_page = prolly
        .prove_range_page(&tree, &cursor, Some(b"k08"), 2)
        .unwrap();
    let page_summary = inspect_proof_bundle(&proved_page.proof.to_bundle_bytes().unwrap()).unwrap();
    assert_eq!(page_summary.kind, ProofBundleKind::RangePage);
    assert_eq!(page_summary.root, tree.root);
    assert_eq!(page_summary.after, Some(b"k03".to_vec()));
    assert_eq!(page_summary.end, proved_page.proof.end);
    assert_eq!(page_summary.path_node_count, proved_page.proof.path.len());

    let other = prolly.delete(&tree, b"k02").unwrap();
    let other = prolly
        .put(&other, b"k04".to_vec(), b"v04x".to_vec())
        .unwrap();
    let other = prolly
        .put(&other, b"k05a".to_vec(), b"bonus".to_vec())
        .unwrap();
    let proved_diff = prolly
        .prove_diff_page(&tree, &other, &RangeCursor::start(), None, 1)
        .unwrap();
    let diff_summary = inspect_proof_bundle(&proved_diff.proof.to_bundle_bytes().unwrap()).unwrap();
    assert_eq!(diff_summary.kind, ProofBundleKind::DiffPage);
    assert_eq!(diff_summary.kind_name(), "diff_page");
    assert_eq!(diff_summary.root, tree.root);
    assert_eq!(diff_summary.other_root, other.root);
    assert_eq!(diff_summary.after, None);
    assert_eq!(diff_summary.requested_end, None);
    assert_eq!(diff_summary.limit, Some(1));
    assert!(diff_summary.has_lookahead);
    assert_eq!(
        diff_summary.path_node_count,
        proved_diff.proof.base.path.len()
            + proved_diff.proof.other.path.len()
            + proved_diff
                .proof
                .lookahead_base
                .as_ref()
                .map_or(0, |proof| proof.path.len())
            + proved_diff
                .proof
                .lookahead_other
                .as_ref()
                .map_or(0, |proof| proof.path.len())
    );

    let envelope = sign_proof_bundle_hmac_sha256(
        key_proof.to_bundle_bytes().unwrap(),
        b"summary-key".to_vec(),
        b"shared secret",
        b"routing".to_vec(),
        None,
        None,
        b"nonce".to_vec(),
    )
    .unwrap();
    assert!(inspect_proof_bundle(&envelope.to_bytes().unwrap()).is_err());
}

#[test]
fn verify_proof_bundle_routes_and_verifies_bundle_kinds() {
    let (prolly, tree) = populated_tree();

    let key_bundle = prolly
        .prove_key(&tree, b"k03")
        .unwrap()
        .to_bundle_bytes()
        .unwrap();
    let key_verified = verify_proof_bundle(&key_bundle).unwrap();
    assert!(key_verified.valid);
    assert_eq!(key_verified.kind_name(), "key");
    assert_eq!(key_verified.summary.kind, ProofBundleKind::Key);
    assert_eq!(key_verified.exists_count, 1);
    assert_eq!(key_verified.absence_count, 0);

    let absence_bundle = prolly
        .prove_key(&tree, b"k99")
        .unwrap()
        .to_bundle_bytes()
        .unwrap();
    let absence_verified = verify_proof_bundle(&absence_bundle).unwrap();
    assert!(absence_verified.valid);
    assert_eq!(absence_verified.exists_count, 0);
    assert_eq!(absence_verified.absence_count, 1);

    let multi_bundle = prolly
        .prove_keys(&tree, &[b"k03".as_slice(), b"k99".as_slice()])
        .unwrap()
        .to_bundle_bytes()
        .unwrap();
    let multi_verified = verify_proof_bundle(&multi_bundle).unwrap();
    assert!(multi_verified.valid);
    assert_eq!(multi_verified.summary.kind, ProofBundleKind::MultiKey);
    assert_eq!(multi_verified.exists_count, 1);
    assert_eq!(multi_verified.absence_count, 1);

    let range_bundle = prolly
        .prove_range(&tree, b"k02", Some(b"k06"))
        .unwrap()
        .to_bundle_bytes()
        .unwrap();
    let range_verified = verify_proof_bundle(&range_bundle).unwrap();
    assert!(range_verified.valid);
    assert_eq!(range_verified.summary.kind, ProofBundleKind::Range);
    assert_eq!(range_verified.entry_count, 4);

    let page_bundle = prolly
        .prove_range_page(
            &tree,
            &RangeCursor::after_key(b"k03".to_vec()),
            Some(b"k08"),
            2,
        )
        .unwrap()
        .proof
        .to_bundle_bytes()
        .unwrap();
    let page_verified = verify_proof_bundle(&page_bundle).unwrap();
    assert!(page_verified.valid);
    assert_eq!(page_verified.summary.kind, ProofBundleKind::RangePage);
    assert_eq!(page_verified.entry_count, 2);

    let other = prolly.delete(&tree, b"k02").unwrap();
    let other = prolly
        .put(&other, b"k04".to_vec(), b"v04x".to_vec())
        .unwrap();
    let other = prolly
        .put(&other, b"k05a".to_vec(), b"bonus".to_vec())
        .unwrap();
    let diff_bundle = prolly
        .prove_diff_page(&tree, &other, &RangeCursor::start(), None, 1)
        .unwrap()
        .proof
        .to_bundle_bytes()
        .unwrap();
    let diff_verified = verify_proof_bundle(&diff_bundle).unwrap();
    assert!(diff_verified.valid);
    assert_eq!(diff_verified.summary.kind, ProofBundleKind::DiffPage);
    assert_eq!(diff_verified.diff_count, 1);
    assert!(diff_verified.next_cursor.is_some());

    let mut tampered = prolly.prove_key(&tree, b"k03").unwrap();
    let leaf = tampered.path.iter_mut().find(|node| node.leaf).unwrap();
    leaf.vals[0] = b"tampered".to_vec();
    let tampered_verified = verify_proof_bundle(&tampered.to_bundle_bytes().unwrap()).unwrap();
    assert!(!tampered_verified.valid);
    assert_eq!(tampered_verified.exists_count, 0);
    assert_eq!(tampered_verified.absence_count, 0);
}

#[test]
fn tampered_multi_key_proofs_do_not_verify() {
    let (prolly, tree) = populated_tree();
    let proof = prolly
        .prove_keys(&tree, &[b"k01".as_slice(), b"k10".as_slice()])
        .unwrap();

    let mut wrong_root = proof.clone();
    wrong_root.root = Some(prolly::Cid::from_bytes(b"not this root"));
    assert!(!wrong_root.verify().valid);

    let mut wrong_leaf = proof;
    let leaf = wrong_leaf.path.iter_mut().find(|node| node.leaf).unwrap();
    leaf.vals[0] = b"tampered".to_vec();
    assert!(!wrong_leaf.verify().valid);
}

#[test]
fn range_proof_verifies_complete_bounded_range_without_store() {
    let (prolly, tree) = populated_tree();
    let proof = prolly.prove_range(&tree, b"k03", Some(b"k08")).unwrap();
    assert_eq!(proof.root, tree.root);
    assert_eq!(proof.start, b"k03");
    assert_eq!(proof.end, Some(b"k08".to_vec()));

    let expected = prolly
        .range(&tree, b"k03", Some(b"k08"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let verification = verify_range_proof(&proof);

    assert!(verification.valid);
    assert_eq!(verification.entries, expected);
    assert_eq!(verification.entries.len(), 5);
}

#[test]
fn prefix_proof_uses_prefix_bounds_and_verifies_without_store() {
    let prolly = Prolly::new(Arc::new(MemStore::new()), proof_config());
    let tree = prolly
        .build_from_sorted_entries(vec![
            (b"account/1/a".to_vec(), b"a1".to_vec()),
            (b"account/1/b".to_vec(), b"b1".to_vec()),
            (b"account/2/a".to_vec(), b"a2".to_vec()),
            (b"audit/1".to_vec(), b"log".to_vec()),
        ])
        .unwrap();

    let proof = prolly.prove_prefix(&tree, b"account/1/").unwrap();
    assert_eq!(proof.start, b"account/1/");
    assert_eq!(proof.end, Some(b"account/10".to_vec()));

    let expected = prolly
        .range(&tree, b"account/1/", Some(b"account/10"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let verified = proof.verify();

    assert!(verified.valid);
    assert_eq!(verified.entries, expected);
    assert_eq!(
        verified
            .entries
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect::<Vec<_>>(),
        vec![
            (b"account/1/a".as_slice(), b"a1".as_slice()),
            (b"account/1/b".as_slice(), b"b1".as_slice())
        ]
    );
}

#[test]
fn range_proof_verifies_empty_ranges_and_empty_trees() {
    let (prolly, tree) = populated_tree();

    let before_first = prolly
        .prove_range(&tree, b"a", Some(b"k00"))
        .unwrap()
        .verify();
    assert!(before_first.valid);
    assert!(before_first.is_empty());

    let empty_by_bounds = prolly
        .prove_range(&tree, b"k03", Some(b"k03"))
        .unwrap()
        .verify();
    assert!(empty_by_bounds.valid);
    assert!(empty_by_bounds.is_empty());

    let empty_prolly = Prolly::new(MemStore::new(), proof_config());
    let empty_tree = empty_prolly.create();
    let empty_tree_proof = empty_prolly
        .prove_range(&empty_tree, b"a", Some(b"z"))
        .unwrap()
        .verify();
    assert!(empty_tree_proof.valid);
    assert!(empty_tree_proof.is_empty());
}

#[test]
fn range_proof_round_trips_through_node_bytes_and_bundle_bytes() {
    let (prolly, tree) = populated_tree();
    let proof = prolly.prove_range(&tree, b"k02", Some(b"k06")).unwrap();

    let decoded = RangeProof::from_node_bytes(
        proof.root.clone(),
        proof.start.clone(),
        proof.end.clone(),
        proof.path_node_bytes(),
    )
    .unwrap();
    assert_eq!(decoded.verify().entries, proof.verify().entries);

    let bytes = proof.to_bundle_bytes().unwrap();
    assert_eq!(bytes, proof.to_bundle_bytes().unwrap());
    let bundled = RangeProof::from_bundle_bytes(&bytes).unwrap();
    assert_eq!(bundled, proof);
    assert!(bundled.verify().valid);
}

#[test]
fn range_page_proof_verifies_exclusive_cursor_window_without_store() {
    let prolly = Prolly::new(Arc::new(MemStore::new()), proof_config());
    let tree = prolly
        .build_from_sorted_entries(vec![
            (b"a".to_vec(), b"root".to_vec()),
            (b"a\0".to_vec(), b"nul".to_vec()),
            (b"a/1".to_vec(), b"child".to_vec()),
            (b"b".to_vec(), b"bee".to_vec()),
            (b"c".to_vec(), b"see".to_vec()),
        ])
        .unwrap();

    let cursor = RangeCursor::after_key(b"a".to_vec());
    let proved = prolly.prove_range_page(&tree, &cursor, None, 2).unwrap();
    assert_eq!(
        proved.page.entries,
        vec![
            (b"a\0".to_vec(), b"nul".to_vec()),
            (b"a/1".to_vec(), b"child".to_vec()),
        ]
    );
    assert_eq!(
        proved
            .page
            .next_cursor
            .as_ref()
            .and_then(RangeCursor::after),
        Some(b"a/1".as_slice())
    );
    assert_eq!(proved.proof.after, Some(b"a".to_vec()));
    assert_eq!(proved.proof.end, Some(b"b".to_vec()));

    let verification = verify_range_page_proof(&proved.proof);
    assert!(verification.valid);
    assert_eq!(verification.entries, proved.page.entries);

    let from_nodes = RangePageProof::from_node_bytes(
        proved.proof.root.clone(),
        proved.proof.after.clone(),
        proved.proof.end.clone(),
        proved.proof.path_node_bytes(),
    )
    .unwrap();
    assert_eq!(from_nodes.verify().entries, proved.page.entries);

    let bundle = proved.proof.to_bundle_bytes().unwrap();
    assert_eq!(bundle, proved.proof.to_bundle_bytes().unwrap());
    let from_bundle = RangePageProof::from_bundle_bytes(&bundle).unwrap();
    assert_eq!(from_bundle, proved.proof);
    assert_eq!(from_bundle.verify().entries, proved.page.entries);
}

#[test]
fn range_page_proof_final_and_zero_limit_pages_verify_empty_windows() {
    let (prolly, tree) = populated_tree();

    let final_page = prolly
        .prove_range_page(&tree, &RangeCursor::after_key(b"k10".to_vec()), None, 10)
        .unwrap();
    assert_eq!(
        final_page.page.entries,
        vec![(b"k11".to_vec(), b"v11".to_vec())]
    );
    assert!(final_page.page.next_cursor.is_none());
    assert_eq!(final_page.proof.after, Some(b"k10".to_vec()));
    assert_eq!(final_page.proof.end, None);
    assert_eq!(final_page.proof.verify().entries, final_page.page.entries);

    let empty_page = prolly
        .prove_range_page(&tree, &RangeCursor::after_key(b"k11".to_vec()), None, 10)
        .unwrap();
    assert!(empty_page.page.entries.is_empty());
    assert!(empty_page.page.next_cursor.is_none());
    assert!(empty_page.proof.verify().is_empty());

    let cursor = RangeCursor::after_key(b"k04".to_vec());
    let zero_page = prolly.prove_range_page(&tree, &cursor, None, 0).unwrap();
    assert!(zero_page.page.entries.is_empty());
    assert_eq!(zero_page.page.next_cursor, Some(cursor));
    assert!(zero_page.proof.verify().is_empty());
}

#[test]
fn diff_page_proof_verifies_continued_page_with_lookahead_without_store() {
    let (prolly, base) = populated_tree();
    let other = prolly.delete(&base, b"k02").unwrap();
    let other = prolly
        .put(&other, b"k04".to_vec(), b"v04x".to_vec())
        .unwrap();
    let other = prolly
        .put(&other, b"k05a".to_vec(), b"bonus".to_vec())
        .unwrap();
    let other = prolly
        .put(&other, b"k12".to_vec(), b"v12".to_vec())
        .unwrap();

    let proved = prolly
        .prove_diff_page(&base, &other, &RangeCursor::start(), None, 2)
        .unwrap();
    assert_eq!(
        proved.page.diffs,
        vec![
            Diff::Removed {
                key: b"k02".to_vec(),
                val: b"v02".to_vec(),
            },
            Diff::Changed {
                key: b"k04".to_vec(),
                old: b"v04".to_vec(),
                new: b"v04x".to_vec(),
            },
        ]
    );
    assert_eq!(
        proved
            .page
            .next_cursor
            .as_ref()
            .and_then(RangeCursor::after),
        Some(b"k04".as_slice())
    );
    assert_eq!(proved.proof.base.after, None);
    assert_eq!(proved.proof.base.end, Some(b"k05a".to_vec()));
    assert_eq!(
        proved.proof.lookahead_base.as_ref().map(|proof| &proof.key),
        Some(&b"k05a".to_vec())
    );

    let verification = verify_diff_page_proof(&proved.proof);
    assert!(verification.valid);
    assert!(verification.base_valid);
    assert!(verification.other_valid);
    assert!(verification.lookahead_valid);
    assert_eq!(verification.diffs, proved.page.diffs);
    assert_eq!(verification.next_cursor, proved.page.next_cursor);

    let bytes = proved.proof.to_bundle_bytes().unwrap();
    assert_eq!(bytes, proved.proof.to_bundle_bytes().unwrap());
    let decoded = DiffPageProof::from_bundle_bytes(&bytes).unwrap();
    assert_eq!(decoded, proved.proof);
    assert_eq!(decoded.verify().diffs, proved.page.diffs);

    let mut wrong_lookahead = proved.proof.clone();
    wrong_lookahead.lookahead_other.as_mut().unwrap().key = b"k06".to_vec();
    assert!(!wrong_lookahead.verify().valid);

    let mut wrong_limit = proved.proof;
    wrong_limit.limit = 1;
    assert!(!wrong_limit.verify().valid);
}

#[test]
fn diff_page_proof_verifies_final_and_zero_limit_pages_without_store() {
    let (prolly, base) = populated_tree();
    let other = prolly.delete(&base, b"k02").unwrap();
    let other = prolly
        .put(&other, b"k04".to_vec(), b"v04x".to_vec())
        .unwrap();
    let other = prolly
        .put(&other, b"k05a".to_vec(), b"bonus".to_vec())
        .unwrap();
    let other = prolly
        .put(&other, b"k12".to_vec(), b"v12".to_vec())
        .unwrap();

    let final_page = prolly
        .prove_diff_page(
            &base,
            &other,
            &RangeCursor::after_key(b"k04".to_vec()),
            None,
            8,
        )
        .unwrap();
    assert_eq!(
        final_page.page.diffs,
        vec![
            Diff::Added {
                key: b"k05a".to_vec(),
                val: b"bonus".to_vec(),
            },
            Diff::Added {
                key: b"k12".to_vec(),
                val: b"v12".to_vec(),
            },
        ]
    );
    assert!(final_page.page.next_cursor.is_none());
    assert!(final_page.proof.lookahead_base.is_none());
    assert_eq!(final_page.proof.requested_end, None);

    let final_verification = final_page.proof.verify();
    assert!(final_verification.valid);
    assert_eq!(final_verification.diffs, final_page.page.diffs);
    assert_eq!(final_verification.next_cursor, None);

    let cursor = RangeCursor::after_key(b"k04".to_vec());
    let zero_page = prolly
        .prove_diff_page(&base, &other, &cursor, None, 0)
        .unwrap();
    assert!(zero_page.page.diffs.is_empty());
    assert_eq!(zero_page.page.next_cursor, Some(cursor));
    let zero_verification = zero_page.proof.verify();
    assert!(zero_verification.valid);
    assert!(zero_verification.is_empty());
    assert_eq!(zero_verification.next_cursor, zero_page.page.next_cursor);
}

#[test]
fn authenticated_proof_envelope_signs_verifies_and_round_trips_bundle() {
    let (prolly, tree) = populated_tree();
    let proof = prolly.prove_key(&tree, b"k03").unwrap();
    let bundle = proof.to_bundle_bytes().unwrap();

    let envelope = sign_proof_bundle_hmac_sha256(
        bundle.clone(),
        b"test-key".to_vec(),
        b"shared secret",
        b"tenant=t1".to_vec(),
        Some(1_700_000_000_000),
        Some(1_700_000_100_000),
        b"nonce-1".to_vec(),
    )
    .unwrap();
    assert_eq!(envelope.proof_bundle, bundle);
    assert_eq!(envelope.key_id, b"test-key".to_vec());
    assert_eq!(envelope.context, b"tenant=t1".to_vec());
    assert_eq!(envelope.signature.len(), 32);

    let encoded = envelope.to_bytes().unwrap();
    assert_eq!(encoded, envelope.to_bytes().unwrap());
    let decoded = AuthenticatedProofEnvelope::from_bytes(&encoded).unwrap();
    assert_eq!(decoded, envelope);

    let verified =
        verify_authenticated_proof_envelope(&decoded, b"shared secret", Some(1_700_000_050_000));
    assert!(verified.valid);
    assert!(verified.signature_valid);
    assert!(verified.time_valid);
    assert_eq!(verified.proof_bundle, bundle);

    let decoded_proof = KeyProof::from_bundle_bytes(&verified.proof_bundle).unwrap();
    assert_eq!(decoded_proof.verify().value, Some(b"v03".to_vec()));

    let one_shot =
        verify_authenticated_proof_bundle(&encoded, b"shared secret", Some(1_700_000_050_000))
            .unwrap();
    assert!(one_shot.valid);
    assert!(one_shot.envelope.valid);
    assert_eq!(one_shot.envelope.proof_bundle, bundle);
    assert_eq!(
        one_shot.proof.as_ref().map(|proof| proof.exists_count),
        Some(1)
    );
    assert_eq!(one_shot.proof_error, None);

    let wrong_secret =
        verify_authenticated_proof_envelope(&decoded, b"wrong secret", Some(1_700_000_050_000));
    assert!(!wrong_secret.valid);
    assert!(!wrong_secret.signature_valid);
    assert!(wrong_secret.time_valid);
    let wrong_secret_bundle =
        verify_authenticated_proof_bundle(&encoded, b"wrong secret", Some(1_700_000_050_000))
            .unwrap();
    assert!(!wrong_secret_bundle.valid);
    assert!(!wrong_secret_bundle.envelope.valid);
    assert!(wrong_secret_bundle.proof.is_none());

    let not_yet_valid =
        verify_authenticated_proof_envelope(&decoded, b"shared secret", Some(1_699_999_999_999));
    assert!(!not_yet_valid.valid);
    assert!(not_yet_valid.signature_valid);
    assert!(not_yet_valid.not_yet_valid);

    let expired =
        verify_authenticated_proof_envelope(&decoded, b"shared secret", Some(1_700_000_100_000));
    assert!(!expired.valid);
    assert!(expired.signature_valid);
    assert!(expired.expired);
    let expired_bundle =
        verify_authenticated_proof_bundle(&encoded, b"shared secret", Some(1_700_000_100_000))
            .unwrap();
    assert!(!expired_bundle.valid);
    assert!(expired_bundle.envelope.expired);
    assert!(expired_bundle.proof.is_none());

    let mut tampered = decoded.clone();
    tampered.proof_bundle.push(0);
    let tampered_verification =
        verify_authenticated_proof_envelope(&tampered, b"shared secret", Some(1_700_000_050_000));
    assert!(!tampered_verification.valid);
    assert!(!tampered_verification.signature_valid);

    let malformed_bundle_envelope = sign_proof_bundle_hmac_sha256(
        vec![0, 1, 2, 3],
        b"test-key".to_vec(),
        b"shared secret",
        b"tenant=t1".to_vec(),
        Some(1_700_000_000_000),
        Some(1_700_000_100_000),
        b"nonce-2".to_vec(),
    )
    .unwrap()
    .to_bytes()
    .unwrap();
    let malformed_bundle = verify_authenticated_proof_bundle(
        &malformed_bundle_envelope,
        b"shared secret",
        Some(1_700_000_050_000),
    )
    .unwrap();
    assert!(!malformed_bundle.valid);
    assert!(malformed_bundle.envelope.valid);
    assert!(malformed_bundle.proof.is_none());
    assert!(malformed_bundle
        .proof_error
        .as_deref()
        .is_some_and(|message| message.contains("deserialize")));
}

#[test]
fn tampered_range_proofs_do_not_verify() {
    let (prolly, tree) = populated_tree();
    let proof = prolly.prove_range(&tree, b"k02", Some(b"k09")).unwrap();

    let mut missing_child = proof.clone();
    missing_child.path.pop();
    assert!(!missing_child.verify().valid);

    let mut wrong_root = proof.clone();
    wrong_root.root = Some(prolly::Cid::from_bytes(b"not this root"));
    assert!(!wrong_root.verify().valid);

    let mut wrong_leaf = proof;
    let leaf = wrong_leaf.path.iter_mut().find(|node| node.leaf).unwrap();
    leaf.vals[0] = b"tampered".to_vec();
    assert!(!wrong_leaf.verify().valid);
}
