use prolly_s3_core::{
    encode_canonical, AuthorityLeaseStateV2, AuthorityLeaseV2, AuthorityScopeV2, AuthorityStampV2,
    RepositoryId,
};

fn lease() -> AuthorityLeaseV2 {
    AuthorityLeaseV2 {
        repository: RepositoryId::from_hash([0x11; 32]),
        scope: AuthorityScopeV2::Branch {
            name: "main".to_string(),
        },
        generation: 2,
        writer_id: "writer-b".to_string(),
        fencing_token: [0x22; 32],
        state: AuthorityLeaseStateV2::BarrierPending {
            previous_generation: 1,
        },
        expires_at_millis: 70_000,
        updated_at_millis: 10_000,
    }
}

#[test]
fn v2_authority_records_have_frozen_canonical_encodings() {
    let lease = lease();
    let stamp = AuthorityStampV2 {
        scope: lease.scope.clone(),
        generation: lease.generation,
        writer_id: lease.writer_id.clone(),
        fencing_token_digest: [0x33; 32],
    };
    let lease_hex = hex::encode(encode_canonical(&lease).unwrap());
    let stamp_hex = hex::encode(encode_canonical(&stamp).unwrap());
    assert_eq!(
        lease_hex,
        "a8009820111111111111111111111111111111111111111111111111111111111111111101a100a100646d61696e020203687772697465722d620498201822182218221822182218221822182218221822182218221822182218221822182218221822182218221822182218221822182218221822182218221822182205a101a10001061a0001117007192710"
    );
    assert_eq!(
        stamp_hex,
        "a400a100a100646d61696e010202687772697465722d6203982018331833183318331833183318331833183318331833183318331833183318331833183318331833183318331833183318331833183318331833183318331833"
    );
}
