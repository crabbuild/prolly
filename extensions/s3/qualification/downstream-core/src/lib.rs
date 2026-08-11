use prolly_s3_core::{CommitId, ErrorCode, ObjectVersionId, RepositoryOptions};

pub fn compile_core_surface(
    commit: CommitId,
    version: ObjectVersionId,
) -> (CommitId, ObjectVersionId, ErrorCode, RepositoryOptions) {
    (
        commit,
        version,
        ErrorCode::ProviderNotQualified,
        RepositoryOptions::default(),
    )
}
