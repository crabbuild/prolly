$version: "2"

namespace prolly.native.s3.v1

@documentation("Semantic in-process API model. It does not replace canonical CBOR persistence.")
service NativeVersionedS3 {
    version: "1"
    operations: [
        Initialize, Open, PutObject, GetObject, HeadObject, DeleteObject,
        CopyObject, ListObjects, ListObjectVersions, CreateMultipart,
        CompleteMultipart, AbortMultipart, CreateBatch, CommitBatch,
        CreateBranch, MoveBranch, CreateTag, Log, Diff, Merge, Restore,
        Clone, Fetch, Push, Fsck, Repair, PlanGc, SweepGc
    ]
}

@length(min: 1, max: 1024)
string ObjectKey

@length(min: 1, max: 255)
string RefName

string RepositoryPrefix
string CommitId
string ObjectVersionId
string OperationId
string BatchId
string ProviderVersionId
string ETag
string ContinuationToken
blob Body

structure InitializeInput {
    @required repositoryPrefix: RepositoryPrefix = ".prolly/v1"
    @required defaultBranch: RefName = "main"
    operation: OperationId
}
structure InitializeOutput { @required repositoryId: String, @required head: CommitId }
operation Initialize { input: InitializeInput, output: InitializeOutput, errors: [ProtocolError] }

structure OpenInput { @required repositoryPrefix: RepositoryPrefix = ".prolly/v1" }
structure OpenOutput { @required repositoryId: String, @required formatVersion: Short = 1 }
operation Open { input: OpenInput, output: OpenOutput, errors: [ProtocolError] }

structure PutObjectInput {
    @required branch: RefName = "main"
    @required key: ObjectKey
    @required body: Body
    operation: OperationId
    expectedHead: CommitId
}
structure MutationOutput {
    @required commit: CommitId
    @required operation: OperationId
    @required versions: ObjectVersionIdList
    @required idempotentReplay: Boolean
}
list ObjectVersionIdList { member: ObjectVersionId }
operation PutObject { input: PutObjectInput, output: MutationOutput, errors: [ProtocolError] }

structure ObjectSelector {
    @required branch: RefName = "main"
    @required key: ObjectKey
    revision: CommitId
    version: ObjectVersionId
}
structure GetObjectOutput {
    @required body: Body
    @required commit: CommitId
    @required version: ObjectVersionId
    @required providerVersion: ProviderVersionId
    @required etag: ETag
}
operation GetObject { input: ObjectSelector, output: GetObjectOutput, errors: [ProtocolError] }
operation HeadObject { input: ObjectSelector, output: GetObjectOutput, errors: [ProtocolError] }

structure DeleteObjectInput {
    @required branch: RefName = "main"
    @required key: ObjectKey
    operation: OperationId
    expectedHead: CommitId
}
operation DeleteObject { input: DeleteObjectInput, output: MutationOutput, errors: [ProtocolError] }

structure CopyObjectInput {
    @required branch: RefName = "main"
    @required sourceKey: ObjectKey
    sourceVersion: ObjectVersionId
    @required destinationKey: ObjectKey
    operation: OperationId
    expectedHead: CommitId
}
operation CopyObject { input: CopyObjectInput, output: MutationOutput, errors: [ProtocolError] }

structure ListInput {
    @required branch: RefName = "main"
    prefix: String
    continuation: ContinuationToken
    @range(min: 1, max: 1000) limit: Integer = 1000
}
structure ObjectEntry { @required key: ObjectKey, @required version: ObjectVersionId }
list ObjectEntries { member: ObjectEntry }
structure ListOutput { @required entries: ObjectEntries, continuation: ContinuationToken }
operation ListObjects { input: ListInput, output: ListOutput, errors: [ProtocolError] }
operation ListObjectVersions { input: ListInput, output: ListOutput, errors: [ProtocolError] }

structure MultipartInput { @required branch: RefName = "main", @required key: ObjectKey, operation: OperationId }
structure MultipartOutput { @required uploadId: String, @required operation: OperationId }
operation CreateMultipart { input: MultipartInput, output: MultipartOutput, errors: [ProtocolError] }
structure CompleteMultipartInput { @required uploadId: String, @required session: Blob }
operation CompleteMultipart { input: CompleteMultipartInput, output: MutationOutput, errors: [ProtocolError] }
operation AbortMultipart { input: CompleteMultipartInput, errors: [ProtocolError] }

structure BatchInput { @required branch: RefName = "main", operation: OperationId }
structure BatchOutput { @required batch: BatchId, @required base: CommitId }
operation CreateBatch { input: BatchInput, output: BatchOutput, errors: [ProtocolError] }
structure CommitBatchInput { @required batch: BatchId, @required mutations: Blob }
operation CommitBatch { input: CommitBatchInput, output: MutationOutput, errors: [ProtocolError] }

structure RefInput { @required name: RefName, target: CommitId, operation: OperationId }
structure RefOutput { @required name: RefName, @required target: CommitId, @required generation: Long }
operation CreateBranch { input: RefInput, output: RefOutput, errors: [ProtocolError] }
operation MoveBranch { input: RefInput, output: RefOutput, errors: [ProtocolError] }
operation CreateTag { input: RefInput, output: RefOutput, errors: [ProtocolError] }

structure HistoryInput { @required branch: RefName = "main", revision: CommitId, @range(min: 1) limit: Integer = 100 }
list CommitIds { member: CommitId }
structure HistoryOutput { @required commits: CommitIds }
operation Log { input: HistoryInput, output: HistoryOutput, errors: [ProtocolError] }
structure PairInput { @required left: CommitId, @required right: CommitId }
operation Diff { input: PairInput, output: ListOutput, errors: [ProtocolError] }
operation Merge { input: PairInput, output: MutationOutput, errors: [ProtocolError] }
operation Restore { input: PairInput, output: MutationOutput, errors: [ProtocolError] }

structure SyncInput { @required targetPrefix: RepositoryPrefix, branch: RefName = "main" }
structure SyncOutput { @required commits: Long, @required versions: Long }
operation Clone { input: SyncInput, output: SyncOutput, errors: [ProtocolError] }
operation Fetch { input: SyncInput, output: SyncOutput, errors: [ProtocolError] }
operation Push { input: SyncInput, output: SyncOutput, errors: [ProtocolError] }

structure AdminInput { branch: RefName = "main", operation: OperationId }
structure AdminOutput { @required report: Document }
operation Fsck { input: AdminInput, output: AdminOutput, errors: [ProtocolError] }
operation Repair { input: AdminInput, output: AdminOutput, errors: [ProtocolError] }
operation PlanGc { input: AdminInput, output: AdminOutput, errors: [ProtocolError] }
operation SweepGc { input: AdminInput, output: AdminOutput, errors: [ProtocolError] }

@error("client")
structure ProtocolError {
    @required code: String
    @required retry: String
    @required message: String
    operation: OperationId
    providerCode: String
    providerRequestId: String
}
