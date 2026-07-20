import Foundation

public let GENERAL: UInt32 = 0
public let POINT_UPSERT: UInt32 = 1
public let POINT_DELETE: UInt32 = 2
public let BATCH_MUTATION: UInt32 = 3
public let TREE_BUILD: UInt32 = 4
public let MERGE: UInt32 = 5
public let RANGE_DELETE: UInt32 = 6
public let REPLICATION: UInt32 = 7
public let MAINTENANCE: UInt32 = 8

public extension ForeignRemoteStore {
    func publishNodes(publication: NodePublicationRecord) async -> UnitResultRecord {
        if let hint = publication.hint {
            return await batchPutNodesWithHint(
                nodes: publication.nodes,
                namespace: hint.namespace,
                key: hint.key,
                value: hint.value
            )
        }
        return await batchNodes(
            ops: publication.nodes.map { node in
                NodeMutationRecord(
                    key: node.key,
                    value: OptionalBytesRecord(present: true, value: node.value)
                )
            }
        )
    }
}

public extension HostStoreCallback {
    func publishNodes(publication: NodePublicationRecord) -> HostStoreUnitResultRecord {
        let batchResult = batch(
            ops: publication.nodes.map { node in
                MutationRecord(kind: .upsert, key: node.key, value: node.value)
            }
        )
        if batchResult.error != nil {
            return batchResult
        }
        guard let hint = publication.hint else {
            return batchResult
        }
        return putHint(namespace: hint.namespace, key: hint.key, value: hint.value)
    }
}
