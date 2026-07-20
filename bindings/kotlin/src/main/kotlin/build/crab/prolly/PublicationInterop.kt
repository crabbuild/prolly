package build.crab.prolly

/** Java-friendly accessors for UniFFI records containing Kotlin unsigned scalars. */
object PublicationInterop {
    @JvmStatic
    fun originCode(publication: NodePublicationRecord): Int = publication.origin.code.toInt()

    @JvmStatic
    fun clonePublication(publication: NodePublicationRecord): NodePublicationRecord =
        NodePublicationRecord(
            nodes = publication.nodes.map { node ->
                NodeEntryRecord(node.key.copyOf(), node.value.copyOf())
            },
            hint = publication.hint?.let { hint ->
                NodePublicationHintRecord(
                    hint.namespace.copyOf(),
                    hint.key.copyOf(),
                    hint.value.copyOf(),
                )
            },
            origin = PublicationOriginRecord(publication.origin.code),
        )
}
