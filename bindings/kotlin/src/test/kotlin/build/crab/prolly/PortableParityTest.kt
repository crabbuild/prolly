package build.crab.prolly

import build.crab.prolly.api.Engine
import build.crab.prolly.api.ProximityRecord
import build.crab.prolly.api.verifyKeyProof
import build.crab.prolly.api.verifyMultiKeyProof
import build.crab.prolly.api.verifyRangePageProof
import build.crab.prolly.api.verifyRangeProof
import build.crab.prolly.api.verifyProximityMembershipProof
import build.crab.prolly.api.verifyProximityStructureProof
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class PortableParityTest {
    @Test
    fun hnswAcceleratorLifecycleIsPortable() {
        ProllyNative.useLocalDebugLibrary()
        Engine.memory().use { engine ->
            engine.buildProximity(
                2u,
                (0 until 16).map { index ->
                    ProximityRecord(
                        "vector-%02d".format(index).bytes(),
                        listOf(index.toFloat(), 0f),
                        "value-%02d".format(index).bytes(),
                    )
                },
            ).use { proximity ->
                val built = proximity.buildHnsw()
                assertEquals(16uL, built.stats.records)
                val request = exactProximitySearchRequest(listOf(0f, 0f), 3uL).copy(
                    policy = SearchPolicyKind.FIXED_BUDGET,
                    backend = SearchBackendRecord.HNSW,
                )
                built.index.use { index ->
                    assertEquals(true, index.isCanonical)
                    assertArrayEquals(proximity.descriptor, index.sourceDescriptor)
                    val result = index.search(proximity, request)
                    assertEquals(SearchBackendRecord.HNSW, result.backend)
                    assertArrayEquals("vector-00".bytes(), result.neighbors.first().key)
                    val manifest = index.manifest
                    index.proveSearch(proximity, request).use { proof ->
                        assertEquals(
                            SearchBackendRecord.HNSW,
                            proof.verify(proximity.descriptor).result.backend,
                        )
                    }
                    proximity.loadHnsw(manifest).use { loaded ->
                        assertArrayEquals(manifest, loaded.manifest)
                    }
                }
            }
        }
    }

    @Test
    fun proximityRichSearchRequestIsSharedByMapSessionAndProof() {
        ProllyNative.useLocalDebugLibrary()
        Engine.memory().use { engine ->
            engine.buildProximity(
                2u,
                listOf(
                    ProximityRecord("a".bytes(), listOf(0f, 0f), "alpha".bytes()),
                    ProximityRecord("ab".bytes(), listOf(1f, 0f), "alphabet".bytes()),
                    ProximityRecord("b".bytes(), listOf(0.1f, 0f), "beta".bytes()),
                ),
            ).use { proximity ->
                val request = ProximitySearchRequestRecord(
                    query = listOf(0f, 0f),
                    k = 3uL,
                    policy = SearchPolicyKind.FIXED_BUDGET,
                    adaptiveQuality = null,
                    budget = SearchBudgetRecord(1_000uL, 1_000_000uL, 1_000uL, 1_000uL),
                    filter = ProximityFilterRecord(
                        ProximityFilterKind.PREFIX, null, null, "a".bytes(), emptyList()
                    ),
                    kernel = QueryKernelRecord.SCALAR_DETERMINISTIC,
                    backend = SearchBackendRecord.AUTO,
                    hnswEfSearch = null,
                    pqRerankMultiplier = null,
                )

                val result = proximity.search(request)
                assertEquals(listOf("a", "ab"), result.neighbors.map { String(it.key) })
                assertEquals(true, result.stats.distanceEvaluations > 0uL)
                assertEquals(true, result.planFormatVersion > 0u)
                proximity.read().use { session ->
                    assertEquals(
                        listOf("a", "ab"),
                        session.search(request).neighbors.map { String(it.key) },
                    )
                }
                proximity.proveSearch(request).use { proof ->
                    assertEquals(
                        listOf("a", "ab"),
                        proof.verify(proximity.descriptor).result.neighbors.map { String(it.key) },
                    )
                }
            }
        }
    }

    @Test
    fun versionedBulkPublicationUsesNativePerformancePaths() {
        ProllyNative.useLocalDebugLibrary()
        Engine.memory().use { engine ->
            engine.versionedMap("bulk-publication".bytes()).use { versioned ->
                val initialized = versioned.initializeSorted(listOf(
                    EntryRecord("a".bytes(), "one".bytes()),
                    EntryRecord("b".bytes(), "two".bytes()),
                ))
                assertEquals(MapUpdateKind.APPLIED, initialized.kind)
                versioned.append(listOf(MutationRecord(MutationKind.UPSERT, "c".bytes(), "three".bytes())))
                val parallel = versioned.parallelApply(
                    listOf(
                        MutationRecord(MutationKind.UPSERT, "b".bytes(), "updated".bytes()),
                        MutationRecord(MutationKind.UPSERT, "d".bytes(), "four".bytes()),
                    ),
                    ParallelConfigRecord(1uL, 1uL),
                )
                assertEquals(2uL, parallel.stats.inputMutations)
                val rebuilt = versioned.rebuildSortedIf(parallel.version.id, listOf(
                    EntryRecord("x".bytes(), "nine".bytes()),
                    EntryRecord("y".bytes(), "ten".bytes()),
                ))
                assertEquals(MapUpdateKind.APPLIED, rebuilt.kind)
                val iterRebuilt = versioned.rebuildFromEntriesIf(rebuilt.current!!.id, listOf(
                    EntryRecord("q".bytes(), "queue".bytes()),
                    EntryRecord("p".bytes(), "priority".bytes()),
                ))
                assertEquals(MapUpdateKind.APPLIED, iterRebuilt.kind)
                assertArrayEquals("priority".bytes(), versioned.get("p".bytes()))
            }
        }
    }

    @Test
    fun versionedSnapshotsExposeOrderedNavigationAndBoundedPages() {
        ProllyNative.useLocalDebugLibrary()
        Engine.memory().use { engine ->
            engine.versionedMap("versioned-ordered".bytes()).use { versioned ->
                versioned.initialize()
                versioned.apply(listOf(
                    MutationRecord(MutationKind.UPSERT, "a".bytes(), "one".bytes()),
                    MutationRecord(MutationKind.UPSERT, "ab".bytes(), "two".bytes()),
                    MutationRecord(MutationKind.UPSERT, "b".bytes(), "three".bytes()),
                    MutationRecord(MutationKind.UPSERT, "c".bytes(), "four".bytes()),
                ))
                versioned.snapshot()!!.use { snapshot ->
                    assertEquals(true, snapshot.containsKey("ab".bytes()))
                    assertArrayEquals("one".bytes(), snapshot.getMany(listOf("a".bytes(), "missing".bytes()))[0])
                    assertArrayEquals("a".bytes(), snapshot.firstEntry()!!.key)
                    assertArrayEquals("c".bytes(), snapshot.lastEntry()!!.key)
                    assertArrayEquals("ab".bytes(), snapshot.lowerBound("aa".bytes())!!.key)
                    assertArrayEquals("b".bytes(), snapshot.upperBound("ab".bytes())!!.key)
                    assertEquals(listOf("a", "ab"), snapshot.prefix("a".bytes()).map { String(it.key) })
                    assertEquals(listOf("ab", "b"), snapshot.range("ab".bytes(), "c".bytes()).map { String(it.key) })
                    val prefixPage = snapshot.prefixPage("a".bytes(), limit = 1uL)
                    assertEquals(listOf("a"), prefixPage.entries.map { String(it.key) })
                    assertEquals(true, prefixPage.nextCursor != null)
                    val first = snapshot.rangePage(end = "c".bytes(), limit = 2uL)
                    assertEquals(listOf("a", "ab"), first.entries.map { String(it.key) })
                    val second = snapshot.rangePage(first.nextCursor, "c".bytes(), 2uL)
                    assertEquals(listOf("b"), second.entries.map { String(it.key) })
                    assertEquals(listOf("c", "b"), snapshot.reversePage(start = "a".bytes(), limit = 2uL).entries.map { String(it.key) })
                    assertEquals(listOf("ab", "a"), snapshot.prefixReversePage("a".bytes(), limit = 2uL).entries.map { String(it.key) })
                }
            }
        }
    }

    @Test
    fun versionedIndexedAndProximityMapsUseThePortableApi() {
        ProllyNative.useLocalDebugLibrary()

        Engine.memory().use { engine ->
            engine.versionedMap("users".bytes()).use { versioned ->
                versioned.initialize()
                versioned.put("u1".bytes(), "Ada".bytes())
                assertArrayEquals("Ada".bytes(), versioned.get("u1".bytes()))
            }

            engine.indexRegistry().use { registry ->
                registry.register(
                    "by_team".bytes(),
                    1uL,
                    "team-v1",
                    IndexProjectionRecord.ALL,
                    object : SecondaryIndexExtractorCallback {
                        override fun extract(primaryKey: ByteArray, sourceValue: ByteArray) =
                            listOf(IndexEntryRecord(sourceValue.copyOf(), null))
                    },
                )
                engine.indexedMap("members".bytes(), registry).use { indexed ->
                    indexed.put("u1".bytes(), "red".bytes())
                    indexed.ensureIndex("by_team".bytes())
                    indexed.snapshot().use { snapshot ->
                        snapshot.index("by_team".bytes()).use { index ->
                            val records = index.records("red".bytes())
                            assertEquals(1, records.size)
                            assertArrayEquals("u1".bytes(), records.single().primaryKey)
                            var escaped: build.crab.prolly.api.ScopedBytes? = null
                            val key = index.withExactPage("red".bytes()) { rows ->
                                escaped = rows.single().primaryKey
                                rows.single().primaryKey.bytes()
                            }
                            assertArrayEquals("u1".bytes(), key)
                            assertThrows<IllegalStateException> { escaped!!.bytes() }
                            assertArrayEquals("by_team".bytes(), index.name)
                            assertEquals(1, index.prefixReversePage("r".bytes()).matches.size)
                        }
                    }
                    assertArrayEquals("members".bytes(), indexed.id)
                    val applied = indexed.apply(
                        listOf(MutationRecord(MutationKind.UPSERT, "u2".bytes(), "red".bytes())),
                    )
                    val conditional = indexed.applyIf(
                        applied.sourceVersion,
                        listOf(MutationRecord(MutationKind.UPSERT, "u3".bytes(), "blue".bytes())),
                    )
                    assertEquals(true, conditional.current != null)
                    indexed.snapshotAt(applied.sourceVersion).use { historical ->
                        assertEquals(2, historical.index("by_team".bytes()).use {
                            it.exactPage("red".bytes()).matches.size
                        })
                    }
                    indexed.snapshot().use { current ->
                        indexed.snapshotById(current.id).use { reopened ->
                            assertArrayEquals(current.id.sourceVersion, reopened.id.sourceVersion)
                            assertArrayEquals(current.id.catalogVersion, reopened.id.catalogVersion)
                        }
                    }
                }
            }

            engine.buildProximity(
                2u,
                listOf(ProximityRecord("a".bytes(), listOf(0.0f, 0.0f), "alpha".bytes())),
            ).use { proximity ->
                assertArrayEquals("a".bytes(), proximity.searchExact(listOf(0.1f, 0.1f), 1uL).neighbors.single().key)
                val key = proximity.withSearchView(listOf(0.1f, 0.1f), 1u) { rows ->
                    rows.single().key.bytes()
                }
                assertArrayEquals("a".bytes(), key)
            }
        }
    }

    @Test
    fun suspendWritesCopyMutableInputsBeforeDispatch() = runBlocking {
        ProllyNative.useLocalDebugLibrary()
        Engine.memory().use { engine ->
            engine.versionedMap("async".bytes()).use { versioned ->
                versioned.initialize()
                val key = "k".bytes()
                versioned.putAsync(key, "v".bytes())
                key[0] = 'x'.code.toByte()
                assertArrayEquals("v".bytes(), versioned.get("k".bytes()))
            }
        }
    }

    @Test
    fun proofsSessionsAndMaintenanceAreApplicationFacing() {
        ProllyNative.useLocalDebugLibrary()
        Engine.memory().use { engine ->
            engine.versionedMap("proofs".bytes()).use { versioned ->
                versioned.initialize()
                versioned.put("k".bytes(), "v".bytes())
                versioned.put("ka".bytes(), "v2".bytes())
                versioned.snapshot()!!.use { snapshot ->
                    val verified = verifyKeyProof(snapshot.proveKey("k".bytes()))
                    assertEquals(true, verified.valid)
                    assertArrayEquals("v".bytes(), verified.value)
                    val multi = verifyMultiKeyProof(snapshot.proveKeys(listOf("k".bytes(), "missing".bytes())))
                    assertEquals(listOf(true, false), multi.results.map { it.exists })
                    assertEquals(listOf("k", "ka"), verifyRangeProof(snapshot.proveRange("k".bytes(), "l".bytes())).entries.map { String(it.key) })
                    assertEquals(listOf("k", "ka"), verifyRangeProof(snapshot.provePrefix("k".bytes())).entries.map { String(it.key) })
                    val provedPage = snapshot.proveRangePage(end = "l".bytes(), limit = 1uL)
                    assertEquals(true, verifyRangePageProof(provedPage.proof).valid)
                    assertEquals(listOf("k"), provedPage.page.entries.map { String(it.key) })
                    assertEquals(2uL, snapshot.stats().totalKeyValuePairs)
                    assertEquals(true, snapshot.export().nodes.isNotEmpty())
                    snapshot.read().use { session ->
                        assertArrayEquals("v".bytes(), session.get("k".bytes()))
                        var escaped: build.crab.prolly.api.ScopedBytes? = null
                        val seen = mutableListOf<String>()
                        val outcome = session.scanRangeView("k".bytes(), "l".bytes()) { entry ->
                            if (escaped == null) escaped = entry.key
                            seen += "${String(entry.key.bytes())}=${String(entry.value.bytes())}"
                            true
                        }
                        assertEquals(2L, outcome.visited)
                        assertEquals(false, outcome.stopped)
                        assertEquals(listOf("k=v", "ka=v2"), seen)
                        assertThrows<IllegalStateException> { escaped!!.bytes() }
                        assertEquals(
                            build.crab.prolly.api.ReadScanOutcome(1L, true),
                            session.scanRangeView("k".bytes(), "l".bytes()) { false },
                        )
                    }
                }
                assertEquals(true, versioned.verifyCatalog().versionCount >= 2uL)
                assertEquals(true, versioned.backup().isNotEmpty())
                assertEquals(true, versioned.planGc().reachability.liveCids.isNotEmpty())
            }

            engine.indexRegistry().use { registry ->
                registry.register(
                    "by_value".bytes(), 1uL, "value-v1", IndexProjectionRecord.ALL,
                    object : SecondaryIndexExtractorCallback {
                        override fun extract(primaryKey: ByteArray, sourceValue: ByteArray) =
                            listOf(IndexEntryRecord(sourceValue.copyOf(), null))
                    },
                )
                engine.indexedMap("indexed-maintenance".bytes(), registry).use { indexed ->
                    val version = indexed.put("k".bytes(), "term".bytes())
                    indexed.ensureIndex("by_value".bytes())
                    assertEquals(true, indexed.verifyIndex("by_value".bytes(), version.sourceVersion).valid)
                    assertEquals(true, indexed.metrics().buildAttempts >= 1uL)
                    assertEquals(true, indexed.exportCurrent().isNotEmpty())
                    assertEquals(true, indexed.keepLast(1uL).retainedSourceVersions.isNotEmpty())
                }
            }

            engine.buildProximity(
                2u,
                listOf(ProximityRecord("p".bytes(), listOf(0.0f, 0.0f), "payload".bytes())),
            ).use { proximity ->
                val verified = verifyProximityMembershipProof(
                    proximity.proveMembership("p".bytes()),
                    proximity.descriptor,
                )
                assertArrayEquals("payload".bytes(), verified.record!!.value)
                assertEquals(1uL, proximity.verify().recordCount)
                assertEquals(1uL, proximity.count)
                assertEquals(true, proximity.containsKey("p".bytes()))
                assertEquals(2u, proximity.config.dimensions)
                assertEquals(
                    1uL,
                    verifyProximityStructureProof(
                        proximity.proveStructure(), proximity.descriptor,
                    ).summary.recordCount,
                )
                val (mutated, stats) = proximity.mutate(
                    listOf(ProximityMutationRecord("q".bytes(), listOf(1.0f, 1.0f), "second".bytes())),
                )
                mutated.use { assertEquals(2uL, it.count) }
                assertEquals(true, stats.recordsRebuilt >= 1uL)
                proximity.read().use { retained ->
                    assertArrayEquals(
                        "p".bytes(),
                        retained.searchExact(listOf(0.0f, 0.0f), 1uL).neighbors.single().key,
                    )
                    assertArrayEquals(
                        "p".bytes(),
                        retained.withSearchView(listOf(0.0f, 0.0f), 1u) { rows ->
                            rows.single().key.bytes()
                        },
                    )
                }
                proximity.proveSearchExact(listOf(0.0f, 0.0f), 1uL).use { proof ->
                    val verifiedSearch = proof.verify(proximity.descriptor)
                    assertArrayEquals("p".bytes(), verifiedSearch.result.neighbors.single().key)
                    assertEquals(true, verifiedSearch.replayedEvents > 0uL)
                }
            }
        }
    }

    @Test
    fun versionedComparisonsPinVersionsAndPageDiffs() {
        ProllyNative.useLocalDebugLibrary()
        Engine.memory().use { engine ->
            engine.versionedMap("comparison".bytes()).use { map ->
                val base = map.initialize()
                val target = map.put("k".bytes(), "v".bytes())
                map.compare(base.id, target.id).use { comparison ->
                    assertArrayEquals(base.id, comparison.base().id)
                    assertArrayEquals(target.id, comparison.target().id)
                    assertEquals(listOf("k"), comparison.diff().map { String(it.key) })
                    assertEquals(listOf("k"), comparison.diffPage(limit = 1uL).diffs.map { String(it.key) })
                }
            }
        }
    }

    @Test
    fun versionedHistoryNavigationDiffAndRollbackStayNative() {
        ProllyNative.useLocalDebugLibrary()
        Engine.memory().use { engine ->
            engine.versionedMap("history-navigation".bytes()).use { map ->
                map.initialize()
                map.put("a".bytes(), "one".bytes())
                map.put("ab".bytes(), "two".bytes())
                val base = map.put("b".bytes(), "three".bytes())
                val target = map.put("a".bytes(), "updated".bytes())

                assertEquals(listOf("a", "ab", "b"), map.range("a".bytes(), "c".bytes()).map { String(it.key) })
                assertEquals(listOf("a", "ab"), map.prefix("a".bytes()).map { String(it.key) })
                assertArrayEquals("one".bytes(), map.rangeAt(base.id, "a".bytes(), "b".bytes()).first().value)
                assertEquals(listOf("a", "ab"), map.prefixAt(base.id, "a".bytes()).map { String(it.key) })
                assertEquals(listOf("a", "ab"), map.rangePage(limit = 2uL).entries.map { String(it.key) })
                assertEquals(listOf("a"), map.prefixPage("a".bytes(), limit = 1uL).entries.map { String(it.key) })
                val historicalPage = map.prefixPageAt(base.id, "a".bytes(), limit = 1uL)
                assertEquals(listOf("a"), historicalPage.entries.map { String(it.key) })
                assertEquals(true, historicalPage.nextCursor != null)
                assertEquals(listOf("a"), map.diff(base.id, target.id).map { String(it.key) })
                assertEquals(listOf("a"), map.changesSince(base.id).map { String(it.key) })

                val rolledBack = map.rollbackTo(base.id)
                assertArrayEquals(rolledBack.id, map.headId())
                assertArrayEquals("one".bytes(), map.get("a".bytes()))
                assertEquals(emptyList<String>(), map.changesSince(base.id).map { String(it.key) })
            }
        }
    }

    @Test
    fun versionedTimestampedWritesExposeCompleteMaintenanceAndRetentionRecords() {
        ProllyNative.useLocalDebugLibrary()
        Engine.memory().use { engine ->
            engine.versionedMap("maintenance-complete".bytes()).use { map ->
                val first = map.applyAtMillis(listOf(
                    MutationRecord(MutationKind.UPSERT, "k".bytes(), "one".bytes())
                ), 1_000uL)
                val second = map.applyIfAtMillis(first.id, listOf(
                    MutationRecord(MutationKind.UPSERT, "k".bytes(), "two".bytes())
                ), 2_000uL).current!!
                val third = map.applyAtMillis(listOf(
                    MutationRecord(MutationKind.UPSERT, "k".bytes(), "three".bytes())
                ), 3_000uL)

                assertEquals(1_000uL, first.createdAtMillis)
                assertEquals(2_000uL, second.createdAtMillis)
                assertEquals(NamedRootRetentionKind.PREFIX, map.retentionPolicy().kind)
                val verification = map.verifyCatalog()
                assertArrayEquals(third.id, verification.head)
                assertEquals(3uL, verification.versionCount)
                val plan = map.planGc()
                assertEquals(true, plan.reachability.liveNodes > 0uL)
                assertEquals(true, plan.candidateNodes >= plan.reclaimableNodes)

                val aged = map.keepForAt(3_000uL, 1_500uL)
                assertEquals(true, aged.retained.any { it.contentEquals(second.id) })
                assertEquals(true, aged.removed.any { it.contentEquals(first.id) })
                val explicit = map.keepVersions(listOf(second.id))
                assertEquals(true, explicit.retained.any { it.contentEquals(third.id) })
                val pruned = map.pruneVersions(0uL)
                assertEquals(1, pruned.retained.size)
                assertArrayEquals(third.id, pruned.retained.single())
                assertEquals(true, pruned.removed.any { it.contentEquals(second.id) })
                assertEquals(true, map.keepFor(10_000uL).retained.isNotEmpty())
                assertEquals(true, map.sweepGc().deletedNodes >= 0uL)
            }
        }
    }

    @Test
    fun versionedSubscriptionsResumeAndPollOwnedDiffs() {
        ProllyNative.useLocalDebugLibrary()
        Engine.memory().use { engine ->
            engine.versionedMap("subscription".bytes()).use { map ->
                val initial = map.initialize()
                map.subscribe().use { subscription ->
                    assertArrayEquals(initial.id, subscription.lastSeen())
                    assertEquals(null, subscription.poll())
                    val current = map.put("k".bytes(), "v".bytes())
                    val event = subscription.poll()!!
                    assertArrayEquals(initial.id, event.previous)
                    assertArrayEquals(current.id, event.current.id)
                    assertEquals(listOf("k"), event.diffs.map { String(it.key) })
                    assertArrayEquals(current.id, subscription.lastSeen())
                }
            }
        }
    }

    @Test
    fun multiMapTransactionsAreAtomicAndReadStagedValues() {
        ProllyNative.useLocalDebugLibrary()
        Engine.memory().use { engine ->
            engine.beginVersionedTransaction().use { tx ->
                tx.put("a".bytes(), "k".bytes(), "one".bytes())
                tx.put("b".bytes(), "k".bytes(), "two".bytes())
                assertArrayEquals("one".bytes(), tx.get("a".bytes(), "k".bytes()))
                val committed = tx.commit()
                assertEquals(true, committed.applied)
                assertEquals(2, committed.versions.size)
            }
            engine.versionedMap("a".bytes()).use { assertArrayEquals("one".bytes(), it.get("k".bytes())) }
            engine.versionedMap("b".bytes()).use { assertArrayEquals("two".bytes(), it.get("k".bytes())) }
            engine.beginVersionedTransaction().use { tx ->
                tx.put("a".bytes(), "discard".bytes(), "x".bytes())
                tx.rollback()
            }
            engine.versionedMap("a".bytes()).use { assertEquals(null, it.get("discard".bytes())) }
        }
    }

    @Test
    fun pinnedMergesPageConflictsAndCasPublish() {
        ProllyNative.useLocalDebugLibrary()
        Engine.memory().use { engine ->
            engine.versionedMap("merge".bytes()).use { map ->
                val base = map.initialize()
                val candidate = map.put("k".bytes(), "candidate".bytes())
                map.put("k".bytes(), "head".bytes())
                map.prepareMerge(base.id, candidate.id).use { merge ->
                    assertArrayEquals(base.id, merge.base().id)
                    assertArrayEquals(candidate.id, merge.candidate().id)
                    assertEquals(listOf("k"), merge.conflictPage(limit = 1uL).conflicts.map { String(it.key) })
                    assertArrayEquals(candidate.id, merge.publish("prefer_right").current!!.id)
                }
                assertArrayEquals("candidate".bytes(), map.get("k".bytes()))
            }
        }
    }

    private fun String.bytes() = encodeToByteArray()
}
