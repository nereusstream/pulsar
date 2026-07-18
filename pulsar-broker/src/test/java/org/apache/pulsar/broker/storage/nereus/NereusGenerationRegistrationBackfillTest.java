/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.storage.nereus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.nereusstream.core.capability.GenerationRegistrationBackfillCompletion;
import com.nereusstream.managedledger.generation.ManagedLedgerMaterializationRegistrationCandidate;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

public class NereusGenerationRegistrationBackfillTest {
    private static final String RUN_ID = "abcdefghijklmnopqrstuvwxyz";
    private static final long READINESS_EPOCH = 9;

    @Test
    public void canonicalTraversalIsOrderIndependentAndRegistersOnlyLiveNereusTopics() {
        TenantResources tenants = mock(TenantResources.class);
        NamespaceResources namespaces = mock(NamespaceResources.class);
        TopicResources topics = mock(TopicResources.class);
        NereusStorageClassBindingStore bindings = mock(NereusStorageClassBindingStore.class);
        NereusBrokerCapabilityCoordinator capabilities = capabilities(readiness("11"));
        String topicA = "persistent://tenant-a/ns-a/topic-z";
        String topicB = "persistent://tenant-b/ns-b/topic-a";
        String bookkeeper = "persistent://tenant-a/ns-a/topic-bk";
        when(tenants.listTenantsAsync()).thenReturn(
                completed(List.of("tenant-b", "tenant-a")),
                completed(List.of("tenant-a", "tenant-b")));
        when(namespaces.listNamespacesAsync("tenant-a")).thenReturn(
                completed(List.of("tenant-a/ns-a")));
        when(namespaces.listNamespacesAsync("tenant-b")).thenReturn(
                completed(List.of("tenant-b/ns-b")));
        when(topics.listPersistentTopicsAsync(NamespaceName.get("tenant-a/ns-a"))).thenReturn(
                completed(List.of(topicA, bookkeeper)),
                completed(List.of(bookkeeper, topicA)));
        when(topics.listPersistentTopicsAsync(NamespaceName.get("tenant-b/ns-b"))).thenReturn(
                completed(List.of(topicB)));
        Map<String, StorageClassBindingRecord> records = Map.of(
                persistence(topicA), activeBinding(topicA, StorageClassBindingRecord.NEREUS, 3),
                persistence(topicB), activeBinding(topicB, StorageClassBindingRecord.NEREUS, 5),
                persistence(bookkeeper), activeBinding(bookkeeper, StorageClassBindingRecord.BOOKKEEPER, 2));
        when(bindings.getBinding(any())).thenAnswer(invocation ->
                completed(Optional.ofNullable(records.get(invocation.getArgument(0)))));
        AtomicInteger registrations = new AtomicInteger();
        List<GenerationRegistrationBackfillCompletion> proofs =
                new ArrayList<>();
        AtomicInteger proofConcurrency = new AtomicInteger();
        AtomicReference<Duration> proofTimeout = new AtomicReference<>();
        DefaultNereusGenerationRegistrationBackfill.RegistrationAccess access =
                registrationAccess(registrations);
        var backfill = new DefaultNereusGenerationRegistrationBackfill(
                tenants,
                namespaces,
                topics,
                bindings,
                access,
                (completion, maxConcurrentStreams, timeout) -> {
                    proofs.add(completion);
                    proofConcurrency.set(maxConcurrentStreams);
                    proofTimeout.set(timeout);
                    return completed(null);
                },
                capabilities,
                100);
        var request = request(4);

        GenerationRegistrationBackfillReport first = backfill.run(request).join();
        GenerationRegistrationBackfillReport second = backfill.run(request).join();

        assertThat(first.tenantsScanned()).isEqualTo(2);
        assertThat(first.namespacesScanned()).isEqualTo(2);
        assertThat(first.persistentTopicsScanned()).isEqualTo(3);
        assertThat(first.nereusProjectionsRegistered()).isEqualTo(2);
        assertThat(first.deletedOrNonNereusSkipped()).isEqualTo(1);
        assertThat(first.failureCount()).isZero();
        assertThat(first.boundedFailures()).isEmpty();
        assertThat(first.coverageSha256()).isEqualTo(second.coverageSha256());
        assertThat(first.coverageSha256().value())
                .isEqualTo("2f234d6b9baa3a760460090850d22734f94cd72d51fd0f27706fda272fc01d7c");
        assertThat(registrations).hasValue(4);
        assertThat(proofConcurrency).hasValue(request.maxConcurrency());
        assertThat(proofTimeout.get())
                .isPositive()
                .isLessThan(request.timeout());
        assertThat(proofs).hasSize(2).allSatisfy(proof -> {
            assertThat(proof.runId()).isEqualTo(RUN_ID);
            assertThat(proof.readiness())
                    .isEqualTo(readiness("11").toCore());
            assertThat(proof.coverageSha256())
                    .isEqualTo(first.coverageSha256());
            assertThat(proof.failureCount()).isZero();
        });
    }

    @Test
    public void topicRegistrationConcurrencyNeverExceedsTheRequestBound() {
        TenantResources tenants = mock(TenantResources.class);
        NamespaceResources namespaces = mock(NamespaceResources.class);
        TopicResources topics = mock(TopicResources.class);
        NereusStorageClassBindingStore bindings = mock(NereusStorageClassBindingStore.class);
        NereusBrokerCapabilityCoordinator capabilities = capabilities(readiness("66"));
        NamespaceName namespace = NamespaceName.get("tenant/ns");
        List<String> topicNames = new ArrayList<>();
        for (int index = 0; index < 8; index++) {
            topicNames.add("persistent://tenant/ns/topic-" + index);
        }
        when(tenants.listTenantsAsync()).thenReturn(completed(List.of("tenant")));
        when(namespaces.listNamespacesAsync("tenant")).thenReturn(completed(List.of(namespace.toString())));
        when(topics.listPersistentTopicsAsync(namespace)).thenReturn(completed(topicNames));
        when(bindings.getBinding(any())).thenAnswer(invocation -> {
            String persistenceName = invocation.getArgument(0);
            return completed(Optional.of(StorageClassBindingRecord.claimed(
                            persistenceName,
                            StorageClassBindingRecord.NEREUS,
                            3,
                            1)
                    .transitionTo(StorageClassBindingState.ACTIVE)
                    .withMetadataVersion(1)));
        });
        CompletableFuture<Void> releaseFirstBatch = new CompletableFuture<>();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        AtomicInteger registrations = new AtomicInteger();
        AtomicInteger proofs = new AtomicInteger();
        DefaultNereusGenerationRegistrationBackfill.RegistrationAccess access =
                new DefaultNereusGenerationRegistrationBackfill.RegistrationAccess() {
                    @Override
                    public CompletableFuture<ManagedLedgerMaterializationRegistrationCandidate> inspect(
                            String persistenceName, long expectedBindingGeneration) {
                        int current = inFlight.incrementAndGet();
                        maxObserved.accumulateAndGet(current, Math::max);
                        return releaseFirstBatch
                                .thenApply(ignored -> candidate(
                                        persistenceName, expectedBindingGeneration))
                                .whenComplete((ignored, error) -> inFlight.decrementAndGet());
                    }

                    @Override
                    public CompletableFuture<Void> ensureRegistered(
                            ManagedLedgerMaterializationRegistrationCandidate candidate) {
                        registrations.incrementAndGet();
                        return CompletableFuture.completedFuture(null);
                    }
                };
        var backfill = new DefaultNereusGenerationRegistrationBackfill(
                tenants,
                namespaces,
                topics,
                bindings,
                access,
                proofAccess(proofs),
                capabilities,
                10);

        CompletableFuture<GenerationRegistrationBackfillReport> result =
                backfill.run(request(3));

        assertThat(inFlight).hasValue(3);
        assertThat(maxObserved).hasValue(3);
        releaseFirstBatch.complete(null);
        assertThat(result.join().nereusProjectionsRegistered()).isEqualTo(8);
        assertThat(registrations).hasValue(8);
        assertThat(proofs).hasValue(1);
        assertThat(maxObserved.get()).isLessThanOrEqualTo(3);
    }

    @Test
    public void retainsOnlyFirstHundredCanonicalFailuresWhileHashingAllTopics() {
        TenantResources tenants = mock(TenantResources.class);
        NamespaceResources namespaces = mock(NamespaceResources.class);
        TopicResources topics = mock(TopicResources.class);
        NereusStorageClassBindingStore bindings = mock(NereusStorageClassBindingStore.class);
        NereusBrokerCapabilityCoordinator capabilities = capabilities(readiness("22"));
        NamespaceName namespace = NamespaceName.get("tenant/ns");
        List<String> topicNames = new ArrayList<>();
        for (int index = 100; index >= 0; index--) {
            topicNames.add("persistent://tenant/ns/topic-" + String.format("%03d", index));
        }
        when(tenants.listTenantsAsync()).thenReturn(completed(List.of("tenant")));
        when(namespaces.listNamespacesAsync("tenant")).thenReturn(completed(List.of(namespace.toString())));
        when(topics.listPersistentTopicsAsync(namespace)).thenReturn(completed(topicNames));
        when(bindings.getBinding(any())).thenReturn(
                CompletableFuture.failedFuture(new IllegalStateException("read failed")));
        AtomicInteger proofs = new AtomicInteger();
        var backfill = new DefaultNereusGenerationRegistrationBackfill(
                tenants,
                namespaces,
                topics,
                bindings,
                registrationAccess(new AtomicInteger()),
                proofAccess(proofs),
                capabilities,
                200);

        GenerationRegistrationBackfillReport report = backfill.run(request(7)).join();

        assertThat(report.persistentTopicsScanned()).isEqualTo(101);
        assertThat(report.failureCount()).isEqualTo(101);
        assertThat(report.boundedFailures()).hasSize(100);
        assertThat(report.boundedFailures())
                .allSatisfy(failure -> {
                    assertThat(failure.stage())
                            .isEqualTo(GenerationRegistrationBackfillStage.BINDING_READ);
                    assertThat(failure.errorCode()).isEqualTo("OPERATION_FAILED");
                });
        assertThat(proofs).hasValue(0);
    }

    @Test
    public void finalBindingDriftIsAReportedFailureRatherThanFalseCoverage() {
        TenantResources tenants = mock(TenantResources.class);
        NamespaceResources namespaces = mock(NamespaceResources.class);
        TopicResources topics = mock(TopicResources.class);
        NereusStorageClassBindingStore bindings = mock(NereusStorageClassBindingStore.class);
        NereusBrokerCapabilityCoordinator capabilities = capabilities(readiness("33"));
        String topic = "persistent://tenant/ns/topic";
        NamespaceName namespace = TopicName.get(topic).getNamespaceObject();
        when(tenants.listTenantsAsync()).thenReturn(completed(List.of("tenant")));
        when(namespaces.listNamespacesAsync("tenant")).thenReturn(completed(List.of(namespace.toString())));
        when(topics.listPersistentTopicsAsync(namespace)).thenReturn(completed(List.of(topic)));
        when(bindings.getBinding(persistence(topic))).thenReturn(
                completed(Optional.of(activeBinding(
                        topic, StorageClassBindingRecord.NEREUS, 3))),
                completed(Optional.of(activeBinding(
                        topic, StorageClassBindingRecord.BOOKKEEPER, 4))));
        AtomicInteger registrations = new AtomicInteger();
        AtomicInteger proofs = new AtomicInteger();
        var backfill = new DefaultNereusGenerationRegistrationBackfill(
                tenants,
                namespaces,
                topics,
                bindings,
                registrationAccess(registrations),
                proofAccess(proofs),
                capabilities,
                10);

        GenerationRegistrationBackfillReport report = backfill.run(request(1)).join();

        assertThat(registrations).hasValue(1);
        assertThat(report.nereusProjectionsRegistered()).isZero();
        assertThat(report.failureCount()).isEqualTo(1);
        assertThat(report.boundedFailures().get(0).stage())
                .isEqualTo(GenerationRegistrationBackfillStage.BINDING_READ);
        assertThat(report.boundedFailures().get(0).errorCode())
                .isEqualTo("BINDING_CHANGED");
        assertThat(proofs).hasValue(0);
    }

    @Test
    public void readinessMustRemainExactlyStableAcrossTheWholeTraversal() {
        TenantResources tenants = mock(TenantResources.class);
        NamespaceResources namespaces = mock(NamespaceResources.class);
        TopicResources topics = mock(TopicResources.class);
        NereusStorageClassBindingStore bindings = mock(NereusStorageClassBindingStore.class);
        NereusBrokerCapabilityCoordinator capabilities = mock(NereusBrokerCapabilityCoordinator.class);
        when(capabilities.requireGenerationReadiness()).thenReturn(
                completed(readiness("44")),
                completed(new NereusGenerationCapabilityReadiness(
                        READINESS_EPOCH + 1, "55".repeat(32), 2)));
        when(tenants.listTenantsAsync()).thenReturn(completed(List.of()));
        AtomicInteger proofs = new AtomicInteger();
        var backfill = new DefaultNereusGenerationRegistrationBackfill(
                tenants,
                namespaces,
                topics,
                bindings,
                registrationAccess(new AtomicInteger()),
                proofAccess(proofs),
                capabilities,
                10);

        assertThatThrownBy(() -> backfill.run(request(1)).join())
                .hasRootCauseMessage("NEREUS_GENERATION_BACKFILL_READINESS_CHANGED");
        assertThat(proofs).hasValue(0);
    }

    @Test
    public void requestAndReportRejectNonCanonicalBounds() {
        assertThatThrownBy(() -> new GenerationRegistrationBackfillRequest(
                        "not-base32!", 1, 1, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GenerationRegistrationBackfillRequest(
                        RUN_ID, 1, 0, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackfillFailure(
                        "AA".repeat(32),
                        GenerationRegistrationBackfillStage.TOPIC_LIST,
                        "FAILED"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GenerationRegistrationBackfillReport(
                        RUN_ID,
                        READINESS_EPOCH,
                        1,
                        1,
                        2,
                        1,
                        0,
                        0,
                        new com.nereusstream.api.Checksum(
                                com.nereusstream.api.ChecksumType.SHA256,
                                "00".repeat(32)),
                        List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("registered + skipped + failed");
    }

    private static DefaultNereusGenerationRegistrationBackfill.RegistrationAccess registrationAccess(
            AtomicInteger registrations) {
        return new DefaultNereusGenerationRegistrationBackfill.RegistrationAccess() {
            @Override
            public CompletableFuture<ManagedLedgerMaterializationRegistrationCandidate> inspect(
                    String persistenceName, long expectedBindingGeneration) {
                return completed(candidate(persistenceName, expectedBindingGeneration));
            }

            @Override
            public CompletableFuture<Void> ensureRegistered(
                    ManagedLedgerMaterializationRegistrationCandidate candidate) {
                registrations.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    private static DefaultNereusGenerationRegistrationBackfill.ProofAccess
            proofAccess(AtomicInteger completions) {
        return (completion, maxConcurrentStreams, timeout) -> {
            completions.incrementAndGet();
            return completed(null);
        };
    }

    private static ManagedLedgerMaterializationRegistrationCandidate candidate(
            String persistenceName, long bindingGeneration) {
        ManagedLedgerProjectionIdentity identity = new ManagedLedgerProjectionIdentity(
                bindingGeneration,
                1,
                ManagedLedgerProjectionNames.streamId(persistenceName, 1).value(),
                ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID + 1);
        return new ManagedLedgerMaterializationRegistrationCandidate(
                persistenceName,
                bindingGeneration,
                identity,
                new com.nereusstream.managedledger.generation.ManagedLedgerGenerationProjectionRefV1(
                                persistenceName, identity)
                        .projectionIdentitySha256());
    }

    private static NereusBrokerCapabilityCoordinator capabilities(
            NereusGenerationCapabilityReadiness readiness) {
        NereusBrokerCapabilityCoordinator coordinator = mock(NereusBrokerCapabilityCoordinator.class);
        when(coordinator.requireGenerationReadiness()).thenReturn(completed(readiness));
        return coordinator;
    }

    private static NereusGenerationCapabilityReadiness readiness(String pair) {
        return new NereusGenerationCapabilityReadiness(
                READINESS_EPOCH, pair.repeat(32), 2);
    }

    private static GenerationRegistrationBackfillRequest request(int concurrency) {
        return new GenerationRegistrationBackfillRequest(
                RUN_ID, READINESS_EPOCH, concurrency, Duration.ofSeconds(5));
    }

    private static StorageClassBindingRecord activeBinding(
            String topic, String storageClass, long generation) {
        return StorageClassBindingRecord.claimed(
                        persistence(topic), storageClass, generation, 1)
                .transitionTo(StorageClassBindingState.ACTIVE)
                .withMetadataVersion(1);
    }

    private static String persistence(String topic) {
        return TopicName.get(topic).getPersistenceNamingEncoding();
    }

    private static <T> CompletableFuture<T> completed(T value) {
        return CompletableFuture.completedFuture(value);
    }

}
