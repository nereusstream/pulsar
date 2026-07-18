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

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.NereusException;
import com.nereusstream.core.capability.GenerationRegistrationBackfillCompletion;
import com.nereusstream.managedledger.generation.ManagedLedgerMaterializationRegistrationCandidate;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Canonical one-namespace-at-a-time cold-topic traversal. Topic work is
 * executed in bounded batches and folded in canonical order.
 */
public final class DefaultNereusGenerationRegistrationBackfill
        implements NereusGenerationRegistrationBackfill {
    private static final String DIGEST_DOMAIN =
            "nereus-generation-registration-backfill-v1";

    private final TenantResources tenantResources;
    private final NamespaceResources namespaceResources;
    private final TopicResources topicResources;
    private final NereusStorageClassBindingStore bindingStore;
    private final RegistrationAccess registrations;
    private final ProofAccess proofs;
    private final NereusBrokerCapabilityCoordinator capabilityCoordinator;
    private final int maxTopicsPerNamespace;

    public DefaultNereusGenerationRegistrationBackfill(
            TenantResources tenantResources,
            NamespaceResources namespaceResources,
            TopicResources topicResources,
            NereusStorageClassBindingStore bindingStore,
            NereusManagedLedgerStorage storage,
            NereusBrokerCapabilityCoordinator capabilityCoordinator,
            int maxTopicsPerNamespace) {
        this(
                tenantResources,
                namespaceResources,
                topicResources,
                bindingStore,
                new RegistrationAccess() {
                    @Override
                    public CompletableFuture<ManagedLedgerMaterializationRegistrationCandidate>
                            inspect(
                                    String persistenceName,
                                    long expectedBindingGeneration) {
                        return storage
                                .inspectMaterializationRegistrationCandidate(
                                        persistenceName,
                                        expectedBindingGeneration);
                    }

                    @Override
                    public CompletableFuture<Void> ensureRegistered(
                            ManagedLedgerMaterializationRegistrationCandidate
                                    candidate) {
                        return storage.ensureMaterializationRegistration(
                                candidate);
                    }
                },
                (completion, maxConcurrentStreams, timeout) ->
                        storage.completeGenerationRegistrationBackfill(
                                completion,
                                maxConcurrentStreams,
                                timeout),
                capabilityCoordinator,
                maxTopicsPerNamespace);
    }

    DefaultNereusGenerationRegistrationBackfill(
            TenantResources tenantResources,
            NamespaceResources namespaceResources,
            TopicResources topicResources,
            NereusStorageClassBindingStore bindingStore,
            RegistrationAccess registrations,
            ProofAccess proofs,
            NereusBrokerCapabilityCoordinator capabilityCoordinator,
            int maxTopicsPerNamespace) {
        this.tenantResources =
                Objects.requireNonNull(tenantResources, "tenantResources");
        this.namespaceResources =
                Objects.requireNonNull(namespaceResources, "namespaceResources");
        this.topicResources =
                Objects.requireNonNull(topicResources, "topicResources");
        this.bindingStore =
                Objects.requireNonNull(bindingStore, "bindingStore");
        this.registrations =
                Objects.requireNonNull(registrations, "registrations");
        this.proofs = Objects.requireNonNull(proofs, "proofs");
        this.capabilityCoordinator = Objects.requireNonNull(
                capabilityCoordinator, "capabilityCoordinator");
        if (maxTopicsPerNamespace < 1) {
            throw new IllegalArgumentException(
                    "maxTopicsPerNamespace must be positive");
        }
        this.maxTopicsPerNamespace = maxTopicsPerNamespace;
    }

    @Override
    public CompletableFuture<GenerationRegistrationBackfillReport> run(
            GenerationRegistrationBackfillRequest request) {
        final GenerationRegistrationBackfillRequest exact;
        final long deadlineNanos;
        try {
            exact = Objects.requireNonNull(request, "request");
            deadlineNanos = deadlineNanos(exact);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return bound(
                        capabilityCoordinator::requireGenerationReadiness,
                        deadlineNanos)
                .thenCompose(first -> {
                    if (first.brokerReadinessEpoch()
                            != exact.expectedBrokerReadinessEpoch()) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException(
                                        "NEREUS_GENERATION_BACKFILL_READINESS_EPOCH_MISMATCH"));
                    }
                    Accumulator accumulator = new Accumulator(exact, first);
                    return traverse(accumulator, exact, deadlineNanos)
                            .thenCompose(ignored -> bound(
                                    capabilityCoordinator
                                            ::requireGenerationReadiness,
                                    deadlineNanos))
                            .thenCompose(last -> {
                                if (!first.equals(last)) {
                                    throw new IllegalStateException(
                                            "NEREUS_GENERATION_BACKFILL_READINESS_CHANGED");
                                }
                                GenerationRegistrationBackfillReport report =
                                        accumulator.report();
                                if (report.failureCount() != 0) {
                                    return CompletableFuture.completedFuture(
                                            report);
                                }
                                GenerationRegistrationBackfillCompletion
                                        completion =
                                                new GenerationRegistrationBackfillCompletion(
                                                        report.runId(),
                                                        first.toCore(),
                                                        report.coverageSha256(),
                                                        report.failureCount());
                                final Duration remaining;
                                try {
                                    remaining = remaining(deadlineNanos);
                                } catch (TimeoutException timeout) {
                                    return CompletableFuture.failedFuture(
                                            timeout);
                                }
                                return bound(
                                                () -> proofs.complete(
                                                        completion,
                                                        exact.maxConcurrency(),
                                                        remaining),
                                                deadlineNanos)
                                        .thenApply(ignored -> report);
                            });
                });
    }

    private CompletableFuture<Void> traverse(
            Accumulator accumulator,
            GenerationRegistrationBackfillRequest request,
            long deadlineNanos) {
        return bound(tenantResources::listTenantsAsync, deadlineNanos)
                .handle((tenants, error) -> {
                    if (error != null) {
                        accumulator.failure(
                                "cluster",
                                GenerationRegistrationBackfillStage.TENANT_LIST,
                                errorCode(error));
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    try {
                        return processTenants(
                                canonicalTenants(tenants),
                                0,
                                accumulator,
                                request,
                                deadlineNanos);
                    } catch (Throwable failure) {
                        accumulator.failure(
                                "cluster",
                                GenerationRegistrationBackfillStage.TENANT_LIST,
                                errorCode(failure));
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<Void> processTenants(
            List<String> tenants,
            int index,
            Accumulator accumulator,
            GenerationRegistrationBackfillRequest request,
            long deadlineNanos) {
        if (index == tenants.size()) {
            return CompletableFuture.completedFuture(null);
        }
        String tenant = tenants.get(index);
        accumulator.tenant(tenant);
        return bound(
                        () -> namespaceResources.listNamespacesAsync(tenant),
                        deadlineNanos)
                .handle((namespaces, error) -> {
                    if (error != null) {
                        accumulator.failure(
                                tenant,
                                GenerationRegistrationBackfillStage
                                        .NAMESPACE_LIST,
                                errorCode(error));
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    try {
                        return processNamespaces(
                                tenant,
                                canonicalNamespaces(tenant, namespaces),
                                0,
                                accumulator,
                                request,
                                deadlineNanos);
                    } catch (Throwable failure) {
                        accumulator.failure(
                                tenant,
                                GenerationRegistrationBackfillStage
                                        .NAMESPACE_LIST,
                                errorCode(failure));
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                })
                .thenCompose(Function.identity())
                .thenCompose(ignored -> processTenants(
                        tenants,
                        index + 1,
                        accumulator,
                        request,
                        deadlineNanos));
    }

    private CompletableFuture<Void> processNamespaces(
            String tenant,
            List<NamespaceName> namespaces,
            int index,
            Accumulator accumulator,
            GenerationRegistrationBackfillRequest request,
            long deadlineNanos) {
        if (index == namespaces.size()) {
            return CompletableFuture.completedFuture(null);
        }
        NamespaceName namespace = namespaces.get(index);
        accumulator.namespace(namespace.toString());
        return bound(
                        () -> topicResources.listPersistentTopicsAsync(
                                namespace),
                        deadlineNanos)
                .handle((topics, error) -> {
                    if (error != null) {
                        accumulator.failure(
                                namespace.toString(),
                                GenerationRegistrationBackfillStage.TOPIC_LIST,
                                errorCode(error));
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    try {
                        if (topics.size() > maxTopicsPerNamespace) {
                            throw new IllegalStateException(
                                    "NEREUS_GENERATION_BACKFILL_TOPIC_LIST_LIMIT_EXCEEDED");
                        }
                        return processTopicBatches(
                                canonicalTopics(namespace, topics),
                                0,
                                accumulator,
                                request,
                                deadlineNanos);
                    } catch (Throwable failure) {
                        accumulator.failure(
                                namespace.toString(),
                                GenerationRegistrationBackfillStage.TOPIC_LIST,
                                errorCode(failure));
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                })
                .thenCompose(Function.identity())
                .thenCompose(ignored -> processNamespaces(
                        tenant,
                        namespaces,
                        index + 1,
                        accumulator,
                        request,
                        deadlineNanos));
    }

    private CompletableFuture<Void> processTopicBatches(
            List<TopicName> topics,
            int index,
            Accumulator accumulator,
            GenerationRegistrationBackfillRequest request,
            long deadlineNanos) {
        if (index == topics.size()) {
            return CompletableFuture.completedFuture(null);
        }
        int end = Math.min(
                topics.size(), index + request.maxConcurrency());
        ArrayList<CompletableFuture<TopicOutcome>> operations =
                new ArrayList<>(end - index);
        for (int current = index; current < end; current++) {
            operations.add(processTopic(topics.get(current), deadlineNanos));
        }
        return CompletableFuture.allOf(
                        operations.toArray(CompletableFuture[]::new))
                .thenCompose(ignored -> {
                    for (CompletableFuture<TopicOutcome> operation
                            : operations) {
                        accumulator.topic(operation.join());
                    }
                    return processTopicBatches(
                            topics,
                            end,
                            accumulator,
                            request,
                            deadlineNanos);
                });
    }

    private CompletableFuture<TopicOutcome> processTopic(
            TopicName topic,
            long deadlineNanos) {
        if (SystemTopicNames.isSystemTopic(topic)) {
            return CompletableFuture.completedFuture(
                    TopicOutcome.system(topic.toString()));
        }
        String persistenceName = topic.getPersistenceNamingEncoding();
        return bound(
                        () -> bindingStore.getBinding(persistenceName),
                        deadlineNanos)
                .handle((binding, error) -> {
                    if (error != null) {
                        return CompletableFuture.completedFuture(
                                TopicOutcome.failure(
                                        topic.toString(),
                                        GenerationRegistrationBackfillStage
                                                .BINDING_READ,
                                        errorCode(error)));
                    }
                    return processBinding(
                            topic,
                            persistenceName,
                            binding,
                            deadlineNanos);
                })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<TopicOutcome> processBinding(
            TopicName topic,
            String persistenceName,
            Optional<StorageClassBindingRecord> optional,
            long deadlineNanos) {
        if (optional.isEmpty()) {
            return CompletableFuture.completedFuture(
                    TopicOutcome.skipped(
                            topic.toString(), "BINDING_MISSING"));
        }
        StorageClassBindingRecord binding = optional.orElseThrow();
        if (!StorageClassBindingRecord.NEREUS.equals(
                binding.storageClass())) {
            return CompletableFuture.completedFuture(
                    TopicOutcome.skipped(
                            topic.toString(), "NON_NEREUS"));
        }
        if (binding.state() == StorageClassBindingState.DELETING
                || binding.state() == StorageClassBindingState.DELETED) {
            return CompletableFuture.completedFuture(
                    TopicOutcome.skipped(
                            topic.toString(), "DELETED"));
        }
        long bindingGeneration = binding.bindingGeneration();
        return bound(
                        () -> registrations.inspect(
                                persistenceName, bindingGeneration),
                        deadlineNanos)
                .handle((candidate, error) -> {
                    if (error != null) {
                        return CompletableFuture.completedFuture(
                                TopicOutcome.failure(
                                        topic.toString(),
                                        GenerationRegistrationBackfillStage
                                                .PROJECTION_READ,
                                        errorCode(error)));
                    }
                    return ensureRegistration(
                            topic,
                            persistenceName,
                            bindingGeneration,
                            candidate,
                            deadlineNanos);
                })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<TopicOutcome> ensureRegistration(
            TopicName topic,
            String persistenceName,
            long expectedBindingGeneration,
            ManagedLedgerMaterializationRegistrationCandidate candidate,
            long deadlineNanos) {
        if (candidate.storageClassBindingGeneration()
                        != expectedBindingGeneration
                || !candidate.managedLedgerName().equals(persistenceName)) {
            return CompletableFuture.completedFuture(
                    TopicOutcome.failure(
                            topic.toString(),
                            GenerationRegistrationBackfillStage
                                    .PROJECTION_READ,
                            "PROJECTION_BINDING_MISMATCH"));
        }
        return bound(
                        () -> registrations.ensureRegistered(candidate),
                        deadlineNanos)
                .handle((ignored, error) -> {
                    if (error != null) {
                        return CompletableFuture.completedFuture(
                                TopicOutcome.failure(
                                        topic.toString(),
                                        GenerationRegistrationBackfillStage
                                                .REGISTRATION_WRITE,
                                        errorCode(error)));
                    }
                    return revalidateBinding(
                            topic,
                            persistenceName,
                            expectedBindingGeneration,
                            candidate,
                            deadlineNanos);
                })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<TopicOutcome> revalidateBinding(
            TopicName topic,
            String persistenceName,
            long expectedBindingGeneration,
            ManagedLedgerMaterializationRegistrationCandidate candidate,
            long deadlineNanos) {
        return bound(
                        () -> bindingStore.getBinding(persistenceName),
                        deadlineNanos)
                .handle((optional, error) -> {
                    if (error != null) {
                        return TopicOutcome.failure(
                                topic.toString(),
                                GenerationRegistrationBackfillStage
                                        .BINDING_READ,
                                errorCode(error));
                    }
                    if (optional.isEmpty()) {
                        return TopicOutcome.failure(
                                topic.toString(),
                                GenerationRegistrationBackfillStage
                                        .BINDING_READ,
                                "BINDING_DISAPPEARED");
                    }
                    StorageClassBindingRecord current =
                            optional.orElseThrow();
                    if (current.bindingGeneration()
                                    != expectedBindingGeneration
                            || !StorageClassBindingRecord.NEREUS.equals(
                                    current.storageClass())) {
                        return TopicOutcome.failure(
                                topic.toString(),
                                GenerationRegistrationBackfillStage
                                        .BINDING_READ,
                                "BINDING_CHANGED");
                    }
                    if (current.state()
                                    == StorageClassBindingState.DELETING
                            || current.state()
                                    == StorageClassBindingState.DELETED) {
                        return TopicOutcome.skipped(
                                topic.toString(),
                                "DELETED_AFTER_REGISTRATION");
                    }
                    return TopicOutcome.registered(
                            topic.toString(),
                            candidate.projectionIdentitySha256().value());
                });
    }

    private static List<String> canonicalTenants(List<String> supplied) {
        Objects.requireNonNull(supplied, "tenants");
        TreeSet<String> canonical = new TreeSet<>();
        for (String tenant : supplied) {
            if (tenant == null
                    || tenant.isBlank()
                    || tenant.indexOf('/') >= 0
                    || !canonical.add(tenant)) {
                throw new IllegalArgumentException(
                        "tenant list is not canonical and unique");
            }
        }
        return List.copyOf(canonical);
    }

    private static List<NamespaceName> canonicalNamespaces(
            String tenant,
            List<String> supplied) {
        Objects.requireNonNull(supplied, "namespaces");
        TreeSet<NamespaceName> canonical = new TreeSet<>(
                Comparator.comparing(NamespaceName::toString));
        for (String value : supplied) {
            NamespaceName namespace = NamespaceName.get(value);
            if (!tenant.equals(namespace.getTenant())
                    || !namespace.toString().equals(value)
                    || !canonical.add(namespace)) {
                throw new IllegalArgumentException(
                        "namespace list is not canonical and unique");
            }
        }
        return List.copyOf(canonical);
    }

    private static List<TopicName> canonicalTopics(
            NamespaceName namespace,
            List<String> supplied) {
        Objects.requireNonNull(supplied, "topics");
        TreeSet<TopicName> canonical = new TreeSet<>(
                Comparator.comparing(TopicName::toString));
        for (String value : supplied) {
            TopicName topic = TopicName.get(value);
            if (topic.getDomain() != TopicDomain.persistent
                    || !namespace.equals(topic.getNamespaceObject())
                    || !topic.toString().equals(value)
                    || !canonical.add(topic)) {
                throw new IllegalArgumentException(
                        "topic list is not canonical and unique");
            }
        }
        return List.copyOf(canonical);
    }

    private static <T> CompletableFuture<T> bound(
            Supplier<CompletableFuture<T>> operation,
            long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            return CompletableFuture.failedFuture(
                    new TimeoutException(
                            "Nereus generation registration backfill timed out"));
        }
        try {
            return Objects.requireNonNull(
                            operation.get(), "operation result")
                    .orTimeout(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    private static long deadlineNanos(
            GenerationRegistrationBackfillRequest request) {
        long now = System.nanoTime();
        final long timeout;
        try {
            timeout = request.timeout().toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
        return now > Long.MAX_VALUE - timeout
                ? Long.MAX_VALUE
                : now + timeout;
    }

    private static Duration remaining(long deadlineNanos)
            throws TimeoutException {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos < TimeUnit.MILLISECONDS.toNanos(1)) {
            throw new TimeoutException(
                    "Nereus generation registration backfill timed out before proof completion");
        }
        return Duration.ofNanos(remainingNanos);
    }

    private static String errorCode(Throwable error) {
        Throwable cause = unwrap(error);
        if (cause instanceof TimeoutException) {
            return "TIMEOUT";
        }
        if (cause instanceof CancellationException) {
            return "CANCELLED";
        }
        if (cause instanceof NereusException nereus) {
            return nereus.code().name();
        }
        if (cause instanceof IllegalArgumentException) {
            return "INVALID_METADATA";
        }
        if (cause instanceof IllegalStateException state
                && state.getMessage() != null
                && state.getMessage().startsWith("NEREUS_")) {
            return boundedMachineCode(state.getMessage());
        }
        return "OPERATION_FAILED";
    }

    private static String boundedMachineCode(String value) {
        StringBuilder result = new StringBuilder(64);
        for (int index = 0;
                index < value.length() && result.length() < 64;
                index++) {
            char character = value.charAt(index);
            if ((character >= 'A' && character <= 'Z')
                    || (character >= '0' && character <= '9')
                    || character == '_') {
                result.append(character);
            }
        }
        return result.length() == 0
                ? "OPERATION_FAILED"
                : result.toString();
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException
                        || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String resourceSha256(
            String kind,
            String identity) {
        Digest digest = new Digest();
        digest.text("nereus-generation-backfill-resource-v1");
        digest.text(kind);
        digest.text(identity);
        return digest.hex();
    }

    interface RegistrationAccess {
        CompletableFuture<ManagedLedgerMaterializationRegistrationCandidate>
                inspect(
                        String persistenceName,
                        long expectedBindingGeneration);

        CompletableFuture<Void> ensureRegistered(
                ManagedLedgerMaterializationRegistrationCandidate candidate);
    }

    interface ProofAccess {
        CompletableFuture<Void> complete(
                GenerationRegistrationBackfillCompletion completion,
                int maxConcurrentStreams,
                Duration timeout);
    }

    private record TopicOutcome(
            String topic,
            String classification,
            boolean scanned,
            boolean registered,
            boolean skipped,
            Optional<GenerationRegistrationBackfillStage> failureStage,
            Optional<String> failureCode,
            String projectionIdentitySha256) {
        private TopicOutcome {
            Objects.requireNonNull(topic, "topic");
            Objects.requireNonNull(classification, "classification");
            failureStage = Objects.requireNonNull(
                    failureStage, "failureStage");
            failureCode = Objects.requireNonNull(
                    failureCode, "failureCode");
            Objects.requireNonNull(
                    projectionIdentitySha256,
                    "projectionIdentitySha256");
            int terminalCount = (registered ? 1 : 0)
                    + (skipped ? 1 : 0)
                    + (failureStage.isPresent() ? 1 : 0);
            if (scanned && terminalCount != 1
                    || !scanned && terminalCount != 0
                    || failureStage.isPresent()
                            != failureCode.isPresent()) {
                throw new IllegalArgumentException(
                        "topic outcome terminal classification is invalid");
            }
        }

        private static TopicOutcome system(String topic) {
            return new TopicOutcome(
                    topic,
                    "SYSTEM_TOPIC",
                    false,
                    false,
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    "");
        }

        private static TopicOutcome skipped(
                String topic,
                String classification) {
            return new TopicOutcome(
                    topic,
                    classification,
                    true,
                    false,
                    true,
                    Optional.empty(),
                    Optional.empty(),
                    "");
        }

        private static TopicOutcome registered(
                String topic,
                String projectionIdentitySha256) {
            return new TopicOutcome(
                    topic,
                    "REGISTERED",
                    true,
                    true,
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    projectionIdentitySha256);
        }

        private static TopicOutcome failure(
                String topic,
                GenerationRegistrationBackfillStage stage,
                String errorCode) {
            return new TopicOutcome(
                    topic,
                    "FAILED",
                    true,
                    false,
                    false,
                    Optional.of(stage),
                    Optional.of(errorCode),
                    "");
        }
    }

    private static final class Accumulator {
        private final GenerationRegistrationBackfillRequest request;
        private final Digest digest = new Digest();
        private final ArrayList<BackfillFailure> failures =
                new ArrayList<>(
                        GenerationRegistrationBackfillReport.MAX_FAILURES);
        private long tenantsScanned;
        private long namespacesScanned;
        private long persistentTopicsScanned;
        private long nereusProjectionsRegistered;
        private long deletedOrNonNereusSkipped;
        private long failureCount;

        private Accumulator(
                GenerationRegistrationBackfillRequest request,
                NereusGenerationCapabilityReadiness readiness) {
            this.request = request;
            digest.text(DIGEST_DOMAIN);
            digest.text(
                    Long.toString(readiness.brokerReadinessEpoch()));
            digest.text(readiness.brokerSetSha256());
            digest.text(
                    Integer.toString(readiness.persistentBrokerCount()));
        }

        private void tenant(String tenant) {
            tenantsScanned = Math.addExact(tenantsScanned, 1);
            digest.event("TENANT", tenant);
        }

        private void namespace(String namespace) {
            namespacesScanned = Math.addExact(namespacesScanned, 1);
            digest.event("NAMESPACE", namespace);
        }

        private void topic(TopicOutcome outcome) {
            digest.event(
                    "TOPIC",
                    outcome.topic(),
                    outcome.classification(),
                    outcome.projectionIdentitySha256());
            if (!outcome.scanned()) {
                return;
            }
            persistentTopicsScanned =
                    Math.addExact(persistentTopicsScanned, 1);
            if (outcome.registered()) {
                nereusProjectionsRegistered =
                        Math.addExact(nereusProjectionsRegistered, 1);
            } else if (outcome.skipped()) {
                deletedOrNonNereusSkipped =
                        Math.addExact(deletedOrNonNereusSkipped, 1);
            } else {
                failure(
                        outcome.topic(),
                        outcome.failureStage().orElseThrow(),
                        outcome.failureCode().orElseThrow());
            }
        }

        private void failure(
                String resource,
                GenerationRegistrationBackfillStage stage,
                String errorCode) {
            failureCount = Math.addExact(failureCount, 1);
            String resourceIdentity = resourceSha256(
                    stage.name(), resource);
            digest.event(
                    "FAILURE",
                    resourceIdentity,
                    stage.name(),
                    errorCode);
            if (failures.size()
                    < GenerationRegistrationBackfillReport.MAX_FAILURES) {
                failures.add(new BackfillFailure(
                        resourceIdentity, stage, errorCode));
            }
        }

        private GenerationRegistrationBackfillReport report() {
            return new GenerationRegistrationBackfillReport(
                    request.runId(),
                    request.expectedBrokerReadinessEpoch(),
                    tenantsScanned,
                    namespacesScanned,
                    persistentTopicsScanned,
                    nereusProjectionsRegistered,
                    deletedOrNonNereusSkipped,
                    failureCount,
                    new Checksum(ChecksumType.SHA256, digest.hex()),
                    failures);
        }
    }

    private static final class Digest {
        private final MessageDigest digest;

        private Digest() {
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException failure) {
                throw new IllegalStateException(
                        "SHA-256 is unavailable", failure);
            }
        }

        private void event(String... values) {
            for (String value : values) {
                text(value);
            }
        }

        private void text(String value) {
            byte[] bytes = Objects.requireNonNull(
                            value, "digest value")
                    .getBytes(StandardCharsets.UTF_8);
            digest.update(
                    ByteBuffer.allocate(Integer.BYTES)
                            .order(ByteOrder.BIG_ENDIAN)
                            .putInt(bytes.length)
                            .array());
            digest.update(bytes);
        }

        private String hex() {
            return HexFormat.of().formatHex(digest.digest());
        }
    }
}
