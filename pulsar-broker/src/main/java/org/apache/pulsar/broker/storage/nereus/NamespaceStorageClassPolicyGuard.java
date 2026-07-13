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

import com.nereusstream.api.keys.DeterministicIds;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;

/** Serializes namespace storage-class transitions with both storage classes' first durable creation. */
public final class NamespaceStorageClassPolicyGuard implements AutoCloseable {
    private static final String LOCK_ROOT = "/managed-ledger-storage-policy-locks/v1/";
    private static final String LOCK_DOMAIN = "pulsar-storage-policy-lock-v1\0";
    private static final long INITIAL_RETRY_MILLIS = 10;
    private static final long MAX_RETRY_MILLIS = 500;

    private final LockManager<NamespaceStorageClassLockData> lockManager;
    private final NamespaceResources namespaceResources;
    private final TopicResources topicResources;
    private final NereusStorageClassBindingStore bindingStore;
    private final NereusBrokerCapabilityCoordinator capabilityCoordinator;
    private final Function<TopicName, CompletableFuture<String>> effectiveStorageClassLoader;
    private final Supplier<String> brokerIdSupplier;
    private final ScheduledExecutorService scheduler;
    private final Duration operationTimeout;
    private final int maxBindingScanEntries;
    private final int maxBindingPendingOperations;
    private final AtomicBoolean closed = new AtomicBoolean();

    public NamespaceStorageClassPolicyGuard(
            CoordinationService coordinationService,
            NamespaceResources namespaceResources,
            TopicResources topicResources,
            NereusStorageClassBindingStore bindingStore,
            NereusBrokerCapabilityCoordinator capabilityCoordinator,
            Function<TopicName, CompletableFuture<String>> effectiveStorageClassLoader,
            Supplier<String> brokerIdSupplier,
            ScheduledExecutorService scheduler,
            Duration operationTimeout,
            int maxBindingScanEntries,
            int maxBindingPendingOperations) {
        this.lockManager = java.util.Objects.requireNonNull(coordinationService, "coordinationService")
                .getLockManager(NamespaceStorageClassLockData.class);
        this.namespaceResources = java.util.Objects.requireNonNull(namespaceResources, "namespaceResources");
        this.topicResources = java.util.Objects.requireNonNull(topicResources, "topicResources");
        this.bindingStore = java.util.Objects.requireNonNull(bindingStore, "bindingStore");
        this.capabilityCoordinator = java.util.Objects.requireNonNull(
                capabilityCoordinator, "capabilityCoordinator");
        this.effectiveStorageClassLoader = java.util.Objects.requireNonNull(
                effectiveStorageClassLoader, "effectiveStorageClassLoader");
        this.brokerIdSupplier = java.util.Objects.requireNonNull(brokerIdSupplier, "brokerIdSupplier");
        this.scheduler = java.util.Objects.requireNonNull(scheduler, "scheduler");
        this.operationTimeout = java.util.Objects.requireNonNull(operationTimeout, "operationTimeout");
        if (operationTimeout.isZero() || operationTimeout.isNegative()) {
            throw new IllegalArgumentException("operationTimeout must be positive");
        }
        if (maxBindingScanEntries <= 0 || maxBindingPendingOperations <= 0) {
            throw new IllegalArgumentException("binding scan limits must be positive");
        }
        this.maxBindingScanEntries = maxBindingScanEntries;
        this.maxBindingPendingOperations = maxBindingPendingOperations;
    }

    public CompletableFuture<NamespaceStorageClassPermit> acquireFirstCreate(
            NamespaceName namespace, TopicName topic, String selectedStorageClass) {
        java.util.Objects.requireNonNull(namespace, "namespace");
        java.util.Objects.requireNonNull(topic, "topic");
        String selected = normalizeStorageClass(selectedStorageClass);
        if (!topic.getNamespaceObject().equals(namespace)) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("topic does not belong to namespace"));
        }
        long deadlineNanos = deadlineNanos();
        return acquireLock(namespace, deadlineNanos).thenCompose(lock -> withinDeadline(
                loadSelection(namespace, topic), deadlineNanos)
                .thenCompose(selection -> {
                    if (!selection.storageClass().equals(selected)) {
                        return CompletableFuture.<NamespaceStorageClassPermit>failedFuture(new IllegalStateException(
                                "NEREUS_NAMESPACE_STORAGE_POLICY_CHANGED"));
                    }
                    return CompletableFuture.<NamespaceStorageClassPermit>completedFuture(new Permit(
                            namespace, topic, selected, selection.policyVersion(), deadlineNanos, lock));
                }).exceptionallyCompose(error -> failAndRelease(lock, unwrap(error))));
    }

    public CompletableFuture<Void> updateNamespacePersistence(
            NamespaceName namespace, PersistencePolicies targetPersistence) {
        java.util.Objects.requireNonNull(namespace, "namespace");
        final PersistencePolicies target;
        try {
            target = copyPersistence(targetPersistence);
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        long deadlineNanos = deadlineNanos();
        return withinDeadline(readNamespacePolicy(namespace), deadlineNanos).thenCompose(initial -> {
            String currentClass = storageClass(initial.policies().persistence);
            String targetClass = storageClass(target);
            CompletableFuture<Void> capabilityReady = !currentClass.equals(targetClass)
                    && StorageClassBindingRecord.NEREUS.equals(targetClass)
                    ? capabilityCoordinator.requireClusterReady()
                    : CompletableFuture.completedFuture(null);
            return withinDeadline(capabilityReady, deadlineNanos).thenCompose(ignored -> updateNamespacePersistence(
                    namespace, target, targetClass, initial.policyVersion(), deadlineNanos));
        });
    }

    CompletableFuture<Void> updateTopicPersistence(
            TopicName topic,
            Supplier<CompletableFuture<String>> proposedStorageClassLoader,
            Supplier<CompletableFuture<Void>> policyMutation) {
        java.util.Objects.requireNonNull(topic, "topic");
        java.util.Objects.requireNonNull(proposedStorageClassLoader, "proposedStorageClassLoader");
        java.util.Objects.requireNonNull(policyMutation, "policyMutation");
        long deadlineNanos = deadlineNanos();
        return acquireLock(topic.getNamespaceObject(), deadlineNanos).thenCompose(lock -> withinDeadline(
                loadEffectiveStorageClass(topic)
                .thenCombine(invoke(proposedStorageClassLoader, "proposedStorageClassLoader"),
                        TopicStorageClassUpdate::new)
                .thenCompose(update -> {
                    boolean storageClassChange = !update.current().equals(update.proposed());
                    CompletableFuture<Void> capabilityReady = storageClassChange
                            && StorageClassBindingRecord.NEREUS.equals(update.proposed())
                            ? capabilityCoordinator.requireClusterReady()
                            : CompletableFuture.completedFuture(null);
                    CompletableFuture<Void> bindingReady = capabilityReady.thenCompose(ignored ->
                            storageClassChange
                                    ? bindingStore.validateStorageClassPolicyUpdate(
                                            topic.getPersistenceNamingEncoding(), update.proposed())
                                    : CompletableFuture.completedFuture(null));
                    return bindingReady.thenCompose(ignored -> invoke(policyMutation, "policyMutation"))
                            .thenCompose(ignored -> loadEffectiveStorageClass(topic))
                            .thenAccept(actual -> {
                                if (!actual.equals(update.proposed())) {
                                    throw new IllegalStateException(
                                            "NEREUS_TOPIC_STORAGE_POLICY_READBACK_MISMATCH");
                                }
                            });
                }), deadlineNanos)
                .handle((ignored, error) -> releaseWithResult(lock, error))
                .thenCompose(Function.identity()));
    }

    private CompletableFuture<Void> updateNamespacePersistence(
            NamespaceName namespace,
            PersistencePolicies target,
            String targetClass,
            long expectedPolicyVersion,
            long deadlineNanos) {
        return acquireLock(namespace, deadlineNanos).thenCompose(lock -> withinDeadline(
                readNamespacePolicy(namespace)
                .thenCompose(current -> {
                    if (current.policyVersion() != expectedPolicyVersion) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("NEREUS_NAMESPACE_POLICY_VERSION_CHANGED"));
                    }
                    boolean storageClassChange = !storageClass(current.policies().persistence).equals(targetClass);
                    CompletableFuture<Void> emptyBefore = storageClassChange
                            ? requireEmptyNamespace(namespace)
                            : CompletableFuture.completedFuture(null);
                    return emptyBefore.thenCompose(ignored -> writeNamespacePersistence(namespace, current, target))
                            .thenCompose(updated -> {
                                if (!storageClass(updated.policies().persistence).equals(targetClass)) {
                                    return CompletableFuture.failedFuture(new IllegalStateException(
                                            "NEREUS_NAMESPACE_POLICY_READBACK_MISMATCH"));
                                }
                                return storageClassChange
                                        ? requireEmptyNamespace(namespace)
                                        : CompletableFuture.completedFuture(null);
                            });
                }), deadlineNanos)
                .handle((ignored, error) -> releaseWithResult(lock, error)).thenCompose(Function.identity()));
    }

    private CompletableFuture<PolicySelection> loadSelection(NamespaceName namespace, TopicName topic) {
        return readNamespacePolicy(namespace).thenCombine(
                loadEffectiveStorageClass(topic),
                (policy, storageClass) -> new PolicySelection(policy.policyVersion(), storageClass));
    }

    private CompletableFuture<String> loadEffectiveStorageClass(TopicName topic) {
        try {
            return java.util.Objects.requireNonNull(
                    effectiveStorageClassLoader.apply(topic), "effectiveStorageClassLoader result")
                    .thenApply(NamespaceStorageClassPolicyGuard::normalizeStorageClass);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    private static <T> CompletableFuture<T> invoke(
            Supplier<CompletableFuture<T>> operation, String name) {
        try {
            return java.util.Objects.requireNonNull(operation.get(), name + " result");
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    private CompletableFuture<PolicySnapshot> writeNamespacePersistence(
            NamespaceName namespace, PolicySnapshot current, PersistencePolicies target) {
        final Policies updated;
        try {
            updated = copyPolicies(current.policies());
            updated.persistence = copyPersistence(target);
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        return namespaceResources.setPoliciesWithVersion(namespace, updated, current.policyVersion())
                .thenCompose(ignored -> readNamespacePolicy(namespace));
    }

    private CompletableFuture<Void> requireEmptyNamespace(NamespaceName namespace) {
        CompletableFuture<List<String>> topics = topicResources.listPersistentTopicsAsync(namespace);
        CompletableFuture<List<StorageClassBindingRecord>> bindings = bindingStore.listNonDeletedBindings(
                namespace, maxBindingScanEntries, maxBindingPendingOperations);
        return topics.thenCombine(bindings, (persistentTopics, nonDeletedBindings) -> {
            if (!persistentTopics.isEmpty() || !nonDeletedBindings.isEmpty()) {
                throw new IllegalStateException("NEREUS_NAMESPACE_STORAGE_POLICY_REQUIRES_EMPTY_NAMESPACE");
            }
            return (Void) null;
        });
    }

    private CompletableFuture<PolicySnapshot> readNamespacePolicy(NamespaceName namespace) {
        return namespaceResources.refreshAndGetPoliciesWithVersion(namespace).thenApply(optional -> {
            CacheGetResult<Policies> result = optional.orElseThrow(() ->
                    new IllegalStateException("Nereus namespace policy does not exist"));
            return new PolicySnapshot(result.getValue(), result.getStat().getVersion());
        });
    }

    private CompletableFuture<ResourceLock<NamespaceStorageClassLockData>> acquireLock(
            NamespaceName namespace, long deadlineNanos) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("namespace storage-class policy guard is closed"));
        }
        CompletableFuture<ResourceLock<NamespaceStorageClassLockData>> result = new CompletableFuture<>();
        String brokerId;
        try {
            brokerId = java.util.Objects.requireNonNull(brokerIdSupplier.get(), "brokerId");
            if (brokerId.isBlank()) {
                throw new IllegalStateException("brokerId is not ready");
            }
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        NamespaceStorageClassLockData data = new NamespaceStorageClassLockData(
                brokerId, UUID.randomUUID().toString(), System.currentTimeMillis());
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            return CompletableFuture.failedFuture(
                    new TimeoutException("Nereus namespace storage-policy operation timed out"));
        }
        ScheduledFuture<?> timeoutTask;
        try {
            timeoutTask = scheduler.schedule(
                    () -> result.completeExceptionally(
                            new TimeoutException("Nereus namespace storage-policy lock timed out")),
                    remainingNanos,
                    TimeUnit.NANOSECONDS);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        result.whenComplete((ignored, error) -> timeoutTask.cancel(false));
        acquireLock(namespace, data, deadlineNanos, INITIAL_RETRY_MILLIS, result);
        return result;
    }

    private long deadlineNanos() {
        long now = System.nanoTime();
        long timeoutNanos = operationTimeout.toNanos();
        return now > Long.MAX_VALUE - timeoutNanos ? Long.MAX_VALUE : now + timeoutNanos;
    }

    private static <T> CompletableFuture<T> withinDeadline(
            CompletableFuture<T> operation, long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            return CompletableFuture.failedFuture(
                    new TimeoutException("Nereus namespace storage-policy operation timed out"));
        }
        return operation.orTimeout(remainingNanos, TimeUnit.NANOSECONDS);
    }

    public CompletableFuture<Void> asyncClose() {
        if (!closed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        return lockManager.asyncClose();
    }

    @Override
    public void close() throws Exception {
        try {
            asyncClose().get();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw error;
        } catch (java.util.concurrent.ExecutionException error) {
            Throwable cause = unwrap(error);
            if (cause instanceof Exception exception) {
                throw exception;
            }
            throw new RuntimeException(cause);
        }
    }

    private void acquireLock(
            NamespaceName namespace,
            NamespaceStorageClassLockData data,
            long deadlineNanos,
            long retryMillis,
            CompletableFuture<ResourceLock<NamespaceStorageClassLockData>> result) {
        if (result.isDone()) {
            return;
        }
        lockManager.acquireLock(lockPath(namespace), data).whenComplete((lock, error) -> {
            if (error == null) {
                if (!result.complete(lock)) {
                    lock.release();
                }
                return;
            }
            Throwable cause = unwrap(error);
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (!(cause instanceof MetadataStoreException.LockBusyException) || remainingNanos <= 0) {
                result.completeExceptionally(cause);
                return;
            }
            long delayMillis = Math.min(retryMillis,
                    Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
            try {
                scheduler.schedule(
                        () -> acquireLock(
                                namespace,
                                data,
                                deadlineNanos,
                                Math.min(MAX_RETRY_MILLIS, retryMillis * 2),
                                result),
                        delayMillis,
                        TimeUnit.MILLISECONDS);
            } catch (Throwable scheduleError) {
                result.completeExceptionally(scheduleError);
            }
        });
    }

    private CompletableFuture<NamespaceStorageClassPermit> failAndRelease(
            ResourceLock<NamespaceStorageClassLockData> lock, Throwable error) {
        return lock.release().handle((ignored, releaseError) -> {
            if (releaseError != null) {
                error.addSuppressed(unwrap(releaseError));
            }
            throw new CompletionException(error);
        });
    }

    private static CompletableFuture<Void> releaseWithResult(
            ResourceLock<NamespaceStorageClassLockData> lock, Throwable operationError) {
        return lock.release().handle((ignored, releaseError) -> {
            if (operationError != null) {
                Throwable cause = unwrap(operationError);
                if (releaseError != null) {
                    cause.addSuppressed(unwrap(releaseError));
                }
                throw new CompletionException(cause);
            }
            if (releaseError != null) {
                throw new CompletionException(unwrap(releaseError));
            }
            return (Void) null;
        });
    }

    private static String lockPath(NamespaceName namespace) {
        return LOCK_ROOT + DeterministicIds.stableHashComponent(LOCK_DOMAIN + namespace);
    }

    private static String storageClass(PersistencePolicies persistence) {
        return normalizeStorageClass(
                persistence == null ? null : persistence.getManagedLedgerStorageClassName());
    }

    private static String normalizeStorageClass(String storageClass) {
        return storageClass == null || storageClass.isBlank()
                ? StorageClassBindingRecord.BOOKKEEPER : storageClass;
    }

    private static Policies copyPolicies(Policies policies) {
        try {
            byte[] serialized = ObjectMapperFactory.getMapper().writer().writeValueAsBytes(policies);
            return ObjectMapperFactory.getMapper().reader().readValue(serialized, Policies.class);
        } catch (IOException error) {
            throw new IllegalStateException("failed to copy namespace policies", error);
        }
    }

    private static PersistencePolicies copyPersistence(PersistencePolicies persistence) {
        if (persistence == null) {
            return null;
        }
        try {
            byte[] serialized = ObjectMapperFactory.getMapper().writer().writeValueAsBytes(persistence);
            return ObjectMapperFactory.getMapper().reader().readValue(serialized, PersistencePolicies.class);
        } catch (IOException error) {
            throw new IllegalStateException("failed to copy persistence policies", error);
        }
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException
                || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private record PolicySnapshot(Policies policies, long policyVersion) {
    }

    private record PolicySelection(long policyVersion, String storageClass) {
    }

    private record TopicStorageClassUpdate(String current, String proposed) {
        private TopicStorageClassUpdate {
            current = normalizeStorageClass(current);
            proposed = normalizeStorageClass(proposed);
        }
    }

    private final class Permit implements NamespaceStorageClassPermit {
        private final NamespaceName namespace;
        private final TopicName topic;
        private final String selectedStorageClass;
        private final long namespacePolicyVersion;
        private final long deadlineNanos;
        private final ResourceLock<NamespaceStorageClassLockData> lock;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Permit(
                NamespaceName namespace,
                TopicName topic,
                String selectedStorageClass,
                long namespacePolicyVersion,
                long deadlineNanos,
                ResourceLock<NamespaceStorageClassLockData> lock) {
            this.namespace = namespace;
            this.topic = topic;
            this.selectedStorageClass = selectedStorageClass;
            this.namespacePolicyVersion = namespacePolicyVersion;
            this.deadlineNanos = deadlineNanos;
            this.lock = lock;
        }

        @Override
        public NamespaceName namespace() {
            return namespace;
        }

        @Override
        public String selectedStorageClass() {
            return selectedStorageClass;
        }

        @Override
        public long namespacePolicyVersion() {
            return namespacePolicyVersion;
        }

        @Override
        public CompletableFuture<Void> validateBeforeFactoryOpen(StorageClassOpenPermit bindingPermit) {
            java.util.Objects.requireNonNull(bindingPermit, "bindingPermit");
            if (closed.get() || lock.getLockExpiredFuture().isDone()) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("NEREUS_NAMESPACE_STORAGE_POLICY_LOCK_LOST"));
            }
            return withinDeadline(loadSelection(namespace, topic).thenCompose(selection -> {
                if (selection.policyVersion() != namespacePolicyVersion
                        || !selection.storageClass().equals(selectedStorageClass)
                        || !bindingPermit.storageClass().equals(selectedStorageClass)) {
                    IllegalStateException changed = new IllegalStateException(
                            "NEREUS_NAMESPACE_STORAGE_POLICY_CHANGED");
                    return bindingStore.abortStorageClassOpenClaim(bindingPermit)
                            .handle((ignored, abortError) -> {
                                if (abortError != null) {
                                    changed.addSuppressed(unwrap(abortError));
                                }
                                throw new CompletionException(changed);
                            });
                }
                return bindingStore.validateStorageClassOpenPermit(bindingPermit);
            }), deadlineNanos);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            if (!closed.compareAndSet(false, true)) {
                return CompletableFuture.completedFuture(null);
            }
            return lock.release();
        }
    }
}
