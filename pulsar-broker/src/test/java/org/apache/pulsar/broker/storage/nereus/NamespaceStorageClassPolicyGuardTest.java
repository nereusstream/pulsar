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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class NamespaceStorageClassPolicyGuardTest {
    private static final NamespaceName NAMESPACE = NamespaceName.get("tenant/ns");
    private static final TopicName TOPIC = TopicName.get("persistent://tenant/ns/topic");
    private static final TopicName SECOND_TOPIC = TopicName.get("persistent://tenant/ns/second-topic");

    private NamespaceResources namespaceResources;
    private TopicResources topicResources;
    private NereusStorageClassBindingStore bindingStore;
    private NereusBrokerCapabilityCoordinator capabilityCoordinator;
    private LockManager<NamespaceStorageClassLockData> lockManager;
    private ResourceLock<NamespaceStorageClassLockData> lock;
    private NamespaceStorageClassPolicyGuard guard;
    private AtomicReference<String> effectiveStorageClass;

    @BeforeMethod
    public void setUp() {
        CoordinationService coordinationService = mock(CoordinationService.class);
        namespaceResources = mock(NamespaceResources.class);
        topicResources = mock(TopicResources.class);
        bindingStore = mock(NereusStorageClassBindingStore.class);
        capabilityCoordinator = mock(NereusBrokerCapabilityCoordinator.class);
        lockManager = mock(LockManager.class);
        lock = mock(ResourceLock.class);
        when(coordinationService.getLockManager(NamespaceStorageClassLockData.class)).thenReturn(lockManager);
        when(lockManager.acquireLock(any(), any())).thenReturn(CompletableFuture.completedFuture(lock));
        when(lock.release()).thenReturn(CompletableFuture.completedFuture(null));
        when(lock.getLockExpiredFuture()).thenReturn(new CompletableFuture<>());
        when(lockManager.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        when(bindingStore.listNonDeletedBindings(NAMESPACE, 10, 2))
                .thenReturn(CompletableFuture.completedFuture(List.of()));
        when(topicResources.listPersistentTopicsAsync(NAMESPACE))
                .thenReturn(CompletableFuture.completedFuture(List.of()));
        when(capabilityCoordinator.requireClusterReady())
                .thenReturn(CompletableFuture.completedFuture(null));
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(ScheduledFuture.class));
        effectiveStorageClass = new AtomicReference<>(StorageClassBindingRecord.NEREUS);
        guard = new NamespaceStorageClassPolicyGuard(
                coordinationService,
                namespaceResources,
                topicResources,
                bindingStore,
                capabilityCoordinator,
                ignored -> CompletableFuture.completedFuture(effectiveStorageClass.get()),
                () -> "broker",
                scheduler,
                Duration.ofSeconds(5),
                10,
                2);
    }

    @Test
    public void retainsNamespaceLockThroughPostClaimValidation() {
        when(namespaceResources.refreshAndGetPoliciesWithVersion(NAMESPACE))
                .thenReturn(CompletableFuture.completedFuture(policy(StorageClassBindingRecord.BOOKKEEPER, 4)));
        StorageClassOpenPermit bindingPermit = new StorageClassOpenPermit(
                TOPIC.getPersistenceNamingEncoding(), StorageClassBindingRecord.NEREUS, 1, 2, true);
        when(bindingStore.validateStorageClassOpenPermit(bindingPermit))
                .thenReturn(CompletableFuture.completedFuture(null));

        NamespaceStorageClassPermit permit = guard.acquireFirstCreate(
                NAMESPACE, TOPIC, StorageClassBindingRecord.NEREUS).join();
        permit.validateBeforeFactoryOpen(bindingPermit).join();
        permit.closeAsync().join();

        verify(bindingStore).validateStorageClassOpenPermit(bindingPermit);
        verify(lock).release();
    }

    @Test
    public void serializesFirstCreateAcquisitionsWithinBroker() {
        when(namespaceResources.refreshAndGetPoliciesWithVersion(NAMESPACE))
                .thenAnswer(ignored -> CompletableFuture.completedFuture(
                        policy(StorageClassBindingRecord.BOOKKEEPER, 4)));

        NamespaceStorageClassPermit first = guard.acquireFirstCreate(
                NAMESPACE, TOPIC, StorageClassBindingRecord.NEREUS).join();
        CompletableFuture<NamespaceStorageClassPermit> secondFuture = guard.acquireFirstCreate(
                NAMESPACE, SECOND_TOPIC, StorageClassBindingRecord.NEREUS);

        assertThat(secondFuture).isNotDone();

        first.closeAsync().join();
        NamespaceStorageClassPermit second = secondFuture.join();
        second.closeAsync().join();

        verify(lockManager, times(2)).acquireLock(any(), any());
        verify(lock, times(2)).release();
    }

    @Test
    public void abortsNoStorageClaimWhenPolicyChangesBeforeFactoryOpen() {
        when(namespaceResources.refreshAndGetPoliciesWithVersion(NAMESPACE)).thenReturn(
                CompletableFuture.completedFuture(policy(StorageClassBindingRecord.BOOKKEEPER, 4)),
                CompletableFuture.completedFuture(policy(StorageClassBindingRecord.BOOKKEEPER, 5)));
        StorageClassOpenPermit bindingPermit = new StorageClassOpenPermit(
                TOPIC.getPersistenceNamingEncoding(), StorageClassBindingRecord.NEREUS, 1, 2, true);
        when(bindingStore.abortStorageClassOpenClaim(bindingPermit))
                .thenReturn(CompletableFuture.completedFuture(null));

        NamespaceStorageClassPermit permit = guard.acquireFirstCreate(
                NAMESPACE, TOPIC, StorageClassBindingRecord.NEREUS).join();

        assertThatThrownBy(() -> permit.validateBeforeFactoryOpen(bindingPermit).join())
                .hasRootCauseMessage("NEREUS_NAMESPACE_STORAGE_POLICY_CHANGED");
        verify(bindingStore).abortStorageClassOpenClaim(bindingPermit);
        verify(bindingStore, never()).validateStorageClassOpenPermit(any());
    }

    @Test
    public void rejectsStorageClassTransitionWhenNamespaceHasTopics() {
        when(namespaceResources.refreshAndGetPoliciesWithVersion(NAMESPACE)).thenReturn(
                CompletableFuture.completedFuture(policy(StorageClassBindingRecord.BOOKKEEPER, 4)),
                CompletableFuture.completedFuture(policy(StorageClassBindingRecord.BOOKKEEPER, 4)));
        when(topicResources.listPersistentTopicsAsync(NAMESPACE))
                .thenReturn(CompletableFuture.completedFuture(List.of(TOPIC.toString())));

        assertThatThrownBy(() -> guard.updateNamespacePersistence(
                NAMESPACE, persistence(StorageClassBindingRecord.NEREUS)).join())
                .hasRootCauseMessage("NEREUS_NAMESPACE_STORAGE_POLICY_REQUIRES_EMPTY_NAMESPACE");

        verify(capabilityCoordinator).requireClusterReady();
        verify(namespaceResources, never()).setPoliciesWithVersion(any(), any(), anyLong());
        verify(lock).release();
    }

    @Test
    public void changesEmptyNamespaceWithVersionedWriteAndPostScan() {
        when(namespaceResources.refreshAndGetPoliciesWithVersion(NAMESPACE)).thenReturn(
                CompletableFuture.completedFuture(policy(StorageClassBindingRecord.BOOKKEEPER, 4)),
                CompletableFuture.completedFuture(policy(StorageClassBindingRecord.BOOKKEEPER, 4)),
                CompletableFuture.completedFuture(policy(StorageClassBindingRecord.NEREUS, 5)));
        when(namespaceResources.setPoliciesWithVersion(eq(NAMESPACE), any(), eq(4L)))
                .thenReturn(CompletableFuture.completedFuture(null));

        guard.updateNamespacePersistence(NAMESPACE, persistence(StorageClassBindingRecord.NEREUS)).join();

        verify(namespaceResources).setPoliciesWithVersion(eq(NAMESPACE), any(), eq(4L));
        verify(topicResources, times(2)).listPersistentTopicsAsync(NAMESPACE);
        verify(bindingStore, times(2)).listNonDeletedBindings(NAMESPACE, 10, 2);
        verify(lock).release();
    }

    @Test
    public void releasesNamespaceLockWhenEmptyNamespaceScanTimesOut() {
        when(namespaceResources.refreshAndGetPoliciesWithVersion(NAMESPACE)).thenReturn(
                CompletableFuture.completedFuture(policy(StorageClassBindingRecord.BOOKKEEPER, 4)),
                CompletableFuture.completedFuture(policy(StorageClassBindingRecord.BOOKKEEPER, 4)));
        when(topicResources.listPersistentTopicsAsync(NAMESPACE)).thenReturn(new CompletableFuture<>());
        NamespaceStorageClassPolicyGuard shortDeadlineGuard = new NamespaceStorageClassPolicyGuard(
                mockCoordinationService(),
                namespaceResources,
                topicResources,
                bindingStore,
                capabilityCoordinator,
                ignored -> CompletableFuture.completedFuture(StorageClassBindingRecord.BOOKKEEPER),
                () -> "broker",
                mockScheduler(),
                Duration.ofMillis(25),
                10,
                2);

        assertThatThrownBy(() -> shortDeadlineGuard.updateNamespacePersistence(
                NAMESPACE, persistence(StorageClassBindingRecord.NEREUS)).join())
                .hasRootCauseInstanceOf(java.util.concurrent.TimeoutException.class);
        verify(lock).release();
    }

    @Test
    public void rejectsTopicStorageClassChangeBeforePolicyMutationWhenBindingExists() {
        AtomicBoolean mutated = new AtomicBoolean();
        when(bindingStore.validateStorageClassPolicyUpdate(
                TOPIC.getPersistenceNamingEncoding(), StorageClassBindingRecord.BOOKKEEPER))
                .thenReturn(CompletableFuture.failedFuture(
                        new IllegalStateException("storage-class migration is required")));

        assertThatThrownBy(() -> guard.updateTopicPersistence(
                TOPIC,
                () -> CompletableFuture.completedFuture(StorageClassBindingRecord.BOOKKEEPER),
                () -> {
                    mutated.set(true);
                    return CompletableFuture.completedFuture(null);
                }).join()).hasRootCauseMessage("storage-class migration is required");

        org.assertj.core.api.Assertions.assertThat(mutated).isFalse();
        verify(lock).release();
    }

    @Test
    public void serializesTopicPolicyChangeAndVerifiesAuthoritativeReadback() {
        effectiveStorageClass.set(StorageClassBindingRecord.BOOKKEEPER);
        when(bindingStore.validateStorageClassPolicyUpdate(
                TOPIC.getPersistenceNamingEncoding(), StorageClassBindingRecord.NEREUS))
                .thenReturn(CompletableFuture.completedFuture(null));

        guard.updateTopicPersistence(
                TOPIC,
                () -> CompletableFuture.completedFuture(StorageClassBindingRecord.NEREUS),
                () -> {
                    effectiveStorageClass.set(StorageClassBindingRecord.NEREUS);
                    return CompletableFuture.completedFuture(null);
                }).join();

        verify(capabilityCoordinator).requireClusterReady();
        verify(bindingStore).validateStorageClassPolicyUpdate(
                TOPIC.getPersistenceNamingEncoding(), StorageClassBindingRecord.NEREUS);
        verify(lock).release();
    }

    @Test
    public void skipsMigrationChecksWhenTopicEffectiveClassDoesNotChange() {
        AtomicBoolean mutated = new AtomicBoolean();

        guard.updateTopicPersistence(
                TOPIC,
                () -> CompletableFuture.completedFuture(StorageClassBindingRecord.NEREUS),
                () -> {
                    mutated.set(true);
                    return CompletableFuture.completedFuture(null);
                }).join();

        org.assertj.core.api.Assertions.assertThat(mutated).isTrue();
        verify(capabilityCoordinator, never()).requireClusterReady();
        verify(bindingStore, never()).validateStorageClassPolicyUpdate(any(), any());
        verify(lock).release();
    }

    @Test
    public void retriesBusyNamespaceLockWithinDeadline() {
        when(lockManager.acquireLock(any(), any())).thenReturn(
                CompletableFuture.failedFuture(new org.apache.pulsar.metadata.api.MetadataStoreException
                        .LockBusyException()),
                CompletableFuture.completedFuture(lock));
        when(namespaceResources.refreshAndGetPoliciesWithVersion(NAMESPACE))
                .thenReturn(CompletableFuture.completedFuture(policy(StorageClassBindingRecord.BOOKKEEPER, 4)));
        ScheduledExecutorService retryScheduler = Executors.newSingleThreadScheduledExecutor();
        NamespaceStorageClassPolicyGuard retryingGuard = new NamespaceStorageClassPolicyGuard(
                mockCoordinationService(),
                namespaceResources,
                topicResources,
                bindingStore,
                capabilityCoordinator,
                ignored -> CompletableFuture.completedFuture(StorageClassBindingRecord.NEREUS),
                () -> "broker",
                retryScheduler,
                Duration.ofSeconds(1),
                10,
                2);
        try {
            NamespaceStorageClassPermit permit = retryingGuard.acquireFirstCreate(
                    NAMESPACE, TOPIC, StorageClassBindingRecord.NEREUS).join();
            permit.closeAsync().join();
            verify(lockManager, times(2)).acquireLock(any(), any());
        } finally {
            retryScheduler.shutdownNow();
        }
    }

    @Test
    public void closesOwnedLockManagerOnceAndRejectsNewAcquisition() {
        guard.asyncClose().join();
        guard.asyncClose().join();

        assertThatThrownBy(() -> guard.acquireFirstCreate(
                NAMESPACE, TOPIC, StorageClassBindingRecord.NEREUS).join())
                .hasRootCauseMessage("namespace storage-class policy guard is closed");
        verify(lockManager).asyncClose();
    }

    private CoordinationService mockCoordinationService() {
        CoordinationService coordinationService = mock(CoordinationService.class);
        when(coordinationService.getLockManager(NamespaceStorageClassLockData.class)).thenReturn(lockManager);
        return coordinationService;
    }

    private static ScheduledExecutorService mockScheduler() {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(ScheduledFuture.class));
        return scheduler;
    }

    private static Optional<CacheGetResult<Policies>> policy(String storageClass, long version) {
        Policies policies = new Policies();
        policies.persistence = persistence(storageClass);
        return Optional.of(new CacheGetResult<>(policies, stat(version)));
    }

    private static PersistencePolicies persistence(String storageClass) {
        return new PersistencePolicies(1, 1, 1, 0, storageClass);
    }

    private static Stat stat(long version) {
        return new Stat("/admin/policies/tenant/ns", version, 0, 0, false, true);
    }
}
