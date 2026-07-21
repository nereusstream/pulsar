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

import com.nereusstream.api.StorageProfile;
import com.nereusstream.managedledger.NereusDurableStorageState;
import com.nereusstream.managedledger.NereusManagedLedgerFactory;
import com.nereusstream.managedledger.NereusStorageStateSnapshot;
import com.nereusstream.managedledger.integration.NereusCreationGuard;
import com.nereusstream.managedledger.integration.NereusCreationPermit;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerNotFoundException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/** Single-key Nereus storage-class claim used before projection publication. */
public final class NereusStorageClassBindingStore implements AutoCloseable {
    private static final String BOOKKEEPER_MANAGED_LEDGER_ROOT = "/managed-ledgers/";

    private final MetadataStoreExtended metadataStore;
    private final ManagedLedgerFactory bookkeeperFactory;
    private final Duration timeout;
    private final Supplier<CompletableFuture<Void>> nereusCapabilityCheck;
    private final Function<StorageProfile, CompletableFuture<Void>> storageProfileCapabilityCheck;
    private final Function<StorageProfile, CompletableFuture<Void>> writableStorageProfileCapabilityCheck;
    private final StorageClassBindingKeyspace keyspace = new StorageClassBindingKeyspace();
    private final StorageClassBindingCodec codec = new StorageClassBindingCodec();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<NereusManagedLedgerFactory> nereusFactory = new AtomicReference<>();

    public NereusStorageClassBindingStore(
            MetadataStoreExtended metadataStore,
            ManagedLedgerFactory bookkeeperFactory,
            Duration timeout) {
        this(
                metadataStore,
                bookkeeperFactory,
                timeout,
                () -> CompletableFuture.completedFuture(null),
                ignored -> CompletableFuture.completedFuture(null),
                ignored -> CompletableFuture.completedFuture(null));
    }

    public NereusStorageClassBindingStore(
            MetadataStoreExtended metadataStore,
            ManagedLedgerFactory bookkeeperFactory,
            Duration timeout,
            Supplier<CompletableFuture<Void>> nereusCapabilityCheck) {
        this(
                metadataStore,
                bookkeeperFactory,
                timeout,
                nereusCapabilityCheck,
                ignored -> CompletableFuture.completedFuture(null),
                ignored -> CompletableFuture.completedFuture(null));
    }

    public NereusStorageClassBindingStore(
            MetadataStoreExtended metadataStore,
            ManagedLedgerFactory bookkeeperFactory,
            Duration timeout,
            Supplier<CompletableFuture<Void>> nereusCapabilityCheck,
            Function<StorageProfile, CompletableFuture<Void>> storageProfileCapabilityCheck) {
        this(
                metadataStore,
                bookkeeperFactory,
                timeout,
                nereusCapabilityCheck,
                storageProfileCapabilityCheck,
                storageProfileCapabilityCheck);
    }

    public NereusStorageClassBindingStore(
            MetadataStoreExtended metadataStore,
            ManagedLedgerFactory bookkeeperFactory,
            Duration timeout,
            Supplier<CompletableFuture<Void>> nereusCapabilityCheck,
            Function<StorageProfile, CompletableFuture<Void>> storageProfileCapabilityCheck,
            Function<StorageProfile, CompletableFuture<Void>> writableStorageProfileCapabilityCheck) {
        this.metadataStore = java.util.Objects.requireNonNull(metadataStore, "metadataStore");
        this.bookkeeperFactory = java.util.Objects.requireNonNull(bookkeeperFactory, "bookkeeperFactory");
        this.timeout = java.util.Objects.requireNonNull(timeout, "timeout");
        this.nereusCapabilityCheck = java.util.Objects.requireNonNull(
                nereusCapabilityCheck, "nereusCapabilityCheck");
        this.storageProfileCapabilityCheck = java.util.Objects.requireNonNull(
                storageProfileCapabilityCheck, "storageProfileCapabilityCheck");
        this.writableStorageProfileCapabilityCheck = java.util.Objects.requireNonNull(
                writableStorageProfileCapabilityCheck, "writableStorageProfileCapabilityCheck");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }

    public NereusCreationGuard creationGuard() {
        return this::acquire;
    }

    public void attachNereusFactory(NereusManagedLedgerFactory factory) {
        java.util.Objects.requireNonNull(factory, "factory");
        if (!nereusFactory.compareAndSet(null, factory)) {
            throw new IllegalStateException("Nereus managed-ledger factory is already attached");
        }
    }

    public CompletableFuture<Optional<StorageClassBindingRecord>> getBinding(String persistenceName) {
        final String path;
        try {
            requireOpen();
            path = bindingPath(persistenceName);
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        return metadataStore.sync(path)
                .thenCompose(ignored -> metadataStore.get(path))
                .thenApply(current -> current.map(result -> decode(result, persistenceName)))
                .orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    public CompletableFuture<StorageClassOpenPermit> prepareStorageClassOpen(
            String persistenceName,
            String selectedStorageClass,
            boolean createIfMissing) {
        NereusManagedLedgerFactory factory;
        try {
            factory = requireAttachedFactory();
            requireOpen();
            requireStorageClass(selectedStorageClass);
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        String path;
        try {
            path = bindingPath(persistenceName);
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        CompletableFuture<Void> capabilityReady = StorageClassBindingRecord.NEREUS.equals(selectedStorageClass)
                && createIfMissing ? requireNereusCapability() : CompletableFuture.completedFuture(null);
        return capabilityReady.thenCompose(ignored -> prepareStorageClassOpen(
                path, persistenceName, selectedStorageClass, createIfMissing, factory, 0))
                .orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    public CompletableFuture<Void> completeStorageClassOpen(StorageClassOpenPermit permit) {
        java.util.Objects.requireNonNull(permit, "permit");
        try {
            requireOpen();
            requireStorageClass(permit.storageClass());
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        String path = bindingPath(permit.persistenceName());
        return completeStorageClassOpen(path, permit, 0)
                .orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    private CompletableFuture<Void> completeStorageClassOpen(
            String path, StorageClassOpenPermit permit, int conflictCount) {
        if (conflictCount > 8) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("storage-class binding CAS retry limit exceeded"));
        }
        return metadataStore.sync(path).thenCompose(ignored -> metadataStore.get(path)).thenCompose(current -> {
            GetResult result = current.orElseThrow(() ->
                    new IllegalStateException("storage-class binding disappeared before open completion"));
            StorageClassBindingRecord binding = decode(result, permit.persistenceName());
            requirePermitGeneration(binding, permit);
            if (binding.state() == StorageClassBindingState.ACTIVE) {
                return CompletableFuture.completedFuture(null);
            }
            if (!permit.activationRequired() || binding.state() != StorageClassBindingState.CLAIMED) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("storage-class binding cannot complete open"));
            }
            requirePermitMetadataVersion(binding, permit);
            StorageClassBindingRecord active = binding.transitionTo(StorageClassBindingState.ACTIVE);
            return metadataStore.put(
                    path, codec.encode(active), Optional.of(binding.metadataVersion())).thenApply(stat -> (Void) null);
        }).exceptionallyCompose(error -> {
            if (!isBadVersion(error)) {
                return CompletableFuture.failedFuture(unwrap(error));
            }
            return completeStorageClassOpen(path, permit, conflictCount + 1);
        });
    }

    public CompletableFuture<Void> validateStorageClassOpenPermit(StorageClassOpenPermit permit) {
        java.util.Objects.requireNonNull(permit, "permit");
        try {
            requireOpen();
            requireStorageClass(permit.storageClass());
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        String path = bindingPath(permit.persistenceName());
        return metadataStore.sync(path).thenCompose(ignored -> metadataStore.get(path)).thenAccept(current -> {
            StorageClassBindingRecord binding = decode(current.orElseThrow(() ->
                    new IllegalStateException("storage-class binding disappeared before factory open")),
                    permit.persistenceName());
            requirePermitGeneration(binding, permit);
            if (binding.state() != StorageClassBindingState.CLAIMED
                    && binding.state() != StorageClassBindingState.ACTIVE) {
                throw new IllegalStateException("storage-class binding is not openable");
            }
            if (binding.state() == StorageClassBindingState.CLAIMED) {
                requirePermitMetadataVersion(binding, permit);
            }
        }).orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    public CompletableFuture<Void> abortStorageClassOpenClaim(StorageClassOpenPermit permit) {
        java.util.Objects.requireNonNull(permit, "permit");
        if (!permit.activationRequired()) {
            return CompletableFuture.completedFuture(null);
        }
        final NereusManagedLedgerFactory factory;
        try {
            factory = requireAttachedFactory();
            requireOpen();
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        String path = bindingPath(permit.persistenceName());
        return metadataStore.sync(path).thenCompose(ignored -> metadataStore.get(path)).thenCompose(current -> {
            StorageClassBindingRecord binding = decode(current.orElseThrow(() ->
                    new IllegalStateException("storage-class binding disappeared before claim abort")),
                    permit.persistenceName());
            requirePermitGeneration(binding, permit);
            requirePermitMetadataVersion(binding, permit);
            if (binding.state() != StorageClassBindingState.CLAIMED) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("storage-class binding claim is no longer abortable"));
            }
            return bookkeeperDurableStateExists(permit.persistenceName()).thenCombine(
                    factory.inspectStorageState(permit.persistenceName()), StorageObservations::new)
                    .thenCompose(observations -> {
                        if (observations.bookkeeperExists()
                                || observations.nereus().state() != NereusDurableStorageState.MISSING) {
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException("storage-class binding claim already published storage"));
                        }
                        StorageClassBindingRecord deleting =
                                binding.transitionTo(StorageClassBindingState.DELETING);
                        return transitionBinding(path, binding, deleting)
                                .thenCompose(updated -> transitionBinding(
                                        path,
                                        updated,
                                        updated.transitionTo(StorageClassBindingState.DELETED)))
                                .thenApply(updated -> (Void) null);
                    });
        }).orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    public CompletableFuture<Void> validateStorageClassPolicyUpdate(
            String persistenceName, String targetStorageClass) {
        final NereusManagedLedgerFactory factory;
        final String path;
        try {
            factory = requireAttachedFactory();
            requireOpen();
            requireStorageClass(targetStorageClass);
            path = bindingPath(persistenceName);
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        return metadataStore.sync(path).thenCompose(ignored -> metadataStore.get(path)).thenCompose(current -> {
            CompletableFuture<Boolean> bookkeeperState = bookkeeperDurableStateExists(persistenceName);
            CompletableFuture<NereusStorageStateSnapshot> nereusState = factory.inspectStorageState(persistenceName);
            return bookkeeperState.thenCombine(nereusState, StorageObservations::new).thenAccept(observations -> {
                if (current.isEmpty()) {
                    if (observations.bookkeeperExists()) {
                        if (!StorageClassBindingRecord.BOOKKEEPER.equals(targetStorageClass)) {
                            throw new IllegalStateException("storage-class migration is required");
                        }
                        return;
                    }
                    if (observations.nereus().state() != NereusDurableStorageState.MISSING) {
                        throw new IllegalStateException(
                                "durable storage exists without a storage-class binding");
                    }
                    return;
                }
                StorageClassBindingRecord binding = decode(current.orElseThrow(), persistenceName);
                if (binding.state() != StorageClassBindingState.DELETED) {
                    if (!binding.storageClass().equals(targetStorageClass)) {
                        throw new IllegalStateException("storage-class migration is required");
                    }
                    return;
                }
                if (observations.bookkeeperExists()) {
                    throw new IllegalStateException("deleted binding still has BookKeeper storage");
                }
                requireTerminalNereus(observations.nereus(), binding);
            });
        }).orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    public CompletableFuture<List<StorageClassBindingRecord>> listNonDeletedBindings(
            NamespaceName namespace, int maxEntries, int maxPendingOperations) {
        java.util.Objects.requireNonNull(namespace, "namespace");
        if (maxEntries <= 0 || maxPendingOperations <= 0) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("binding scan limits must be positive"));
        }
        final String root;
        try {
            requireOpen();
            String namespaceRoot = keyspace.namespaceRoot(namespace);
            root = namespaceRoot.substring(0, namespaceRoot.length() - 1);
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        List<StorageClassBindingRecord> bindings = new ArrayList<>();
        return metadataStore.sync(root)
                .thenCompose(ignored -> metadataStore.getChildren(root)).thenCompose(children -> {
            if (children.size() > maxEntries) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Nereus namespace binding scan limit exceeded"));
            }
            return readBindingBatch(namespace, root, children, 0, maxPendingOperations, bindings);
        }).thenApply(ignored -> List.copyOf(bindings))
                .orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    public CompletableFuture<Optional<StorageClassDeletePermit>> prepareStorageClassDelete(
            String persistenceName) {
        NereusManagedLedgerFactory factory;
        try {
            factory = requireAttachedFactory();
            requireOpen();
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        String path;
        try {
            path = bindingPath(persistenceName);
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        return prepareStorageClassDelete(path, persistenceName, factory, 0)
                .orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    public CompletableFuture<Void> completeStorageClassDelete(StorageClassDeletePermit permit) {
        java.util.Objects.requireNonNull(permit, "permit");
        NereusManagedLedgerFactory factory;
        try {
            factory = requireAttachedFactory();
            requireOpen();
            requireStorageClass(permit.storageClass());
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        String path = bindingPath(permit.persistenceName());
        return completeStorageClassDelete(path, permit, factory, 0)
                .orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private CompletableFuture<NereusCreationPermit> acquire(String persistenceName) {
        if (persistenceName == null || persistenceName.isBlank()) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("persistenceName cannot be blank"));
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Nereus binding store is closed"));
        }
        String path = bindingPath(persistenceName);
        StorageClassBindingRecord candidate = StorageClassBindingRecord.claimed(
                persistenceName,
                StorageClassBindingRecord.NEREUS,
                1,
                System.currentTimeMillis());
        CompletableFuture<NereusCreationPermit> operation = metadataStore.sync(path)
                .thenCompose(ignored -> metadataStore.get(path)).thenCompose(existing -> {
            if (existing.isPresent()) {
                StorageClassBindingRecord binding = decode(existing.orElseThrow(), persistenceName);
                return CompletableFuture.completedFuture(permit(path, binding));
            }
            return bookkeeperDurableStateExists(persistenceName).thenCompose(bookkeeperExists -> {
                if (bookkeeperExists) {
                    return CompletableFuture.failedFuture(new IllegalStateException(
                            "existing BookKeeper storage cannot be opened as Nereus"));
                }
                return metadataStore.put(path, codec.encode(candidate), Optional.of(-1L))
                    .thenApply(stat -> permit(path, candidate.withMetadataVersion(stat.getVersion())))
                    .exceptionallyCompose(failure -> {
                        if (!isBadVersion(failure)) {
                            return CompletableFuture.failedFuture(unwrap(failure));
                        }
                        return metadataStore.get(path).thenApply(raced -> {
                            GetResult result = raced.orElseThrow(() ->
                                    new IllegalStateException("binding disappeared after create conflict"));
                            return permit(path, decode(result, persistenceName));
                        });
                    });
            });
        });
        return operation.orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    private NereusCreationPermit permit(String path, StorageClassBindingRecord binding) {
        requireNereusOpenState(binding);
        return new NereusCreationPermit() {
            @Override
            public String persistenceName() {
                return binding.persistenceName();
            }

            @Override
            public long bindingGeneration() {
                return binding.bindingGeneration();
            }

            @Override
            public CompletableFuture<Void> validateStorageProfileBeforeCreate(StorageProfile profile) {
                if (closed.get()) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Nereus binding store is closed"));
                }
                try {
                    return java.util.Objects.requireNonNull(
                            storageProfileCapabilityCheck.apply(profile),
                            "storageProfileCapabilityCheck result");
                } catch (Throwable error) {
                    return CompletableFuture.failedFuture(error);
                }
            }

            @Override
            public CompletableFuture<Void> validateStorageProfileBeforeWritableOpen(
                    StorageProfile profile) {
                if (closed.get()) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Nereus binding store is closed"));
                }
                try {
                    return java.util.Objects.requireNonNull(
                            writableStorageProfileCapabilityCheck.apply(profile),
                            "writableStorageProfileCapabilityCheck result");
                } catch (Throwable error) {
                    return CompletableFuture.failedFuture(error);
                }
            }

            @Override
            public CompletableFuture<Void> validateBeforeProjectionPublish() {
                if (closed.get()) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Nereus binding store is closed"));
                }
                return metadataStore.sync(path).thenCompose(ignored -> metadataStore.get(path)).thenCombine(
                        bookkeeperDurableStateExists(binding.persistenceName()),
                        (current, bookkeeperExists) -> {
                    if (bookkeeperExists) {
                        throw new IllegalStateException(
                                "BookKeeper storage appeared before Nereus projection publish");
                    }
                    GetResult result = current.orElseThrow(() ->
                            new IllegalStateException("Nereus binding disappeared before projection publish"));
                    StorageClassBindingRecord actual = decode(result, binding.persistenceName());
                    if (!actual.equals(binding)) {
                        throw new IllegalStateException("Nereus binding changed before projection publish");
                    }
                    return (Void) null;
                }).orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
            }
        };
    }

    private StorageClassBindingRecord decode(GetResult result, String expectedName) {
        StorageClassBindingRecord binding = codec.decode(result.getValue(), result.getStat().getVersion());
        if (!binding.persistenceName().equals(expectedName)) {
            throw new IllegalStateException("Nereus storage-class binding hash collision");
        }
        return binding;
    }

    private CompletableFuture<StorageClassOpenPermit> prepareStorageClassOpen(
            String path,
            String persistenceName,
            String selectedStorageClass,
            boolean createIfMissing,
            NereusManagedLedgerFactory factory,
            int conflictCount) {
        if (conflictCount > 8) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("storage-class binding CAS retry limit exceeded"));
        }
        return metadataStore.sync(path).thenCompose(ignored -> metadataStore.get(path)).thenCompose(current -> {
            CompletableFuture<Boolean> bookkeeperState = bookkeeperDurableStateExists(persistenceName);
            CompletableFuture<NereusStorageStateSnapshot> nereusState = factory.inspectStorageState(persistenceName);
            return bookkeeperState.thenCombine(nereusState, StorageObservations::new).thenCompose(observations -> {
                if (current.isEmpty()) {
                    return prepareMissingBinding(
                            path,
                            persistenceName,
                            selectedStorageClass,
                            createIfMissing,
                            factory,
                            conflictCount,
                            observations);
                }
                StorageClassBindingRecord binding = decode(current.orElseThrow(), persistenceName);
                return prepareExistingBinding(
                        path,
                        binding,
                        selectedStorageClass,
                        createIfMissing,
                        factory,
                        conflictCount,
                        observations);
            });
        });
    }

    private CompletableFuture<Optional<StorageClassDeletePermit>> prepareStorageClassDelete(
            String path,
            String persistenceName,
            NereusManagedLedgerFactory factory,
            int conflictCount) {
        if (conflictCount > 8) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("storage-class binding CAS retry limit exceeded"));
        }
        return metadataStore.sync(path).thenCompose(ignored -> metadataStore.get(path)).thenCompose(current -> {
            CompletableFuture<Boolean> bookkeeperState = bookkeeperDurableStateExists(persistenceName);
            CompletableFuture<NereusStorageStateSnapshot> nereusState = factory.inspectStorageState(persistenceName);
            return bookkeeperState.thenCombine(nereusState, StorageObservations::new).thenCompose(observations -> {
                if (current.isEmpty()) {
                    return prepareMissingBindingDelete(
                            path, persistenceName, factory, conflictCount, observations);
                }
                StorageClassBindingRecord binding = decode(current.orElseThrow(), persistenceName);
                return prepareExistingBindingDelete(path, binding, factory, conflictCount, observations);
            });
        });
    }

    private CompletableFuture<Void> readBindingBatch(
            NamespaceName namespace,
            String root,
            List<String> children,
            int offset,
            int maxPendingOperations,
            List<StorageClassBindingRecord> bindings) {
        if (offset >= children.size()) {
            return CompletableFuture.completedFuture(null);
        }
        int end = Math.min(children.size(), offset + maxPendingOperations);
        List<CompletableFuture<StorageClassBindingRecord>> reads = new ArrayList<>(end - offset);
        for (int index = offset; index < end; index++) {
            String path = root + "/" + children.get(index);
            reads.add(metadataStore.get(path).thenApply(current -> {
                GetResult result = current.orElseThrow(() ->
                        new IllegalStateException("Nereus namespace binding disappeared during scan"));
                StorageClassBindingRecord binding = codec.decode(
                        result.getValue(), result.getStat().getVersion());
                if (!keyspace.bindingKey(namespace, binding.persistenceName()).equals(path)) {
                    throw new IllegalStateException("Nereus namespace binding key mismatch");
                }
                return binding;
            }));
        }
        return CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new)).thenCompose(ignored -> {
            reads.stream().map(CompletableFuture::join)
                    .filter(binding -> binding.state() != StorageClassBindingState.DELETED)
                    .forEach(bindings::add);
            return readBindingBatch(namespace, root, children, end, maxPendingOperations, bindings);
        });
    }

    private CompletableFuture<Optional<StorageClassDeletePermit>> prepareMissingBindingDelete(
            String path,
            String persistenceName,
            NereusManagedLedgerFactory factory,
            int conflictCount,
            StorageObservations observations) {
        if (observations.bookkeeperExists()
                && observations.nereus().state() == NereusDurableStorageState.MISSING) {
            StorageClassBindingRecord adopted = StorageClassBindingRecord.claimed(
                    persistenceName,
                    StorageClassBindingRecord.BOOKKEEPER,
                    1,
                    System.currentTimeMillis()).transitionTo(StorageClassBindingState.ACTIVE);
            return createBinding(path, adopted)
                    .thenCompose(created -> transitionBinding(
                            path, created, created.transitionTo(StorageClassBindingState.DELETING)))
                    .thenApply(deleting -> Optional.of(deletePermit(deleting)))
                    .exceptionallyCompose(error -> retryDeleteAfterConflict(
                            error, path, persistenceName, factory, conflictCount));
        }
        if (!observations.bookkeeperExists()
                && observations.nereus().state() == NereusDurableStorageState.MISSING) {
            return CompletableFuture.failedFuture(new ManagedLedgerNotFoundException("Ledger not found"));
        }
        return CompletableFuture.failedFuture(
                new IllegalStateException("durable storage exists without a storage-class binding"));
    }

    private CompletableFuture<Optional<StorageClassDeletePermit>> prepareExistingBindingDelete(
            String path,
            StorageClassBindingRecord binding,
            NereusManagedLedgerFactory factory,
            int conflictCount,
            StorageObservations observations) {
        return switch (binding.state()) {
            case DELETED -> validateDeletedBinding(binding, observations);
            case DELETING -> CompletableFuture.completedFuture(Optional.of(deletePermit(binding)));
            case CLAIMED -> prepareClaimedDelete(path, binding, factory, conflictCount, observations);
            case ACTIVE -> prepareActiveDelete(path, binding, factory, conflictCount, observations);
        };
    }

    private CompletableFuture<Optional<StorageClassDeletePermit>> validateDeletedBinding(
            StorageClassBindingRecord binding,
            StorageObservations observations) {
        if (observations.bookkeeperExists()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("deleted binding still has BookKeeper storage"));
        }
        requireTerminalNereus(observations.nereus(), binding);
        return CompletableFuture.completedFuture(Optional.empty());
    }

    private CompletableFuture<Optional<StorageClassDeletePermit>> prepareClaimedDelete(
            String path,
            StorageClassBindingRecord binding,
            NereusManagedLedgerFactory factory,
            int conflictCount,
            StorageObservations observations) {
        boolean storagePublished;
        if (StorageClassBindingRecord.BOOKKEEPER.equals(binding.storageClass())) {
            requireNoLiveNereus(observations.nereus(), binding.bindingGeneration());
            storagePublished = observations.bookkeeperExists();
        } else {
            if (observations.bookkeeperExists()) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("both storage classes contain live state"));
            }
            storagePublished = isPublishedNereusGeneration(observations.nereus(), binding.bindingGeneration());
        }
        StorageClassBindingRecord deleting = binding.transitionTo(StorageClassBindingState.DELETING);
        return transitionBinding(path, binding, deleting).thenCompose(updated -> {
            if (storagePublished) {
                return CompletableFuture.completedFuture(Optional.of(deletePermit(updated)));
            }
            return transitionBinding(
                    path, updated, updated.transitionTo(StorageClassBindingState.DELETED))
                    .thenApply(ignored -> Optional.<StorageClassDeletePermit>empty());
        }).exceptionallyCompose(error -> retryDeleteAfterConflict(
                error, path, binding.persistenceName(), factory, conflictCount));
    }

    private CompletableFuture<Optional<StorageClassDeletePermit>> prepareActiveDelete(
            String path,
            StorageClassBindingRecord binding,
            NereusManagedLedgerFactory factory,
            int conflictCount,
            StorageObservations observations) {
        if (StorageClassBindingRecord.BOOKKEEPER.equals(binding.storageClass())) {
            requireNoLiveNereus(observations.nereus(), binding.bindingGeneration());
            if (!observations.bookkeeperExists()) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("active BookKeeper storage is missing"));
            }
        } else {
            if (observations.bookkeeperExists()) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("both storage classes contain live state"));
            }
            NereusDurableStorageState state = observations.nereus().state();
            if (state != NereusDurableStorageState.ACTIVE
                    && state != NereusDurableStorageState.SEALED
                    && state != NereusDurableStorageState.DELETING) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("active Nereus storage is missing"));
            }
            requireNereusGeneration(observations.nereus(), binding.bindingGeneration());
        }
        StorageClassBindingRecord deleting = binding.transitionTo(StorageClassBindingState.DELETING);
        return transitionBinding(path, binding, deleting)
                .thenApply(updated -> Optional.of(deletePermit(updated)))
                .exceptionallyCompose(error -> retryDeleteAfterConflict(
                        error, path, binding.persistenceName(), factory, conflictCount));
    }

    private CompletableFuture<Void> completeStorageClassDelete(
            String path,
            StorageClassDeletePermit permit,
            NereusManagedLedgerFactory factory,
            int conflictCount) {
        if (conflictCount > 8) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("storage-class binding CAS retry limit exceeded"));
        }
        return metadataStore.sync(path).thenCompose(ignored -> metadataStore.get(path)).thenCompose(current -> {
            GetResult result = current.orElseThrow(() ->
                    new IllegalStateException("storage-class binding disappeared before delete completion"));
            StorageClassBindingRecord binding = decode(result, permit.persistenceName());
            if (!binding.storageClass().equals(permit.storageClass())
                    || binding.bindingGeneration() != permit.bindingGeneration()) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("storage-class delete permit is stale"));
            }
            if (binding.state() == StorageClassBindingState.DELETED) {
                return CompletableFuture.completedFuture(null);
            }
            if (binding.state() != StorageClassBindingState.DELETING) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("storage-class binding cannot complete delete"));
            }
            CompletableFuture<Boolean> bookkeeperState = bookkeeperDurableStateExists(permit.persistenceName());
            CompletableFuture<NereusStorageStateSnapshot> nereusState =
                    factory.inspectStorageState(permit.persistenceName());
            return bookkeeperState.thenCombine(nereusState, StorageObservations::new).thenCompose(observations -> {
                requireBoundStorageDeleted(binding, observations);
                StorageClassBindingRecord deleted = binding.transitionTo(StorageClassBindingState.DELETED);
                return transitionBinding(path, binding, deleted).thenApply(ignored -> (Void) null);
            });
        }).exceptionallyCompose(error -> {
            if (!isBadVersion(error)) {
                return CompletableFuture.failedFuture(unwrap(error));
            }
            return completeStorageClassDelete(path, permit, factory, conflictCount + 1);
        });
    }

    private CompletableFuture<Optional<StorageClassDeletePermit>> retryDeleteAfterConflict(
            Throwable error,
            String path,
            String persistenceName,
            NereusManagedLedgerFactory factory,
            int conflictCount) {
        if (!isBadVersion(error)) {
            return CompletableFuture.failedFuture(unwrap(error));
        }
        return prepareStorageClassDelete(path, persistenceName, factory, conflictCount + 1);
    }

    private CompletableFuture<StorageClassOpenPermit> prepareMissingBinding(
            String path,
            String persistenceName,
            String selectedStorageClass,
            boolean createIfMissing,
            NereusManagedLedgerFactory factory,
            int conflictCount,
            StorageObservations observations) {
        if (observations.bookkeeperExists()
                && observations.nereus().state() == NereusDurableStorageState.MISSING) {
            StorageClassBindingRecord adopted = StorageClassBindingRecord.claimed(
                    persistenceName,
                    StorageClassBindingRecord.BOOKKEEPER,
                    1,
                    System.currentTimeMillis()).transitionTo(StorageClassBindingState.ACTIVE);
            return createBinding(path, adopted).thenCompose(created -> {
                if (!StorageClassBindingRecord.BOOKKEEPER.equals(selectedStorageClass)) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("storage-class migration is required"));
                }
                return CompletableFuture.completedFuture(openPermit(created, false));
            }).exceptionallyCompose(error -> retryOpenAfterConflict(
                    error, path, persistenceName, selectedStorageClass, createIfMissing, factory, conflictCount));
        }
        if (observations.bookkeeperExists()
                || observations.nereus().state() != NereusDurableStorageState.MISSING) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("durable storage exists without a storage-class binding"));
        }
        if (!createIfMissing) {
            return CompletableFuture.failedFuture(new ManagedLedgerNotFoundException("Ledger not found"));
        }
        StorageClassBindingRecord claimed = StorageClassBindingRecord.claimed(
                persistenceName, selectedStorageClass, 1, System.currentTimeMillis());
        return createBinding(path, claimed)
                .thenApply(created -> openPermit(created, true))
                .exceptionallyCompose(error -> retryOpenAfterConflict(
                        error, path, persistenceName, selectedStorageClass, true, factory, conflictCount));
    }

    private CompletableFuture<StorageClassOpenPermit> prepareExistingBinding(
            String path,
            StorageClassBindingRecord binding,
            String selectedStorageClass,
            boolean createIfMissing,
            NereusManagedLedgerFactory factory,
            int conflictCount,
            StorageObservations observations) {
        if (binding.state() != StorageClassBindingState.DELETED
                && !binding.storageClass().equals(selectedStorageClass)) {
            return CompletableFuture.failedFuture(new IllegalStateException("storage-class migration is required"));
        }
        return switch (binding.state()) {
            case CLAIMED -> validateClaimed(binding, observations);
            case ACTIVE -> validateActive(binding, observations);
            case DELETING -> CompletableFuture.failedFuture(
                    new IllegalStateException("storage-class binding is deleting"));
            case DELETED -> prepareNextGeneration(
                    path, binding, selectedStorageClass, createIfMissing, factory, conflictCount, observations);
        };
    }

    private CompletableFuture<StorageClassOpenPermit> validateClaimed(
            StorageClassBindingRecord binding,
            StorageObservations observations) {
        if (StorageClassBindingRecord.BOOKKEEPER.equals(binding.storageClass())) {
            requireNoLiveNereus(observations.nereus(), binding.bindingGeneration());
            return CompletableFuture.completedFuture(openPermit(binding, true));
        }
        if (observations.bookkeeperExists()) {
            return CompletableFuture.failedFuture(new IllegalStateException("both storage classes contain live state"));
        }
        NereusDurableStorageState state = observations.nereus().state();
        if (state == NereusDurableStorageState.MISSING) {
            return CompletableFuture.completedFuture(openPermit(binding, true));
        }
        if (state == NereusDurableStorageState.ACTIVE || state == NereusDurableStorageState.SEALED) {
            requireNereusGeneration(observations.nereus(), binding.bindingGeneration());
            return CompletableFuture.completedFuture(openPermit(binding, true));
        }
        return CompletableFuture.failedFuture(
                new IllegalStateException("claimed Nereus storage is not openable"));
    }

    private CompletableFuture<StorageClassOpenPermit> validateActive(
            StorageClassBindingRecord binding,
            StorageObservations observations) {
        if (StorageClassBindingRecord.BOOKKEEPER.equals(binding.storageClass())) {
            requireNoLiveNereus(observations.nereus(), binding.bindingGeneration());
            if (!observations.bookkeeperExists()) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("active BookKeeper storage is missing"));
            }
            return CompletableFuture.completedFuture(openPermit(binding, false));
        }
        if (observations.bookkeeperExists()) {
            return CompletableFuture.failedFuture(new IllegalStateException("both storage classes contain live state"));
        }
        NereusDurableStorageState state = observations.nereus().state();
        if (state != NereusDurableStorageState.ACTIVE && state != NereusDurableStorageState.SEALED) {
            return CompletableFuture.failedFuture(new IllegalStateException("active Nereus storage is missing"));
        }
        requireNereusGeneration(observations.nereus(), binding.bindingGeneration());
        return CompletableFuture.completedFuture(openPermit(binding, false));
    }

    private CompletableFuture<StorageClassOpenPermit> prepareNextGeneration(
            String path,
            StorageClassBindingRecord binding,
            String selectedStorageClass,
            boolean createIfMissing,
            NereusManagedLedgerFactory factory,
            int conflictCount,
            StorageObservations observations) {
        if (!createIfMissing) {
            return CompletableFuture.failedFuture(new ManagedLedgerNotFoundException("Ledger not found"));
        }
        if (observations.bookkeeperExists()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("deleted binding still has BookKeeper storage"));
        }
        requireTerminalNereus(observations.nereus(), binding);
        StorageClassBindingRecord next = binding.nextGeneration(selectedStorageClass, System.currentTimeMillis());
        return transitionBinding(path, binding, next)
                .thenApply(created -> openPermit(created, true))
                .exceptionallyCompose(error -> retryOpenAfterConflict(
                        error,
                        path,
                        binding.persistenceName(),
                        selectedStorageClass,
                        true,
                        factory,
                        conflictCount));
    }

    private CompletableFuture<StorageClassBindingRecord> createBinding(
            String path, StorageClassBindingRecord candidate) {
        return metadataStore.put(path, codec.encode(candidate), Optional.of(-1L))
                .thenApply(stat -> candidate.withMetadataVersion(stat.getVersion()));
    }

    private CompletableFuture<StorageClassBindingRecord> transitionBinding(
            String path,
            StorageClassBindingRecord current,
            StorageClassBindingRecord updated) {
        return metadataStore.put(path, codec.encode(updated), Optional.of(current.metadataVersion()))
                .thenApply(stat -> updated.withMetadataVersion(stat.getVersion()));
    }

    private CompletableFuture<StorageClassOpenPermit> retryOpenAfterConflict(
            Throwable error,
            String path,
            String persistenceName,
            String selectedStorageClass,
            boolean createIfMissing,
            NereusManagedLedgerFactory factory,
            int conflictCount) {
        if (!isBadVersion(error)) {
            return CompletableFuture.failedFuture(unwrap(error));
        }
        return prepareStorageClassOpen(
                path,
                persistenceName,
                selectedStorageClass,
                createIfMissing,
                factory,
                conflictCount + 1);
    }

    private static StorageClassOpenPermit openPermit(
            StorageClassBindingRecord binding, boolean activationRequired) {
        return new StorageClassOpenPermit(
                binding.persistenceName(),
                binding.storageClass(),
                binding.bindingGeneration(),
                binding.metadataVersion(),
                activationRequired);
    }

    private static StorageClassDeletePermit deletePermit(StorageClassBindingRecord binding) {
        return new StorageClassDeletePermit(
                binding.persistenceName(),
                binding.storageClass(),
                binding.bindingGeneration(),
                binding.metadataVersion());
    }

    private static void requirePermitGeneration(
            StorageClassBindingRecord binding, StorageClassOpenPermit permit) {
        if (!binding.persistenceName().equals(permit.persistenceName())
                || !binding.storageClass().equals(permit.storageClass())
                || binding.bindingGeneration() != permit.bindingGeneration()) {
            throw new IllegalStateException("storage-class open permit is stale");
        }
    }

    private static void requirePermitMetadataVersion(
            StorageClassBindingRecord binding, StorageClassOpenPermit permit) {
        if (binding.metadataVersion() != permit.expectedMetadataVersion()) {
            throw new IllegalStateException("storage-class open permit is stale");
        }
    }

    private static void requireNereusGeneration(NereusStorageStateSnapshot snapshot, long expectedGeneration) {
        snapshot.projection().orElseThrow(() ->
                new IllegalStateException("Nereus durable state has no projection"))
                .requireStorageClassBindingGeneration(expectedGeneration);
    }

    private static boolean isPublishedNereusGeneration(
            NereusStorageStateSnapshot snapshot, long expectedGeneration) {
        if (snapshot.state() == NereusDurableStorageState.MISSING) {
            return false;
        }
        requireNereusGeneration(snapshot, expectedGeneration);
        return snapshot.state() != NereusDurableStorageState.DELETED;
    }

    private CompletableFuture<Boolean> bookkeeperDurableStateExists(String persistenceName) {
        return bookkeeperFactory.asyncExists(persistenceName).thenCompose(exists -> {
            if (!exists) {
                return CompletableFuture.completedFuture(false);
            }
            String path = BOOKKEEPER_MANAGED_LEDGER_ROOT + persistenceName;
            return metadataStore.get(path).thenApply(current -> current
                    .map(result -> result.getValue().length > 0)
                    // A disappeared node after asyncExists is an observation race. Fail closed instead of
                    // authorizing a different storage class from an unstable absence.
                    .orElse(true));
        });
    }

    private static void requireBoundStorageDeleted(
            StorageClassBindingRecord binding, StorageObservations observations) {
        if (StorageClassBindingRecord.BOOKKEEPER.equals(binding.storageClass())) {
            if (observations.bookkeeperExists()) {
                throw new IllegalStateException("BookKeeper storage still exists after delete");
            }
            requireNoLiveNereus(observations.nereus(), binding.bindingGeneration());
            return;
        }
        if (observations.bookkeeperExists()) {
            throw new IllegalStateException("non-bound BookKeeper storage exists after Nereus delete");
        }
        if (observations.nereus().state() != NereusDurableStorageState.DELETED) {
            throw new IllegalStateException("Nereus storage is not deleted");
        }
        requireNereusGeneration(observations.nereus(), binding.bindingGeneration());
    }

    private static void requireNoLiveNereus(NereusStorageStateSnapshot snapshot, long currentGeneration) {
        if (snapshot.state() == NereusDurableStorageState.MISSING) {
            return;
        }
        if (snapshot.state() == NereusDurableStorageState.DELETED) {
            long generation = snapshot.projection().orElseThrow().storageClassBindingGeneration();
            if (generation < currentGeneration) {
                return;
            }
        }
        throw new IllegalStateException("non-bound Nereus storage contains live state");
    }

    private static void requireTerminalNereus(
            NereusStorageStateSnapshot snapshot, StorageClassBindingRecord deletedBinding) {
        if (snapshot.state() == NereusDurableStorageState.MISSING) {
            if (StorageClassBindingRecord.NEREUS.equals(deletedBinding.storageClass())) {
                throw new IllegalStateException("deleted Nereus binding lost its projection tombstone");
            }
            return;
        }
        if (snapshot.state() != NereusDurableStorageState.DELETED) {
            throw new IllegalStateException("prior Nereus generation is not terminal");
        }
        long generation = snapshot.projection().orElseThrow().storageClassBindingGeneration();
        long maximumGeneration = StorageClassBindingRecord.NEREUS.equals(deletedBinding.storageClass())
                ? deletedBinding.bindingGeneration()
                : deletedBinding.bindingGeneration() - 1;
        if (generation > maximumGeneration) {
            throw new IllegalStateException("Nereus projection generation is newer than deleted binding");
        }
    }

    private NereusManagedLedgerFactory requireAttachedFactory() {
        NereusManagedLedgerFactory factory = nereusFactory.get();
        if (factory == null) {
            throw new IllegalStateException("Nereus managed-ledger factory is not attached");
        }
        return factory;
    }

    private CompletableFuture<Void> requireNereusCapability() {
        try {
            return java.util.Objects.requireNonNull(
                    nereusCapabilityCheck.get(), "nereusCapabilityCheck result");
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Nereus binding store is closed");
        }
    }

    private static void requireStorageClass(String storageClass) {
        if (!StorageClassBindingRecord.BOOKKEEPER.equals(storageClass)
                && !StorageClassBindingRecord.NEREUS.equals(storageClass)) {
            throw new IllegalArgumentException("unsupported managed-ledger storage class");
        }
    }

    private String bindingPath(String persistenceName) {
        TopicName topicName = TopicName.get(TopicName.fromPersistenceNamingEncoding(persistenceName));
        return keyspace.bindingKey(topicName.getNamespaceObject(), persistenceName);
    }

    private static void requireNereusOpenState(StorageClassBindingRecord binding) {
        if (!StorageClassBindingRecord.NEREUS.equals(binding.storageClass())) {
            throw new IllegalStateException("storage-class migration is required before Nereus open");
        }
        if (binding.state() != StorageClassBindingState.CLAIMED
                && binding.state() != StorageClassBindingState.ACTIVE) {
            throw new IllegalStateException("Nereus binding is not openable");
        }
    }

    private static boolean isBadVersion(Throwable failure) {
        return unwrap(failure) instanceof MetadataStoreException.BadVersionException;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private record StorageObservations(
            boolean bookkeeperExists,
            NereusStorageStateSnapshot nereus) {
    }
}
