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
import com.nereusstream.bookkeeper.BookKeeperDeletionActivationRequest;
import com.nereusstream.bookkeeper.BookKeeperDeletionActivationResult;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivation;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationUpdate;
import com.nereusstream.core.capability.GenerationRegistrationBackfillCompletion;
import com.nereusstream.managedledger.NereusManagedLedgerFactory;
import com.nereusstream.managedledger.NereusManagedLedgerRuntime;
import com.nereusstream.managedledger.generation.ManagedLedgerMaterializationRegistrationCandidate;
import com.nereusstream.managedledger.generation.ManagedLedgerPhysicalDeletionActivationRequest;
import com.nereusstream.managedledger.generation.ManagedLedgerPhysicalDeletionActivationResult;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import com.nereusstream.pulsar.BookKeeperPrimaryWalAdministration;
import com.nereusstream.pulsar.NereusProcessIdentity;
import com.nereusstream.pulsar.NereusRuntimeConfiguration;
import com.nereusstream.pulsar.NereusRuntimeContext;
import com.nereusstream.pulsar.NereusRuntimeProvider;
import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ManagedLedgerClientFactory;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.storage.BookkeeperManagedLedgerStorageClass;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/** Hybrid BookKeeper-default plus opt-in Nereus managed-ledger storage provider. */
public final class NereusManagedLedgerStorage implements ManagedLedgerStorage {
    private static final NereusTopicFeatureValidator FEATURE_VALIDATOR = new NereusTopicFeatureValidator();
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<NereusGenerationRegistrationBackfill>
            generationRegistrationBackfill = new AtomicReference<>();
    private final AtomicReference<BookKeeperPrimaryWalAdministration>
            bookKeeperPrimaryWalAdministration = new AtomicReference<>();
    private ManagedLedgerClientFactory bookkeeperStorage;
    private NereusManagedLedgerFactory nereusFactory;
    private NereusStorageClassBindingStore bindingStore;
    private NereusBrokerCapabilityCoordinator capabilityCoordinator;
    private ManagedLedgerStorageClass bookkeeperClass;
    private ManagedLedgerStorageClass nereusClass;
    private int generationRegistrationBackfillConcurrency;
    private Duration generationRegistrationBackfillTimeout;
    private int generationRegistrationBackfillMaxTopicsPerNamespace;
    private boolean generationProtocolEnabled;
    private boolean physicalGcMutationsAllowed;
    private Collection<ManagedLedgerStorageClass> storageClasses = List.of();

    public NereusManagedLedgerStorage() {
    }

    @Override
    public void initialize(
            ServiceConfiguration conf,
            MetadataStoreExtended metadataStore,
            BookKeeperClientFactory bookkeeperProvider,
            EventLoopGroup eventLoopGroup,
            OpenTelemetry openTelemetry) throws Exception {
        if (!initialized.compareAndSet(false, true)) {
            throw new IllegalStateException("Nereus managed-ledger storage is already initialized");
        }
        NereusManagedLedgerRuntime runtime = null;
        try {
            NereusBrokerStorageConfiguration checked = new NereusBrokerStorageConfiguration(conf);
            generationRegistrationBackfillConcurrency =
                    checked.generationRegistrationBackfillConcurrency();
            generationRegistrationBackfillTimeout =
                    checked.generationRegistrationBackfillTimeout();
            generationRegistrationBackfillMaxTopicsPerNamespace =
                    checked.generationRegistrationBackfillMaxTopicsPerNamespace();
            generationProtocolEnabled =
                    checked.generationProtocolEnabled();
            capabilityCoordinator = new NereusBrokerCapabilityCoordinator(
                    metadataStore,
                    Duration.ofSeconds(conf.getNereusMetadataTimeoutSeconds()));
            NereusProcessIdentity identity = NereusProcessIdentity.generate(new SecureRandom());
            NereusRuntimeConfiguration runtimeConfiguration = checked.runtimeConfiguration(identity);
            physicalGcMutationsAllowed = runtimeConfiguration.physicalGc().mutationsAllowed();
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            ObjectStoreSecretResolver secretResolver = Reflections.createInstance(
                    checked.secretResolverClassName(), ObjectStoreSecretResolver.class, classLoader);
            NereusRuntimeProvider runtimeProvider = Reflections.createInstance(
                    checked.runtimeProviderClassName(), NereusRuntimeProvider.class, classLoader);

            ManagedLedgerClientFactory localBookkeeper = new ManagedLedgerClientFactory();
            localBookkeeper.initialize(
                    conf, metadataStore, bookkeeperProvider, eventLoopGroup, openTelemetry);
            bookkeeperStorage = localBookkeeper;
            bookkeeperClass = localBookkeeper.getDefaultStorageClass();
            if (!"bookkeeper".equals(bookkeeperClass.getName())) {
                throw new IllegalStateException("stock managed-ledger storage class must be bookkeeper");
            }
            BookKeeper borrowedBookKeeperClient = requireBorrowedBookKeeperClient(bookkeeperClass);

            bindingStore = new NereusStorageClassBindingStore(
                    metadataStore,
                    bookkeeperClass.getManagedLedgerFactory(),
                    Duration.ofSeconds(conf.getNereusMetadataTimeoutSeconds()),
                    capabilityCoordinator::requireClusterReady,
                    capabilityCoordinator::requireStorageProfileReady,
                    capabilityCoordinator::requireLocalStorageProfileReady);
            NereusRuntimeContext context = new NereusRuntimeContext(
                    eventLoopGroup,
                    openTelemetry,
                    bindingStore.creationGuard(),
                    capabilityCoordinator,
                    capabilityCoordinator,
                    checked.generationProtocolEnabled(),
                    secretResolver,
                    classLoader,
                    Optional.of(borrowedBookKeeperClient),
                    capabilityCoordinator,
                    capabilityCoordinator::installBookKeeperPrimaryWalCapability,
                    this::installBookKeeperPrimaryWalAdministration);
            runtime = runtimeProvider.create(runtimeConfiguration, context);
            ManagedLedgerFactoryConfig compatibilityFactoryConfig = new ManagedLedgerFactoryConfig();
            compatibilityFactoryConfig.setMaxCacheSize(0);
            nereusFactory = new NereusManagedLedgerFactory(
                    runtime,
                    bindingStore.creationGuard(),
                    new ManagedLedgerConfig(),
                    compatibilityFactoryConfig,
                    true);
            runtime = null;
            bindingStore.attachNereusFactory(nereusFactory);
            nereusClass = new NereusManagedLedgerStorageClass(nereusFactory);
            storageClasses = List.of(bookkeeperClass, nereusClass);
            capabilityCoordinator.markStorageInitialized();
        } catch (Throwable failure) {
            if (runtime != null) {
                suppressClose(failure, runtime);
            }
            suppressClose(failure, bindingStore);
            suppressClose(failure, bookkeeperStorage);
            storageClasses = List.of();
            if (failure instanceof Exception exception) {
                throw exception;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("unexpected Nereus initialization failure", failure);
        }
    }

    static BookKeeper requireBorrowedBookKeeperClient(ManagedLedgerStorageClass storageClass) {
        if (!(storageClass instanceof BookkeeperManagedLedgerStorageClass bookkeeperBacked)) {
            throw new IllegalStateException(
                    "stock managed-ledger storage class must expose its borrowed BookKeeper client");
        }
        return Objects.requireNonNull(
                bookkeeperBacked.getBookKeeperClient(),
                "stock managed-ledger storage class returned a null BookKeeper client");
    }

    @Override
    public Collection<ManagedLedgerStorageClass> getStorageClasses() {
        ensureReady();
        return storageClasses;
    }

    @Override
    public ManagedLedgerStorageClass getDefaultStorageClass() {
        ensureReady();
        return bookkeeperClass;
    }

    @Override
    public Optional<ManagedLedgerStorageClass> getManagedLedgerStorageClass(String name) {
        ensureReady();
        if (name == null || "bookkeeper".equals(name)) {
            return Optional.of(bookkeeperClass);
        }
        if (NereusManagedLedgerStorageClass.NAME.equals(name)) {
            return Optional.of(nereusClass);
        }
        return Optional.empty();
    }

    public NereusStorageClassBindingStore bindingStore() {
        ensureReady();
        return bindingStore;
    }

    public NereusBrokerCapabilityCoordinator capabilityCoordinator() {
        ensureReady();
        return capabilityCoordinator;
    }

    public BookKeeperPrimaryWalAdministration bookKeeperPrimaryWalAdministration() {
        ensureReady();
        BookKeeperPrimaryWalAdministration administration =
                bookKeeperPrimaryWalAdministration.get();
        if (administration == null) {
            throw new IllegalStateException(
                    "BookKeeper primary-WAL administration is not installed");
        }
        return administration;
    }

    public CompletableFuture<BookKeeperDeletionActivationResult>
            activateBookKeeperLedgerDeletion(
                    BookKeeperDeletionActivationRequest request) {
        try {
            return bookKeeperPrimaryWalAdministration().activateDeletion(request);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    public CompletableFuture<BookKeeperLedgerIdNamespaceReservation>
            provisionBookKeeperLedgerIdNamespace(
                    String operatorEvidenceSha256, Duration timeout) {
        try {
            return bookKeeperPrimaryWalAdministration().provisionNamespace(
                    operatorEvidenceSha256, timeout);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    public CompletableFuture<BookKeeperLedgerIdNamespaceReservation>
            revokeBookKeeperLedgerIdNamespace(
                    String revocationEvidenceSha256,
                    long expectedMetadataVersion,
                    Duration timeout) {
        try {
            return bookKeeperPrimaryWalAdministration().revokeNamespace(
                    revocationEvidenceSha256,
                    expectedMetadataVersion,
                    timeout);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    public CompletableFuture<BookKeeperProtocolActivation>
            prepareBookKeeperPrimaryWalActivation(
                    long brokerReadinessEpoch,
                    String brokerReadinessSha256,
                    Duration timeout) {
        try {
            BookKeeperPrimaryWalAdministration administration =
                    bookKeeperPrimaryWalAdministration();
            return capabilityCoordinator
                    .requireBookKeeperPublicationReadiness(
                            brokerReadinessEpoch, brokerReadinessSha256)
                    .thenCompose(ignored -> administration.prepareActivation(
                            brokerReadinessEpoch,
                            brokerReadinessSha256,
                            timeout));
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    public CompletableFuture<BookKeeperProtocolActivation>
            activateBookKeeperPrimaryWalPublications(
                    BookKeeperProtocolActivationUpdate update, Duration timeout) {
        try {
            BookKeeperProtocolActivationUpdate exact =
                    Objects.requireNonNull(update, "update");
            BookKeeperPrimaryWalAdministration administration =
                    bookKeeperPrimaryWalAdministration();
            return capabilityCoordinator
                    .requireBookKeeperPublicationReadiness(
                            exact.brokerReadinessEpoch(),
                            exact.brokerReadinessSha256())
                    .thenCompose(ignored -> administration.activate(exact, timeout));
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    public CompletableFuture<Optional<BookKeeperProtocolActivation>>
            readBookKeeperPrimaryWalActivation(Duration timeout) {
        try {
            return bookKeeperPrimaryWalAdministration().readActivation(timeout);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    public boolean generationProtocolEnabled() {
        ensureReady();
        return generationProtocolEnabled;
    }

    public void attachGenerationRegistrationBackfill(
            TenantResources tenantResources,
            NamespaceResources namespaceResources,
            TopicResources topicResources) {
        ensureReady();
        NereusGenerationRegistrationBackfill backfill =
                new DefaultNereusGenerationRegistrationBackfill(
                        tenantResources,
                        namespaceResources,
                        topicResources,
                        bindingStore,
                        this,
                        capabilityCoordinator,
                        generationRegistrationBackfillMaxTopicsPerNamespace);
        if (!generationRegistrationBackfill.compareAndSet(null, backfill)) {
            throw new IllegalStateException(
                    "Nereus generation registration backfill is already attached");
        }
    }

    public CompletableFuture<GenerationRegistrationBackfillReport>
            runGenerationRegistrationBackfill(
                    GenerationRegistrationBackfillRequest request) {
        final NereusGenerationRegistrationBackfill backfill;
        try {
            ensureReady();
            backfill = requireGenerationRegistrationBackfill();
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return backfill.run(request)
                .thenCompose(report -> activateAfterSuccessfulBackfill(
                        report,
                        generationProtocolEnabled,
                        physicalGcMutationsAllowed,
                        this::activateGenerationPublication,
                        () -> activatePhysicalDeletion(
                                new ManagedLedgerPhysicalDeletionActivationRequest(
                                        request.runId(),
                                        request.maxConcurrency(),
                                        request.timeout()))));
    }

    public CompletableFuture<GenerationRegistrationBackfillReport>
            runGenerationRegistrationBackfill(String runId) {
        try {
            ensureReady();
            requireGenerationRegistrationBackfill();
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return capabilityCoordinator.requireGenerationReadiness()
                .thenCompose(readiness -> runGenerationRegistrationBackfill(
                        new GenerationRegistrationBackfillRequest(
                                runId,
                                readiness.brokerReadinessEpoch(),
                                generationRegistrationBackfillConcurrency,
                                generationRegistrationBackfillTimeout)));
    }

    public CompletableFuture<ManagedLedgerMaterializationRegistrationCandidate>
            inspectMaterializationRegistrationCandidate(
                    String persistenceName,
                    long expectedBindingGeneration) {
        try {
            ensureReady();
            return nereusFactory
                    .inspectMaterializationRegistrationCandidate(
                            persistenceName, expectedBindingGeneration);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    public CompletableFuture<Void> ensureMaterializationRegistration(
            ManagedLedgerMaterializationRegistrationCandidate candidate) {
        try {
            ensureReady();
            return nereusFactory.ensureMaterializationRegistration(candidate);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    public CompletableFuture<Void> completeGenerationRegistrationBackfill(
            GenerationRegistrationBackfillCompletion completion) {
        return completeGenerationRegistrationBackfill(
                completion, 1, Duration.ofHours(1));
    }

    public CompletableFuture<Void> completeGenerationRegistrationBackfill(
            GenerationRegistrationBackfillCompletion completion,
            int maxConcurrentStreams,
            Duration timeout) {
        try {
            ensureReady();
            return nereusFactory.completeGenerationRegistrationBackfill(
                    completion, maxConcurrentStreams, timeout);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    public CompletableFuture<Void> activateGenerationPublication() {
        try {
            ensureReady();
            return nereusFactory.activateGenerationPublication();
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    public CompletableFuture<ManagedLedgerPhysicalDeletionActivationResult>
            activatePhysicalDeletion(
                    ManagedLedgerPhysicalDeletionActivationRequest request) {
        try {
            ensureReady();
            return nereusFactory.activatePhysicalDeletion(request);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    static CompletableFuture<GenerationRegistrationBackfillReport>
            activateAfterSuccessfulBackfill(
                    GenerationRegistrationBackfillReport report,
                    boolean publicationActivationEnabled,
                    boolean physicalDeletionActivationEnabled,
                    Supplier<CompletableFuture<Void>> publicationActivation,
                    Supplier<CompletableFuture<ManagedLedgerPhysicalDeletionActivationResult>>
                            physicalDeletionActivation) {
        final GenerationRegistrationBackfillReport exact;
        try {
            exact = Objects.requireNonNull(report, "report");
            if (!publicationActivationEnabled || exact.failureCount() != 0) {
                return CompletableFuture.completedFuture(exact);
            }
            Objects.requireNonNull(
                    publicationActivation, "publicationActivation");
            return Objects.requireNonNull(
                            publicationActivation.get(),
                            "publication activation future")
                    .thenCompose(ignored -> {
                        if (!physicalDeletionActivationEnabled) {
                            return CompletableFuture.completedFuture(exact);
                        }
                        Objects.requireNonNull(
                                physicalDeletionActivation,
                                "physicalDeletionActivation");
                        return Objects.requireNonNull(
                                        physicalDeletionActivation.get(),
                                        "physical deletion activation future")
                                .thenApply(activated -> exact);
                    });
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    public CompletableFuture<Void> validateUnloadedAdminOperation(
            TopicName topicName, NereusAdminOperation operation) {
        return validateBoundAdminOperation(topicName, operation);
    }

    /**
     * Applies one durable-profile path to both loaded and unloaded topics.
     * The current broker default is deliberately not consulted.
     */
    public CompletableFuture<Void> validateBoundAdminOperation(
            TopicName topicName, NereusAdminOperation operation) {
        final String persistenceName;
        try {
            ensureReady();
            persistenceName = Objects.requireNonNull(topicName, "topicName")
                    .getPersistenceNamingEncoding();
            Objects.requireNonNull(operation, "operation");
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        return bindingStore.getBinding(persistenceName).thenCompose(binding -> {
            if (binding.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            StorageClassBindingRecord current = binding.orElseThrow();
            if (current.state() == StorageClassBindingState.DELETED
                    || !StorageClassBindingRecord.NEREUS.equals(current.storageClass())) {
                return CompletableFuture.completedFuture(null);
            }
            return nereusFactory.inspectStorageState(persistenceName)
                    .thenCompose(snapshot -> {
                        StorageProfile profile = requireDurableProfile(
                                current, snapshot);
                        return validateBoundNereusAdminOperation(
                                operation,
                                generationProtocolEnabled,
                                profile,
                                capabilityCoordinator::requireGenerationReadiness,
                                capabilityCoordinator::requireStorageProfileReady);
                    });
        });
    }

    /** Returns true only for an existing, live Nereus topic incarnation. */
    public CompletableFuture<Boolean> hasActiveNereusBinding(
            TopicName topicName) {
        final String persistenceName;
        try {
            ensureReady();
            persistenceName = Objects.requireNonNull(
                            topicName, "topicName")
                    .getPersistenceNamingEncoding();
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        return bindingStore.getBinding(persistenceName)
                .thenApply(binding -> binding
                        .filter(current -> current.state()
                                == StorageClassBindingState.ACTIVE)
                        .filter(current -> StorageClassBindingRecord.NEREUS
                                .equals(current.storageClass()))
                        .isPresent());
    }

    /**
     * Resolves the strongest durable BK profile in a namespace for service-unit ownership filtering.
     *
     * <p>The binding selects the storage class only. The exact profile remains owned by the F2 projection/L0
     * stream and is reloaded here; broker defaults and topic policy are never allowed to reinterpret it.
     */
    public CompletableFuture<Optional<StorageProfile>> requiredBookKeeperOwnershipProfile(
            NamespaceName namespace,
            int maxEntries,
            int maxPendingOperations) {
        try {
            ensureReady();
            Objects.requireNonNull(namespace, "namespace");
            if (maxEntries <= 0 || maxPendingOperations <= 0) {
                throw new IllegalArgumentException(
                        "BookKeeper ownership scan limits must be positive");
            }
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        return bindingStore.listNonDeletedBindings(
                        namespace, maxEntries, maxPendingOperations)
                .thenCompose(bindings -> {
                    List<StorageClassBindingRecord> activeNereus = bindings.stream()
                            .filter(binding -> binding.state()
                                    == StorageClassBindingState.ACTIVE)
                            .filter(binding -> StorageClassBindingRecord.NEREUS.equals(
                                    binding.storageClass()))
                            .toList();
                    return scanBookKeeperOwnershipProfiles(
                            activeNereus,
                            0,
                            maxPendingOperations,
                            Optional.empty());
                });
    }

    private CompletableFuture<Optional<StorageProfile>> scanBookKeeperOwnershipProfiles(
            List<StorageClassBindingRecord> bindings,
            int index,
            int maxPendingOperations,
            Optional<StorageProfile> strongest) {
        if (index >= bindings.size()
                || strongest.filter(profile -> profile
                        == StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT).isPresent()) {
            return CompletableFuture.completedFuture(strongest);
        }
        int end = Math.min(bindings.size(), index + maxPendingOperations);
        List<CompletableFuture<StorageProfile>> reads = bindings.subList(index, end).stream()
                .map(binding -> nereusFactory.inspectStorageState(binding.persistenceName())
                        .thenApply(snapshot -> requireDurableProfile(binding, snapshot)))
                .toList();
        return CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new))
                .thenCompose(ignored -> {
                    Optional<StorageProfile> next = strongest;
                    for (CompletableFuture<StorageProfile> read : reads) {
                        StorageProfile profile = read.join().canonical();
                        if (profile.usesBookKeeperWal()
                                && (next.isEmpty()
                                        || bookKeeperOwnershipRank(profile)
                                                > bookKeeperOwnershipRank(next.orElseThrow()))) {
                            next = Optional.of(profile);
                        }
                    }
                    return scanBookKeeperOwnershipProfiles(
                            bindings, end, maxPendingOperations, next);
                });
    }

    private static int bookKeeperOwnershipRank(StorageProfile profile) {
        return switch (profile.canonical()) {
            case BOOKKEEPER_WAL_ONLY -> 1;
            case BOOKKEEPER_WAL_ASYNC_OBJECT -> 2;
            case BOOKKEEPER_WAL_SYNC_OBJECT -> 3;
            default -> 0;
        };
    }

    static CompletableFuture<Void> validateBoundNereusAdminOperation(
            NereusAdminOperation operation,
            boolean generationProtocolEnabled,
            Supplier<? extends CompletableFuture<?>> readiness) {
        return validateBoundNereusAdminOperation(
                operation,
                generationProtocolEnabled,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                readiness,
                ignored -> CompletableFuture.completedFuture(null));
    }

    static CompletableFuture<Void> validateBoundNereusAdminOperation(
            NereusAdminOperation operation,
            boolean generationProtocolEnabled,
            StorageProfile durableProfile,
            Supplier<? extends CompletableFuture<?>> generationReadiness,
            Function<StorageProfile, ? extends CompletableFuture<?>> profileReadiness) {
        Objects.requireNonNull(operation, "operation");
        StorageProfile profile = Objects.requireNonNull(
                durableProfile, "durableProfile").canonical();
        Objects.requireNonNull(generationReadiness, "generationReadiness");
        Objects.requireNonNull(profileReadiness, "profileReadiness");
        CompletableFuture<Void> validated = validateAdminOperation(
                operation, generationProtocolEnabled);
        if (operation == NereusAdminOperation.TRIM_TOPIC) {
            validated = validated.thenCompose(ignored -> invokeReadiness(
                    generationReadiness, "generation readiness future"));
        }
        if (operation.requiresStorageProfileReadiness()) {
            validated = validated.thenCompose(ignored -> invokeReadiness(
                    () -> profileReadiness.apply(profile),
                    "storage-profile readiness future"));
        }
        return validated;
    }

    static StorageProfile requireDurableProfile(
            StorageClassBindingRecord binding,
            com.nereusstream.managedledger.NereusStorageStateSnapshot snapshot) {
        StorageClassBindingRecord exactBinding = Objects.requireNonNull(
                binding, "binding");
        com.nereusstream.managedledger.NereusStorageStateSnapshot exactSnapshot =
                Objects.requireNonNull(snapshot, "snapshot");
        if (!StorageClassBindingRecord.NEREUS.equals(exactBinding.storageClass())
                || exactBinding.state() == StorageClassBindingState.DELETED) {
            throw new IllegalArgumentException(
                    "durable profile requires a live Nereus binding");
        }
        if (exactSnapshot.state()
                == com.nereusstream.managedledger.NereusDurableStorageState.MISSING) {
            throw new IllegalStateException(
                    "active Nereus binding has no durable topic projection");
        }
        var projection = exactSnapshot.projection().orElseThrow(() ->
                new IllegalStateException(
                        "Nereus durable state has no topic projection"));
        if (projection.storageClassBindingGeneration()
                != exactBinding.bindingGeneration()) {
            throw new IllegalStateException(
                    "Nereus durable profile binding generation mismatch");
        }
        return exactSnapshot.streamMetadata().orElseThrow(() ->
                        new IllegalStateException(
                                "Nereus durable state has no stream metadata"))
                .profile()
                .canonical();
    }

    private static CompletableFuture<Void> invokeReadiness(
            Supplier<? extends CompletableFuture<?>> readiness,
            String name) {
        try {
            return Objects.requireNonNull(readiness.get(), name)
                    .thenApply(ignored -> null);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    private static CompletableFuture<Void> validateAdminOperation(
            NereusAdminOperation operation,
            boolean generationProtocolRuntimeReady) {
        try {
            FEATURE_VALIDATOR.validateAdminOperation(operation, generationProtocolRuntimeReady);
            return CompletableFuture.completedFuture(null);
        } catch (NotAllowedException error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        IOException failure = null;
        failure = closeFactory(nereusFactory, failure);
        failure = closeResource(bookkeeperStorage, failure);
        failure = closeResource(bindingStore, failure);
        generationRegistrationBackfill.set(null);
        bookKeeperPrimaryWalAdministration.set(null);
        storageClasses = List.of();
        if (failure != null) {
            throw failure;
        }
    }

    private void ensureReady() {
        if (!initialized.get() || storageClasses.isEmpty() || closed.get()) {
            throw new IllegalStateException("Nereus managed-ledger storage is not available");
        }
    }

    private NereusGenerationRegistrationBackfill
            requireGenerationRegistrationBackfill() {
        NereusGenerationRegistrationBackfill backfill =
                generationRegistrationBackfill.get();
        if (backfill == null) {
            throw new IllegalStateException(
                    "Nereus generation registration backfill is not attached");
        }
        return backfill;
    }

    private void installBookKeeperPrimaryWalAdministration(
            BookKeeperPrimaryWalAdministration administration) {
        Objects.requireNonNull(administration, "administration");
        if (!bookKeeperPrimaryWalAdministration.compareAndSet(null, administration)) {
            throw new IllegalStateException(
                    "BookKeeper primary-WAL administration is already installed");
        }
    }

    private static IOException closeFactory(NereusManagedLedgerFactory factory, IOException failure) {
        if (factory == null) {
            return failure;
        }
        try {
            factory.shutdown();
            return failure;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return accumulate(failure, interrupted);
        } catch (ManagedLedgerException error) {
            return accumulate(failure, error);
        }
    }

    private static IOException closeResource(AutoCloseable resource, IOException failure) {
        if (resource == null) {
            return failure;
        }
        try {
            resource.close();
            return failure;
        } catch (Exception error) {
            return accumulate(failure, error);
        }
    }

    private static IOException accumulate(IOException existing, Throwable error) {
        IOException mapped = error instanceof IOException io ? io : new IOException(error);
        if (existing == null) {
            return mapped;
        }
        existing.addSuppressed(mapped);
        return existing;
    }

    private static void suppressClose(Throwable root, AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Throwable closeFailure) {
            root.addSuppressed(closeFailure);
        }
    }
}
