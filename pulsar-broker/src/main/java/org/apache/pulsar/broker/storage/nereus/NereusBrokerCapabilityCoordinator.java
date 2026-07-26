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

import com.fasterxml.jackson.databind.JsonNode;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadiness;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadinessProvider;
import com.nereusstream.core.capability.GenerationCapabilityReadiness;
import com.nereusstream.core.capability.GenerationCapabilityReadinessProvider;
import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import com.nereusstream.managedledger.cursor.CursorProtocolActivationGuard;
import com.nereusstream.pulsar.BookKeeperPrimaryWalCapabilityBinding;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.extensions.BrokerRegistry;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;

/** Publishes and independently verifies the cluster-wide binding, cursor, and generation protocols. */
public final class NereusBrokerCapabilityCoordinator
        implements CursorProtocolActivationGuard,
                GenerationCapabilityReadinessProvider,
                BookKeeperBrokerReadinessProvider {
    public static final String PROPERTY = "nereus.storage-binding-protocol";
    public static final String VERSION = "1";

    private final MetadataStore metadataStore;
    private final Duration operationTimeout;
    private final AtomicBoolean storageInitialized = new AtomicBoolean();
    private final AtomicReference<String> localBrokerId = new AtomicReference<>();
    private final AtomicReference<BrokerRegistry> brokerRegistry = new AtomicReference<>();
    private final AtomicLong brokerRegistryRevision = new AtomicLong();
    private final AtomicReference<CachedGenerationReadiness> generationReadiness =
            new AtomicReference<>();
    private final AtomicReference<CachedBookKeeperReadiness> bookKeeperReadiness =
            new AtomicReference<>();
    private final AtomicReference<BookKeeperPrimaryWalCapabilityBinding> bookKeeperBinding =
            new AtomicReference<>();

    public NereusBrokerCapabilityCoordinator(Duration operationTimeout) {
        this(null, operationTimeout);
    }

    public NereusBrokerCapabilityCoordinator(
            MetadataStore metadataStore, Duration operationTimeout) {
        this.metadataStore = metadataStore;
        this.operationTimeout = java.util.Objects.requireNonNull(operationTimeout, "operationTimeout");
        if (operationTimeout.isZero() || operationTimeout.isNegative()) {
            throw new IllegalArgumentException("operationTimeout must be positive");
        }
        if (metadataStore != null) {
            metadataStore.registerListener(this::handleMetadataStoreNotification);
        }
    }

    public void markStorageInitialized() {
        if (!storageInitialized.compareAndSet(false, true)) {
            throw new IllegalStateException("Nereus storage capability was already initialized");
        }
    }

    public void installBookKeeperPrimaryWalCapability(
            BookKeeperPrimaryWalCapabilityBinding binding) {
        java.util.Objects.requireNonNull(binding, "binding");
        if (storageInitialized.get()) {
            throw new IllegalStateException(
                    "BookKeeper primary-WAL capability cannot change after storage initialization");
        }
        if (!bookKeeperBinding.compareAndSet(null, binding)) {
            throw new IllegalStateException(
                    "BookKeeper primary-WAL capability was already installed");
        }
    }

    public void attachLocalBroker(String brokerId) {
        String exact = java.util.Objects.requireNonNull(brokerId, "brokerId");
        if (exact.isBlank()) {
            throw new IllegalArgumentException("brokerId cannot be blank");
        }
        if (!storageInitialized.get()) {
            throw new IllegalStateException("Nereus storage is not initialized");
        }
        if (!localBrokerId.compareAndSet(null, exact)) {
            throw new IllegalStateException("Nereus local broker is already attached");
        }
    }

    public void attachBrokerRegistry(BrokerRegistry registry) {
        java.util.Objects.requireNonNull(registry, "registry");
        if (!storageInitialized.get()) {
            throw new IllegalStateException("Nereus storage is not initialized");
        }
        if (!brokerRegistry.compareAndSet(null, registry)) {
            throw new IllegalStateException("Nereus broker registry is already attached");
        }
        registry.addListener((ignored, notification) -> {
            brokerRegistryRevision.incrementAndGet();
            generationReadiness.set(null);
            bookKeeperReadiness.set(null);
        });
    }

    public Map<String, String> decorateLookupProperties(Map<String, String> configured) {
        NereusStorageBindingCapability.requireUnreserved(configured);
        if (!storageInitialized.get()) {
            throw new IllegalStateException("Nereus storage is not initialized");
        }
        Map<String, String> properties = new HashMap<>(configured);
        properties.put(PROPERTY, VERSION);
        properties.put(NereusCursorProtocolCapability.PROPERTY, NereusCursorProtocolCapability.VERSION);
        properties.put(NereusGenerationProtocolCapability.PROPERTY, NereusGenerationProtocolCapability.VERSION);
        Optional.ofNullable(bookKeeperBinding.get())
                .map(NereusBookKeeperPrimaryWalCapability::properties)
                .ifPresent(properties::putAll);
        return Map.copyOf(properties);
    }

    /** F2 creation barrier. Cursor capability is deliberately not inferred from this property. */
    public CompletableFuture<Void> requireClusterReady() {
        return requireClusterReady(Map.of(PROPERTY, VERSION));
    }

    /** F3 first-activation barrier requiring both independently versioned protocols. */
    public CompletableFuture<Void> requireCursorClusterReady() {
        return requireClusterReady(Map.of(
                PROPERTY, VERSION,
                NereusCursorProtocolCapability.PROPERTY, NereusCursorProtocolCapability.VERSION));
    }

    /** F4 barrier requiring binding, cursor, and generation protocols under one stable broker epoch. */
    public CompletableFuture<Void> requireGenerationClusterReady() {
        return requireGenerationReadiness().thenApply(ignored -> null);
    }

    /** First-create barrier for BK profiles; Object profiles retain the existing capability path. */
    public CompletableFuture<Void> requireStorageProfileReady(StorageProfile profile) {
        StorageProfile exact = java.util.Objects.requireNonNull(profile, "profile").canonical();
        if (!exact.usesBookKeeperWal()) {
            return CompletableFuture.completedFuture(null);
        }
        BookKeeperPrimaryWalCapabilityBinding binding = bookKeeperBinding.get();
        if (binding == null) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "NEREUS_BOOKKEEPER_CAPABILITY_NOT_READY:LOCAL"));
        }
        return requireClusterReady(
                NereusBookKeeperPrimaryWalCapability.requiredProperties(binding, exact));
    }

    /**
     * Existing-topic writable admission is intentionally local.
     *
     * <p>First-create remains an all-broker stable-snapshot barrier. Reusing that barrier here would stop every
     * capable owner while one broker is rolling or deliberately excluded. The installed binding is immutable for
     * this process and exists only after the exact local BK runtime, namespace and publication activation verified.
     */
    public CompletableFuture<Void> requireLocalStorageProfileReady(StorageProfile profile) {
        StorageProfile exact = java.util.Objects.requireNonNull(profile, "profile").canonical();
        if (!exact.usesBookKeeperWal()) {
            return CompletableFuture.completedFuture(null);
        }
        if (!storageInitialized.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "NEREUS_BOOKKEEPER_CAPABILITY_NOT_READY:LOCAL_REGISTRY"));
        }
        BookKeeperPrimaryWalCapabilityBinding binding = bookKeeperBinding.get();
        if (binding == null) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "NEREUS_BOOKKEEPER_CAPABILITY_NOT_READY:LOCAL"));
        }
        if (metadataStore != null) {
            String brokerId = localBrokerId.get();
            if (brokerId == null) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                        "NEREUS_BOOKKEEPER_CAPABILITY_NOT_READY:LOCAL_REGISTRY"));
            }
            Map<String, String> required =
                    NereusBookKeeperPrimaryWalCapability.requiredProperties(binding, exact);
            return readMetadataBroker(brokerId).thenCompose(registered -> {
                if (registered.isEmpty()
                        || !registered.orElseThrow().persistentTopicsEnabled()) {
                    return CompletableFuture.failedFuture(new IllegalStateException(
                            "NEREUS_BOOKKEEPER_CAPABILITY_NOT_READY:LOCAL_REGISTRY"));
                }
                try {
                    requireProperties(brokerId, registered.orElseThrow(), required);
                    return CompletableFuture.completedFuture(null);
                } catch (Throwable error) {
                    return CompletableFuture.failedFuture(error);
                }
            });
        }
        BrokerRegistry registry = brokerRegistry.get();
        if (registry == null
                || !registry.isStarted()
                || !registry.isRegistered()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "NEREUS_BOOKKEEPER_CAPABILITY_NOT_READY:LOCAL_REGISTRY"));
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Returns the deterministic identity of the two stable all-capable persistent broker snapshots.
     *
     * <p>Any failure clears the process-local cached identity. Broker registry notifications also invalidate it.
     */
    public CompletableFuture<NereusGenerationCapabilityReadiness> requireGenerationReadiness() {
        Map<String, String> required = Map.of(
                PROPERTY, VERSION,
                NereusCursorProtocolCapability.PROPERTY, NereusCursorProtocolCapability.VERSION,
                NereusGenerationProtocolCapability.PROPERTY, NereusGenerationProtocolCapability.VERSION);
        CompletableFuture<NereusGenerationCapabilityReadiness> check =
                requireStableSnapshot(required, "nereus-generation-broker-readiness-v1")
                        .thenApply(snapshot -> {
                    CachedGenerationReadiness cached =
                            new CachedGenerationReadiness(snapshot.readiness(), snapshot.brokerRegistryRevision());
                    if (brokerRegistryRevision.get() != cached.brokerRegistryRevision()) {
                        throw new IllegalStateException("NEREUS_CLUSTER_CAPABILITY_SNAPSHOT_CHANGED");
                    }
                    generationReadiness.set(cached);
                    if (brokerRegistryRevision.get() != cached.brokerRegistryRevision()) {
                        generationReadiness.compareAndSet(cached, null);
                        throw new IllegalStateException("NEREUS_CLUSTER_CAPABILITY_SNAPSHOT_CHANGED");
                    }
                    return snapshot.readiness();
                });
        return check.whenComplete((ignored, error) -> {
            if (error != null) {
                generationReadiness.set(null);
            }
        });
    }

    @Override
    public CompletableFuture<BookKeeperBrokerReadiness>
            requireBookKeeperPrimaryWalReadiness() {
        BookKeeperPrimaryWalCapabilityBinding binding = bookKeeperBinding.get();
        if (binding == null) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "NEREUS_BOOKKEEPER_CAPABILITY_NOT_READY:LOCAL"));
        }
        Map<String, String> required =
                NereusBookKeeperPrimaryWalCapability.requiredProperties(
                        binding, StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT);
        CompletableFuture<BookKeeperBrokerReadiness> check = requireStableSnapshot(
                        required,
                        "nereus-bookkeeper-primary-wal-broker-readiness-v1")
                .thenApply(snapshot -> {
                    NereusGenerationCapabilityReadiness exact = snapshot.readiness();
                    BookKeeperBrokerReadiness readiness = new BookKeeperBrokerReadiness(
                            exact.brokerReadinessEpoch(),
                            new com.nereusstream.api.Checksum(
                                    com.nereusstream.api.ChecksumType.SHA256,
                                    exact.brokerSetSha256()),
                            exact.persistentBrokerCount());
                    CachedBookKeeperReadiness cached = new CachedBookKeeperReadiness(
                            readiness, snapshot.brokerRegistryRevision());
                    if (brokerRegistryRevision.get() != cached.brokerRegistryRevision()) {
                        throw new IllegalStateException(
                                "NEREUS_CLUSTER_CAPABILITY_SNAPSHOT_CHANGED");
                    }
                    bookKeeperReadiness.set(cached);
                    if (brokerRegistryRevision.get() != cached.brokerRegistryRevision()) {
                        bookKeeperReadiness.compareAndSet(cached, null);
                        throw new IllegalStateException(
                                "NEREUS_CLUSTER_CAPABILITY_SNAPSHOT_CHANGED");
                    }
                    return readiness;
                });
        return check.whenComplete((ignored, error) -> {
            if (error != null) {
                bookKeeperReadiness.set(null);
            }
        });
    }

    /**
     * Verifies that a publication mutation is bound to the strongest readiness
     * identity this broker can currently publish.
     *
     * <p>Before the first durable activation, brokers cannot advertise the
     * BookKeeper binding, so the generation-capable broker set is the bootstrap
     * authority. After a restart has installed the durable binding, callers must
     * use the stronger BookKeeper primary-WAL readiness identity.
     */
    public CompletableFuture<Void> requireBookKeeperPublicationReadiness(
            long brokerReadinessEpoch, String brokerReadinessSha256) {
        java.util.Objects.requireNonNull(
                brokerReadinessSha256, "brokerReadinessSha256");
        if (bookKeeperBinding.get() == null) {
            return requireGenerationReadiness().thenAccept(readiness ->
                    requireExactReadiness(
                            brokerReadinessEpoch,
                            brokerReadinessSha256,
                            readiness.brokerReadinessEpoch(),
                            readiness.brokerSetSha256()));
        }
        return requireBookKeeperPrimaryWalReadiness().thenAccept(readiness ->
                requireExactReadiness(
                        brokerReadinessEpoch,
                        brokerReadinessSha256,
                        readiness.brokerReadinessEpoch(),
                        readiness.brokerSetSha256().value()));
    }

    @Override
    public Optional<BookKeeperBrokerReadiness>
            currentBookKeeperPrimaryWalReadiness() {
        if (!localSourceAttached()) {
            bookKeeperReadiness.set(null);
            return Optional.empty();
        }
        CachedBookKeeperReadiness cached = bookKeeperReadiness.get();
        if (cached == null) {
            return Optional.empty();
        }
        if (brokerRegistryRevision.get() != cached.brokerRegistryRevision()) {
            bookKeeperReadiness.compareAndSet(cached, null);
            return Optional.empty();
        }
        return Optional.of(cached.readiness());
    }

    public Optional<NereusGenerationCapabilityReadiness> currentGenerationReadiness() {
        if (!localSourceAttached()) {
            generationReadiness.set(null);
            return Optional.empty();
        }
        CachedGenerationReadiness cached = generationReadiness.get();
        if (cached == null) {
            return Optional.empty();
        }
        if (brokerRegistryRevision.get() != cached.brokerRegistryRevision()) {
            generationReadiness.compareAndSet(cached, null);
            return Optional.empty();
        }
        return Optional.of(cached.readiness());
    }

    @Override
    public CompletableFuture<GenerationCapabilityReadiness>
            requireGenerationCapabilityReadiness() {
        return requireGenerationReadiness()
                .thenApply(NereusGenerationCapabilityReadiness::toCore);
    }

    @Override
    public Optional<GenerationCapabilityReadiness>
            currentGenerationCapabilityReadiness() {
        return currentGenerationReadiness()
                .map(NereusGenerationCapabilityReadiness::toCore);
    }

    @Override
    public CompletableFuture<Void> acquireFirstActivationPermit(CursorLedgerIdentity ledger) {
        java.util.Objects.requireNonNull(ledger, "ledger");
        return requireCursorClusterReady();
    }

    private CompletableFuture<Void> requireClusterReady(Map<String, String> requiredProperties) {
        return requireStableSnapshot(
                        requiredProperties,
                        "nereus-generation-broker-readiness-v1")
                .thenApply(ignored -> null);
    }

    private CompletableFuture<CapabilitySnapshot> requireStableSnapshot(
            Map<String, String> requiredProperties,
            String readinessDomain) {
        if (!storageInitialized.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("NEREUS_CLUSTER_CAPABILITY_NOT_READY:LOCAL"));
        }
        if (!localSourceAttached()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("NEREUS_CLUSTER_CAPABILITY_NOT_READY:LOCAL_REGISTRY"));
        }
        long initialBrokerRegistryRevision = brokerRegistryRevision.get();
        CompletableFuture<CapabilitySnapshot> check = availableBrokerData()
                .thenApply(available -> snapshot(
                        available,
                        requiredProperties,
                        readinessDomain,
                        localBrokerId.get()))
                .thenCompose(first -> availableBrokerData().thenApply(second -> {
                    CapabilitySnapshot capableSecond = snapshot(
                            second,
                            requiredProperties,
                            readinessDomain,
                            localBrokerId.get());
                    if (!first.brokers().keySet().equals(capableSecond.brokers().keySet())) {
                        throw new IllegalStateException("NEREUS_CLUSTER_CAPABILITY_BROKER_SET_CHANGED");
                    }
                    if (!first.readiness().equals(capableSecond.readiness())) {
                        throw new IllegalStateException("NEREUS_CLUSTER_CAPABILITY_SNAPSHOT_CHANGED");
                    }
                    if (!localSourceAttached()) {
                        throw new IllegalStateException(
                                "NEREUS_CLUSTER_CAPABILITY_NOT_READY:LOCAL_REGISTRY");
                    }
                    if (brokerRegistryRevision.get() != initialBrokerRegistryRevision) {
                        throw new IllegalStateException("NEREUS_CLUSTER_CAPABILITY_SNAPSHOT_CHANGED");
                    }
                    return capableSecond.withBrokerRegistryRevision(initialBrokerRegistryRevision);
                }));
        return check.orTimeout(operationTimeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    private static CapabilitySnapshot snapshot(
            Map<String, BrokerCapabilityData> available,
            Map<String, String> requiredProperties,
            String readinessDomain,
            String requiredLocalBrokerId) {
        java.util.Objects.requireNonNull(available, "available");
        java.util.Objects.requireNonNull(requiredProperties, "requiredProperties");
        Map<String, BrokerCapabilityData> persistent = new HashMap<>();
        available.forEach((brokerId, data) -> {
            if (data != null && data.persistentTopicsEnabled()) {
                persistent.put(brokerId, data);
            }
        });
        if (persistent.isEmpty()) {
            throw new IllegalStateException("NEREUS_CLUSTER_CAPABILITY_NOT_READY:EMPTY");
        }
        if (requiredLocalBrokerId != null
                && !persistent.containsKey(requiredLocalBrokerId)) {
            throw new IllegalStateException(
                    "NEREUS_CLUSTER_CAPABILITY_NOT_READY:LOCAL_REGISTRY");
        }
        persistent.forEach((brokerId, data) ->
                requireProperties(brokerId, data, requiredProperties));
        Map<String, BrokerCapabilityData> canonical = Map.copyOf(persistent);
        return new CapabilitySnapshot(
                canonical,
                readiness(canonical, requiredProperties, readinessDomain));
    }

    private static void requireProperties(
            String brokerId,
            BrokerCapabilityData data,
            Map<String, String> requiredProperties) {
        for (Map.Entry<String, String> required : requiredProperties.entrySet()) {
            if (!required.getValue().equals(
                    data.properties().get(required.getKey()))) {
                throw new IllegalStateException(
                        "NEREUS_CLUSTER_CAPABILITY_NOT_READY:"
                                + brokerId
                                + ":"
                                + required.getKey());
            }
        }
    }

    private static void requireExactReadiness(
            long requestedEpoch,
            String requestedSha256,
            long currentEpoch,
            String currentSha256) {
        if (requestedEpoch != currentEpoch || !requestedSha256.equals(currentSha256)) {
            throw new IllegalStateException(
                    "NEREUS_BOOKKEEPER_PUBLICATION_READINESS_STALE");
        }
    }

    private static NereusGenerationCapabilityReadiness readiness(
            Map<String, BrokerCapabilityData> brokers,
            Map<String, String> requiredProperties,
            String readinessDomain) {
        MessageDigest digest = sha256();
        add(digest, java.util.Objects.requireNonNull(readinessDomain, "readinessDomain"));
        TreeMap<String, String> required = new TreeMap<>(requiredProperties);
        brokers.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.naturalOrder()))
                .forEach(entry -> {
                    BrokerCapabilityData data = entry.getValue();
                    add(digest, entry.getKey());
                    add(digest, data.brokerId());
                    add(digest, Long.toString(data.startTimestamp()));
                    required.forEach((key, value) -> {
                        add(digest, key);
                        add(digest, value);
                    });
                });
        byte[] bytes = digest.digest();
        String sha256 = HexFormat.of().formatHex(bytes);
        long epoch = ByteBuffer.wrap(bytes)
                .order(ByteOrder.BIG_ENDIAN)
                .getLong()
                & Long.MAX_VALUE;
        if (epoch == 0) {
            epoch = 1;
        }
        return new NereusGenerationCapabilityReadiness(epoch, sha256, brokers.size());
    }

    private boolean localSourceAttached() {
        if (!storageInitialized.get()) {
            return false;
        }
        if (metadataStore != null) {
            return localBrokerId.get() != null;
        }
        BrokerRegistry registry = brokerRegistry.get();
        return registry != null && registry.isStarted() && registry.isRegistered();
    }

    private CompletableFuture<Map<String, BrokerCapabilityData>> availableBrokerData() {
        if (metadataStore != null) {
            return readMetadataBrokers();
        }
        BrokerRegistry registry = brokerRegistry.get();
        if (registry == null) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "NEREUS_CLUSTER_CAPABILITY_NOT_READY:LOCAL_REGISTRY"));
        }
        return registry.getAvailableBrokerLookupDataAsync().thenApply(available -> {
            Map<String, BrokerCapabilityData> converted = new HashMap<>();
            available.forEach((brokerId, data) -> {
                if (data != null) {
                    converted.put(
                            brokerId,
                            new BrokerCapabilityData(
                                    data.brokerId(),
                                    data.persistentTopicsEnabled(),
                                    data.startTimestamp(),
                                    data.properties() == null
                                            ? Map.of()
                                            : Map.copyOf(data.properties())));
                }
            });
            return Map.copyOf(converted);
        });
    }

    private CompletableFuture<Map<String, BrokerCapabilityData>> readMetadataBrokers() {
        return metadataStore.sync(LoadManager.LOADBALANCE_BROKERS_ROOT)
                .thenCompose(ignored -> metadataStore.getChildren(
                        LoadManager.LOADBALANCE_BROKERS_ROOT))
                .thenCompose(children -> {
                    List<CompletableFuture<Optional<Map.Entry<String, BrokerCapabilityData>>>>
                            reads = new ArrayList<>(children.size());
                    for (String brokerId : children) {
                        reads.add(readMetadataBroker(brokerId)
                                .thenApply(value -> value.map(
                                        data -> Map.entry(brokerId, data))));
                    }
                    return CompletableFuture.allOf(
                                    reads.toArray(CompletableFuture[]::new))
                            .thenApply(ignored -> {
                                Map<String, BrokerCapabilityData> available =
                                        new HashMap<>();
                                for (CompletableFuture<
                                        Optional<Map.Entry<String, BrokerCapabilityData>>>
                                        read : reads) {
                                    read.join().ifPresent(entry ->
                                            available.put(
                                                    entry.getKey(),
                                                    entry.getValue()));
                                }
                                return Map.copyOf(available);
                            });
                });
    }

    private CompletableFuture<Optional<BrokerCapabilityData>> readMetadataBroker(
            String brokerId) {
        String path = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + brokerId;
        return metadataStore.sync(path)
                .thenCompose(ignored -> metadataStore.get(path))
                .thenApply(result -> result.map(value ->
                        decodeBrokerData(brokerId, value.getValue())));
    }

    private static BrokerCapabilityData decodeBrokerData(
            String pathBrokerId, byte[] value) {
        try {
            JsonNode root = ObjectMapperFactory.getMapper()
                    .getObjectMapper()
                    .readTree(value);
            String brokerId = requiredText(root, "brokerId");
            if (!pathBrokerId.equals(brokerId)) {
                throw new IllegalStateException(
                        "broker lookup key does not match brokerId");
            }
            JsonNode persistent = root.get("persistentTopicsEnabled");
            JsonNode started = root.get("startTimestamp");
            if (persistent == null
                    || !persistent.isBoolean()
                    || started == null
                    || !started.canConvertToLong()
                    || started.longValue() <= 0) {
                throw new IllegalStateException(
                        "broker lookup record lacks persistent/start identity");
            }
            Map<String, String> properties = new HashMap<>();
            JsonNode configured = root.get("properties");
            if (configured != null && configured.isObject()) {
                configured.fields().forEachRemaining(entry -> {
                    if (!entry.getValue().isTextual()) {
                        throw new IllegalStateException(
                                "broker lookup property is not textual: "
                                        + entry.getKey());
                    }
                    properties.put(entry.getKey(), entry.getValue().textValue());
                });
            }
            return new BrokerCapabilityData(
                    brokerId,
                    persistent.booleanValue(),
                    started.longValue(),
                    Map.copyOf(properties));
        } catch (IOException | RuntimeException error) {
            throw new IllegalStateException(
                    "NEREUS_CLUSTER_CAPABILITY_INVALID:" + pathBrokerId,
                    error);
        }
    }

    private static String requiredText(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isTextual() || value.textValue().isBlank()) {
            throw new IllegalStateException(
                    "broker lookup record lacks " + field);
        }
        return value.textValue();
    }

    private void handleMetadataStoreNotification(Notification notification) {
        String path = notification.getPath();
        String root = LoadManager.LOADBALANCE_BROKERS_ROOT;
        if (path.equals(root) || path.startsWith(root + "/")) {
            brokerRegistryRevision.incrementAndGet();
            generationReadiness.set(null);
            bookKeeperReadiness.set(null);
        }
    }

    private static void add(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(bytes.length)
                .array());
        digest.update(bytes);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private record CapabilitySnapshot(
            Map<String, BrokerCapabilityData> brokers,
            NereusGenerationCapabilityReadiness readiness,
            long brokerRegistryRevision) {
        private CapabilitySnapshot(
                Map<String, BrokerCapabilityData> brokers,
                NereusGenerationCapabilityReadiness readiness) {
            this(brokers, readiness, -1);
        }

        private CapabilitySnapshot withBrokerRegistryRevision(long revision) {
            return new CapabilitySnapshot(brokers, readiness, revision);
        }
    }

    private record BrokerCapabilityData(
            String brokerId,
            boolean persistentTopicsEnabled,
            long startTimestamp,
            Map<String, String> properties) {
    }

    private record CachedGenerationReadiness(
            NereusGenerationCapabilityReadiness readiness,
            long brokerRegistryRevision) {
    }

    private record CachedBookKeeperReadiness(
            BookKeeperBrokerReadiness readiness,
            long brokerRegistryRevision) {
    }
}
