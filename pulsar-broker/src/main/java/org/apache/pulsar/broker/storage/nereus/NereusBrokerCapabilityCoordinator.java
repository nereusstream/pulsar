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

import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import com.nereusstream.managedledger.cursor.CursorProtocolActivationGuard;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.loadbalance.extensions.BrokerRegistry;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;

/** Publishes and independently verifies the cluster-wide binding and cursor protocols. */
public final class NereusBrokerCapabilityCoordinator implements CursorProtocolActivationGuard {
    public static final String PROPERTY = "nereus.storage-binding-protocol";
    public static final String VERSION = "1";

    private final Duration operationTimeout;
    private final AtomicBoolean storageInitialized = new AtomicBoolean();
    private final AtomicReference<BrokerRegistry> brokerRegistry = new AtomicReference<>();

    public NereusBrokerCapabilityCoordinator(Duration operationTimeout) {
        this.operationTimeout = java.util.Objects.requireNonNull(operationTimeout, "operationTimeout");
        if (operationTimeout.isZero() || operationTimeout.isNegative()) {
            throw new IllegalArgumentException("operationTimeout must be positive");
        }
    }

    public void markStorageInitialized() {
        if (!storageInitialized.compareAndSet(false, true)) {
            throw new IllegalStateException("Nereus storage capability was already initialized");
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
    }

    public Map<String, String> decorateLookupProperties(Map<String, String> configured) {
        NereusStorageBindingCapability.requireUnreserved(configured);
        if (!storageInitialized.get()) {
            throw new IllegalStateException("Nereus storage is not initialized");
        }
        if (brokerRegistry.get() == null) {
            throw new IllegalStateException("Nereus broker registry is not attached");
        }
        Map<String, String> properties = new HashMap<>(configured);
        properties.put(PROPERTY, VERSION);
        properties.put(NereusCursorProtocolCapability.PROPERTY, NereusCursorProtocolCapability.VERSION);
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

    @Override
    public CompletableFuture<Void> acquireFirstActivationPermit(CursorLedgerIdentity ledger) {
        java.util.Objects.requireNonNull(ledger, "ledger");
        return requireCursorClusterReady();
    }

    private CompletableFuture<Void> requireClusterReady(Map<String, String> requiredProperties) {
        BrokerRegistry registry = brokerRegistry.get();
        if (!storageInitialized.get() || registry == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("NEREUS_CLUSTER_CAPABILITY_NOT_READY:LOCAL"));
        }
        if (!registry.isStarted() || !registry.isRegistered()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("NEREUS_CLUSTER_CAPABILITY_NOT_READY:LOCAL_REGISTRY"));
        }
        CompletableFuture<Void> check = registry.getAvailableBrokerLookupDataAsync()
                .thenApply(available -> requireCapableBrokers(available, requiredProperties))
                .thenCompose(first -> registry.getAvailableBrokerLookupDataAsync().thenAccept(second -> {
                    Map<String, BrokerLookupData> capableSecond = requireCapableBrokers(second, requiredProperties);
                    if (!first.keySet().equals(capableSecond.keySet())) {
                        throw new IllegalStateException("NEREUS_CLUSTER_CAPABILITY_BROKER_SET_CHANGED");
                    }
                    if (!registry.isStarted() || !registry.isRegistered()) {
                        throw new IllegalStateException(
                                "NEREUS_CLUSTER_CAPABILITY_NOT_READY:LOCAL_REGISTRY");
                    }
                }));
        return check.orTimeout(operationTimeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    private static Map<String, BrokerLookupData> requireCapableBrokers(
            Map<String, BrokerLookupData> available,
            Map<String, String> requiredProperties) {
        java.util.Objects.requireNonNull(available, "available");
        java.util.Objects.requireNonNull(requiredProperties, "requiredProperties");
        Map<String, BrokerLookupData> persistent = new HashMap<>();
        available.forEach((brokerId, data) -> {
            if (data != null && data.persistentTopicsEnabled()) {
                persistent.put(brokerId, data);
            }
        });
        if (persistent.isEmpty()) {
            throw new IllegalStateException("NEREUS_CLUSTER_CAPABILITY_NOT_READY:EMPTY");
        }
        persistent.forEach((brokerId, data) -> {
            for (Map.Entry<String, String> required : requiredProperties.entrySet()) {
                if (data.properties() == null
                        || !required.getValue().equals(data.properties().get(required.getKey()))) {
                    throw new IllegalStateException(
                            "NEREUS_CLUSTER_CAPABILITY_NOT_READY:" + brokerId + ":" + required.getKey());
                }
            }
        });
        return Map.copyOf(persistent);
    }
}
