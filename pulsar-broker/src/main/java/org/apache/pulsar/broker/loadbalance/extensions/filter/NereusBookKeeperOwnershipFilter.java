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
package org.apache.pulsar.broker.loadbalance.extensions.filter;

import com.nereusstream.api.StorageProfile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.storage.nereus.NereusBookKeeperPrimaryWalCapability;
import org.apache.pulsar.broker.storage.nereus.NereusCursorProtocolCapability;
import org.apache.pulsar.broker.storage.nereus.NereusGenerationProtocolCapability;
import org.apache.pulsar.broker.storage.nereus.NereusManagedLedgerStorage;
import org.apache.pulsar.broker.storage.nereus.NereusStorageBindingCapability;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;

/** Excludes brokers that cannot interpret every durable BK profile in a service unit's namespace. */
public final class NereusBookKeeperOwnershipFilter implements BrokerFilter {
    private final Function<NamespaceName, CompletableFuture<Optional<StorageProfile>>>
            profileLoader;

    public NereusBookKeeperOwnershipFilter(
            NereusManagedLedgerStorage storage,
            int maxEntries,
            int maxPendingOperations) {
        this(
                namespace -> storage.requiredBookKeeperOwnershipProfile(
                        namespace, maxEntries, maxPendingOperations));
    }

    NereusBookKeeperOwnershipFilter(
            Function<NamespaceName, CompletableFuture<Optional<StorageProfile>>>
                    profileLoader) {
        this.profileLoader = java.util.Objects.requireNonNull(
                profileLoader, "profileLoader");
    }

    @Override
    public String name() {
        return "nereus-bookkeeper-ownership";
    }

    @Override
    public CompletableFuture<Map<String, BrokerLookupData>> filterAsync(
            Map<String, BrokerLookupData> brokers,
            ServiceUnitId serviceUnit,
            LoadManagerContext context) {
        java.util.Objects.requireNonNull(brokers, "brokers");
        java.util.Objects.requireNonNull(serviceUnit, "serviceUnit");
        final CompletableFuture<Optional<StorageProfile>> loaded;
        try {
            loaded = java.util.Objects.requireNonNull(
                    profileLoader.apply(serviceUnit.getNamespaceObject()),
                    "profileLoader result");
        } catch (Throwable failure) {
            brokers.clear();
            return CompletableFuture.completedFuture(brokers);
        }
        return loaded.handle((profile, failure) -> {
            if (failure != null) {
                brokers.clear();
                return brokers;
            }
            try {
                Optional<StorageProfile> exact = java.util.Objects.requireNonNull(
                        profile, "profileLoader value");
                if (exact.isEmpty()) {
                    return brokers;
                }
                StorageProfile durableProfile = exact.orElseThrow().canonical();
                List<String> keys = requiredKeys(durableProfile);
                Set<Map<String, String>> signatures = new HashSet<>();
                brokers.values().stream()
                        .map(broker -> signature(broker, keys))
                        .flatMap(Optional::stream)
                        .forEach(signatures::add);
                if (signatures.size() != 1) {
                    brokers.clear();
                    return brokers;
                }
                Map<String, String> authoritative = signatures.iterator().next();
                brokers.entrySet().removeIf(entry -> !signature(entry.getValue(), keys)
                        .filter(authoritative::equals)
                        .isPresent());
                return brokers;
            } catch (Throwable invalidRequirement) {
                brokers.clear();
                return brokers;
            }
        });
    }

    private static Optional<Map<String, String>> signature(
            BrokerLookupData broker, List<String> keys) {
        if (broker == null || !broker.persistentTopicsEnabled()
                || broker.properties() == null) {
            return Optional.empty();
        }
        Map<String, String> signature = new HashMap<>();
        for (String key : keys) {
            String value = broker.properties().get(key);
            if (value == null || value.isBlank()) {
                return Optional.empty();
            }
            signature.put(key, value);
        }
        if (!NereusStorageBindingCapability.VERSION.equals(
                        signature.get(NereusStorageBindingCapability.PROPERTY))
                || !NereusCursorProtocolCapability.VERSION.equals(
                        signature.get(NereusCursorProtocolCapability.PROPERTY))
                || !"1".equals(signature.get(
                        NereusBookKeeperPrimaryWalCapability.PROTOCOL_PROPERTY))) {
            return Optional.empty();
        }
        if (keys.contains(NereusGenerationProtocolCapability.PROPERTY)
                && !NereusGenerationProtocolCapability.VERSION.equals(
                        signature.get(NereusGenerationProtocolCapability.PROPERTY))) {
            return Optional.empty();
        }
        if (keys.contains(NereusBookKeeperPrimaryWalCapability
                        .REQUIRED_OBJECT_GENERATION_PROPERTY)
                && !"1".equals(signature.get(NereusBookKeeperPrimaryWalCapability
                        .REQUIRED_OBJECT_GENERATION_PROPERTY))) {
            return Optional.empty();
        }
        return Optional.of(Map.copyOf(signature));
    }

    private static List<String> requiredKeys(StorageProfile profile) {
        java.util.ArrayList<String> keys = new java.util.ArrayList<>(List.of(
                NereusStorageBindingCapability.PROPERTY,
                NereusCursorProtocolCapability.PROPERTY,
                NereusBookKeeperPrimaryWalCapability.PROTOCOL_PROPERTY,
                NereusBookKeeperPrimaryWalCapability.CONFIGURATION_PROPERTY,
                NereusBookKeeperPrimaryWalCapability.NAMESPACE_PROPERTY,
                NereusBookKeeperPrimaryWalCapability.ACTIVATION_PROPERTY));
        if (profile.objectMaterializationEnabled()) {
            keys.add(NereusGenerationProtocolCapability.PROPERTY);
        }
        if (profile == StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT) {
            keys.add(NereusBookKeeperPrimaryWalCapability
                    .REQUIRED_OBJECT_GENERATION_PROPERTY);
        }
        return List.copyOf(keys);
    }
}
