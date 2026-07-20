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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.pulsar.BookKeeperPrimaryWalCapabilityBinding;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.loadbalance.extensions.BrokerRegistry;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.testng.annotations.Test;

public class NereusBookKeeperPrimaryWalCapabilityTest {
    @Test
    public void publishesOnlyTheExactVerifiedLocalBindingAndProtectsReservedProperties() {
        NereusBrokerCapabilityCoordinator coordinator = coordinator();
        BookKeeperPrimaryWalCapabilityBinding binding = binding("11", "22");
        coordinator.installBookKeeperPrimaryWalCapability(binding);
        coordinator.markStorageInitialized();
        coordinator.attachBrokerRegistry(readyRegistry());

        assertThat(coordinator.decorateLookupProperties(Map.of()))
                .containsAllEntriesOf(NereusBookKeeperPrimaryWalCapability.properties(binding));
        assertThatThrownBy(() -> coordinator.installBookKeeperPrimaryWalCapability(binding))
                .hasMessageContaining("cannot change");
        for (String property : NereusBookKeeperPrimaryWalCapability.properties(binding).keySet()) {
            assertThatThrownBy(() -> NereusStorageBindingCapability.requireUnreserved(
                            Map.of(property, "spoofed")))
                    .hasMessage(property + " is reserved by the broker");
        }
    }

    @Test
    public void firstCreateRequiresTwoStableAllBrokerSnapshotsWithTheExactBinding() {
        NereusBrokerCapabilityCoordinator coordinator = coordinator();
        BookKeeperPrimaryWalCapabilityBinding binding = binding("11", "22");
        BrokerRegistry registry = readyRegistry();
        coordinator.installBookKeeperPrimaryWalCapability(binding);
        coordinator.markStorageInitialized();
        coordinator.attachBrokerRegistry(registry);
        Map<String, String> exact = allBaseCapabilities();
        exact = merge(exact, NereusBookKeeperPrimaryWalCapability.properties(binding));
        BrokerLookupData capable = lookupData("broker-a", exact);
        when(registry.getAvailableBrokerLookupDataAsync())
                .thenReturn(CompletableFuture.completedFuture(Map.of("a", capable)));

        coordinator.requireStorageProfileReady(StorageProfile.BOOKKEEPER_WAL_ONLY).join();
        coordinator.requireStorageProfileReady(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT).join();
        coordinator.requireStorageProfileReady(StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT).join();
        coordinator.requireStorageProfileReady(StorageProfile.OBJECT_WAL_SYNC_OBJECT).join();

        Map<String, String> drifted = new HashMap<>(exact);
        drifted.put(NereusBookKeeperPrimaryWalCapability.NAMESPACE_PROPERTY, "33".repeat(32));
        when(registry.getAvailableBrokerLookupDataAsync()).thenReturn(
                CompletableFuture.completedFuture(Map.of("a", lookupData("broker-a", drifted))));
        assertThatThrownBy(() -> coordinator
                        .requireStorageProfileReady(StorageProfile.BOOKKEEPER_WAL_ONLY)
                        .join())
                .hasRootCauseMessage(
                        "NEREUS_CLUSTER_CAPABILITY_NOT_READY:a:nereus.bookkeeper-ledger-namespace");
    }

    @Test
    public void syncRequiresTheIndependentRequiredObjectCompletionCapability() {
        NereusBrokerCapabilityCoordinator coordinator = coordinator();
        BookKeeperPrimaryWalCapabilityBinding binding = binding("11", "22");
        BrokerRegistry registry = readyRegistry();
        coordinator.installBookKeeperPrimaryWalCapability(binding);
        coordinator.markStorageInitialized();
        coordinator.attachBrokerRegistry(registry);
        Map<String, String> properties = merge(
                allBaseCapabilities(),
                NereusBookKeeperPrimaryWalCapability.properties(binding));
        properties = new HashMap<>(properties);
        properties.remove(NereusBookKeeperPrimaryWalCapability.REQUIRED_OBJECT_GENERATION_PROPERTY);
        BrokerLookupData withoutSync = lookupData("broker-a", properties);
        when(registry.getAvailableBrokerLookupDataAsync())
                .thenReturn(CompletableFuture.completedFuture(Map.of("a", withoutSync)));

        coordinator.requireStorageProfileReady(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT).join();
        assertThatThrownBy(() -> coordinator
                        .requireStorageProfileReady(StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT)
                        .join())
                .hasRootCauseMessage(
                        "NEREUS_CLUSTER_CAPABILITY_NOT_READY:a:"
                                + NereusBookKeeperPrimaryWalCapability.REQUIRED_OBJECT_GENERATION_PROPERTY);
    }

    @Test
    public void objectEnabledBookKeeperProfilesRequireGenerationInTheSameStableSnapshot() {
        NereusBrokerCapabilityCoordinator coordinator = coordinator();
        BookKeeperPrimaryWalCapabilityBinding binding = binding("11", "22");
        BrokerRegistry registry = readyRegistry();
        coordinator.installBookKeeperPrimaryWalCapability(binding);
        coordinator.markStorageInitialized();
        coordinator.attachBrokerRegistry(registry);
        Map<String, String> properties = new HashMap<>(merge(
                allBaseCapabilities(),
                NereusBookKeeperPrimaryWalCapability.properties(binding)));
        properties.remove(NereusGenerationProtocolCapability.PROPERTY);
        BrokerLookupData withoutGeneration = lookupData("broker-a", properties);
        when(registry.getAvailableBrokerLookupDataAsync())
                .thenReturn(CompletableFuture.completedFuture(Map.of("a", withoutGeneration)));

        coordinator.requireStorageProfileReady(StorageProfile.BOOKKEEPER_WAL_ONLY).join();
        assertThatThrownBy(() -> coordinator
                        .requireStorageProfileReady(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT)
                        .join())
                .hasRootCauseMessage(
                        "NEREUS_CLUSTER_CAPABILITY_NOT_READY:a:"
                                + NereusGenerationProtocolCapability.PROPERTY);
    }

    private static NereusBrokerCapabilityCoordinator coordinator() {
        return new NereusBrokerCapabilityCoordinator(Duration.ofSeconds(5));
    }

    private static BookKeeperPrimaryWalCapabilityBinding binding(String config, String namespace) {
        return new BookKeeperPrimaryWalCapabilityBinding(
                1,
                new Checksum(ChecksumType.SHA256, config.repeat(32)),
                new Checksum(ChecksumType.SHA256, namespace.repeat(32)),
                1);
    }

    private static BrokerRegistry readyRegistry() {
        BrokerRegistry registry = mock(BrokerRegistry.class);
        when(registry.isStarted()).thenReturn(true);
        when(registry.isRegistered()).thenReturn(true);
        return registry;
    }

    private static Map<String, String> allBaseCapabilities() {
        return Map.of(
                NereusStorageBindingCapability.PROPERTY,
                NereusStorageBindingCapability.VERSION,
                NereusCursorProtocolCapability.PROPERTY,
                NereusCursorProtocolCapability.VERSION,
                NereusGenerationProtocolCapability.PROPERTY,
                NereusGenerationProtocolCapability.VERSION);
    }

    private static Map<String, String> merge(
            Map<String, String> first,
            Map<String, String> second) {
        Map<String, String> merged = new HashMap<>(first);
        merged.putAll(second);
        return Map.copyOf(merged);
    }

    private static BrokerLookupData lookupData(String brokerId, Map<String, String> properties) {
        return new BrokerLookupData(
                brokerId,
                "http://" + brokerId,
                null,
                "pulsar://" + brokerId,
                null,
                Map.of(),
                Map.of(),
                true,
                true,
                "load-manager",
                1L,
                "version",
                properties);
    }
}
