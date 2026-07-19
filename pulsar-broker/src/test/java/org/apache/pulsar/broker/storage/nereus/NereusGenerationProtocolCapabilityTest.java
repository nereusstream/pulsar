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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.pulsar.broker.loadbalance.extensions.BrokerRegistry;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.metadata.api.NotificationType;
import org.testng.annotations.Test;

public class NereusGenerationProtocolCapabilityTest {
    @Test
    public void readinessIdentityRejectsNonCanonicalFields() {
        String digest = "80806f90349e89afb16f65d2e90f06339f48babe836f9954ad41fefc2869ab75";

        assertThatThrownBy(() -> new NereusGenerationCapabilityReadiness(-1, digest, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("brokerReadinessEpoch must be non-negative");
        assertThatThrownBy(() -> new NereusGenerationCapabilityReadiness(1, digest.toUpperCase(), 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("brokerSetSha256 must be lowercase SHA-256");
        assertThatThrownBy(() -> new NereusGenerationCapabilityReadiness(1, digest, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("persistentBrokerCount must be positive");
    }

    @Test
    public void generationReadinessRequiresAllThreeProtocolVersions() {
        NereusBrokerCapabilityCoordinator coordinator = coordinator();
        BrokerRegistry registry = readyRegistry();
        coordinator.markStorageInitialized();
        coordinator.attachBrokerRegistry(registry);
        BrokerLookupData cursorOnly = lookupData(
                "broker", 10, Map.of(
                        NereusStorageBindingCapability.PROPERTY,
                        NereusStorageBindingCapability.VERSION,
                        NereusCursorProtocolCapability.PROPERTY,
                        NereusCursorProtocolCapability.VERSION));
        when(registry.getAvailableBrokerLookupDataAsync())
                .thenReturn(CompletableFuture.completedFuture(Map.of("broker", cursorOnly)));

        assertThatThrownBy(() -> coordinator.requireGenerationClusterReady().join())
                .hasRootCauseMessage(
                        "NEREUS_CLUSTER_CAPABILITY_NOT_READY:broker:nereus.generation-protocol");
        assertThat(coordinator.currentGenerationReadiness()).isEmpty();
    }

    @Test
    public void stableSnapshotProducesOrderIndependentFrozenReadiness() {
        NereusBrokerCapabilityCoordinator coordinator = coordinator();
        BrokerRegistry registry = readyRegistry();
        coordinator.markStorageInitialized();
        coordinator.attachBrokerRegistry(registry);
        BrokerLookupData firstBroker = lookupData("broker-a", 10, allCapabilities());
        BrokerLookupData secondBroker = lookupData("broker-b", 20, allCapabilities());
        LinkedHashMap<String, BrokerLookupData> first = new LinkedHashMap<>();
        first.put("b", secondBroker);
        first.put("a", firstBroker);
        LinkedHashMap<String, BrokerLookupData> second = new LinkedHashMap<>();
        second.put("a", firstBroker);
        second.put("b", secondBroker);
        when(registry.getAvailableBrokerLookupDataAsync()).thenReturn(
                CompletableFuture.completedFuture(first),
                CompletableFuture.completedFuture(second));

        NereusGenerationCapabilityReadiness readiness =
                coordinator.requireGenerationReadiness().join();

        assertThat(readiness.persistentBrokerCount()).isEqualTo(2);
        assertThat(readiness.brokerReadinessEpoch()).isEqualTo(36151462167742895L);
        assertThat(readiness.brokerSetSha256())
                .isEqualTo("80806f90349e89afb16f65d2e90f06339f48babe836f9954ad41fefc2869ab75");
        assertThat(coordinator.currentGenerationReadiness()).contains(readiness);
        assertThat(coordinator.currentGenerationCapabilityReadiness())
                .contains(readiness.toCore());
    }

    @Test
    public void sameBrokerIdRestartBetweenSnapshotsInvalidatesReadiness() {
        NereusBrokerCapabilityCoordinator coordinator = coordinator();
        BrokerRegistry registry = readyRegistry();
        coordinator.markStorageInitialized();
        coordinator.attachBrokerRegistry(registry);
        BrokerLookupData before = lookupData("broker", 10, allCapabilities());
        BrokerLookupData restarted = lookupData("broker", 11, allCapabilities());
        when(registry.getAvailableBrokerLookupDataAsync()).thenReturn(
                CompletableFuture.completedFuture(Map.of("broker", before)),
                CompletableFuture.completedFuture(Map.of("broker", restarted)));

        assertThatThrownBy(() -> coordinator.requireGenerationReadiness().join())
                .hasRootCauseMessage("NEREUS_CLUSTER_CAPABILITY_SNAPSHOT_CHANGED");
        assertThat(coordinator.currentGenerationReadiness()).isEmpty();
    }

    @Test
    public void registryNotificationInvalidatesCachedGenerationEpoch() {
        NereusBrokerCapabilityCoordinator coordinator = coordinator();
        BrokerRegistry registry = readyRegistry();
        AtomicReference<BiConsumer<String, NotificationType>> listener = new AtomicReference<>();
        doAnswer(invocation -> {
            listener.set(invocation.getArgument(0));
            return null;
        }).when(registry).addListener(any());
        coordinator.markStorageInitialized();
        coordinator.attachBrokerRegistry(registry);
        BrokerLookupData capable = lookupData("broker", 10, allCapabilities());
        when(registry.getAvailableBrokerLookupDataAsync())
                .thenReturn(CompletableFuture.completedFuture(Map.of("broker", capable)));

        coordinator.requireGenerationReadiness().join();
        assertThat(coordinator.currentGenerationReadiness()).isPresent();

        listener.get().accept("broker", NotificationType.Modified);

        assertThat(coordinator.currentGenerationReadiness()).isEmpty();
    }

    @Test
    public void registryNotificationBetweenEqualSnapshotsRejectsGenerationEpoch() {
        NereusBrokerCapabilityCoordinator coordinator = coordinator();
        BrokerRegistry registry = readyRegistry();
        AtomicReference<BiConsumer<String, NotificationType>> listener = new AtomicReference<>();
        doAnswer(invocation -> {
            listener.set(invocation.getArgument(0));
            return null;
        }).when(registry).addListener(any());
        coordinator.markStorageInitialized();
        coordinator.attachBrokerRegistry(registry);
        BrokerLookupData capable = lookupData("broker", 10, allCapabilities());
        when(registry.getAvailableBrokerLookupDataAsync())
                .thenReturn(CompletableFuture.completedFuture(Map.of("broker", capable)))
                .thenAnswer(invocation -> {
                    listener.get().accept("broker", NotificationType.Modified);
                    return CompletableFuture.completedFuture(Map.of("broker", capable));
                });

        assertThatThrownBy(() -> coordinator.requireGenerationReadiness().join())
                .hasRootCauseMessage("NEREUS_CLUSTER_CAPABILITY_SNAPSHOT_CHANGED");
        assertThat(coordinator.currentGenerationReadiness()).isEmpty();
    }

    private static NereusBrokerCapabilityCoordinator coordinator() {
        return new NereusBrokerCapabilityCoordinator(Duration.ofSeconds(5));
    }

    private static BrokerRegistry readyRegistry() {
        BrokerRegistry registry = mock(BrokerRegistry.class);
        when(registry.isStarted()).thenReturn(true);
        when(registry.isRegistered()).thenReturn(true);
        return registry;
    }

    private static Map<String, String> allCapabilities() {
        return Map.of(
                NereusStorageBindingCapability.PROPERTY,
                NereusStorageBindingCapability.VERSION,
                NereusCursorProtocolCapability.PROPERTY,
                NereusCursorProtocolCapability.VERSION,
                NereusGenerationProtocolCapability.PROPERTY,
                NereusGenerationProtocolCapability.VERSION);
    }

    private static BrokerLookupData lookupData(
            String brokerId, long startTimestamp, Map<String, String> properties) {
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
                startTimestamp,
                "version",
                properties);
    }
}
