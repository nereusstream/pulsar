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
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.loadbalance.extensions.BrokerRegistry;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.testng.annotations.Test;

public class NereusStorageBindingCapabilityTest {
    @Test
    public void publishesCapabilityOnlyAfterStorageAndRegistryAreReady() {
        NereusBrokerCapabilityCoordinator coordinator = coordinator();
        BrokerRegistry registry = readyRegistry();

        assertThatThrownBy(() -> coordinator.decorateLookupProperties(Map.of()))
                .hasMessage("Nereus storage is not initialized");
        coordinator.markStorageInitialized();
        assertThatThrownBy(() -> coordinator.decorateLookupProperties(Map.of()))
                .hasMessage("Nereus broker registry is not attached");
        coordinator.attachBrokerRegistry(registry);
        Map<String, String> properties = coordinator.decorateLookupProperties(Map.of("existing", "value"));

        assertThat(properties).containsEntry("existing", "value");
        assertThat(properties).containsEntry(
                NereusStorageBindingCapability.PROPERTY, NereusStorageBindingCapability.VERSION);
        assertThat(properties).containsEntry(
                NereusCursorProtocolCapability.PROPERTY, NereusCursorProtocolCapability.VERSION);
        assertThatThrownBy(() -> coordinator.attachBrokerRegistry(registry))
                .hasMessage("Nereus broker registry is already attached");
    }

    @Test
    public void rejectsConfiguredCapabilitySpoofing() {
        assertThat(NereusStorageBindingCapability.requireUnreserved(Map.of())).isEmpty();
        assertThatThrownBy(() -> NereusStorageBindingCapability.requireUnreserved(
                Map.of(NereusStorageBindingCapability.PROPERTY, "1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nereus.storage-binding-protocol is reserved by the broker");
        assertThatThrownBy(() -> NereusStorageBindingCapability.requireUnreserved(
                Map.of(NereusCursorProtocolCapability.PROPERTY, "1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nereus.cursor-protocol is reserved by the broker");
    }

    @Test
    public void requiresEveryPersistentBrokerAndStableBrokerSet() {
        NereusBrokerCapabilityCoordinator coordinator = coordinator();
        BrokerRegistry registry = readyRegistry();
        coordinator.markStorageInitialized();
        coordinator.attachBrokerRegistry(registry);
        BrokerLookupData capable = lookupData(true, Map.of(
                NereusBrokerCapabilityCoordinator.PROPERTY,
                NereusBrokerCapabilityCoordinator.VERSION));
        BrokerLookupData incapable = lookupData(true, Map.of());

        when(registry.getAvailableBrokerLookupDataAsync()).thenReturn(
                CompletableFuture.completedFuture(Map.of("a", capable, "b", incapable)));
        assertThatThrownBy(() -> coordinator.requireClusterReady().join())
                .hasRootCauseMessage(
                        "NEREUS_CLUSTER_CAPABILITY_NOT_READY:b:nereus.storage-binding-protocol");

        Map<String, BrokerLookupData> first = new LinkedHashMap<>();
        first.put("a", capable);
        Map<String, BrokerLookupData> second = new LinkedHashMap<>();
        second.put("a", capable);
        second.put("b", capable);
        when(registry.getAvailableBrokerLookupDataAsync()).thenReturn(
                CompletableFuture.completedFuture(first),
                CompletableFuture.completedFuture(second));
        assertThatThrownBy(() -> coordinator.requireClusterReady().join())
                .hasRootCauseMessage("NEREUS_CLUSTER_CAPABILITY_BROKER_SET_CHANGED");
    }

    @Test
    public void acceptsStableCapablePersistentSetAndIgnoresNonPersistentBroker() {
        NereusBrokerCapabilityCoordinator coordinator = coordinator();
        BrokerRegistry registry = readyRegistry();
        coordinator.markStorageInitialized();
        coordinator.attachBrokerRegistry(registry);
        BrokerLookupData capable = lookupData(true, Map.of(
                NereusBrokerCapabilityCoordinator.PROPERTY,
                NereusBrokerCapabilityCoordinator.VERSION));
        BrokerLookupData nonPersistent = lookupData(false, Map.of());
        Map<String, BrokerLookupData> brokers = Map.of("a", capable, "web-only", nonPersistent);
        when(registry.getAvailableBrokerLookupDataAsync())
                .thenReturn(CompletableFuture.completedFuture(brokers));

        coordinator.requireClusterReady().join();
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

    private static BrokerLookupData lookupData(boolean persistent, Map<String, String> properties) {
        return new BrokerLookupData(
                "broker", "http://broker", null, "pulsar://broker", null,
                Map.of(), Map.of(), persistent, true, "load-manager", 1L, "version", properties);
    }
}
