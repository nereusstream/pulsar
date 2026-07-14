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
import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.loadbalance.extensions.BrokerRegistry;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.testng.annotations.Test;

public class NereusCursorProtocolCapabilityTest {
    @Test
    public void publishesTwoIndependentReservedCapabilities() {
        NereusBrokerCapabilityCoordinator coordinator = coordinator();
        BrokerRegistry registry = readyRegistry();
        coordinator.markStorageInitialized();
        coordinator.attachBrokerRegistry(registry);

        assertThat(coordinator.decorateLookupProperties(Map.of("operator", "value")))
                .containsEntry(NereusStorageBindingCapability.PROPERTY, NereusStorageBindingCapability.VERSION)
                .containsEntry(NereusCursorProtocolCapability.PROPERTY, NereusCursorProtocolCapability.VERSION)
                .containsEntry("operator", "value");
        assertThatThrownBy(() -> NereusCursorProtocolCapability.requireUnreserved(
                Map.of(NereusCursorProtocolCapability.PROPERTY, "spoof")))
                .hasMessage("nereus.cursor-protocol is reserved by the broker");
    }

    @Test
    public void bindingReadinessDoesNotImplyCursorReadiness() {
        NereusBrokerCapabilityCoordinator coordinator = coordinator();
        BrokerRegistry registry = readyRegistry();
        coordinator.markStorageInitialized();
        coordinator.attachBrokerRegistry(registry);
        BrokerLookupData bindingOnly = lookupData(Map.of(
                NereusStorageBindingCapability.PROPERTY, NereusStorageBindingCapability.VERSION));
        when(registry.getAvailableBrokerLookupDataAsync())
                .thenReturn(CompletableFuture.completedFuture(Map.of("broker", bindingOnly)));

        coordinator.requireClusterReady().join();
        assertThatThrownBy(() -> coordinator.acquireFirstActivationPermit(
                mock(CursorLedgerIdentity.class)).join())
                .hasRootCauseMessage(
                        "NEREUS_CLUSTER_CAPABILITY_NOT_READY:broker:nereus.cursor-protocol");
    }

    @Test
    public void firstActivationRequiresTwoStableAllCapableSnapshotsAndFailsOnDowngrade() {
        NereusBrokerCapabilityCoordinator coordinator = coordinator();
        BrokerRegistry registry = readyRegistry();
        coordinator.markStorageInitialized();
        coordinator.attachBrokerRegistry(registry);
        BrokerLookupData capable = lookupData(Map.of(
                NereusStorageBindingCapability.PROPERTY, NereusStorageBindingCapability.VERSION,
                NereusCursorProtocolCapability.PROPERTY, NereusCursorProtocolCapability.VERSION));
        Map<String, BrokerLookupData> stable = Map.of("broker", capable);
        when(registry.getAvailableBrokerLookupDataAsync())
                .thenReturn(CompletableFuture.completedFuture(stable));
        coordinator.acquireFirstActivationPermit(mock(CursorLedgerIdentity.class)).join();

        BrokerLookupData downgraded = lookupData(Map.of(
                NereusStorageBindingCapability.PROPERTY, NereusStorageBindingCapability.VERSION));
        when(registry.getAvailableBrokerLookupDataAsync()).thenReturn(
                CompletableFuture.completedFuture(stable),
                CompletableFuture.completedFuture(Map.of("broker", downgraded)));
        assertThatThrownBy(() -> coordinator.acquireFirstActivationPermit(
                mock(CursorLedgerIdentity.class)).join())
                .hasRootCauseMessage(
                        "NEREUS_CLUSTER_CAPABILITY_NOT_READY:broker:nereus.cursor-protocol");
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

    private static BrokerLookupData lookupData(Map<String, String> properties) {
        return new BrokerLookupData(
                "broker", "http://broker", null, "pulsar://broker", null,
                Map.of(), Map.of(), true, true, "load-manager", 1L, "version", properties);
    }
}
