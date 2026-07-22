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

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.api.StorageProfile;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.storage.nereus.NereusBookKeeperPrimaryWalCapability;
import org.apache.pulsar.broker.storage.nereus.NereusCursorProtocolCapability;
import org.apache.pulsar.broker.storage.nereus.NereusGenerationProtocolCapability;
import org.apache.pulsar.broker.storage.nereus.NereusStorageBindingCapability;
import org.apache.pulsar.common.naming.NamespaceName;
import org.testng.annotations.Test;

public class NereusBookKeeperOwnershipFilterTest {

    @Test
    public void excludesEveryBrokerWithoutTheExactDurableProfileCapability() {
        NereusBookKeeperOwnershipFilter filter = new NereusBookKeeperOwnershipFilter(
                ignored -> CompletableFuture.completedFuture(
                        Optional.of(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT)));
        Map<String, BrokerLookupData> brokers = new ConcurrentHashMap<>();
        brokers.put("capable", broker("capable", capability("a")));
        brokers.put("missing", broker("missing", Map.of()));

        Map<String, BrokerLookupData> filtered = filter.filterAsync(
                        brokers, NamespaceName.get("tenant/namespace"), null)
                .join();

        assertThat(filtered).containsOnlyKeys("capable");
    }

    @Test
    public void leavesOrdinaryNamespacesUnchanged() {
        NereusBookKeeperOwnershipFilter filter = new NereusBookKeeperOwnershipFilter(
                ignored -> CompletableFuture.completedFuture(Optional.empty()));
        Map<String, BrokerLookupData> brokers = new ConcurrentHashMap<>();
        brokers.put("old", broker("old", Map.of()));

        assertThat(filter.filterAsync(
                        brokers, NamespaceName.get("tenant/namespace"), null).join())
                .containsOnlyKeys("old");
    }

    @Test
    public void failsClosedWhenDurableProfileResolutionFails() {
        NereusBookKeeperOwnershipFilter filter = new NereusBookKeeperOwnershipFilter(
                ignored -> CompletableFuture.failedFuture(
                        new IllegalStateException("projection scan failed")));
        Map<String, BrokerLookupData> brokers = new ConcurrentHashMap<>();
        brokers.put("capable", broker("capable", capability("a")));

        assertThat(filter.filterAsync(
                        brokers, NamespaceName.get("tenant/namespace"), null).join())
                .isEmpty();
    }

    @Test
    public void failsClosedWhenTwoCompleteCapabilitySignaturesConflict() {
        NereusBookKeeperOwnershipFilter filter = new NereusBookKeeperOwnershipFilter(
                ignored -> CompletableFuture.completedFuture(
                        Optional.of(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT)));
        Map<String, BrokerLookupData> brokers = new ConcurrentHashMap<>();
        brokers.put("current", broker("current", capability("a")));
        brokers.put("drifted", broker("drifted", capability("b")));

        assertThat(filter.filterAsync(
                        brokers, NamespaceName.get("tenant/namespace"), null).join())
                .isEmpty();
    }

    private static Map<String, String> capability(String identity) {
        return Map.of(
                NereusStorageBindingCapability.PROPERTY,
                NereusStorageBindingCapability.VERSION,
                NereusCursorProtocolCapability.PROPERTY,
                NereusCursorProtocolCapability.VERSION,
                NereusGenerationProtocolCapability.PROPERTY,
                NereusGenerationProtocolCapability.VERSION,
                NereusBookKeeperPrimaryWalCapability.PROTOCOL_PROPERTY,
                "1",
                NereusBookKeeperPrimaryWalCapability.CONFIGURATION_PROPERTY,
                identity.repeat(64),
                NereusBookKeeperPrimaryWalCapability.NAMESPACE_PROPERTY,
                "c".repeat(64),
                NereusBookKeeperPrimaryWalCapability.ACTIVATION_PROPERTY,
                "d".repeat(64));
    }

    private static BrokerLookupData broker(
            String brokerId, Map<String, String> properties) {
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
                1,
                "version",
                properties);
    }
}
