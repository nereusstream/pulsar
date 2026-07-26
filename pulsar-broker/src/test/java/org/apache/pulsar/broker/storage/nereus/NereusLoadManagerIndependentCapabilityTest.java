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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.testng.annotations.Test;

public class NereusLoadManagerIndependentCapabilityTest {

    @Test
    public void readsModularAndExtensibleBrokerRecordsThroughMetadataStore()
            throws Exception {
        MetadataStore metadata = mock(MetadataStore.class);
        AtomicReference<Consumer<Notification>> listener = new AtomicReference<>();
        doAnswer(invocation -> {
            listener.set(invocation.getArgument(0));
            return null;
        }).when(metadata).registerListener(org.mockito.ArgumentMatchers.any());
        when(metadata.sync(anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(metadata.getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT))
                .thenReturn(CompletableFuture.completedFuture(
                        List.of("extensible", "modular")));

        Map<String, String> capabilities = Map.of(
                NereusStorageBindingCapability.PROPERTY,
                NereusStorageBindingCapability.VERSION,
                NereusCursorProtocolCapability.PROPERTY,
                NereusCursorProtocolCapability.VERSION,
                NereusGenerationProtocolCapability.PROPERTY,
                NereusGenerationProtocolCapability.VERSION);
        LocalBrokerData modular = new LocalBrokerData(
                "modular",
                "http://modular",
                null,
                "pulsar://modular",
                null);
        modular.setLoadManagerClassName(
                "org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl");
        modular.setProperties(capabilities);
        BrokerLookupData extensible = new BrokerLookupData(
                "extensible",
                "http://extensible",
                null,
                "pulsar://extensible",
                null,
                Map.of(),
                Map.of(),
                true,
                true,
                "org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl",
                1,
                "5.0.0-M1",
                capabilities);
        Map<String, byte[]> records = Map.of(
                "modular",
                ObjectMapperFactory.getMapper()
                        .getObjectMapper()
                        .writeValueAsBytes(modular),
                "extensible",
                ObjectMapperFactory.getMapper()
                        .getObjectMapper()
                        .writeValueAsBytes(extensible));
        when(metadata.get(anyString())).thenAnswer(invocation -> {
            String path = invocation.getArgument(0);
            String brokerId = path.substring(path.lastIndexOf('/') + 1);
            byte[] value = records.get(brokerId);
            return CompletableFuture.completedFuture(
                    Optional.ofNullable(value)
                            .map(bytes -> new GetResult(bytes, null)));
        });

        NereusBrokerCapabilityCoordinator coordinator =
                new NereusBrokerCapabilityCoordinator(
                        metadata, Duration.ofSeconds(5));
        coordinator.markStorageInitialized();
        coordinator.attachLocalBroker("modular");

        NereusGenerationCapabilityReadiness readiness =
                coordinator.requireGenerationReadiness().join();

        assertThat(readiness.persistentBrokerCount()).isEqualTo(2);
        assertThat(coordinator.currentGenerationReadiness())
                .contains(readiness);

        listener.get().accept(new Notification(
                NotificationType.Modified,
                LoadManager.LOADBALANCE_BROKERS_ROOT + "/extensible"));
        assertThat(coordinator.currentGenerationReadiness()).isEmpty();
    }
}
