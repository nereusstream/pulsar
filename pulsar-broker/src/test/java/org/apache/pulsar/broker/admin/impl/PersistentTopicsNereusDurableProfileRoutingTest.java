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
package org.apache.pulsar.broker.admin.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.storage.nereus.NereusAdminOperation;
import org.apache.pulsar.broker.storage.nereus.NereusManagedLedgerStorage;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PersistentTopicsNereusDurableProfileRoutingTest {
    private PulsarService pulsar;
    private BrokerService brokerService;
    private NereusManagedLedgerStorage storage;
    private TestResource resource;

    @BeforeMethod
    public void setup() {
        pulsar = mock(PulsarService.class);
        brokerService = mock(BrokerService.class);
        storage = mock(NereusManagedLedgerStorage.class);
        resource = new TestResource();
        resource.setPulsar(pulsar);
        when(pulsar.getBrokerService()).thenReturn(brokerService);
        when(pulsar.getManagedLedgerStorage()).thenReturn(storage);
        when(brokerService.pulsar()).thenReturn(pulsar);
        when(storage.validateBoundAdminOperation(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    public void loadedNereusTopicUsesTheSameDurableStorageRoute() {
        TopicName topic = TopicName.get("persistent://tenant/ns/loaded");
        PersistentTopic loaded = loadedNereusTopic(topic);
        resource.topic(topic);
        when(brokerService.fetchPartitionedTopicMetadataAsync(topic))
                .thenReturn(CompletableFuture.completedFuture(new PartitionedTopicMetadata(0)));
        when(brokerService.getTopicReference(topic.toString()))
                .thenReturn(Optional.of(loaded));

        resource.validate(NereusAdminOperation.TERMINATE_TOPIC).join();

        verify(storage).validateBoundAdminOperation(
                topic, NereusAdminOperation.TERMINATE_TOPIC);
        verify(loaded, never()).validateNereusAdminOperation(any());
    }

    @Test
    public void unloadedNereusBindingUsesTheDurableStorageRoute() {
        TopicName topic = TopicName.get("persistent://tenant/ns/unloaded");
        resource.topic(topic);
        when(brokerService.fetchPartitionedTopicMetadataAsync(topic))
                .thenReturn(CompletableFuture.completedFuture(new PartitionedTopicMetadata(0)));
        when(brokerService.getTopicReference(topic.toString()))
                .thenReturn(Optional.empty());

        resource.validate(NereusAdminOperation.ANALYZE_BACKLOG).join();

        verify(storage).validateUnloadedAdminOperation(
                topic, NereusAdminOperation.ANALYZE_BACKLOG);
    }

    @Test
    public void partitionedParentValidatesEveryConcretePartition() {
        TopicName parent = TopicName.get("persistent://tenant/ns/partitioned");
        TopicName partition0 = parent.getPartition(0);
        TopicName partition1 = parent.getPartition(1);
        TopicName partition2 = parent.getPartition(2);
        PersistentTopic loadedPartition0 = loadedNereusTopic(partition0);
        resource.topic(parent);
        when(brokerService.fetchPartitionedTopicMetadataAsync(parent))
                .thenReturn(CompletableFuture.completedFuture(new PartitionedTopicMetadata(3)));
        when(brokerService.getTopicReference(partition0.toString()))
                .thenReturn(Optional.of(loadedPartition0));
        when(brokerService.getTopicReference(partition1.toString()))
                .thenReturn(Optional.empty());
        when(brokerService.getTopicReference(partition2.toString()))
                .thenReturn(Optional.empty());

        resource.validate(NereusAdminOperation.TRIM_TOPIC).join();

        verify(storage).validateBoundAdminOperation(
                partition0, NereusAdminOperation.TRIM_TOPIC);
        verify(storage).validateUnloadedAdminOperation(
                partition1, NereusAdminOperation.TRIM_TOPIC);
        verify(storage).validateUnloadedAdminOperation(
                partition2, NereusAdminOperation.TRIM_TOPIC);
    }

    private PersistentTopic loadedNereusTopic(TopicName topic) {
        PersistentTopic loaded = mock(PersistentTopic.class);
        when(loaded.isNereusManagedLedger()).thenReturn(true);
        when(loaded.getBrokerService()).thenReturn(brokerService);
        when(loaded.getName()).thenReturn(topic.toString());
        return loaded;
    }

    private static final class TestResource extends PersistentTopicsBase {
        private void topic(TopicName value) {
            topicName = value;
        }

        private CompletableFuture<Void> validate(NereusAdminOperation operation) {
            return validateNereusAdminOperationForLoadedOrBoundTopic(operation);
        }
    }
}
