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
package org.apache.pulsar.broker.service.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.nereusstream.managedledger.NereusManagedLedger;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.apache.pulsar.broker.service.BacklogQuotaManager;
import org.apache.pulsar.broker.service.BrokerService;
import org.testng.annotations.Test;

public class NereusPersistentTopicCursorRecoveryTest {
    @Test
    public void reconstructsSubscriptionOnlyFromTheAlreadyHydratedLedgerCursorView() {
        ManagedCursor hydrated = mock(ManagedCursor.class);
        when(hydrated.getName()).thenReturn("durable-subscription");
        when(hydrated.getProperties()).thenReturn(Map.of());
        when(hydrated.getCursorProperties()).thenReturn(Map.of("owner", "test"));
        NereusManagedLedger ledger = mock(NereusManagedLedger.class);
        when(ledger.getCursors()).thenReturn(List.of(hydrated));
        PersistentSubscription reconstructed = mock(PersistentSubscription.class);
        TestPersistentTopic topic = new TestPersistentTopic(
                "persistent://tenant/ns/topic", brokerService(), ledger, reconstructed);

        topic.createPersistentSubscriptions();

        assertThat(topic.getSubscription("durable-subscription")).isSameAs(reconstructed);
        assertThat(topic.reconstructedCursor).isSameAs(hydrated);
        assertThat(topic.reconstructedProperties).containsEntry("owner", "test");
        verify(reconstructed).deactivateCursor();
    }

    private static final class TestPersistentTopic extends PersistentTopic {
        private final PersistentSubscription reconstructed;
        private ManagedCursor reconstructedCursor;
        private Map<String, String> reconstructedProperties;

        private TestPersistentTopic(
                String topic,
                BrokerService brokerService,
                NereusManagedLedger ledger,
                PersistentSubscription reconstructed) {
            super(topic, brokerService, ledger, mock(MessageDeduplication.class));
            this.reconstructed = reconstructed;
        }

        @Override
        protected PersistentSubscription createPersistentSubscription(
                String subscriptionName,
                ManagedCursor cursor,
                Boolean replicated,
                Map<String, String> subscriptionProperties) {
            this.reconstructedCursor = cursor;
            this.reconstructedProperties = subscriptionProperties;
            return reconstructed;
        }
    }

    private static BrokerService brokerService() {
        BrokerService brokerService = mock(BrokerService.class);
        PulsarService pulsar = mock(PulsarService.class);
        when(brokerService.pulsar()).thenReturn(pulsar);
        when(brokerService.getPulsar()).thenReturn(pulsar);
        when(brokerService.getBacklogQuotaManager()).thenReturn(mock(BacklogQuotaManager.class));
        when(pulsar.getConfiguration()).thenReturn(new ServiceConfiguration());
        when(pulsar.getMonotonicClock()).thenReturn(AsyncTokenBucket.DEFAULT_SNAPSHOT_CLOCK);
        return brokerService;
    }
}
