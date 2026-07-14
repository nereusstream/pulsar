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

import static java.util.Collections.emptyMap;
import static org.apache.pulsar.client.api.MessageId.latest;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.nereusstream.managedledger.NereusManagedLedger;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.testng.annotations.Test;

public class NereusConsumerAckOrderingTest {
    @Test
    public void durableIndividualAckMutatesPendingStateExactlyOnceAfterPersistence() throws Exception {
        PersistentSubscription subscription = mock(PersistentSubscription.class);
        ManagedCursor cursor = mock(ManagedCursor.class);
        when(subscription.getCursor()).thenReturn(cursor);
        when(cursor.isDurable()).thenReturn(true);
        when(cursor.getManagedLedger()).thenReturn(mock(NereusManagedLedger.class));
        PersistentTopic topic = mock(PersistentTopic.class);
        BrokerService brokerService = mock(BrokerService.class);
        PulsarService pulsar = mock(PulsarService.class);
        ServiceConfiguration configuration = new ServiceConfiguration();
        when(subscription.getTopic()).thenReturn(topic);
        when(topic.getBrokerService()).thenReturn(brokerService);
        when(brokerService.getPulsar()).thenReturn(pulsar);
        when(pulsar.getConfiguration()).thenReturn(configuration);
        when(pulsar.getConfig()).thenReturn(configuration);
        TransportCnx connection = mock(TransportCnx.class);
        Consumer consumer = new Consumer(
                subscription,
                SubType.Shared,
                "persistent://tenant/ns/topic",
                1,
                0,
                "consumer",
                false,
                connection,
                "role",
                emptyMap(),
                false,
                null,
                latest,
                DEFAULT_CONSUMER_EPOCH);
        assertThat(consumer.getPendingAcks().addPendingAckIfAllowed(7, 11, 1, 0)).isTrue();

        CompletableFuture<Void> failedPersistence = new CompletableFuture<>();
        when(subscription.acknowledgeMessageAsync(any(), any(), any())).thenReturn(failedPersistence);
        CompletableFuture<Void> failed = consumer.messageAcked(individualAck(7, 11));
        assertThat(failed).isNotDone();
        assertThat(consumer.getPendingAcks().size()).isEqualTo(1);
        failedPersistence.completeExceptionally(new IllegalStateException("cursor CAS failed"));
        assertThatThrownBy(failed::get).hasRootCauseMessage("cursor CAS failed");
        assertThat(consumer.getPendingAcks().size()).isEqualTo(1);
        assertThat(consumer.getMessageAckCounter()).isZero();

        CompletableFuture<Void> successfulPersistence = new CompletableFuture<>();
        when(subscription.acknowledgeMessageAsync(any(), any(), any())).thenReturn(successfulPersistence);
        CompletableFuture<Void> successful = consumer.messageAcked(individualAck(7, 11));
        assertThat(consumer.getPendingAcks().size()).isEqualTo(1);
        successfulPersistence.complete(null);
        successful.get();
        assertThat(consumer.getPendingAcks().size()).isZero();
        assertThat(consumer.getMessageAckCounter()).isEqualTo(1);
    }

    private static CommandAck individualAck(long ledgerId, long entryId) {
        CommandAck ack = new CommandAck()
                .setConsumerId(1)
                .setAckType(CommandAck.AckType.Individual);
        ack.addMessageId().setLedgerId(ledgerId).setEntryId(entryId);
        return ack;
    }
}
