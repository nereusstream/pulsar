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
import static org.apache.pulsar.common.api.proto.CommandAck.AckType.Cumulative;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Exclusive;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Shared;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.nereusstream.managedledger.NereusManagedLedger;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class NereusAcknowledgeValidatorTest {
    private PersistentSubscription subscription;
    private ManagedCursor cursor;
    private NereusAcknowledgeValidator validator;

    @BeforeMethod
    public void setUp() {
        subscription = mock(PersistentSubscription.class);
        cursor = mock(ManagedCursor.class);
        when(subscription.getCursor()).thenReturn(cursor);
        when(cursor.getManagedLedger()).thenReturn(mock(NereusManagedLedger.class));
        Topic topic = mock(Topic.class);
        BrokerService brokerService = mock(BrokerService.class);
        PulsarService pulsarService = mock(PulsarService.class);
        when(subscription.getTopic()).thenReturn(topic);
        when(topic.getBrokerService()).thenReturn(brokerService);
        when(brokerService.getPulsar()).thenReturn(pulsarService);
        when(pulsarService.getConfiguration()).thenReturn(mock(ServiceConfiguration.class));
        validator = new NereusAcknowledgeValidator();
    }

    @Test
    public void admitsOnlyTheLimitedNonDurableCumulativeShape() {
        when(cursor.isDurable()).thenReturn(false);
        CommandAck admitted = cumulativeAck();
        assertThat(validator.rejection(subscription, Exclusive, admitted, false)).isEmpty();

        when(cursor.isDurable()).thenReturn(true);
        assertReason(validator.rejection(subscription, Exclusive, admitted, false), "DURABLE_CURSOR");
        when(cursor.isDurable()).thenReturn(false);

        CommandAck individual = cumulativeAck().setAckType(CommandAck.AckType.Individual);
        assertReason(validator.rejection(subscription, Exclusive, individual, false), "ACK_TYPE");

        assertReason(validator.rejection(subscription, Shared, cumulativeAck(), false), "SUBSCRIPTION_TYPE");
        assertReason(validator.rejection(subscription, Exclusive, cumulativeAck(), true), "PERSISTED_CONFIRMATION");

        CommandAck multiple = cumulativeAck();
        multiple.addMessageId().setLedgerId(1).setEntryId(3);
        assertReason(validator.rejection(subscription, Exclusive, multiple, false), "MESSAGE_ID_COUNT");

        CommandAck batched = cumulativeAck();
        batched.getMessageIdAt(0).setBatchIndex(0);
        assertReason(validator.rejection(subscription, Exclusive, batched, false), "BATCH_INDEX");

        CommandAck batchSize = cumulativeAck();
        batchSize.getMessageIdAt(0).setBatchSize(2);
        assertReason(validator.rejection(subscription, Exclusive, batchSize, false), "BATCH_INDEX");

        CommandAck ackSet = cumulativeAck();
        ackSet.getMessageIdAt(0).addAckSet(1);
        assertReason(validator.rejection(subscription, Exclusive, ackSet, false), "ACK_SET");

        CommandAck transactional = cumulativeAck().setTxnidMostBits(1);
        assertReason(validator.rejection(subscription, Exclusive, transactional, false), "TRANSACTION");

        CommandAck invalid = cumulativeAck().setValidationError(CommandAck.ValidationError.ChecksumMismatch);
        assertReason(validator.rejection(subscription, Exclusive, invalid, false), "VALIDATION_ERROR");
    }

    @Test
    public void isNoOpForBookKeeperSubscription() {
        when(cursor.getManagedLedger()).thenReturn(mock(ManagedLedger.class));
        assertThat(validator.rejection(subscription, Exclusive, cumulativeAck(), true)).isEmpty();
    }

    @Test
    public void consumerRejectsBeforeMutationAndWaitsForAdmittedLocalAck() throws Exception {
        when(cursor.isDurable()).thenReturn(true);
        Consumer consumer = consumer();
        long initialTimestamp = consumer.getStats().lastAckedTimestamp;

        assertThatThrownBy(() -> consumer.messageAcked(cumulativeAck()).get())
                .hasRootCauseMessage("NEREUS_UNSUPPORTED_ACK:DURABLE_CURSOR");
        assertThat(consumer.getStats().lastAckedTimestamp).isEqualTo(initialTimestamp);
        assertThat(consumer.getMessageAckCounter()).isZero();
        verify(subscription, never()).acknowledgeMessageAsync(any(), any(), any());

        when(cursor.isDurable()).thenReturn(false);
        CompletableFuture<Void> localAck = new CompletableFuture<>();
        when(subscription.acknowledgeMessageAsync(any(), any(), any())).thenReturn(localAck);
        CompletableFuture<Void> result = consumer.messageAcked(cumulativeAck());
        assertThat(result).isNotDone();
        assertThat(consumer.getMessageAckCounter()).isZero();
        localAck.complete(null);
        result.get();
        assertThat(consumer.getMessageAckCounter()).isEqualTo(1);
    }

    private Consumer consumer() {
        TransportCnx connection = mock(TransportCnx.class);
        return new Consumer(
                subscription,
                Exclusive,
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
    }

    private static CommandAck cumulativeAck() {
        CommandAck ack = new CommandAck().setAckType(Cumulative).setConsumerId(1);
        ack.addMessageId().setLedgerId(1).setEntryId(2);
        return ack;
    }

    private static void assertReason(
            Optional<? extends Throwable> rejection, String reason) {
        assertThat(rejection).hasValueSatisfying(error -> assertThat(error)
                .hasMessage("NEREUS_UNSUPPORTED_ACK:" + reason));
    }
}
