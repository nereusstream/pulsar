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
import static org.apache.pulsar.common.api.proto.CommandAck.AckType.Individual;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Exclusive;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Failover;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Key_Shared;
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
    public void implementsTheF3DurabilityTypeAndBatchDecisionTable() {
        when(cursor.isDurable()).thenReturn(true);
        assertThat(validator.rejection(subscription, Exclusive, cumulativeAck(), true)).isEmpty();
        assertThat(validator.rejection(subscription, Failover, cumulativeAck(), false)).isEmpty();
        assertThat(validator.rejection(subscription, Exclusive, individualAck(2), true)).isEmpty();
        assertThat(validator.rejection(subscription, Shared, individualAck(2), false)).isEmpty();
        assertReason(validator.rejection(subscription, Shared, cumulativeAck(), false), "SHARED_CUMULATIVE");
        assertReason(validator.rejection(subscription, Key_Shared, individualAck(1), false), "KEY_SHARED");

        when(cursor.isDurable()).thenReturn(false);
        assertThat(validator.rejection(subscription, Exclusive, cumulativeAck(), false)).isEmpty();
        assertReason(
                validator.rejection(subscription, Exclusive, cumulativeAck(), true),
                "PERSISTED_CONFIRMATION_NON_DURABLE");

        CommandAck validBatch = individualAck(1);
        validBatch.getMessageIdAt(0).setBatchSize(2).setBatchIndex(0).addAckSet(2);
        assertThat(validator.rejection(
                subscription, Shared, validBatch, false, true, 10)).isEmpty();
        assertReason(validator.rejection(
                subscription, Shared, validBatch, false, false, 10), "PARTIAL_BATCH_DISABLED");
        CommandAck invalidBatch = individualAck(1);
        invalidBatch.getMessageIdAt(0).setBatchIndex(2).setBatchSize(2);
        assertReason(validator.rejection(
                subscription, Shared, invalidBatch, false, true, 10), "BATCH_SHAPE");
        assertReason(validator.rejection(
                subscription, Shared, individualAck(2), false, true, 1), "MESSAGE_ID_COUNT");

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
    public void consumerRejectsBeforeMutationAndPublishesDurableAckEffectsOnlyAfterCallback() throws Exception {
        when(cursor.isDurable()).thenReturn(true);
        Consumer consumer = consumer();
        long initialTimestamp = consumer.getStats().lastAckedTimestamp;

        assertThatThrownBy(() -> consumer.messageAcked(
                cumulativeAck().setValidationError(CommandAck.ValidationError.ChecksumMismatch)).get())
                .hasRootCauseMessage("NEREUS_UNSUPPORTED_ACK:VALIDATION_ERROR");
        assertThat(consumer.getStats().lastAckedTimestamp).isEqualTo(initialTimestamp);
        assertThat(consumer.getMessageAckCounter()).isZero();
        verify(subscription, never()).acknowledgeMessageAsync(any(), any(), any());

        CompletableFuture<Void> failedAck = new CompletableFuture<>();
        when(subscription.acknowledgeMessageAsync(any(), any(), any())).thenReturn(failedAck);
        CompletableFuture<Void> result = consumer.messageAcked(cumulativeAck());
        assertThat(result).isNotDone();
        assertThat(consumer.getMessageAckCounter()).isZero();
        failedAck.completeExceptionally(new IllegalStateException("cursor CAS failed"));
        assertThatThrownBy(result::get).hasRootCauseMessage("cursor CAS failed");
        assertThat(consumer.getStats().lastAckedTimestamp).isEqualTo(initialTimestamp);
        assertThat(consumer.getMessageAckCounter()).isZero();

        CompletableFuture<Void> durableAck = new CompletableFuture<>();
        when(subscription.acknowledgeMessageAsync(any(), any(), any())).thenReturn(durableAck);
        CompletableFuture<Void> successful = consumer.messageAcked(cumulativeAck());
        assertThat(successful).isNotDone();
        durableAck.complete(null);
        successful.get();
        assertThat(consumer.getMessageAckCounter()).isEqualTo(1);
        assertThat(consumer.getStats().lastAckedTimestamp).isGreaterThanOrEqualTo(initialTimestamp);
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

    private static CommandAck individualAck(int count) {
        CommandAck ack = new CommandAck().setAckType(Individual).setConsumerId(1);
        for (int i = 0; i < count; i++) {
            ack.addMessageId().setLedgerId(1).setEntryId(2 + i);
        }
        return ack;
    }

    private static void assertReason(
            Optional<? extends Throwable> rejection, String reason) {
        assertThat(rejection).hasValueSatisfying(error -> assertThat(error)
                .hasMessage("NEREUS_UNSUPPORTED_ACK:" + reason));
    }
}
