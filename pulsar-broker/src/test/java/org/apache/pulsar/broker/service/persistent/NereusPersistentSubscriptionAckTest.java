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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.nereusstream.managedledger.NereusManagedLedger;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.testng.annotations.Test;

public class NereusPersistentSubscriptionAckTest {
    @Test
    public void durableIndividualAckDefersRedeliveryCleanupUntilCursorSuccess() throws Exception {
        Fixture fixture = fixture();
        Position position = PositionFactory.create(7, 11);

        CompletableFuture<Void> result = fixture.subscription.acknowledgeMessageAsync(
                List.of(position), AckType.Individual, Map.of());

        assertThat(result).isNotDone();
        verify(fixture.redeliveryTracker, never()).removeBatch(anyList());
        fixture.callback.get().deleteComplete(null);
        result.get();
        verify(fixture.redeliveryTracker).removeBatch(List.of(position));
    }

    @Test
    public void cursorFailureLeavesRedeliveryStateUntouched() {
        Fixture fixture = fixture();
        Position position = PositionFactory.create(7, 12);

        CompletableFuture<Void> result = fixture.subscription.acknowledgeMessageAsync(
                List.of(position), AckType.Individual, Map.of());
        fixture.callback.get().deleteFailed(new ManagedLedgerException("cursor CAS failed"), null);

        assertThatThrownBy(result::join).hasRootCauseMessage("cursor CAS failed");
        verify(fixture.redeliveryTracker, never()).removeBatch(anyList());
    }

    private static Fixture fixture() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        PersistentTopic topic = mock(PersistentTopic.class);
        BrokerService brokerService = mock(BrokerService.class);
        PulsarService pulsar = mock(PulsarService.class);
        when(topic.getBrokerService()).thenReturn(brokerService);
        when(brokerService.getPulsar()).thenReturn(pulsar);
        when(brokerService.pulsar()).thenReturn(pulsar);
        when(pulsar.getConfig()).thenReturn(configuration);
        when(topic.getName()).thenReturn("persistent://tenant/ns/topic");
        NereusManagedLedger ledger = mock(NereusManagedLedger.class);
        when(topic.getManagedLedger()).thenReturn(ledger);
        when(ledger.isTerminated()).thenReturn(false);

        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.getName()).thenReturn("subscription");
        when(cursor.isDurable()).thenReturn(true);
        when(cursor.getManagedLedger()).thenReturn(ledger);
        Position markDelete = PositionFactory.create(7, 1);
        when(cursor.getMarkDeletedPosition()).thenReturn(markDelete);
        AtomicReference<DeleteCallback> callback = new AtomicReference<>();
        doAnswer(invocation -> {
            callback.set(invocation.getArgument(1));
            return null;
        }).when(cursor).asyncDelete(anyList(), any(DeleteCallback.class), any());

        PersistentSubscription subscription = new PersistentSubscription(topic, "subscription", cursor, null);
        Dispatcher dispatcher = mock(Dispatcher.class);
        RedeliveryTracker tracker = mock(RedeliveryTracker.class);
        when(dispatcher.getRedeliveryTracker()).thenReturn(tracker);
        subscription.dispatcher = dispatcher;
        return new Fixture(subscription, callback, tracker);
    }

    private record Fixture(
            PersistentSubscription subscription,
            AtomicReference<DeleteCallback> callback,
            RedeliveryTracker redeliveryTracker) {
    }
}
