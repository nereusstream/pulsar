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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.managedledger.NereusManagedLedger;
import com.nereusstream.managedledger.NereusWriteFenceResolution;
import com.nereusstream.managedledger.NereusWriteFenceSnapshot;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.apache.pulsar.broker.service.BacklogQuotaManager;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.broker.storage.nereus.NereusAdminOperation;
import org.apache.pulsar.broker.storage.nereus.NereusResolvedTopicFeatures;
import org.apache.pulsar.broker.storage.nereus.NereusTopicOpenContext;
import org.apache.pulsar.broker.storage.nereus.NereusTopicPolicySnapshot;
import org.apache.pulsar.common.policies.data.Policies;
import org.testng.annotations.Test;

public class PersistentTopicNereusAdmissionTest {
    @Test
    public void gatesAdminOperationsOnlyForNereusLedger() throws Exception {
        BrokerService brokerService = brokerService();
        PersistentTopic nereusTopic = new PersistentTopic(
                "persistent://tenant/ns/nereus",
                brokerService,
                mock(NereusManagedLedger.class),
                mock(MessageDeduplication.class));
        nereusTopic.installNereusTopicOpenContext(new NereusTopicOpenContext(
                new ManagedLedgerConfig(),
                new NereusResolvedTopicFeatures(
                        Set.of(), false, 0, 0, 0, false, false, false, false, false, false)));

        assertThat(nereusTopic.isNereusManagedLedger()).isTrue();
        nereusTopic.validateNereusAdminOperation(NereusAdminOperation.TERMINATE_TOPIC).get();
        assertThatThrownBy(() -> nereusTopic
                .validateNereusAdminOperation(NereusAdminOperation.TRUNCATE_TOPIC).get())
                .hasRootCauseMessage("NEREUS_UNSUPPORTED_ADMIN_OPERATION:TRUNCATE_TOPIC");

        PersistentTopic bookKeeperTopic = new PersistentTopic(
                "persistent://tenant/ns/bookkeeper",
                brokerService,
                mock(ManagedLedger.class),
                mock(MessageDeduplication.class));
        assertThat(bookKeeperTopic.isNereusManagedLedger()).isFalse();
        bookKeeperTopic.validateNereusAdminOperation(NereusAdminOperation.TRUNCATE_TOPIC).get();
    }

    @Test
    public void rejectsUnsafeLiveSnapshotBeforeLedgerConfigMutation() {
        BrokerService brokerService = brokerService();
        NereusManagedLedger ledger = mock(NereusManagedLedger.class);
        PersistentTopic topic = new PersistentTopic(
                "persistent://tenant/ns/nereus-policy",
                brokerService,
                ledger,
                mock(MessageDeduplication.class));
        topic.installNereusTopicOpenContext(new NereusTopicOpenContext(
                new ManagedLedgerConfig(),
                safeFeatures()));
        ManagedLedgerConfig unsafeConfig = new ManagedLedgerConfig();
        unsafeConfig.setStorageClassName("nereus");
        NereusResolvedTopicFeatures unsafeFeatures = new NereusResolvedTopicFeatures(
                Set.of(), false, 1, 1, 0, true, false, false, false, false, false);

        assertThatThrownBy(() -> topic.applyNereusPolicySnapshot(new NereusTopicPolicySnapshot(
                new NereusTopicOpenContext(unsafeConfig, unsafeFeatures),
                new Policies(),
                java.util.Optional.empty(),
                java.util.Optional.empty())))
                .hasRootCauseMessage("NEREUS_UNSUPPORTED_TOPIC_FEATURE:RETENTION");
        verify(ledger, never()).setConfig(any());
    }

    @Test
    public void waitsForNewestWriteFenceGenerationBeforeAutoUnfence() throws Exception {
        BrokerService brokerService = brokerService();
        NereusManagedLedger ledger = mock(NereusManagedLedger.class);
        AtomicReference<Optional<NereusWriteFenceSnapshot>> current = new AtomicReference<>(fence(1));
        CompletableFuture<NereusWriteFenceResolution> first = new CompletableFuture<>();
        CompletableFuture<NereusWriteFenceResolution> second = new CompletableFuture<>();
        when(ledger.currentWriteFence()).thenAnswer(ignored -> current.get());
        when(ledger.awaitWriteFence(1)).thenReturn(first);
        when(ledger.awaitWriteFence(2)).thenReturn(second);
        MessageDeduplication deduplication = mock(MessageDeduplication.class);
        PersistentTopic topic = new PersistentTopic(
                "persistent://tenant/ns/nereus-write-fence",
                brokerService,
                ledger,
                deduplication);
        setPendingWrites(topic, 1);

        topic.addFailed(new ManagedLedgerException("retryably uncertain"), mock(PublishContext.class));

        assertThat(topic.isFenced()).isTrue();
        verify(ledger, never()).readyToCreateNewLedger();

        current.set(fence(2));
        first.complete(NereusWriteFenceResolution.COMMITTED);
        assertThat(topic.isFenced()).isTrue();
        verify(ledger).awaitWriteFence(2);
        verify(ledger, never()).readyToCreateNewLedger();

        current.set(Optional.empty());
        second.complete(NereusWriteFenceResolution.PROVEN_NOT_COMMITTED);

        assertThat(topic.isFenced()).isFalse();
        verify(ledger).readyToCreateNewLedger();
        verify(ledger).unfenceForInterceptorException();
        verify(deduplication).resetHighestSequenceIdPushed();
    }

    @Test
    public void permanentWriteFenceFailureClosesWithoutAutoUnfence() throws Exception {
        BrokerService brokerService = brokerService();
        NereusManagedLedger ledger = mock(NereusManagedLedger.class);
        AtomicReference<Optional<NereusWriteFenceSnapshot>> current = new AtomicReference<>(fence(1));
        CompletableFuture<NereusWriteFenceResolution> terminal = new CompletableFuture<>();
        when(ledger.currentWriteFence()).thenAnswer(ignored -> current.get());
        when(ledger.awaitWriteFence(1)).thenReturn(terminal);
        PersistentTopic topic = spy(new PersistentTopic(
                "persistent://tenant/ns/nereus-write-fence-failure",
                brokerService,
                ledger,
                mock(MessageDeduplication.class)));
        doReturn(CompletableFuture.completedFuture(null)).when(topic).close();
        setPendingWrites(topic, 1);

        topic.addFailed(new ManagedLedgerException("retryably uncertain"), mock(PublishContext.class));
        current.set(Optional.empty());
        terminal.completeExceptionally(new IllegalStateException("permanent recovery failure"));

        verify(topic).close();
        verify(ledger, never()).readyToCreateNewLedger();
        verify(ledger, never()).unfenceForInterceptorException();
        assertThat(topic.isFenced()).isTrue();
    }

    @Test
    public void retainsEarlyWriteFenceCompletionUntilProducerDisconnects() throws Exception {
        BrokerService brokerService = brokerService();
        NereusManagedLedger ledger = mock(NereusManagedLedger.class);
        AtomicReference<Optional<NereusWriteFenceSnapshot>> current = new AtomicReference<>(fence(1));
        CompletableFuture<NereusWriteFenceResolution> terminal = new CompletableFuture<>();
        when(ledger.currentWriteFence()).thenAnswer(ignored -> current.get());
        when(ledger.awaitWriteFence(1)).thenReturn(terminal);
        MessageDeduplication deduplication = mock(MessageDeduplication.class);
        PersistentTopic topic = new PersistentTopic(
                "persistent://tenant/ns/nereus-early-write-fence-completion",
                brokerService,
                ledger,
                deduplication);
        Producer producer = mock(Producer.class);
        CompletableFuture<Void> disconnect = new CompletableFuture<>();
        when(producer.disconnect()).thenReturn(disconnect);
        topic.getProducers().put("producer", producer);
        setPendingWrites(topic, 1);

        topic.addFailed(new ManagedLedgerException("retryably uncertain"), mock(PublishContext.class));
        current.set(Optional.empty());
        terminal.complete(NereusWriteFenceResolution.COMMITTED);

        assertThat(topic.isFenced()).isTrue();
        verify(ledger, never()).readyToCreateNewLedger();
        verify(ledger).awaitWriteFence(1);

        disconnect.complete(null);

        assertThat(topic.isFenced()).isFalse();
        verify(ledger).readyToCreateNewLedger();
        verify(ledger).unfenceForInterceptorException();
        verify(deduplication).resetHighestSequenceIdPushed();
    }

    @Test
    public void handlesWriteFenceResolvingWhileBrokerAttaches() throws Exception {
        BrokerService brokerService = brokerService();
        NereusManagedLedger ledger = mock(NereusManagedLedger.class);
        AtomicInteger fenceReads = new AtomicInteger();
        when(ledger.currentWriteFence()).thenAnswer(
                ignored -> fenceReads.getAndIncrement() == 0 ? fence(1) : Optional.empty());
        when(ledger.awaitWriteFence(1)).thenReturn(
                CompletableFuture.completedFuture(NereusWriteFenceResolution.COMMITTED));
        MessageDeduplication deduplication = mock(MessageDeduplication.class);
        PersistentTopic topic = new PersistentTopic(
                "persistent://tenant/ns/nereus-write-fence-attach-race",
                brokerService,
                ledger,
                deduplication);
        setPendingWrites(topic, 1);

        topic.addFailed(new ManagedLedgerException("retryably uncertain"), mock(PublishContext.class));

        assertThat(topic.isFenced()).isFalse();
        verify(ledger).awaitWriteFence(1);
        verify(ledger).readyToCreateNewLedger();
        verify(ledger).unfenceForInterceptorException();
        verify(deduplication).resetHighestSequenceIdPushed();
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

    private static NereusResolvedTopicFeatures safeFeatures() {
        return new NereusResolvedTopicFeatures(
                Set.of(), false, 0, 0, 0, false, false, false, false, false, false);
    }

    private static Optional<NereusWriteFenceSnapshot> fence(long generation) {
        return Optional.of(new NereusWriteFenceSnapshot(
                generation, new AppendAttemptId("attempt-" + generation)));
    }

    private static void setPendingWrites(PersistentTopic topic, long pending) {
        topic.setPendingWriteOpsForTest(pending);
    }
}
