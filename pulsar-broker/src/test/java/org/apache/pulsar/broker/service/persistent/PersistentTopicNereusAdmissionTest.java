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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.managedledger.NereusManagedLedger;
import com.nereusstream.managedledger.NereusWriteFenceResolution;
import com.nereusstream.managedledger.NereusWriteFenceSnapshot;
import com.nereusstream.managedledger.retention.RetentionPolicySnapshot;
import java.util.EnumMap;
import java.util.Map;
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
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.testng.annotations.Test;

public class PersistentTopicNereusAdmissionTest {
    @Test
    public void gatesAdminOperationsOnlyForNereusLedger() throws Exception {
        BrokerService brokerService = brokerService();
        NereusManagedLedger ledger = mock(NereusManagedLedger.class);
        PersistentTopic nereusTopic = new PersistentTopic(
                "persistent://tenant/ns/nereus",
                brokerService,
                ledger,
                mock(MessageDeduplication.class));
        RetentionPolicySnapshot disabled = retention(0, 0);
        nereusTopic.installNereusTopicOpenContext(new NereusTopicOpenContext(
                new ManagedLedgerConfig(), safeFeatures(true), disabled));

        assertThat(nereusTopic.isNereusManagedLedger()).isTrue();
        verify(ledger).installRetentionPolicy(disabled);
        nereusTopic.validateNereusAdminOperation(NereusAdminOperation.TERMINATE_TOPIC).get();
        nereusTopic.validateNereusAdminOperation(NereusAdminOperation.TRIM_TOPIC).get();
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
                safeFeatures(false),
                retention(0, 0)));
        ManagedLedgerConfig unsafeConfig = new ManagedLedgerConfig();
        unsafeConfig.setStorageClassName("nereus");
        NereusResolvedTopicFeatures unsafeFeatures = new NereusResolvedTopicFeatures(
                Set.of(), false, 1, 1, 0,
                Optional.of(new RetentionPolicies(30, 64)), disabledBacklogQuotas(), false,
                false, false, false, false, false);

        assertThatThrownBy(() -> topic.applyNereusPolicySnapshot(new NereusTopicPolicySnapshot(
                new NereusTopicOpenContext(unsafeConfig, unsafeFeatures, retention(30, 64)),
                new Policies(),
                java.util.Optional.empty(),
                java.util.Optional.empty())))
                .hasRootCauseMessage("NEREUS_UNSUPPORTED_TOPIC_FEATURE:GENERATION_PROTOCOL_NOT_READY");
        verify(ledger, never()).setConfig(any());
    }

    @Test
    public void generationPolicyPreparationWaitsForMarkerAdmissionAndStablePolicyReload() {
        BrokerService brokerService = brokerService();
        NereusManagedLedger ledger = mock(NereusManagedLedger.class);
        PersistentTopic topic = new PersistentTopic(
                "persistent://tenant/ns/nereus-retention-policy",
                brokerService,
                ledger,
                mock(MessageDeduplication.class));
        topic.installNereusTopicOpenContext(new NereusTopicOpenContext(
                new ManagedLedgerConfig(),
                safeFeatures(false),
                retention(0, 0)));
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setStorageClassName("nereus");
        RetentionPolicySnapshot activeRetention = retention(30, 64);
        NereusTopicPolicySnapshot snapshot = new NereusTopicPolicySnapshot(
                new NereusTopicOpenContext(
                        config,
                        retentionFeatures(new RetentionPolicies(30, 64), true),
                        activeRetention),
                new Policies(),
                Optional.empty(),
                Optional.empty());
        CompletableFuture<Void> markerAdmission = new CompletableFuture<>();
        when(ledger.ensureGenerationProtocolReadyForPolicy()).thenReturn(markerAdmission);
        TopicName topicName = TopicName.get(topic.getName());
        when(brokerService.getNereusTopicPolicySnapshot(topicName)).thenReturn(
                CompletableFuture.completedFuture(snapshot),
                CompletableFuture.completedFuture(snapshot));

        CompletableFuture<NereusTopicPolicySnapshot> prepared =
                topic.loadPreparedNereusPolicySnapshot(topicName, 0);

        assertThat(prepared).isNotDone();
        verify(brokerService).getNereusTopicPolicySnapshot(topicName);
        verify(ledger, never()).installRetentionPolicy(activeRetention);
        markerAdmission.complete(null);
        assertThat(prepared.join()).isSameAs(snapshot);
        verify(brokerService, times(2)).getNereusTopicPolicySnapshot(topicName);
        verify(ledger, never()).installRetentionPolicy(activeRetention);
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

    private static NereusResolvedTopicFeatures safeFeatures(boolean generationReady) {
        return retentionFeatures(new RetentionPolicies(0, 0), generationReady);
    }

    private static NereusResolvedTopicFeatures retentionFeatures(
            RetentionPolicies retention,
            boolean generationReady) {
        return new NereusResolvedTopicFeatures(
                Set.of(), false, 0, 0, 0,
                Optional.of(retention), disabledBacklogQuotas(), false,
                false, false, false, false, generationReady);
    }

    private static Map<BacklogQuotaType, BacklogQuota> disabledBacklogQuotas() {
        EnumMap<BacklogQuotaType, BacklogQuota> quotas = new EnumMap<>(BacklogQuotaType.class);
        for (BacklogQuotaType type : BacklogQuotaType.values()) {
            quotas.put(type, BacklogQuota.builder()
                    .limitSize(-1)
                    .limitTime(-1)
                    .retentionPolicy(BacklogQuota.RetentionPolicy.producer_request_hold)
                    .build());
        }
        return quotas;
    }

    private static RetentionPolicySnapshot retention(long minutes, long mebibytes) {
        return RetentionPolicySnapshot.fromCanonicalMinutesAndMebibytes(minutes, mebibytes);
    }

    private static Optional<NereusWriteFenceSnapshot> fence(long generation) {
        return Optional.of(new NereusWriteFenceSnapshot(
                generation, new AppendAttemptId("attempt-" + generation)));
    }

    private static void setPendingWrites(PersistentTopic topic, long pending) {
        topic.setPendingWriteOpsForTest(pending);
    }
}
