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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.nereusstream.managedledger.retention.RetentionPolicySnapshot;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class NereusTopicFeatureResolverTest {
    private static final TopicName USER_TOPIC = TopicName.get("persistent://tenant/ns/topic");

    @Test
    public void resolvesSafeBrokerDefaults() {
        ServiceConfiguration broker = new ServiceConfiguration();
        broker.setClusterName("local");

        NereusResolvedTopicFeatures features = NereusTopicFeatureResolver.resolve(
                broker, new Policies(), Optional.empty(), Optional.empty(), USER_TOPIC);

        assertThat(features).isEqualTo(safeFeatures());
        assertThatCode(() -> new NereusTopicFeatureValidator().validateTopicOpen(
                USER_TOPIC, new ManagedLedgerConfig(), features)).doesNotThrowAnyException();
    }

    @Test
    public void openContextRejectsRetentionProjectionThatDoesNotMatchExactPulsarFacts() {
        assertThatThrownBy(() -> new NereusTopicOpenContext(
                new ManagedLedgerConfig(),
                safeFeatures(),
                RetentionPolicySnapshot.fromCanonicalMinutesAndMebibytes(1, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact resolved Pulsar retention values");
    }

    @Test
    public void appliesLocalGlobalNamespaceBrokerPrecedenceAndExcludesLocalReplication() {
        ServiceConfiguration broker = new ServiceConfiguration();
        broker.setClusterName("local");
        broker.setBrokerDeduplicationEnabled(true);
        Policies namespace = new Policies();
        namespace.deduplicationEnabled = false;
        namespace.replication_clusters = Set.of("local", "remote-a");
        TopicPolicies global = TopicPolicies.builder()
                .deduplicationEnabled(true)
                .messageTTLInSeconds(40)
                .build();
        TopicPolicies local = TopicPolicies.builder()
                .deduplicationEnabled(false)
                .messageTTLInSeconds(12)
                .replicationClusters(List.of("local", "remote-b"))
                .build();

        NereusResolvedTopicFeatures features = NereusTopicFeatureResolver.resolve(
                broker, namespace, Optional.of(local), Optional.of(global), USER_TOPIC);

        assertThat(features.deduplicationEnabled()).isFalse();
        assertThat(features.messageTtlSeconds()).isEqualTo(12);
        assertThat(features.remoteReplicationClusters()).containsExactly("remote-b");
    }

    @Test
    public void detectsCursorMutatingBacklogEvictionAndSystemTopics() {
        ServiceConfiguration broker = new ServiceConfiguration();
        broker.setClusterName("local");
        Policies namespace = new Policies();
        namespace.backlog_quota_map.put(
                BacklogQuotaType.destination_storage,
                BacklogQuota.builder()
                        .limitSize(1024)
                        .limitTime(-1)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                        .build());

        NereusResolvedTopicFeatures userFeatures = NereusTopicFeatureResolver.resolve(
                broker, namespace, Optional.empty(), Optional.empty(), USER_TOPIC);
        NereusResolvedTopicFeatures systemFeatures = NereusTopicFeatureResolver.resolve(
                broker,
                new Policies(),
                Optional.empty(),
                Optional.empty(),
                TopicName.get("persistent://pulsar/system/transaction_coordinator_assign"));

        assertThat(userFeatures.backlogEvictionEnabled()).isTrue();
        assertThat(userFeatures.sizeBacklogEvictionEnabled()).isTrue();
        assertThat(userFeatures.timeBacklogEvictionEnabled()).isFalse();
        assertThat(userFeatures.backlogQuotas().get(BacklogQuotaType.destination_storage).getLimitSize())
                .isEqualTo(1024);
        assertThat(systemFeatures.systemOrInternalTopic()).isTrue();
    }

    @Test
    public void preservesExactRetentionAndBacklogPrecedenceInImmutableCopies() {
        ServiceConfiguration broker = new ServiceConfiguration();
        broker.setClusterName("local");
        broker.setPreciseTimeBasedBacklogQuotaCheck(true);
        Policies namespace = new Policies();
        namespace.retention_policies = new RetentionPolicies(11, 12);
        namespace.backlog_quota_map.put(
                BacklogQuotaType.destination_storage,
                quota(1_024, -1, BacklogQuota.RetentionPolicy.consumer_backlog_eviction));
        TopicPolicies global = TopicPolicies.builder()
                .retentionPolicies(new RetentionPolicies(21, 22))
                .build();
        global.getBackLogQuotaMap().put(
                BacklogQuotaType.message_age.toString(),
                quota(-1, 31, BacklogQuota.RetentionPolicy.consumer_backlog_eviction));
        TopicPolicies local = TopicPolicies.builder()
                .retentionPolicies(new RetentionPolicies(-1, 32))
                .build();
        BacklogQuotaImpl mutableSource = quota(
                2_048, -1, BacklogQuota.RetentionPolicy.producer_exception);
        local.getBackLogQuotaMap().put(BacklogQuotaType.destination_storage.toString(), mutableSource);

        NereusResolvedTopicFeatures features = NereusTopicFeatureResolver.resolve(
                broker, namespace, Optional.of(local), Optional.of(global), USER_TOPIC, true);

        assertThat(features.retention()).contains(new RetentionPolicies(-1, 32));
        assertThat(features.backlogQuotas().get(BacklogQuotaType.destination_storage).getLimitSize())
                .isEqualTo(2_048);
        assertThat(features.backlogQuotas().get(BacklogQuotaType.destination_storage))
                .isNotSameAs(mutableSource);
        assertThat(features.backlogQuotas().get(BacklogQuotaType.message_age).getLimitTime())
                .isEqualTo(31);
        assertThat(features.preciseTimeBasedBacklogQuotaCheck()).isTrue();
        assertThat(features.generationProtocolRuntimeReady()).isTrue();
        assertThatThrownBy(() -> features.backlogQuotas().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @DataProvider
    public Object[][] unsupportedFeatures() {
        return new Object[][] {
            {"SYSTEM_OR_INTERNAL_TOPIC"},
            {"GEO_REPLICATION"},
            {"DEDUPLICATION"},
            {"COMPACTION"},
            {"PULSAR_OFFLOAD"},
            {"ENTRY_FILTERS"},
            {"SHADOW_OR_MIGRATION"}
        };
    }

    @Test
    public void admitsTtlAndSubscriptionExpirationWithoutAdmittingPhysicalGcPolicies() {
        NereusResolvedTopicFeatures lifecyclePolicies = new NereusResolvedTopicFeatures(
                Set.of(), false, 30, 60, 0,
                Optional.of(new RetentionPolicies(0, 0)), disabledBacklogQuotas(), false,
                false, false, false, false, false);
        assertThatCode(() -> new NereusTopicFeatureValidator().validateTopicOpen(
                USER_TOPIC, new ManagedLedgerConfig(), lifecyclePolicies)).doesNotThrowAnyException();
    }

    @Test(dataProvider = "unsupportedFeatures")
    public void rejectsEveryUnsupportedResolvedFeature(String feature) {
        assertThatThrownBy(() -> new NereusTopicFeatureValidator().validateTopicOpen(
                USER_TOPIC, new ManagedLedgerConfig(), unsupported(feature)))
                .hasMessage("NEREUS_UNSUPPORTED_TOPIC_FEATURE:" + feature);
    }

    @Test
    public void rejectsUnsafeManagedLedgerConfig() {
        ManagedLedgerConfig interceptorConfig = new ManagedLedgerConfig();
        interceptorConfig.setManagedLedgerInterceptor(mock(ManagedLedgerInterceptor.class));
        assertThatThrownBy(() -> new NereusTopicFeatureValidator().validateTopicOpen(
                USER_TOPIC, interceptorConfig, safeFeatures()))
                .hasMessage("NEREUS_UNSUPPORTED_TOPIC_FEATURE:MANAGED_LEDGER_INTERCEPTOR");

        ManagedLedgerConfig skipConfig = new ManagedLedgerConfig();
        skipConfig.setAutoSkipNonRecoverableData(true);
        assertThatThrownBy(() -> new NereusTopicFeatureValidator().validateTopicOpen(
                USER_TOPIC, skipConfig, safeFeatures()))
                .hasMessage("NEREUS_UNSUPPORTED_TOPIC_FEATURE:AUTO_SKIP_NON_RECOVERABLE_DATA");
    }

    @Test
    public void validatesPublishMetadataWithoutChangingBufferState() throws Exception {
        NereusTopicFeatureValidator validator = new NereusTopicFeatureValidator();
        ByteBuf ordinary = entry(new MessageMetadata()
                .setProducerName("producer")
                .setSequenceId(1)
                .setPublishTime(10));
        int readerIndex = ordinary.readerIndex();
        int writerIndex = ordinary.writerIndex();
        int referenceCount = ordinary.refCnt();
        try {
            validator.validatePublish(ordinary, false);
            assertThat(ordinary.readerIndex()).isEqualTo(readerIndex);
            assertThat(ordinary.writerIndex()).isEqualTo(writerIndex);
            assertThat(ordinary.refCnt()).isEqualTo(referenceCount);
        } finally {
            ordinary.release();
        }

        ByteBuf delayed = entry(new MessageMetadata()
                .setProducerName("producer")
                .setSequenceId(2)
                .setPublishTime(10)
                .setDeliverAtTime(11));
        try {
            assertThatThrownBy(() -> validator.validatePublish(delayed, false))
                    .hasMessage("NEREUS_UNSUPPORTED_PUBLISH:DELAYED_DELIVERY");
            assertThat(delayed.readerIndex()).isZero();
        } finally {
            delayed.release();
        }

        ByteBuf marker = entry(new MessageMetadata()
                .setProducerName("producer")
                .setSequenceId(3)
                .setPublishTime(10)
                .setMarkerType(MarkerType.TXN_ABORT_VALUE));
        try {
            assertThatThrownBy(() -> validator.validatePublish(marker, false))
                    .hasMessage("NEREUS_UNSUPPORTED_PUBLISH:MARKER");
        } finally {
            marker.release();
        }
    }

    @Test
    public void rejectsTransactionalPublishRemoteProducerAndUnsafeSubscriptions() {
        NereusTopicFeatureValidator validator = new NereusTopicFeatureValidator();
        ByteBuf invalidButTransactional = Unpooled.wrappedBuffer(new byte[] {1});
        try {
            assertThatThrownBy(() -> validator.validatePublish(invalidButTransactional, true))
                    .hasMessage("NEREUS_UNSUPPORTED_PUBLISH:TRANSACTIONAL");
        } finally {
            invalidButTransactional.release();
        }

        Producer remote = mock(Producer.class);
        when(remote.isRemote()).thenReturn(true);
        assertThatThrownBy(() -> validator.validateProducer(remote))
                .hasMessage("NEREUS_UNSUPPORTED_PRODUCER:REMOTE_REPLICATION");
        assertThatCode(() -> validator.validateSubscribe(
                SubType.Shared, false, false, false, null)).doesNotThrowAnyException();
        assertThatCode(() -> validator.validateSubscribe(
                SubType.Exclusive, true, false, false, null)).doesNotThrowAnyException();
        assertThatThrownBy(() -> validator.validateSubscribe(
                SubType.Key_Shared, true, false, false, null))
                .hasMessage("NEREUS_UNSUPPORTED_SUBSCRIPTION:KEY_SHARED");
        KeySharedMeta exclusiveHashRange = new KeySharedMeta();
        exclusiveHashRange.addHashRange().setStart(0).setEnd(99);
        assertThatThrownBy(() -> validator.validateSubscribe(
                SubType.Exclusive, false, false, false, exclusiveHashRange))
                .hasMessage("NEREUS_UNSUPPORTED_SUBSCRIPTION:KEY_SHARED_META");
        assertThatThrownBy(() -> validator.validateExistingDurableCursors(false))
                .hasMessage("NEREUS_UNSUPPORTED_SUBSCRIPTION:CURSOR_PROTOCOL_NOT_READY");
        assertThatCode(() -> validator.validateExistingDurableCursors(true)).doesNotThrowAnyException();
        assertThatThrownBy(() -> validator.validateCreateSubscription(false))
                .hasMessage("NEREUS_UNSUPPORTED_SUBSCRIPTION:CURSOR_PROTOCOL_NOT_READY");
        assertThatCode(() -> validator.validateCreateSubscription(true)).doesNotThrowAnyException();
        assertThatCode(() -> validator.validateSubscribe(
                SubType.Failover, false, false, false, null)).doesNotThrowAnyException();
        assertThatCode(() -> validator.validateSubscribe(
                SubType.Exclusive, false, false, false, new KeySharedMeta())).doesNotThrowAnyException();
    }

    @Test
    public void validatesTheClosedAdminOperationSet() {
        NereusTopicFeatureValidator validator = new NereusTopicFeatureValidator();
        assertThatCode(() -> validator.validateAdminOperation(NereusAdminOperation.TERMINATE_TOPIC, false))
                .doesNotThrowAnyException();
        assertThatCode(() -> validator.validateAdminOperation(NereusAdminOperation.DELETE_TOPIC, false))
                .doesNotThrowAnyException();
        java.util.Set<NereusAdminOperation> allowed = java.util.Set.of(
                NereusAdminOperation.TERMINATE_TOPIC,
                NereusAdminOperation.DELETE_TOPIC,
                NereusAdminOperation.UNLOAD_TOPIC,
                NereusAdminOperation.DELETE_DURABLE_SUBSCRIPTION,
                NereusAdminOperation.ANALYZE_BACKLOG,
                NereusAdminOperation.CLEAR_BACKLOG,
                NereusAdminOperation.SKIP_MESSAGES,
                NereusAdminOperation.EXPIRE_MESSAGES,
                NereusAdminOperation.RESET_CURSOR);
        allowed.forEach(operation -> assertThatCode(() -> validator.validateAdminOperation(operation, false))
                .doesNotThrowAnyException());
        for (NereusAdminOperation operation : NereusAdminOperation.values()) {
            if (allowed.contains(operation)) {
                continue;
            }
            if (operation == NereusAdminOperation.TRIM_TOPIC) {
                assertThatThrownBy(() -> validator.validateAdminOperation(operation, false))
                        .hasMessage("NEREUS_UNSUPPORTED_ADMIN_OPERATION:TRIM_TOPIC:GENERATION_PROTOCOL_NOT_READY");
            } else {
                assertThatThrownBy(() -> validator.validateAdminOperation(operation, false))
                        .hasMessage("NEREUS_UNSUPPORTED_ADMIN_OPERATION:" + operation.name());
            }
        }
    }

    @Test
    public void dropsStalePolicyRefreshCompletion() {
        NereusTopicPolicyUpdateCoordinator coordinator = new NereusTopicPolicyUpdateCoordinator();
        CompletableFuture<NereusTopicPolicySnapshot> firstLoad = new CompletableFuture<>();
        CompletableFuture<NereusTopicPolicySnapshot> secondLoad = new CompletableFuture<>();
        List<NereusTopicPolicySnapshot> applied = new ArrayList<>();

        CompletableFuture<Void> first = coordinator.refresh(
                () -> firstLoad, Runnable::run, snapshot -> {
                    applied.add(snapshot);
                    return CompletableFuture.completedFuture(null);
                });
        CompletableFuture<Void> second = coordinator.refresh(
                () -> secondLoad, Runnable::run, snapshot -> {
                    applied.add(snapshot);
                    return CompletableFuture.completedFuture(null);
                });
        NereusTopicPolicySnapshot firstSnapshot = safePolicySnapshot();
        NereusTopicPolicySnapshot secondSnapshot = safePolicySnapshot();
        secondLoad.complete(secondSnapshot);
        second.join();
        firstLoad.complete(firstSnapshot);
        first.join();

        assertThat(applied).containsExactly(secondSnapshot);
    }

    @Test
    public void stalePolicyRefreshWaitsForLatestFailure() {
        NereusTopicPolicyUpdateCoordinator coordinator = new NereusTopicPolicyUpdateCoordinator();
        CompletableFuture<NereusTopicPolicySnapshot> firstLoad = new CompletableFuture<>();
        CompletableFuture<NereusTopicPolicySnapshot> secondLoad = new CompletableFuture<>();
        CompletableFuture<Void> first = coordinator.refresh(
                () -> firstLoad, Runnable::run, ignored -> CompletableFuture.completedFuture(null));
        CompletableFuture<Void> second = coordinator.refresh(
                () -> secondLoad, Runnable::run, ignored -> CompletableFuture.completedFuture(null));

        firstLoad.complete(safePolicySnapshot());
        assertThat(first).isNotDone();
        secondLoad.completeExceptionally(new IllegalStateException("latest failed"));

        assertThatThrownBy(first::join).hasRootCauseMessage("latest failed");
        assertThatThrownBy(second::join).hasRootCauseMessage("latest failed");
    }

    @Test
    public void stalePolicyRefreshFailureWaitsForLatestSuccess() {
        NereusTopicPolicyUpdateCoordinator coordinator = new NereusTopicPolicyUpdateCoordinator();
        CompletableFuture<NereusTopicPolicySnapshot> firstLoad = new CompletableFuture<>();
        CompletableFuture<NereusTopicPolicySnapshot> secondLoad = new CompletableFuture<>();
        CompletableFuture<Void> first = coordinator.refresh(
                () -> firstLoad, Runnable::run, ignored -> CompletableFuture.completedFuture(null));
        CompletableFuture<Void> second = coordinator.refresh(
                () -> secondLoad, Runnable::run, ignored -> CompletableFuture.completedFuture(null));

        firstLoad.completeExceptionally(new IllegalStateException("stale failed"));
        assertThat(first).isNotDone();
        secondLoad.complete(safePolicySnapshot());

        first.join();
        second.join();
    }

    private static NereusResolvedTopicFeatures safeFeatures() {
        return new NereusResolvedTopicFeatures(
                Set.of(), false, 0, 0, 0,
                Optional.of(new RetentionPolicies(0, 0)), disabledBacklogQuotas(), false,
                false, false, false, false, false);
    }

    private static NereusTopicPolicySnapshot safePolicySnapshot() {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setStorageClassName("nereus");
        RetentionPolicySnapshot retention =
                RetentionPolicySnapshot.fromCanonicalMinutesAndMebibytes(0, 0);
        return new NereusTopicPolicySnapshot(
                new NereusTopicOpenContext(config, safeFeatures(), retention),
                new Policies(),
                Optional.empty(),
                Optional.empty());
    }

    private static NereusResolvedTopicFeatures unsupported(String feature) {
        return switch (feature) {
            case "SYSTEM_OR_INTERNAL_TOPIC" -> featureFlags(
                    Set.of(), false, 0, false, false, false, true);
            case "GEO_REPLICATION" -> featureFlags(
                    Set.of("remote"), false, 0, false, false, false, false);
            case "DEDUPLICATION" -> featureFlags(
                    Set.of(), true, 0, false, false, false, false);
            case "COMPACTION" -> featureFlags(
                    Set.of(), false, 1, false, false, false, false);
            case "PULSAR_OFFLOAD" -> featureFlags(
                    Set.of(), false, 0, true, false, false, false);
            case "ENTRY_FILTERS" -> featureFlags(
                    Set.of(), false, 0, false, true, false, false);
            case "SHADOW_OR_MIGRATION" -> featureFlags(
                    Set.of(), false, 0, false, false, true, false);
            default -> throw new IllegalArgumentException("unknown feature " + feature);
        };
    }

    private static NereusResolvedTopicFeatures featureFlags(
            Set<String> remote,
            boolean deduplication,
            long compaction,
            boolean offload,
            boolean filters,
            boolean shadowOrMigration,
            boolean systemOrInternal) {
        return new NereusResolvedTopicFeatures(
                remote, deduplication, 0, 0, compaction,
                Optional.of(new RetentionPolicies(0, 0)), disabledBacklogQuotas(), false,
                offload, filters, shadowOrMigration, systemOrInternal, false);
    }

    private static Map<BacklogQuotaType, BacklogQuota> disabledBacklogQuotas() {
        EnumMap<BacklogQuotaType, BacklogQuota> quotas = new EnumMap<>(BacklogQuotaType.class);
        for (BacklogQuotaType type : BacklogQuotaType.values()) {
            quotas.put(type, quota(-1, -1, BacklogQuota.RetentionPolicy.producer_request_hold));
        }
        return quotas;
    }

    private static BacklogQuotaImpl quota(
            long size,
            int time,
            BacklogQuota.RetentionPolicy policy) {
        return BacklogQuotaImpl.builder()
                .limitSize(size)
                .limitTime(time)
                .retentionPolicy(policy)
                .build();
    }

    private static ByteBuf entry(MessageMetadata metadata) {
        ByteBuf payload = Unpooled.copiedBuffer("payload", StandardCharsets.UTF_8);
        try {
            return Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
        } finally {
            payload.release();
        }
    }
}
