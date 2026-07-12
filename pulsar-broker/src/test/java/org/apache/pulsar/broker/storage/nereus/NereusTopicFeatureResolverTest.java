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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
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
        assertThat(systemFeatures.systemOrInternalTopic()).isTrue();
    }

    @DataProvider
    public Object[][] unsupportedFeatures() {
        return new Object[][] {
            {"SYSTEM_OR_INTERNAL_TOPIC"},
            {"GEO_REPLICATION"},
            {"DEDUPLICATION"},
            {"MESSAGE_TTL"},
            {"SUBSCRIPTION_EXPIRATION"},
            {"COMPACTION"},
            {"RETENTION"},
            {"BACKLOG_EVICTION"},
            {"PULSAR_OFFLOAD"},
            {"ENTRY_FILTERS"},
            {"SHADOW_OR_MIGRATION"}
        };
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
        assertThatThrownBy(() -> validator.validateSubscribe(
                SubType.Shared, false, false, false, null))
                .hasMessage("NEREUS_UNSUPPORTED_SUBSCRIPTION:SUBSCRIPTION_TYPE");
        assertThatThrownBy(() -> validator.validateSubscribe(
                SubType.Exclusive, true, false, false, null))
                .hasMessage("NEREUS_UNSUPPORTED_SUBSCRIPTION:DURABLE");
        assertThatThrownBy(() -> validator.validateExistingDurableCursors(true))
                .hasMessage("NEREUS_UNSUPPORTED_SUBSCRIPTION:EXISTING_DURABLE_CURSOR");
        assertThatCode(() -> validator.validateExistingDurableCursors(false)).doesNotThrowAnyException();
        assertThatCode(() -> validator.validateSubscribe(
                SubType.Failover, false, false, false, null)).doesNotThrowAnyException();
    }

    private static NereusResolvedTopicFeatures safeFeatures() {
        return new NereusResolvedTopicFeatures(
                Set.of(), false, 0, 0, 0, false, false, false, false, false, false);
    }

    private static NereusResolvedTopicFeatures unsupported(String feature) {
        return switch (feature) {
            case "SYSTEM_OR_INTERNAL_TOPIC" -> new NereusResolvedTopicFeatures(
                    Set.of(), false, 0, 0, 0, false, false, false, false, false, true);
            case "GEO_REPLICATION" -> new NereusResolvedTopicFeatures(
                    Set.of("remote"), false, 0, 0, 0, false, false, false, false, false, false);
            case "DEDUPLICATION" -> new NereusResolvedTopicFeatures(
                    Set.of(), true, 0, 0, 0, false, false, false, false, false, false);
            case "MESSAGE_TTL" -> new NereusResolvedTopicFeatures(
                    Set.of(), false, 1, 0, 0, false, false, false, false, false, false);
            case "SUBSCRIPTION_EXPIRATION" -> new NereusResolvedTopicFeatures(
                    Set.of(), false, 0, 1, 0, false, false, false, false, false, false);
            case "COMPACTION" -> new NereusResolvedTopicFeatures(
                    Set.of(), false, 0, 0, 1, false, false, false, false, false, false);
            case "RETENTION" -> new NereusResolvedTopicFeatures(
                    Set.of(), false, 0, 0, 0, true, false, false, false, false, false);
            case "BACKLOG_EVICTION" -> new NereusResolvedTopicFeatures(
                    Set.of(), false, 0, 0, 0, false, true, false, false, false, false);
            case "PULSAR_OFFLOAD" -> new NereusResolvedTopicFeatures(
                    Set.of(), false, 0, 0, 0, false, false, true, false, false, false);
            case "ENTRY_FILTERS" -> new NereusResolvedTopicFeatures(
                    Set.of(), false, 0, 0, 0, false, false, false, true, false, false);
            case "SHADOW_OR_MIGRATION" -> new NereusResolvedTopicFeatures(
                    Set.of(), false, 0, 0, 0, false, false, false, false, true, false);
            default -> throw new IllegalArgumentException("unknown feature " + feature);
        };
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
