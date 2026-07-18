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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.testng.annotations.Test;

public class NereusTopicFeatureValidatorTest {
    private final NereusTopicFeatureValidator validator = new NereusTopicFeatureValidator();

    @Test
    public void admitsTheExactF3SubscriptionMatrix() {
        for (SubType type : new SubType[] {SubType.Exclusive, SubType.Failover, SubType.Shared}) {
            assertThatCode(() -> validator.validateSubscribe(type, true, false, false, null))
                    .doesNotThrowAnyException();
            assertThatCode(() -> validator.validateSubscribe(type, false, false, false, new KeySharedMeta()))
                    .doesNotThrowAnyException();
        }
        assertThatThrownBy(() -> validator.validateSubscribe(
                SubType.Key_Shared, true, false, false, null))
                .hasMessage("NEREUS_UNSUPPORTED_SUBSCRIPTION:KEY_SHARED");
        assertThatThrownBy(() -> validator.validateSubscribe(
                SubType.Exclusive, true, true, false, null))
                .hasMessage("NEREUS_UNSUPPORTED_SUBSCRIPTION:READ_COMPACTED");
        assertThatThrownBy(() -> validator.validateSubscribe(
                SubType.Exclusive, true, false, true, null))
                .hasMessage("NEREUS_UNSUPPORTED_SUBSCRIPTION:REPLICATED");
    }

    @Test
    public void admitsCursorLifecycleAndGenerationReadyRetentionPolicies() {
        TopicName topic = TopicName.get("persistent://tenant/ns/topic");
        NereusResolvedTopicFeatures ttlAndExpiration = features(
                new RetentionPolicies(0, 0), disabledBacklogQuotas(), false, false);
        assertThatCode(() -> validator.validateTopicOpen(
                topic, new ManagedLedgerConfig(), ttlAndExpiration)).doesNotThrowAnyException();

        NereusResolvedTopicFeatures retentionNotReady = features(
                new RetentionPolicies(30, 64), disabledBacklogQuotas(), false, false);
        assertThatThrownBy(() -> validator.validateTopicOpen(
                topic, new ManagedLedgerConfig(), retentionNotReady))
                .hasMessage("NEREUS_UNSUPPORTED_TOPIC_FEATURE:GENERATION_PROTOCOL_NOT_READY");

        NereusResolvedTopicFeatures retentionReady = retentionNotReady
                .withGenerationProtocolRuntimeReady(true);
        assertThatCode(() -> validator.validateTopicOpen(
                topic, new ManagedLedgerConfig(), retentionReady)).doesNotThrowAnyException();
    }

    @Test
    public void admitsOnlyGenerationReadySizeAndPreciseTimeEviction() {
        TopicName topic = TopicName.get("persistent://tenant/ns/topic");
        Map<BacklogQuotaType, BacklogQuota> sizeEviction = disabledBacklogQuotas();
        sizeEviction.put(
                BacklogQuotaType.destination_storage,
                quota(1_024, -1, BacklogQuota.RetentionPolicy.consumer_backlog_eviction));
        NereusResolvedTopicFeatures sizeNotReady = features(
                new RetentionPolicies(0, 0), sizeEviction, false, false);
        assertThatThrownBy(() -> validator.validateTopicOpen(
                topic, new ManagedLedgerConfig(), sizeNotReady))
                .hasMessage("NEREUS_UNSUPPORTED_TOPIC_FEATURE:GENERATION_PROTOCOL_NOT_READY");
        assertThatCode(() -> validator.validateTopicOpen(
                topic,
                new ManagedLedgerConfig(),
                sizeNotReady.withGenerationProtocolRuntimeReady(true)))
                .doesNotThrowAnyException();

        Map<BacklogQuotaType, BacklogQuota> timeEviction = disabledBacklogQuotas();
        timeEviction.put(
                BacklogQuotaType.message_age,
                quota(-1, 60, BacklogQuota.RetentionPolicy.consumer_backlog_eviction));
        NereusResolvedTopicFeatures nonPrecise = features(
                new RetentionPolicies(0, 0), timeEviction, false, true);
        assertThatThrownBy(() -> validator.validateTopicOpen(
                topic, new ManagedLedgerConfig(), nonPrecise))
                .hasMessage("NEREUS_UNSUPPORTED_TOPIC_FEATURE:BACKLOG_TIME_EVICTION_REQUIRES_PRECISE_CHECK");
        assertThatCode(() -> validator.validateTopicOpen(
                topic,
                new ManagedLedgerConfig(),
                features(new RetentionPolicies(0, 0), timeEviction, true, true)))
                .doesNotThrowAnyException();
    }

    @Test
    public void producerBacklogPoliciesDoNotClaimDeleteAuthority() {
        Map<BacklogQuotaType, BacklogQuota> quotas = disabledBacklogQuotas();
        quotas.put(
                BacklogQuotaType.destination_storage,
                quota(1_024, -1, BacklogQuota.RetentionPolicy.producer_exception));
        NereusResolvedTopicFeatures features = features(
                new RetentionPolicies(0, 0), quotas, false, false);

        assertThatCode(() -> validator.validateTopicOpen(
                TopicName.get("persistent://tenant/ns/topic"),
                new ManagedLedgerConfig(),
                features)).doesNotThrowAnyException();
    }

    @Test
    public void rejectsUnrepresentableRetentionBeforeItCanBecomeAStoredSnapshot() {
        assertThatThrownBy(() -> features(
                new RetentionPolicies(-1, Long.MAX_VALUE),
                disabledBacklogQuotas(),
                false,
                true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflows");
        assertThatThrownBy(() -> features(
                new RetentionPolicies(0, 1),
                disabledBacklogQuotas(),
                false,
                true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both be zero");
    }

    private static NereusResolvedTopicFeatures features(
            RetentionPolicies retention,
            Map<BacklogQuotaType, BacklogQuota> quotas,
            boolean preciseTimeCheck,
            boolean generationReady) {
        return new NereusResolvedTopicFeatures(
                Set.of(), false, 30, 60, 0,
                Optional.of(retention), quotas, preciseTimeCheck,
                false, false, false, false, generationReady);
    }

    private static Map<BacklogQuotaType, BacklogQuota> disabledBacklogQuotas() {
        EnumMap<BacklogQuotaType, BacklogQuota> quotas = new EnumMap<>(BacklogQuotaType.class);
        for (BacklogQuotaType type : BacklogQuotaType.values()) {
            quotas.put(type, quota(-1, -1, BacklogQuota.RetentionPolicy.producer_request_hold));
        }
        return quotas;
    }

    private static BacklogQuota quota(
            long size,
            int time,
            BacklogQuota.RetentionPolicy policy) {
        return BacklogQuota.builder()
                .limitSize(size)
                .limitTime(time)
                .retentionPolicy(policy)
                .build();
    }
}
