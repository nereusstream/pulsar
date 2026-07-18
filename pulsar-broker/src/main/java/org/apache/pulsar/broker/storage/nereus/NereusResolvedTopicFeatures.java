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

import com.nereusstream.managedledger.retention.RetentionPolicySnapshot;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.RetentionPolicies;

/** Immutable effective feature view resolved from one policy snapshot. */
public record NereusResolvedTopicFeatures(
        Set<String> remoteReplicationClusters,
        boolean deduplicationEnabled,
        int messageTtlSeconds,
        int subscriptionExpirationMinutes,
        long compactionThresholdBytes,
        Optional<RetentionPolicies> retention,
        Map<BacklogQuotaType, BacklogQuota> backlogQuotas,
        boolean preciseTimeBasedBacklogQuotaCheck,
        boolean pulsarOffloadEnabled,
        boolean entryFiltersEnabled,
        boolean shadowOrMigrationEnabled,
        boolean systemOrInternalTopic,
        boolean generationProtocolRuntimeReady) {
    public NereusResolvedTopicFeatures {
        remoteReplicationClusters = Set.copyOf(
                java.util.Objects.requireNonNull(remoteReplicationClusters, "remoteReplicationClusters"));
        if (messageTtlSeconds < 0 || subscriptionExpirationMinutes < 0 || compactionThresholdBytes < 0) {
            throw new IllegalArgumentException("normalized topic feature values cannot be negative");
        }
        retention = copyRetention(retention);
        retention.ifPresent(value -> RetentionPolicySnapshot.fromCanonicalMinutesAndMebibytes(
                value.getRetentionTimeInMinutes(), value.getRetentionSizeInMB()));
        backlogQuotas = copyBacklogQuotas(backlogQuotas);
    }

    public boolean retentionEnabled() {
        return retention.map(value -> value.getRetentionTimeInMinutes() != 0
                        || value.getRetentionSizeInMB() != 0)
                .orElse(false);
    }

    public boolean sizeBacklogEvictionEnabled() {
        return isConsumerEviction(BacklogQuotaType.destination_storage, true);
    }

    public boolean timeBacklogEvictionEnabled() {
        return isConsumerEviction(BacklogQuotaType.message_age, false);
    }

    public boolean backlogEvictionEnabled() {
        return sizeBacklogEvictionEnabled() || timeBacklogEvictionEnabled();
    }

    public boolean requiresGenerationProtocolRuntime() {
        return retentionEnabled() || backlogEvictionEnabled();
    }

    public NereusResolvedTopicFeatures withGenerationProtocolRuntimeReady(boolean ready) {
        if (generationProtocolRuntimeReady == ready) {
            return this;
        }
        return new NereusResolvedTopicFeatures(
                remoteReplicationClusters,
                deduplicationEnabled,
                messageTtlSeconds,
                subscriptionExpirationMinutes,
                compactionThresholdBytes,
                retention,
                backlogQuotas,
                preciseTimeBasedBacklogQuotaCheck,
                pulsarOffloadEnabled,
                entryFiltersEnabled,
                shadowOrMigrationEnabled,
                systemOrInternalTopic,
                ready);
    }

    private boolean isConsumerEviction(BacklogQuotaType type, boolean sizeDimension) {
        BacklogQuota quota = backlogQuotas.get(type);
        return quota.getPolicy() == BacklogQuota.RetentionPolicy.consumer_backlog_eviction
                && (sizeDimension ? quota.getLimitSize() >= 0 : quota.getLimitTime() >= 0);
    }

    private static Optional<RetentionPolicies> copyRetention(Optional<RetentionPolicies> source) {
        java.util.Objects.requireNonNull(source, "retention");
        return source.map(value -> new RetentionPolicies(
                value.getRetentionTimeInMinutes(), value.getRetentionSizeInMB()));
    }

    private static Map<BacklogQuotaType, BacklogQuota> copyBacklogQuotas(
            Map<BacklogQuotaType, BacklogQuota> source) {
        java.util.Objects.requireNonNull(source, "backlogQuotas");
        if (!source.keySet().equals(EnumSet.allOf(BacklogQuotaType.class))) {
            throw new IllegalArgumentException("backlogQuotas must contain every backlog quota type exactly once");
        }
        EnumMap<BacklogQuotaType, BacklogQuota> copy = new EnumMap<>(BacklogQuotaType.class);
        source.forEach((type, quota) -> copy.put(
                java.util.Objects.requireNonNull(type, "backlog quota type"),
                ImmutableBacklogQuota.copyOf(quota)));
        return Map.copyOf(copy);
    }

    private record ImmutableBacklogQuota(
            long limitSize,
            int limitTime,
            BacklogQuota.RetentionPolicy policy) implements BacklogQuota {
        private ImmutableBacklogQuota {
            if (limitSize < -1) {
                throw new IllegalArgumentException("backlog quota limitSize must be -1 or non-negative");
            }
            if (limitTime < -1) {
                throw new IllegalArgumentException("backlog quota limitTime must be -1 or non-negative");
            }
            java.util.Objects.requireNonNull(policy, "backlog quota policy");
        }

        private static ImmutableBacklogQuota copyOf(BacklogQuota source) {
            BacklogQuota exact = java.util.Objects.requireNonNull(source, "backlog quota");
            return new ImmutableBacklogQuota(
                    exact.getLimitSize(), exact.getLimitTime(), exact.getPolicy());
        }

        @Deprecated
        @Override
        public long getLimit() {
            return limitSize;
        }

        @Override
        public long getLimitSize() {
            return limitSize;
        }

        @Override
        public int getLimitTime() {
            return limitTime;
        }

        @Override
        public BacklogQuota.RetentionPolicy getPolicy() {
            return policy;
        }
    }
}
