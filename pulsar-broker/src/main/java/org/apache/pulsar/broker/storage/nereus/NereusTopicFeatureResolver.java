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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;

/** Resolves the F2 admission feature view using Pulsar's local-over-global-over-namespace precedence. */
public final class NereusTopicFeatureResolver {
    private NereusTopicFeatureResolver() {
    }

    public static NereusResolvedTopicFeatures resolve(
            ServiceConfiguration broker,
            Policies namespacePolicies,
            Optional<TopicPolicies> localPolicies,
            Optional<TopicPolicies> globalPolicies,
            TopicName topic) {
        java.util.Objects.requireNonNull(broker, "broker");
        java.util.Objects.requireNonNull(namespacePolicies, "namespacePolicies");
        java.util.Objects.requireNonNull(localPolicies, "localPolicies");
        java.util.Objects.requireNonNull(globalPolicies, "globalPolicies");
        java.util.Objects.requireNonNull(topic, "topic");

        Set<String> remoteClusters = new HashSet<>(effectiveTopicValue(
                localPolicies,
                globalPolicies,
                TopicPolicies::getReplicationClusters,
                namespacePolicies.replication_clusters,
                Set.of()));
        remoteClusters.remove(broker.getClusterName());
        boolean deduplicationEnabled = effectiveTopicValue(
                localPolicies,
                globalPolicies,
                TopicPolicies::getDeduplicationEnabled,
                namespacePolicies.deduplicationEnabled,
                broker.isBrokerDeduplicationEnabled());
        int ttl = normalizedNonNegative(effectiveTopicValue(
                localPolicies,
                globalPolicies,
                TopicPolicies::getMessageTTLInSeconds,
                normalize(namespacePolicies.message_ttl_in_seconds),
                broker.getTtlDurationDefaultInSeconds()));
        int expiration = normalizedNonNegative(effectiveTopicValue(
                localPolicies,
                globalPolicies,
                TopicPolicies::getSubscriptionExpirationTimeInMinutes,
                normalize(namespacePolicies.subscription_expiration_time_minutes),
                broker.getSubscriptionExpirationTimeMinutes()));
        long compaction = normalizedNonNegative(effectiveTopicValue(
                localPolicies,
                globalPolicies,
                TopicPolicies::getCompactionThreshold,
                normalize(namespacePolicies.compaction_threshold),
                broker.getBrokerServiceCompactionThresholdInBytes()));
        RetentionPolicies retention = effectiveTopicValue(
                localPolicies,
                globalPolicies,
                TopicPolicies::getRetentionPolicies,
                namespacePolicies.retention_policies,
                new RetentionPolicies(
                        broker.getDefaultRetentionTimeInMinutes(), broker.getDefaultRetentionSizeInMB()));
        boolean retentionEnabled = retention != null
                && (retention.getRetentionTimeInMinutes() != 0 || retention.getRetentionSizeInMB() != 0);
        boolean backlogEvictionEnabled = hasFiniteBacklogEviction(
                broker, namespacePolicies, localPolicies, globalPolicies);
        boolean offloadEnabled = isOffloadEnabled(
                broker, namespacePolicies, localPolicies, globalPolicies);
        EntryFilters entryFilters = effectiveTopicValue(
                localPolicies,
                globalPolicies,
                TopicPolicies::getEntryFilters,
                namespacePolicies.entryFilters,
                new EntryFilters(String.join(",", broker.getEntryFilterNames())));
        boolean entryFiltersEnabled = entryFilters != null
                && StringUtils.isNotBlank(entryFilters.getEntryFilterNames());
        Collection<String> shadowTopics = effectiveTopicValue(
                localPolicies,
                globalPolicies,
                TopicPolicies::getShadowTopics,
                null,
                Set.of());
        boolean shadowOrMigration = !shadowTopics.isEmpty() || namespacePolicies.migrated;
        boolean systemOrInternal = SystemTopicNames.isSystemTopic(topic)
                || NamespaceService.isSystemServiceNamespace(topic.getNamespace())
                || NamespaceService.isHeartbeatNamespace(topic.getNamespaceObject())
                || ExtensibleLoadManagerImpl.isInternalTopic(topic.toString());
        return new NereusResolvedTopicFeatures(
                remoteClusters,
                deduplicationEnabled,
                ttl,
                expiration,
                compaction,
                retentionEnabled,
                backlogEvictionEnabled,
                offloadEnabled,
                entryFiltersEnabled,
                shadowOrMigration,
                systemOrInternal);
    }

    private static boolean hasFiniteBacklogEviction(
            ServiceConfiguration broker,
            Policies namespacePolicies,
            Optional<TopicPolicies> localPolicies,
            Optional<TopicPolicies> globalPolicies) {
        for (BacklogQuotaType type : BacklogQuotaType.values()) {
            BacklogQuota quota = effectiveTopicValue(
                    localPolicies,
                    globalPolicies,
                    policies -> quota(policies.getBackLogQuotaMap(), type),
                    namespacePolicies.backlog_quota_map.get(type),
                    defaultQuota(broker));
            if (quota != null
                    && quota.getPolicy() == BacklogQuota.RetentionPolicy.consumer_backlog_eviction
                    && (quota.getLimitSize() >= 0 || quota.getLimitTime() >= 0)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isOffloadEnabled(
            ServiceConfiguration broker,
            Policies namespacePolicies,
            Optional<TopicPolicies> localPolicies,
            Optional<TopicPolicies> globalPolicies) {
        OffloadPoliciesImpl topicPolicies = effectiveTopicValue(
                localPolicies,
                globalPolicies,
                TopicPolicies::getOffloadPolicies,
                null,
                null);
        OffloadPoliciesImpl namespaceOffload = namespacePolicies.offload_policies instanceof OffloadPoliciesImpl value
                ? value
                : null;
        OffloadPoliciesImpl merged = OffloadPoliciesImpl.mergeConfiguration(
                topicPolicies,
                OffloadPoliciesImpl.oldPoliciesCompatible(namespaceOffload, namespacePolicies),
                broker.getProperties());
        return merged != null && (StringUtils.isNotBlank(merged.getManagedLedgerOffloadDriver())
                || nonNegative(merged.getManagedLedgerOffloadThresholdInBytes())
                || nonNegative(merged.getManagedLedgerOffloadThresholdInSeconds())
                || nonNegative(merged.getManagedLedgerOffloadDeletionLagInMillis()));
    }

    private static BacklogQuota quota(Map<String, ? extends BacklogQuota> quotas, BacklogQuotaType type) {
        return quotas == null ? null : quotas.get(type.toString());
    }

    @SuppressWarnings("deprecation")
    private static BacklogQuota defaultQuota(ServiceConfiguration broker) {
        double legacyGigabytes = broker.getBacklogQuotaDefaultLimitGB();
        long size = legacyGigabytes > 0
                ? (long) (legacyGigabytes * BacklogQuotaImpl.BYTES_IN_GIGABYTE)
                : broker.getBacklogQuotaDefaultLimitBytes();
        return BacklogQuota.builder()
                .limitSize(size)
                .limitTime(broker.getBacklogQuotaDefaultLimitSecond())
                .retentionPolicy(broker.getBacklogQuotaDefaultRetentionPolicy())
                .build();
    }

    private static <T> T effectiveTopicValue(
            Optional<TopicPolicies> localPolicies,
            Optional<TopicPolicies> globalPolicies,
            Function<TopicPolicies, T> getter,
            T namespaceValue,
            T brokerValue) {
        T local = localPolicies.map(getter).orElse(null);
        if (local != null) {
            return local;
        }
        T global = globalPolicies.map(getter).orElse(null);
        if (global != null) {
            return global;
        }
        return namespaceValue != null ? namespaceValue : brokerValue;
    }

    private static Integer normalize(Integer value) {
        return value != null && value >= 0 ? value : null;
    }

    private static Long normalize(Long value) {
        return value != null && value >= 0 ? value : null;
    }

    private static int normalizedNonNegative(Integer value) {
        return value == null || value < 0 ? 0 : value;
    }

    private static long normalizedNonNegative(Long value) {
        return value == null || value < 0 ? 0 : value;
    }

    private static boolean nonNegative(Long value) {
        return value != null && value >= 0;
    }
}
