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

import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.common.naming.TopicName;

/** Closed F2 topic-open admission gate for the Nereus storage class. */
public final class NereusTopicFeatureValidator {
    public void validateTopicOpen(
            TopicName topic,
            ManagedLedgerConfig config,
            NereusResolvedTopicFeatures features) throws NotAllowedException {
        java.util.Objects.requireNonNull(topic, "topic");
        java.util.Objects.requireNonNull(config, "config");
        java.util.Objects.requireNonNull(features, "features");
        reject(features.systemOrInternalTopic(), "SYSTEM_OR_INTERNAL_TOPIC");
        reject(!features.remoteReplicationClusters().isEmpty(), "GEO_REPLICATION");
        reject(features.deduplicationEnabled(), "DEDUPLICATION");
        reject(features.messageTtlSeconds() != 0, "MESSAGE_TTL");
        reject(features.subscriptionExpirationMinutes() != 0, "SUBSCRIPTION_EXPIRATION");
        reject(features.compactionThresholdBytes() != 0, "COMPACTION");
        reject(features.retentionEnabled(), "RETENTION");
        reject(features.backlogEvictionEnabled(), "BACKLOG_EVICTION");
        reject(features.pulsarOffloadEnabled(), "PULSAR_OFFLOAD");
        reject(features.entryFiltersEnabled(), "ENTRY_FILTERS");
        reject(features.shadowOrMigrationEnabled(), "SHADOW_OR_MIGRATION");
        reject(config.getManagedLedgerInterceptor() != null, "MANAGED_LEDGER_INTERCEPTOR");
        reject(config.isAutoSkipNonRecoverableData(), "AUTO_SKIP_NON_RECOVERABLE_DATA");
        reject(config.getShadowSource() != null, "SHADOW_SOURCE");
    }

    private static void reject(boolean rejected, String feature) throws NotAllowedException {
        if (rejected) {
            throw new NotAllowedException("NEREUS_UNSUPPORTED_TOPIC_FEATURE:" + feature);
        }
    }
}
