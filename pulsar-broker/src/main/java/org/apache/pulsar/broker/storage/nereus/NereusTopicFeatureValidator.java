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

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;

/** Closed F3 topic, subscription, and admin admission gate for the Nereus storage class. */
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

    public void validateProducer(Producer producer) throws NotAllowedException {
        java.util.Objects.requireNonNull(producer, "producer");
        if (producer.isRemote()) {
            throw new NotAllowedException("NEREUS_UNSUPPORTED_PRODUCER:REMOTE_REPLICATION");
        }
    }

    public void validateSubscribe(
            SubType type,
            boolean durable,
            boolean readCompacted,
            boolean replicated,
            KeySharedMeta keySharedMeta) throws NotAllowedException {
        java.util.Objects.requireNonNull(type, "type");
        rejectSubscription(type == SubType.Key_Shared, "KEY_SHARED");
        rejectSubscription(type != SubType.Exclusive
                && type != SubType.Failover
                && type != SubType.Shared, "SUBSCRIPTION_TYPE");
        rejectSubscription(readCompacted, "READ_COMPACTED");
        rejectSubscription(replicated, "REPLICATED");
        rejectSubscription(keySharedMeta != null && !keySharedMeta.getHashRangesList().isEmpty(), "KEY_SHARED_META");
    }

    public void validateCreateSubscription(boolean cursorProtocolRuntimeReady) throws NotAllowedException {
        requireCursorProtocolRuntime(cursorProtocolRuntimeReady);
    }

    public void validateExistingDurableCursors(boolean cursorProtocolRuntimeReady) throws NotAllowedException {
        requireCursorProtocolRuntime(cursorProtocolRuntimeReady);
    }

    public void validatePublish(ByteBuf entry, boolean transactional) throws NotAllowedException {
        java.util.Objects.requireNonNull(entry, "entry");
        if (transactional) {
            throw new NotAllowedException("NEREUS_UNSUPPORTED_PUBLISH:TRANSACTIONAL");
        }
        int readerIndex = entry.readerIndex();
        int writerIndex = entry.writerIndex();
        int referenceCount = entry.refCnt();
        try {
            MessageMetadata metadata = Commands.parseMessageMetadata(entry);
            if (metadata.hasMarkerType()) {
                throw new NotAllowedException("NEREUS_UNSUPPORTED_PUBLISH:MARKER");
            }
            if (metadata.hasDeliverAtTime()) {
                throw new NotAllowedException("NEREUS_UNSUPPORTED_PUBLISH:DELAYED_DELIVERY");
            }
        } catch (NotAllowedException error) {
            throw error;
        } catch (RuntimeException parseFailure) {
            throw new NotAllowedException("NEREUS_UNSUPPORTED_PUBLISH:INVALID_METADATA");
        } finally {
            entry.setIndex(readerIndex, writerIndex);
            if (entry.refCnt() != referenceCount) {
                throw new IllegalStateException("message metadata validation changed ByteBuf reference count");
            }
        }
    }

    public void validateEndTransaction() throws NotAllowedException {
        throw new NotAllowedException("NEREUS_UNSUPPORTED_TRANSACTION");
    }

    public void validateAdminOperation(NereusAdminOperation operation) throws NotAllowedException {
        java.util.Objects.requireNonNull(operation, "operation");
        switch (operation) {
            case TERMINATE_TOPIC, DELETE_TOPIC, UNLOAD_TOPIC, DELETE_DURABLE_SUBSCRIPTION,
                    ANALYZE_BACKLOG, CLEAR_BACKLOG, SKIP_MESSAGES, EXPIRE_MESSAGES, RESET_CURSOR -> {
                return;
            }
            case TRIGGER_COMPACTION, READ_COMPACTION_STATUS, TRIGGER_OFFLOAD, READ_OFFLOAD_STATUS,
                    TRIM_TOPIC, TRUNCATE_TOPIC, SET_SHADOW_TOPICS, MIGRATE_TOPIC -> throw new NotAllowedException(
                            "NEREUS_UNSUPPORTED_ADMIN_OPERATION:" + operation.name());
        }
    }

    private static void requireCursorProtocolRuntime(boolean ready) throws NotAllowedException {
        rejectSubscription(!ready, "CURSOR_PROTOCOL_NOT_READY");
    }

    private static void reject(boolean rejected, String feature) throws NotAllowedException {
        if (rejected) {
            throw new NotAllowedException("NEREUS_UNSUPPORTED_TOPIC_FEATURE:" + feature);
        }
    }

    private static void rejectSubscription(boolean rejected, String reason) throws NotAllowedException {
        if (rejected) {
            throw new NotAllowedException("NEREUS_UNSUPPORTED_SUBSCRIPTION:" + reason);
        }
    }
}
