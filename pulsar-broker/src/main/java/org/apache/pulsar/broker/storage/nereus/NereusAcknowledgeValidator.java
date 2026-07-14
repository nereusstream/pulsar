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

import com.nereusstream.managedledger.NereusManagedLedger;
import java.util.Optional;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.MessageIdData;

/** Closed command-shape boundary for the F3 acknowledgement surface. */
public final class NereusAcknowledgeValidator {
    public static final int DEFAULT_MAX_POSITIONS = 1_000;

    public Optional<NotAllowedException> rejection(
            Subscription subscription,
            SubType subscriptionType,
            CommandAck ack,
            boolean requirePersistedAck) {
        return rejection(
                subscription, subscriptionType, ack, requirePersistedAck, true, DEFAULT_MAX_POSITIONS);
    }

    public Optional<NotAllowedException> rejection(
            Subscription subscription,
            SubType subscriptionType,
            CommandAck ack,
            boolean requirePersistedAck,
            boolean batchIndexAcknowledgmentEnabled,
            int maxPositions) {
        java.util.Objects.requireNonNull(subscription, "subscription");
        java.util.Objects.requireNonNull(subscriptionType, "subscriptionType");
        java.util.Objects.requireNonNull(ack, "ack");
        if (maxPositions <= 0) {
            throw new IllegalArgumentException("maxPositions must be positive");
        }
        if (!isNereusSubscription(subscription)) {
            return Optional.empty();
        }
        PersistentSubscription persistentSubscription = (PersistentSubscription) subscription;
        if (ack.hasTxnidMostBits() || ack.hasTxnidLeastBits()) {
            return rejected("TRANSACTION");
        }
        if (ack.hasValidationError()) {
            return rejected("VALIDATION_ERROR");
        }
        if (subscriptionType == SubType.Key_Shared) {
            return rejected("KEY_SHARED");
        }
        if (subscriptionType != SubType.Exclusive
                && subscriptionType != SubType.Failover
                && subscriptionType != SubType.Shared) {
            return rejected("SUBSCRIPTION_TYPE");
        }
        if (requirePersistedAck && !persistentSubscription.getCursor().isDurable()) {
            return rejected("PERSISTED_CONFIRMATION_NON_DURABLE");
        }
        if (ack.getAckType() == AckType.Cumulative) {
            if (subscriptionType == SubType.Shared) {
                return rejected("SHARED_CUMULATIVE");
            }
            if (ack.getMessageIdsCount() != 1) {
                return rejected("MESSAGE_ID_COUNT");
            }
        } else if (ack.getAckType() == AckType.Individual) {
            if (ack.getMessageIdsCount() == 0 || ack.getMessageIdsCount() > maxPositions) {
                return rejected("MESSAGE_ID_COUNT");
            }
        } else {
            return rejected("ACK_TYPE");
        }
        for (MessageIdData messageId : ack.getMessageIdsList()) {
            Optional<NotAllowedException> batchRejection = validateBatchShape(
                    messageId, batchIndexAcknowledgmentEnabled);
            if (batchRejection.isPresent()) {
                return batchRejection;
            }
        }
        return Optional.empty();
    }

    private static Optional<NotAllowedException> validateBatchShape(
            MessageIdData messageId, boolean batchIndexAcknowledgmentEnabled) {
        boolean partialBatch = messageId.getAckSetsCount() != 0 || messageId.hasBatchIndex();
        if (partialBatch && !batchIndexAcknowledgmentEnabled) {
            return rejected("PARTIAL_BATCH_DISABLED");
        }
        if (messageId.hasBatchSize() && messageId.getBatchSize() <= 0) {
            return rejected("BATCH_SHAPE");
        }
        if (messageId.hasBatchIndex()) {
            if (!messageId.hasBatchSize()
                    || messageId.getBatchIndex() < 0
                    || messageId.getBatchIndex() >= messageId.getBatchSize()) {
                return rejected("BATCH_SHAPE");
            }
        }
        return Optional.empty();
    }

    public boolean isNereusSubscription(Subscription subscription) {
        return subscription instanceof PersistentSubscription persistentSubscription
                && persistentSubscription.getCursor().getManagedLedger() instanceof NereusManagedLedger;
    }

    public boolean isNereusDurableSubscription(Subscription subscription) {
        return isNereusSubscription(subscription)
                && ((PersistentSubscription) subscription).getCursor().isDurable();
    }

    private static Optional<NotAllowedException> rejected(String reason) {
        return Optional.of(new NotAllowedException("NEREUS_UNSUPPORTED_ACK:" + reason));
    }
}
