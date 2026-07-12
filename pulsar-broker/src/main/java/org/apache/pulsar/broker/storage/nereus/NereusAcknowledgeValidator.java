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

/** Closed acknowledgement boundary for the limited F2 non-durable cursor surface. */
public final class NereusAcknowledgeValidator {
    public Optional<NotAllowedException> rejection(
            Subscription subscription,
            SubType subscriptionType,
            CommandAck ack,
            boolean requirePersistedAck) {
        java.util.Objects.requireNonNull(subscription, "subscription");
        java.util.Objects.requireNonNull(subscriptionType, "subscriptionType");
        java.util.Objects.requireNonNull(ack, "ack");
        if (!isNereusSubscription(subscription)) {
            return Optional.empty();
        }
        PersistentSubscription persistentSubscription = (PersistentSubscription) subscription;
        if (persistentSubscription.getCursor().isDurable()) {
            return rejected("DURABLE_CURSOR");
        }
        if (subscriptionType != SubType.Exclusive && subscriptionType != SubType.Failover) {
            return rejected("SUBSCRIPTION_TYPE");
        }
        if (requirePersistedAck) {
            return rejected("PERSISTED_CONFIRMATION");
        }
        if (ack.getAckType() != AckType.Cumulative) {
            return rejected("ACK_TYPE");
        }
        if (ack.getMessageIdsCount() != 1) {
            return rejected("MESSAGE_ID_COUNT");
        }
        if (ack.hasTxnidMostBits() || ack.hasTxnidLeastBits()) {
            return rejected("TRANSACTION");
        }
        if (ack.hasValidationError()) {
            return rejected("VALIDATION_ERROR");
        }
        MessageIdData messageId = ack.getMessageIdAt(0);
        if (messageId.getAckSetsCount() != 0) {
            return rejected("ACK_SET");
        }
        if (messageId.hasBatchIndex() || messageId.hasBatchSize()) {
            return rejected("BATCH_INDEX");
        }
        return Optional.empty();
    }

    public boolean isNereusSubscription(Subscription subscription) {
        return subscription instanceof PersistentSubscription persistentSubscription
                && persistentSubscription.getCursor().getManagedLedger() instanceof NereusManagedLedger;
    }

    private static Optional<NotAllowedException> rejected(String reason) {
        return Optional.of(new NotAllowedException("NEREUS_UNSUPPORTED_ACK:" + reason));
    }
}
