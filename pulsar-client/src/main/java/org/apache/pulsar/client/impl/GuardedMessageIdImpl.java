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
package org.apache.pulsar.client.impl;

import java.util.Objects;
import org.apache.pulsar.client.api.GuardedMessageId;
import org.apache.pulsar.client.api.GuardedSendSuccessEvidence;
import org.apache.pulsar.client.api.TopicResourceGuard;

final class GuardedMessageIdImpl extends MessageIdImpl implements GuardedMessageId {
    private static final long serialVersionUID = 1L;

    private final TopicResourceGuard resourceGuard;
    private final String physicalTopic;
    private final int partition;
    private final long brokerEntryTimestamp;
    private final GuardedSendSuccessEvidence responseEvidence;

    GuardedMessageIdImpl(long ledgerId, long entryId, int partition, TopicResourceGuard resourceGuard,
                         String physicalTopic, long brokerEntryTimestamp, GuardedSendSuccessEvidence responseEvidence) {
        super(ledgerId, entryId, partition);
        this.resourceGuard = Objects.requireNonNull(resourceGuard, "resourceGuard");
        this.physicalTopic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        if (partition < 0 || brokerEntryTimestamp < 0) {
            throw new IllegalArgumentException("Guarded message identity values must be non-negative");
        }
        this.partition = partition;
        this.brokerEntryTimestamp = brokerEntryTimestamp;
        this.responseEvidence = Objects.requireNonNull(responseEvidence, "responseEvidence");
    }

    @Override
    public TopicResourceGuard resourceGuard() {
        return resourceGuard;
    }

    @Override
    public String physicalTopic() {
        return physicalTopic;
    }

    @Override
    public int partition() {
        return partition;
    }

    @Override
    public long brokerEntryTimestamp() {
        return brokerEntryTimestamp;
    }

    @Override
    public GuardedSendSuccessEvidence responseEvidence() {
        return responseEvidence;
    }
}
