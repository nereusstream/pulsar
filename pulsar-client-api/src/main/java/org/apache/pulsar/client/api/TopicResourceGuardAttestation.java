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
package org.apache.pulsar.client.api;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/** Immutable broker attestation for a physical topic and partition. */
public record TopicResourceGuardAttestation(
        int guardVersion,
        String authenticatedClusterId,
        byte[] resourceIncarnation,
        long topicCreationTimestamp,
        String physicalTopic,
        int partition) implements Serializable {

    public TopicResourceGuardAttestation {
        if (guardVersion != TopicResourceGuard.VERSION) {
            throw new IllegalArgumentException("Unsupported topic resource guard version: " + guardVersion);
        }
        if (authenticatedClusterId == null || authenticatedClusterId.isBlank()) {
            throw new IllegalArgumentException("authenticatedClusterId must not be blank");
        }
        Objects.requireNonNull(resourceIncarnation, "resourceIncarnation");
        if (resourceIncarnation.length != TopicResourceGuard.RESOURCE_INCARNATION_BYTES) {
            throw new IllegalArgumentException("resourceIncarnation must contain exactly 32 bytes");
        }
        if (physicalTopic == null || physicalTopic.isBlank()) {
            throw new IllegalArgumentException("physicalTopic must not be blank");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative");
        }
        resourceIncarnation = resourceIncarnation.clone();
    }

    public TopicResourceGuardAttestation(TopicResourceGuard guard, String physicalTopic, int partition) {
        this(guard.guardVersion(), guard.authenticatedClusterId(), guard.resourceIncarnation(),
                guard.topicCreationTimestamp(), physicalTopic, partition);
    }

    @Override
    public byte[] resourceIncarnation() {
        return resourceIncarnation.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TopicResourceGuardAttestation)) {
            return false;
        }
        TopicResourceGuardAttestation that = (TopicResourceGuardAttestation) other;
        return guardVersion == that.guardVersion
                && topicCreationTimestamp == that.topicCreationTimestamp
                && partition == that.partition
                && authenticatedClusterId.equals(that.authenticatedClusterId)
                && physicalTopic.equals(that.physicalTopic)
                && Arrays.equals(resourceIncarnation, that.resourceIncarnation);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(guardVersion, authenticatedClusterId, topicCreationTimestamp,
                physicalTopic, partition);
        return 31 * result + Arrays.hashCode(resourceIncarnation);
    }
}
