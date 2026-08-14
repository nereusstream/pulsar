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
package org.apache.pulsar.broker.service;

import io.netty.buffer.ByteBuf;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;

/**
 * Immutable, allocation-free comparison view of a topic resource guard.
 *
 * <p>An invalid view is deliberately sticky until a complete, strictly parsed property tuple is
 * published.  It is never treated as an ordinary unguarded topic.</p>
 */
public final class ValidatedTopicResourceGuard {
    public static final String VERSION_PROPERTY = "nereus.resource.guard.version";
    public static final String INCARNATION_PROPERTY = "nereus.resource.incarnation";
    public static final String CREATED_AT_PROPERTY = "nereus.resource.created-at";

    private final boolean valid;
    private final String reason;
    private final TopicResourceGuard guard;
    private final TopicResourceGuardAttestation attestation;
    private final byte[] incarnation;

    private ValidatedTopicResourceGuard(boolean valid, String reason, TopicResourceGuard guard,
                                        TopicResourceGuardAttestation attestation, byte[] incarnation) {
        this.valid = valid;
        this.reason = reason;
        this.guard = guard;
        this.attestation = attestation;
        this.incarnation = incarnation;
    }

    public static ValidatedTopicResourceGuard invalid(String reason) {
        return new ValidatedTopicResourceGuard(false, Objects.requireNonNull(reason, "reason"),
                null, null, null);
    }

    public static ValidatedTopicResourceGuard fromProperties(String physicalTopic, String clusterId,
                                                             int partition, Map<String, String> properties) {
        try {
            if (properties == null) {
                return invalid("resource properties are absent");
            }
            if (!"1".equals(properties.get(VERSION_PROPERTY))) {
                return invalid("resource guard version is absent or unsupported");
            }
            String encodedIncarnation = properties.get(INCARNATION_PROPERTY);
            if (encodedIncarnation == null || encodedIncarnation.isEmpty()
                    || encodedIncarnation.indexOf('=') >= 0) {
                return invalid("resource incarnation is not unpadded base64url");
            }
            byte[] incarnation = Base64.getUrlDecoder().decode(encodedIncarnation);
            if (incarnation.length != TopicResourceGuard.RESOURCE_INCARNATION_BYTES
                    || !Base64.getUrlEncoder().withoutPadding().encodeToString(incarnation)
                    .equals(encodedIncarnation)) {
                return invalid("resource incarnation must be canonical base64url with 32 bytes");
            }
            String createdAt = properties.get(CREATED_AT_PROPERTY);
            if (createdAt == null || createdAt.isEmpty()) {
                return invalid("resource creation timestamp is absent");
            }
            long timestamp = Long.parseUnsignedLong(createdAt);
            TopicResourceGuard guard = new TopicResourceGuard(clusterId, incarnation, timestamp);
            TopicResourceGuardAttestation attestation = new TopicResourceGuardAttestation(
                    guard, physicalTopic, Math.max(0, partition));
            return new ValidatedTopicResourceGuard(true, "", guard, attestation, incarnation);
        } catch (RuntimeException e) {
            return invalid("invalid resource guard properties: " + e.getMessage());
        }
    }

    public boolean isValid() {
        return valid;
    }

    public String reason() {
        return reason;
    }

    public TopicResourceGuard guard() {
        return guard;
    }

    public TopicResourceGuardAttestation attestation() {
        return attestation;
    }

    public boolean sameIdentity(ValidatedTopicResourceGuard other) {
        if (!valid || other == null || !other.valid
                || !guard.authenticatedClusterId().equals(other.guard.authenticatedClusterId())
                || guard.topicCreationTimestamp() != other.guard.topicCreationTimestamp()) {
            return false;
        }
        for (int i = 0; i < incarnation.length; i++) {
            if (incarnation[i] != other.incarnation[i]) {
                return false;
            }
        }
        return attestation.physicalTopic().equals(other.attestation.physicalTopic())
                && attestation.partition() == other.attestation.partition();
    }

    public boolean matchesProtoGuard(org.apache.pulsar.common.api.proto.TopicResourceGuard actual) {
        if (!valid || actual == null || !actual.hasGuardVersion() || actual.getGuardVersion() != guard.guardVersion()
                || !actual.hasAuthenticatedClusterId()
                || !guard.authenticatedClusterId().equals(actual.getAuthenticatedClusterId())
                || !actual.hasTopicCreationTimestamp()
                || guard.topicCreationTimestamp() != actual.getTopicCreationTimestamp()
                || !actual.hasResourceIncarnation()
                || actual.getResourceIncarnationSize() != incarnation.length) {
            return false;
        }
        ByteBuf actualIncarnation = actual.getResourceIncarnationSlice();
        for (int i = 0; i < incarnation.length; i++) {
            if (actualIncarnation.getByte(actualIncarnation.readerIndex() + i) != incarnation[i]) {
                return false;
            }
        }
        return true;
    }

    public byte[] resourceIncarnationCopy() {
        return incarnation.clone();
    }

    @Override
    public String toString() {
        return valid ? attestation.toString() : "INVALID(" + reason + ")";
    }
}
