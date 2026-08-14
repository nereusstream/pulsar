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
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Immutable identity of one physical persistent topic incarnation.
 *
 * <p>The creation timestamp is the raw bit pattern of an unsigned uint64.  Callers must not use
 * signed ordering on that value.</p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public final class TopicResourceGuard implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int VERSION = 1;
    public static final int RESOURCE_INCARNATION_BYTES = 32;

    private final String authenticatedClusterId;
    private final byte[] resourceIncarnation;
    private final long topicCreationTimestamp;

    public TopicResourceGuard(String authenticatedClusterId, byte[] resourceIncarnation,
                              long topicCreationTimestamp) {
        if (authenticatedClusterId == null || authenticatedClusterId.isBlank()) {
            throw new IllegalArgumentException("authenticatedClusterId must not be blank");
        }
        Objects.requireNonNull(resourceIncarnation, "resourceIncarnation");
        if (resourceIncarnation.length != RESOURCE_INCARNATION_BYTES) {
            throw new IllegalArgumentException("resourceIncarnation must contain exactly 32 bytes");
        }
        this.authenticatedClusterId = authenticatedClusterId;
        this.resourceIncarnation = resourceIncarnation.clone();
        this.topicCreationTimestamp = topicCreationTimestamp;
    }

    public int guardVersion() {
        return VERSION;
    }

    public String authenticatedClusterId() {
        return authenticatedClusterId;
    }

    public byte[] resourceIncarnation() {
        return resourceIncarnation.clone();
    }

    /** Returns the raw uint64 bit pattern of the topic creation timestamp. */
    public long topicCreationTimestamp() {
        return topicCreationTimestamp;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TopicResourceGuard)) {
            return false;
        }
        TopicResourceGuard that = (TopicResourceGuard) other;
        return topicCreationTimestamp == that.topicCreationTimestamp
                && authenticatedClusterId.equals(that.authenticatedClusterId)
                && Arrays.equals(resourceIncarnation, that.resourceIncarnation);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(authenticatedClusterId, topicCreationTimestamp);
        return 31 * result + Arrays.hashCode(resourceIncarnation);
    }

    @Override
    public String toString() {
        return "TopicResourceGuard{authenticatedClusterId='" + authenticatedClusterId
                + "', resourceIncarnationBytes=" + resourceIncarnation.length
                + ", topicCreationTimestamp=" + Long.toUnsignedString(topicCreationTimestamp) + '}';
    }
}
