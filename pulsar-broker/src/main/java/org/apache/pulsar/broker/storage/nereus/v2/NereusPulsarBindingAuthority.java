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
package org.apache.pulsar.broker.storage.nereus.v2;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import java.util.Objects;

/** Exact ACTIVE selector/aggregate projection consumed by the native P1 cache installer. */
public record NereusPulsarBindingAuthority(
        PulsarTopicIncarnationIdentity incarnation,
        TopicBindingId bindingId,
        StorageEpochId storageEpochId,
        long selectorBackendVersion,
        long aggregateBackendVersion,
        Sha256Digest selectorDigest,
        Sha256Digest aggregateDigest) {
    public NereusPulsarBindingAuthority {
        Objects.requireNonNull(incarnation, "incarnation");
        Objects.requireNonNull(bindingId, "bindingId");
        Objects.requireNonNull(storageEpochId, "storageEpochId");
        Objects.requireNonNull(selectorDigest, "selectorDigest");
        Objects.requireNonNull(aggregateDigest, "aggregateDigest");
        if (selectorBackendVersion < 0 || aggregateBackendVersion < 0) {
            throw new IllegalArgumentException("authority backend versions must be non-negative");
        }
    }
}
