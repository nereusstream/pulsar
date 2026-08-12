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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** Exact authoritative local-ownership read, including backend version and stored bytes. */
public record NereusOwnershipWitness(
        String serviceUnit,
        String localBrokerId,
        NereusOwnershipId brokerIncarnationId,
        NereusOwnershipId acquisitionId,
        CanonicalBytes canonicalStoredBytes,
        Sha256Digest canonicalStoredDigest,
        long backendVersion) {
    public NereusOwnershipWitness {
        if (serviceUnit == null || serviceUnit.isBlank()) {
            throw new IllegalArgumentException("service unit must be present");
        }
        if (localBrokerId == null || localBrokerId.isBlank()) {
            throw new IllegalArgumentException("local broker ID must be present");
        }
        Objects.requireNonNull(brokerIncarnationId, "brokerIncarnationId");
        Objects.requireNonNull(acquisitionId, "acquisitionId");
        Objects.requireNonNull(canonicalStoredBytes, "canonicalStoredBytes");
        Objects.requireNonNull(canonicalStoredDigest, "canonicalStoredDigest");
        if (!Sha256Digest.hash(canonicalStoredBytes).equals(canonicalStoredDigest)) {
            throw new IllegalArgumentException("ownership witness digest does not match exact stored bytes");
        }
        if (backendVersion < 0) {
            throw new IllegalArgumentException("ownership backend version must be non-negative");
        }
    }
}
