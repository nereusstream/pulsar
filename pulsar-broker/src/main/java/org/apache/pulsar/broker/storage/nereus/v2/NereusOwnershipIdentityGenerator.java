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

import java.security.SecureRandom;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Process-scoped CSPRNG identity source with explicit non-zero and duplicate rejection. */
public final class NereusOwnershipIdentityGenerator {
    private static final int MAX_COLLISION_RETRIES = 1024;

    private final SecureRandom random;
    private final Set<NereusOwnershipId> issued = ConcurrentHashMap.newKeySet();
    private final NereusOwnershipId brokerIncarnationId;

    public NereusOwnershipIdentityGenerator() {
        this(new SecureRandom());
    }

    public NereusOwnershipIdentityGenerator(SecureRandom random) {
        this.random = Objects.requireNonNull(random, "random");
        brokerIncarnationId = issue();
    }

    public NereusOwnershipId brokerIncarnationId() {
        return brokerIncarnationId;
    }

    public NereusOwnershipId newAcquisitionId() {
        return issue();
    }

    private NereusOwnershipId issue() {
        byte[] bytes = new byte[16];
        for (int attempt = 0; attempt < MAX_COLLISION_RETRIES; attempt++) {
            random.nextBytes(bytes);
            try {
                NereusOwnershipId candidate = NereusOwnershipId.fromBytes(bytes);
                if (issued.add(candidate)) {
                    return candidate;
                }
            } catch (IllegalArgumentException ignored) {
                // Retry all-zero output exactly like a process-local collision.
            }
        }
        throw new IllegalStateException("unable to generate a unique non-zero ownership identity");
    }
}
