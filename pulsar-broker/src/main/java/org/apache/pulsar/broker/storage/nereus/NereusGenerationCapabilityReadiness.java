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

import java.util.Objects;

/**
 * Deterministic identity of one two-snapshot-capable persistent broker set.
 *
 * <p>The epoch is derived from the full SHA-256 identity and is used only as the bounded V1 activation/backfill
 * field. The complete digest remains available for exact in-process comparison.
 */
public record NereusGenerationCapabilityReadiness(
        long brokerReadinessEpoch,
        String brokerSetSha256,
        int persistentBrokerCount) {
    public NereusGenerationCapabilityReadiness {
        if (brokerReadinessEpoch < 0) {
            throw new IllegalArgumentException("brokerReadinessEpoch must be non-negative");
        }
        brokerSetSha256 = Objects.requireNonNull(brokerSetSha256, "brokerSetSha256");
        if (brokerSetSha256.length() != 64 || !isLowerHex(brokerSetSha256)) {
            throw new IllegalArgumentException("brokerSetSha256 must be lowercase SHA-256");
        }
        if (persistentBrokerCount < 1) {
            throw new IllegalArgumentException("persistentBrokerCount must be positive");
        }
    }

    private static boolean isLowerHex(String value) {
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (!((current >= '0' && current <= '9') || (current >= 'a' && current <= 'f'))) {
                return false;
            }
        }
        return true;
    }
}
