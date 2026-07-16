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

import java.time.Duration;
import java.util.Objects;

/** One bounded cold-topic registration traversal under an expected broker readiness epoch. */
public record GenerationRegistrationBackfillRequest(
        String runId,
        long expectedBrokerReadinessEpoch,
        int maxConcurrency,
        Duration timeout) {
    public GenerationRegistrationBackfillRequest {
        runId = requireBase32Id(runId);
        if (expectedBrokerReadinessEpoch < 0) {
            throw new IllegalArgumentException("expectedBrokerReadinessEpoch must be non-negative");
        }
        if (maxConcurrency < 1) {
            throw new IllegalArgumentException("maxConcurrency must be positive");
        }
        timeout = Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }

    private static String requireBase32Id(String value) {
        Objects.requireNonNull(value, "runId");
        if (value.length() < 26 || value.length() > 128) {
            throw new IllegalArgumentException("runId must encode at least 128 bits and be at most 128 characters");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= 'a' && character <= 'z') || (character >= '2' && character <= '7'))) {
                throw new IllegalArgumentException("runId must be lowercase base32 without padding");
            }
        }
        return value;
    }
}
