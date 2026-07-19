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

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/** Complete bounded report for one canonical cold-topic registration traversal. */
public record GenerationRegistrationBackfillReport(
        String runId,
        long brokerReadinessEpoch,
        long tenantsScanned,
        long namespacesScanned,
        long persistentTopicsScanned,
        long nereusProjectionsRegistered,
        long deletedOrNonNereusSkipped,
        long failureCount,
        Checksum coverageSha256,
        List<BackfillFailure> boundedFailures) {
    public static final int MAX_FAILURES = 100;

    public GenerationRegistrationBackfillReport {
        new GenerationRegistrationBackfillRequest(
                runId,
                brokerReadinessEpoch,
                1,
                Duration.ofNanos(1));
        requireNonNegative(tenantsScanned, "tenantsScanned");
        requireNonNegative(namespacesScanned, "namespacesScanned");
        requireNonNegative(persistentTopicsScanned, "persistentTopicsScanned");
        requireNonNegative(nereusProjectionsRegistered, "nereusProjectionsRegistered");
        requireNonNegative(deletedOrNonNereusSkipped, "deletedOrNonNereusSkipped");
        requireNonNegative(failureCount, "failureCount");
        long successfulTopics;
        try {
            successfulTopics = Math.addExact(
                    nereusProjectionsRegistered,
                    deletedOrNonNereusSkipped);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "topic outcome counters overflow", overflow);
        }
        if (successfulTopics > persistentTopicsScanned
                || persistentTopicsScanned - successfulTopics > failureCount) {
            throw new IllegalArgumentException(
                    "persistentTopicsScanned must equal registered + skipped + topic failures, "
                            + "and topic failures cannot exceed total failures");
        }
        coverageSha256 = Objects.requireNonNull(coverageSha256, "coverageSha256");
        if (coverageSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("coverageSha256 must be SHA-256");
        }
        boundedFailures = List.copyOf(Objects.requireNonNull(boundedFailures, "boundedFailures"));
        if (boundedFailures.stream().anyMatch(Objects::isNull)
                || boundedFailures.size() != Math.min(MAX_FAILURES, failureCount)) {
            throw new IllegalArgumentException(
                    "boundedFailures must contain exactly the first min(100, failureCount) failures");
        }
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }
}
