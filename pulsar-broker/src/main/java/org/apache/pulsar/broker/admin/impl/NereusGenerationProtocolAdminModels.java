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
package org.apache.pulsar.broker.admin.impl;

import com.nereusstream.bookkeeper.BookKeeperBrokerReadiness;
import java.util.List;
import java.util.Objects;
import org.apache.pulsar.broker.storage.nereus.BackfillFailure;
import org.apache.pulsar.broker.storage.nereus.GenerationRegistrationBackfillReport;
import org.apache.pulsar.broker.storage.nereus.NereusGenerationCapabilityReadiness;

/** Stable REST models for Nereus readiness and generation registration rollout administration. */
public final class NereusGenerationProtocolAdminModels {

    private NereusGenerationProtocolAdminModels() {
    }

    public record RegistrationBackfillRequest(String runId) {
        public RegistrationBackfillRequest {
            runId = requireRunId(runId);
        }
    }

    public record ReadinessView(
            long brokerReadinessEpoch,
            String brokerReadinessSha256,
            int persistentBrokerCount) {

        static ReadinessView from(BookKeeperBrokerReadiness readiness) {
            BookKeeperBrokerReadiness exact = Objects.requireNonNull(readiness, "readiness");
            return new ReadinessView(
                    exact.brokerReadinessEpoch(),
                    exact.brokerSetSha256().value(),
                    exact.persistentBrokerCount());
        }

        static ReadinessView from(NereusGenerationCapabilityReadiness readiness) {
            NereusGenerationCapabilityReadiness exact =
                    Objects.requireNonNull(readiness, "readiness");
            return new ReadinessView(
                    exact.brokerReadinessEpoch(),
                    exact.brokerSetSha256(),
                    exact.persistentBrokerCount());
        }
    }

    public record BackfillFailureView(
            String resourceIdentitySha256,
            String stage,
            String errorCode) {

        static BackfillFailureView from(BackfillFailure failure) {
            BackfillFailure exact = Objects.requireNonNull(failure, "failure");
            return new BackfillFailureView(
                    exact.resourceIdentitySha256(),
                    exact.stage().name(),
                    exact.errorCode());
        }
    }

    public record RegistrationBackfillView(
            String runId,
            long brokerReadinessEpoch,
            long tenantsScanned,
            long namespacesScanned,
            long persistentTopicsScanned,
            long nereusProjectionsRegistered,
            long deletedOrNonNereusSkipped,
            long failureCount,
            String coverageSha256,
            List<BackfillFailureView> boundedFailures) {

        static RegistrationBackfillView from(GenerationRegistrationBackfillReport report) {
            GenerationRegistrationBackfillReport exact =
                    Objects.requireNonNull(report, "report");
            return new RegistrationBackfillView(
                    exact.runId(),
                    exact.brokerReadinessEpoch(),
                    exact.tenantsScanned(),
                    exact.namespacesScanned(),
                    exact.persistentTopicsScanned(),
                    exact.nereusProjectionsRegistered(),
                    exact.deletedOrNonNereusSkipped(),
                    exact.failureCount(),
                    exact.coverageSha256().value(),
                    exact.boundedFailures().stream()
                            .map(BackfillFailureView::from)
                            .toList());
        }
    }

    static String requireRunId(String value) {
        if (value == null || value.length() < 26 || value.length() > 128) {
            throw NereusBookKeeperPrimaryWalAdminModels.invalid(
                    "runId must be lowercase base32, encode at least 128 bits, "
                            + "and be at most 128 characters");
        }
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (!((current >= 'a' && current <= 'z')
                    || (current >= '2' && current <= '7'))) {
                throw NereusBookKeeperPrimaryWalAdminModels.invalid(
                        "runId must be lowercase base32 without padding");
            }
        }
        return value;
    }
}
