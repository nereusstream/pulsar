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

import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivation;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationValue;
import com.nereusstream.bookkeeper.BookKeeperDeletionActivationResult;
import java.time.Duration;
import java.util.Objects;
import org.apache.pulsar.broker.web.RestException;

/** Stable, proof-safe REST models for BookKeeper primary-WAL rollout administration. */
public final class NereusBookKeeperPrimaryWalAdminModels {
    static final long MAX_TIMEOUT_SECONDS = 86_400;

    private NereusBookKeeperPrimaryWalAdminModels() {
    }

    public record NamespaceProvisionRequest(
            String operatorEvidenceSha256,
            long timeoutSeconds) {
    }

    public record NamespaceRevokeRequest(
            String revocationEvidenceSha256,
            long expectedMetadataVersion,
            long timeoutSeconds) {
    }

    public record ActivationPrepareRequest(
            long brokerReadinessEpoch,
            String brokerReadinessSha256,
            long timeoutSeconds) {
    }

    /** Deliberately has no deletion flag or proof fields. */
    public record PublicationActivationRequest(
            long brokerReadinessEpoch,
            String brokerReadinessSha256,
            boolean asyncPublicationEnabled,
            boolean syncPublicationEnabled,
            long expectedMetadataVersion,
            long timeoutSeconds) {
    }

    /** Deliberately has no caller-supplied proof fields. */
    public record DeletionActivationRequest(
            String runId,
            long expectedActivationMetadataVersion,
            long timeoutSeconds) {
    }

    public record NamespaceReservationView(
            String reservationId,
            String nereusDeploymentId,
            String clusterAlias,
            String bookKeeperProviderScopeSha256,
            int ledgerIdPrefixBits,
            long ledgerIdPrefixValue,
            String lifecycle,
            long reservationEpoch,
            long createdAtMillis,
            long revokedAtMillis,
            String operatorEvidenceSha256,
            long metadataVersion,
            String storedValueSha256,
            String ledgerIdNamespaceSha256) {
        static NamespaceReservationView from(BookKeeperLedgerIdNamespaceReservation value) {
            BookKeeperLedgerIdNamespaceReservation exact = Objects.requireNonNull(value, "value");
            return new NamespaceReservationView(
                    exact.reservationId(),
                    exact.nereusDeploymentId(),
                    exact.clusterAlias(),
                    exact.bookKeeperProviderScopeSha256(),
                    exact.ledgerIdPrefixBits(),
                    exact.ledgerIdPrefixValue(),
                    exact.lifecycle().name(),
                    exact.reservationEpoch(),
                    exact.createdAtMillis(),
                    exact.revokedAtMillis(),
                    exact.operatorEvidenceSha256(),
                    exact.metadataVersion(),
                    exact.storedValueSha256().value(),
                    exact.ledgerIdNamespaceSha256().value());
        }
    }

    public record ActivationView(
            String lifecycle,
            int protocolVersion,
            String clusterAlias,
            String providerScopeSha256,
            long brokerReadinessEpoch,
            String brokerReadinessSha256,
            String configurationBindingSha256,
            String ledgerIdNamespaceSha256,
            boolean walOnlyPublicationEnabled,
            boolean asyncPublicationEnabled,
            boolean syncPublicationEnabled,
            boolean ledgerDeletionEnabled,
            String rootCoverageProofSha256,
            String streamCoverageProofSha256,
            String bookKeeperScopeProofSha256,
            long activatedAtMillis,
            long metadataVersion,
            String storedValueSha256,
            String activationRecordSha256,
            String publicationActivationSha256) {
        static ActivationView from(BookKeeperProtocolActivation activation) {
            BookKeeperProtocolActivation exact = Objects.requireNonNull(activation, "activation");
            BookKeeperProtocolActivationValue value = exact.value();
            return new ActivationView(
                    value.lifecycle().name(),
                    value.protocolVersion(),
                    value.clusterAlias(),
                    value.providerScopeSha256(),
                    value.brokerReadinessEpoch(),
                    value.brokerReadinessSha256(),
                    value.configurationBindingSha256(),
                    value.ledgerIdNamespaceSha256(),
                    value.walOnlyPublicationEnabled(),
                    value.asyncPublicationEnabled(),
                    value.syncPublicationEnabled(),
                    value.ledgerDeletionEnabled(),
                    value.rootCoverageProofSha256(),
                    value.streamCoverageProofSha256(),
                    value.bookKeeperScopeProofSha256(),
                    value.activatedAtMillis(),
                    exact.metadataVersion(),
                    exact.storedValueSha256().value(),
                    exact.activationRecordSha256().value(),
                    exact.supportsAllPublications()
                            ? exact.publicationActivationSha256().value()
                            : null);
        }
    }

    public record DeletionActivationView(
            ActivationView activation,
            boolean newlyActivated) {
        static DeletionActivationView from(BookKeeperDeletionActivationResult result) {
            BookKeeperDeletionActivationResult exact = Objects.requireNonNull(result, "result");
            return new DeletionActivationView(
                    ActivationView.from(exact.activation()), exact.newlyActivated());
        }
    }

    static Duration timeout(long timeoutSeconds) {
        if (timeoutSeconds <= 0 || timeoutSeconds > MAX_TIMEOUT_SECONDS) {
            throw invalid("timeoutSeconds must be between 1 and " + MAX_TIMEOUT_SECONDS);
        }
        return Duration.ofSeconds(timeoutSeconds);
    }

    static String required(String value, String field) {
        if (value == null || value.isBlank()) {
            throw invalid(field + " cannot be blank");
        }
        return value;
    }

    static long positive(long value, String field) {
        if (value <= 0) {
            throw invalid(field + " must be positive");
        }
        return value;
    }

    static long nonNegative(long value, String field) {
        if (value < 0) {
            throw invalid(field + " must be non-negative");
        }
        return value;
    }

    static RestException invalid(String message) {
        return new RestException(jakarta.ws.rs.core.Response.Status.PRECONDITION_FAILED, message);
    }
}
