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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivation;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationKeys;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationLifecycle;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationUpdate;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationValue;
import com.nereusstream.bookkeeper.BookKeeperDeletionActivationRequest;
import com.nereusstream.bookkeeper.BookKeeperDeletionActivationResult;
import jakarta.ws.rs.container.AsyncResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.v2.Brokers;
import org.apache.pulsar.broker.storage.nereus.NereusManagedLedgerStorage;
import org.apache.pulsar.broker.web.RestException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class NereusBookKeeperPrimaryWalAdminTest {
    private static final String SHA_1 = "1".repeat(64);
    private static final String SHA_2 = "2".repeat(64);
    private static final String SHA_3 = "3".repeat(64);
    private static final String SHA_4 = "4".repeat(64);
    private static final String SHA_5 = "5".repeat(64);
    private static final String ZERO_SHA = "0".repeat(64);

    private PulsarService pulsar;
    private NereusManagedLedgerStorage storage;
    private Brokers brokers;

    @BeforeMethod
    public void setup() {
        pulsar = mock(PulsarService.class);
        storage = mock(NereusManagedLedgerStorage.class);
        brokers = spy(Brokers.class);
        brokers.setPulsar(pulsar);
        doReturn(CompletableFuture.completedFuture(null))
                .when(brokers).validateSuperUserAccessAsync();
        when(pulsar.getManagedLedgerStorage()).thenReturn(storage);
    }

    @Test
    public void rejectsBeforeLookingUpStorageWhenSuperUserValidationFails() {
        doReturn(CompletableFuture.failedFuture(
                        new RestException(jakarta.ws.rs.core.Response.Status.UNAUTHORIZED, "denied")))
                .when(brokers).validateSuperUserAccessAsync();
        AsyncResponse response = mock(AsyncResponse.class);

        brokers.provisionBookKeeperPrimaryWalNamespace(
                new NereusBookKeeperPrimaryWalAdminModels.NamespaceProvisionRequest(SHA_1, 30),
                response);

        verify(response, timeout(5_000)).resume(any(Throwable.class));
        verify(pulsar, never()).getManagedLedgerStorage();
        verifyNoInteractions(storage);
    }

    @Test
    public void exposesNoCallerControlledDeletionProofFields() {
        assertThat(componentNames(
                        NereusBookKeeperPrimaryWalAdminModels.PublicationActivationRequest.class))
                .doesNotContain(
                        "ledgerDeletionEnabled",
                        "rootCoverageProofSha256",
                        "streamCoverageProofSha256",
                        "bookKeeperScopeProofSha256");
        assertThat(componentNames(
                        NereusBookKeeperPrimaryWalAdminModels.DeletionActivationRequest.class))
                .containsExactlyInAnyOrder(
                        "runId", "expectedActivationMetadataVersion", "timeoutSeconds");
    }

    @Test
    public void mapsNamespaceProvisionAndRevokeExactly() {
        BookKeeperLedgerIdNamespaceReservation active = namespace(
                BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE, 7, 0);
        BookKeeperLedgerIdNamespaceReservation revoked = namespace(
                BookKeeperLedgerIdNamespaceReservation.Lifecycle.REVOKED, 8, 2_000);
        when(storage.provisionBookKeeperLedgerIdNamespace(anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(active));
        when(storage.revokeBookKeeperLedgerIdNamespace(anyString(), anyLong(), any()))
                .thenReturn(CompletableFuture.completedFuture(revoked));
        AsyncResponse provisionResponse = mock(AsyncResponse.class);
        AsyncResponse revokeResponse = mock(AsyncResponse.class);

        brokers.provisionBookKeeperPrimaryWalNamespace(
                new NereusBookKeeperPrimaryWalAdminModels.NamespaceProvisionRequest(SHA_4, 31),
                provisionResponse);
        brokers.revokeBookKeeperPrimaryWalNamespace(
                new NereusBookKeeperPrimaryWalAdminModels.NamespaceRevokeRequest(SHA_5, 7, 32),
                revokeResponse);

        verify(storage).provisionBookKeeperLedgerIdNamespace(SHA_4, Duration.ofSeconds(31));
        verify(storage).revokeBookKeeperLedgerIdNamespace(SHA_5, 7, Duration.ofSeconds(32));
        ArgumentCaptor<Object> provisionView = ArgumentCaptor.forClass(Object.class);
        verify(provisionResponse, timeout(5_000)).resume(provisionView.capture());
        assertThat(provisionView.getValue())
                .isInstanceOf(NereusBookKeeperPrimaryWalAdminModels.NamespaceReservationView.class);
        assertThat(((NereusBookKeeperPrimaryWalAdminModels.NamespaceReservationView)
                        provisionView.getValue()).metadataVersion())
                .isEqualTo(7);
        ArgumentCaptor<Object> revokeView = ArgumentCaptor.forClass(Object.class);
        verify(revokeResponse, timeout(5_000)).resume(revokeView.capture());
        assertThat(((NereusBookKeeperPrimaryWalAdminModels.NamespaceReservationView)
                        revokeView.getValue()).lifecycle())
                .isEqualTo("REVOKED");
    }

    @Test
    public void mapsPreparePublicationDeletionAndReadExactly() {
        BookKeeperProtocolActivation publication = activation(false);
        BookKeeperProtocolActivation deletion = activation(true);
        when(storage.prepareBookKeeperPrimaryWalActivation(anyLong(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(publication));
        when(storage.activateBookKeeperPrimaryWalPublications(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(publication));
        when(storage.activateBookKeeperLedgerDeletion(any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new BookKeeperDeletionActivationResult(
                                deletion,
                                new Checksum(ChecksumType.SHA256, SHA_3),
                                new Checksum(ChecksumType.SHA256, SHA_4),
                                new Checksum(ChecksumType.SHA256, SHA_5),
                                true)));
        when(storage.readBookKeeperPrimaryWalActivation(any()))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(deletion)));

        AsyncResponse prepareResponse = mock(AsyncResponse.class);
        brokers.prepareBookKeeperPrimaryWalActivation(
                new NereusBookKeeperPrimaryWalAdminModels.ActivationPrepareRequest(11, SHA_2, 41),
                prepareResponse);
        AsyncResponse publicationResponse = mock(AsyncResponse.class);
        brokers.activateBookKeeperPrimaryWalPublications(
                new NereusBookKeeperPrimaryWalAdminModels.PublicationActivationRequest(
                        12, SHA_2, true, true, 9, 42),
                publicationResponse);
        AsyncResponse deletionResponse = mock(AsyncResponse.class);
        brokers.activateBookKeeperPrimaryWalDeletion(
                new NereusBookKeeperPrimaryWalAdminModels.DeletionActivationRequest(
                        "rollout_20260720", 10, 43),
                deletionResponse);
        AsyncResponse readResponse = mock(AsyncResponse.class);
        brokers.getBookKeeperPrimaryWalActivation(44, readResponse);

        verify(storage).prepareBookKeeperPrimaryWalActivation(11, SHA_2, Duration.ofSeconds(41));
        ArgumentCaptor<BookKeeperProtocolActivationUpdate> publicationUpdate =
                ArgumentCaptor.forClass(BookKeeperProtocolActivationUpdate.class);
        verify(storage).activateBookKeeperPrimaryWalPublications(
                publicationUpdate.capture(), org.mockito.ArgumentMatchers.eq(Duration.ofSeconds(42)));
        assertThat(publicationUpdate.getValue().ledgerDeletionEnabled()).isFalse();
        assertThat(publicationUpdate.getValue().rootCoverageProofSha256()).isEqualTo(ZERO_SHA);
        assertThat(publicationUpdate.getValue().streamCoverageProofSha256()).isEqualTo(ZERO_SHA);
        assertThat(publicationUpdate.getValue().bookKeeperScopeProofSha256()).isEqualTo(ZERO_SHA);
        ArgumentCaptor<BookKeeperDeletionActivationRequest> deletionRequest =
                ArgumentCaptor.forClass(BookKeeperDeletionActivationRequest.class);
        verify(storage).activateBookKeeperLedgerDeletion(deletionRequest.capture());
        assertThat(deletionRequest.getValue().runId()).isEqualTo("rollout_20260720");
        assertThat(deletionRequest.getValue().expectedActivationMetadataVersion()).isEqualTo(10);
        assertThat(deletionRequest.getValue().timeout()).isEqualTo(Duration.ofSeconds(43));
        verify(storage).readBookKeeperPrimaryWalActivation(Duration.ofSeconds(44));
        verify(prepareResponse, timeout(5_000)).resume(any(
                NereusBookKeeperPrimaryWalAdminModels.ActivationView.class));
        verify(publicationResponse, timeout(5_000)).resume(any(
                NereusBookKeeperPrimaryWalAdminModels.ActivationView.class));
        verify(deletionResponse, timeout(5_000)).resume(any(
                NereusBookKeeperPrimaryWalAdminModels.DeletionActivationView.class));
        verify(readResponse, timeout(5_000)).resume(any(
                NereusBookKeeperPrimaryWalAdminModels.ActivationView.class));
    }

    @Test
    public void validatesRequestOnlyAfterAuthorizationAndBeforeMutation() {
        AsyncResponse response = mock(AsyncResponse.class);

        brokers.activateBookKeeperPrimaryWalPublications(
                new NereusBookKeeperPrimaryWalAdminModels.PublicationActivationRequest(
                        1, SHA_1, false, true, 0, 30),
                response);

        verify(brokers).validateSuperUserAccessAsync();
        verify(storage, never()).activateBookKeeperPrimaryWalPublications(any(), any());
        verify(response, timeout(5_000)).resume(any(Throwable.class));
    }

    private static BookKeeperLedgerIdNamespaceReservation namespace(
            BookKeeperLedgerIdNamespaceReservation.Lifecycle lifecycle,
            long metadataVersion,
            long revokedAtMillis) {
        return new BookKeeperLedgerIdNamespaceReservation(
                1,
                "reservation-1",
                "deployment-1",
                "cluster-1",
                SHA_1,
                8,
                128,
                lifecycle,
                1,
                1_000,
                revokedAtMillis,
                SHA_2,
                metadataVersion,
                new Checksum(ChecksumType.SHA256, SHA_3),
                "/namespace/reservation-1");
    }

    private static BookKeeperProtocolActivation activation(boolean deletionEnabled) {
        String rootProof = deletionEnabled ? SHA_3 : ZERO_SHA;
        String streamProof = deletionEnabled ? SHA_4 : ZERO_SHA;
        String scopeProof = deletionEnabled ? SHA_5 : ZERO_SHA;
        BookKeeperProtocolActivationValue value = new BookKeeperProtocolActivationValue(
                1,
                BookKeeperProtocolActivationLifecycle.ACTIVE,
                1,
                "cluster-1",
                SHA_1,
                11,
                SHA_2,
                SHA_3,
                SHA_4,
                true,
                true,
                true,
                deletionEnabled,
                rootProof,
                streamProof,
                scopeProof,
                2_000);
        return value.materialize(
                BookKeeperProtocolActivationKeys.key("cluster-1", SHA_3, SHA_4),
                deletionEnabled ? 10 : 9,
                new Checksum(ChecksumType.SHA256, SHA_5));
    }

    private static java.util.List<String> componentNames(Class<? extends Record> type) {
        return Arrays.stream(type.getRecordComponents())
                .map(java.lang.reflect.RecordComponent::getName)
                .toList();
    }
}
