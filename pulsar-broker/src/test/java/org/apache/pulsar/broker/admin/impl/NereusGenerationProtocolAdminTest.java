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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
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
import com.nereusstream.bookkeeper.BookKeeperBrokerReadiness;
import jakarta.ws.rs.container.AsyncResponse;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.v2.Brokers;
import org.apache.pulsar.broker.storage.nereus.GenerationRegistrationBackfillReport;
import org.apache.pulsar.broker.storage.nereus.NereusBrokerCapabilityCoordinator;
import org.apache.pulsar.broker.storage.nereus.NereusGenerationCapabilityReadiness;
import org.apache.pulsar.broker.storage.nereus.NereusManagedLedgerStorage;
import org.apache.pulsar.broker.web.RestException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class NereusGenerationProtocolAdminTest {
    private static final String RUN_ID = "abcdefghijklmnopqrstuvwxyz";
    private static final String SHA_1 = "1".repeat(64);
    private static final String SHA_2 = "2".repeat(64);

    private PulsarService pulsar;
    private NereusManagedLedgerStorage storage;
    private NereusBrokerCapabilityCoordinator coordinator;
    private Brokers brokers;

    @BeforeMethod
    public void setup() {
        pulsar = mock(PulsarService.class);
        storage = mock(NereusManagedLedgerStorage.class);
        coordinator = mock(NereusBrokerCapabilityCoordinator.class);
        brokers = spy(Brokers.class);
        brokers.setPulsar(pulsar);
        doReturn(CompletableFuture.completedFuture(null))
                .when(brokers).validateSuperUserAccessAsync();
        when(pulsar.getManagedLedgerStorage()).thenReturn(storage);
        when(storage.capabilityCoordinator()).thenReturn(coordinator);
    }

    @Test
    public void mapsBookKeeperAndGenerationReadiness() {
        when(coordinator.requireBookKeeperPrimaryWalReadiness())
                .thenReturn(CompletableFuture.completedFuture(
                        new BookKeeperBrokerReadiness(
                                11,
                                new Checksum(ChecksumType.SHA256, SHA_1),
                                3)));
        when(coordinator.requireGenerationReadiness())
                .thenReturn(CompletableFuture.completedFuture(
                        new NereusGenerationCapabilityReadiness(12, SHA_2, 3)));
        AsyncResponse bookKeeperResponse = mock(AsyncResponse.class);
        AsyncResponse generationResponse = mock(AsyncResponse.class);

        brokers.getBookKeeperPrimaryWalReadiness(bookKeeperResponse);
        brokers.getNereusGenerationProtocolReadiness(generationResponse);

        ArgumentCaptor<Object> bookKeeperView = ArgumentCaptor.forClass(Object.class);
        verify(bookKeeperResponse, timeout(5_000)).resume(bookKeeperView.capture());
        assertThat(bookKeeperView.getValue())
                .isEqualTo(new NereusGenerationProtocolAdminModels.ReadinessView(
                        11, SHA_1, 3));
        ArgumentCaptor<Object> generationView = ArgumentCaptor.forClass(Object.class);
        verify(generationResponse, timeout(5_000)).resume(generationView.capture());
        assertThat(generationView.getValue())
                .isEqualTo(new NereusGenerationProtocolAdminModels.ReadinessView(
                        12, SHA_2, 3));
    }

    @Test
    public void mapsSuccessfulBackfillAndActivationCompletion() {
        GenerationRegistrationBackfillReport report =
                new GenerationRegistrationBackfillReport(
                        RUN_ID,
                        12,
                        2,
                        3,
                        4,
                        3,
                        1,
                        0,
                        new Checksum(ChecksumType.SHA256, SHA_1),
                        List.of());
        when(storage.runGenerationRegistrationBackfill(RUN_ID))
                .thenReturn(CompletableFuture.completedFuture(report));
        AsyncResponse response = mock(AsyncResponse.class);

        brokers.runNereusGenerationRegistrationBackfill(
                new NereusGenerationProtocolAdminModels.RegistrationBackfillRequest(RUN_ID),
                response);

        verify(storage).runGenerationRegistrationBackfill(RUN_ID);
        ArgumentCaptor<Object> view = ArgumentCaptor.forClass(Object.class);
        verify(response, timeout(5_000)).resume(view.capture());
        assertThat(view.getValue())
                .isEqualTo(new NereusGenerationProtocolAdminModels.RegistrationBackfillView(
                        RUN_ID,
                        12,
                        2,
                        3,
                        4,
                        3,
                        1,
                        0,
                        SHA_1,
                        List.of()));
    }

    @Test
    public void rejectsMalformedRunIdBeforeCallingStorage() {
        assertThatThrownBy(() ->
                new NereusGenerationProtocolAdminModels.RegistrationBackfillRequest(
                        "not_base32"))
                .isInstanceOf(RestException.class);

        verifyNoInteractions(storage);
    }

    @Test
    public void rejectsBeforeLookingUpStorageWhenSuperUserValidationFails() {
        doReturn(CompletableFuture.failedFuture(
                        new RestException(
                                jakarta.ws.rs.core.Response.Status.UNAUTHORIZED,
                                "denied")))
                .when(brokers).validateSuperUserAccessAsync();
        AsyncResponse response = mock(AsyncResponse.class);

        brokers.getNereusGenerationProtocolReadiness(response);

        verify(response, timeout(5_000)).resume(any(Throwable.class));
        verify(pulsar, never()).getManagedLedgerStorage();
        verifyNoInteractions(storage);
    }
}
