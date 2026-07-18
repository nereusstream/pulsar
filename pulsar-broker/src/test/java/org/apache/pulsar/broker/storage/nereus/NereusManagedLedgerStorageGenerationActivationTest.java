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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.managedledger.generation.ManagedLedgerPhysicalDeletionActivationResult;
import com.nereusstream.managedledger.generation.ManagedLedgerPhysicalDeletionActivationResult.Status;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class NereusManagedLedgerStorageGenerationActivationTest {
    @Test
    public void disabledOrFailedBackfillNeverInvokesClusterActivation() {
        AtomicInteger publications = new AtomicInteger();
        AtomicInteger deletions = new AtomicInteger();

        GenerationRegistrationBackfillReport disabled =
                NereusManagedLedgerStorage
                        .activateAfterSuccessfulBackfill(
                                report(0),
                                false,
                                true,
                                () -> {
                                    publications.incrementAndGet();
                                    return CompletableFuture
                                            .completedFuture(null);
                                },
                                () -> {
                                    deletions.incrementAndGet();
                                    return CompletableFuture.completedFuture(
                                            physicalActivation());
                                })
                        .join();
        GenerationRegistrationBackfillReport failed =
                NereusManagedLedgerStorage
                        .activateAfterSuccessfulBackfill(
                                report(1),
                                true,
                                true,
                                () -> {
                                    publications.incrementAndGet();
                                    return CompletableFuture
                                            .completedFuture(null);
                                },
                                () -> {
                                    deletions.incrementAndGet();
                                    return CompletableFuture.completedFuture(
                                            physicalActivation());
                                })
                        .join();

        assertThat(disabled.failureCount()).isZero();
        assertThat(failed.failureCount()).isEqualTo(1);
        assertThat(publications).hasValue(0);
        assertThat(deletions).hasValue(0);
    }

    @Test
    public void successfulEnabledBackfillWaitsForClusterActivation() {
        CompletableFuture<Void> activation =
                new CompletableFuture<>();
        GenerationRegistrationBackfillReport report = report(0);

        CompletableFuture<GenerationRegistrationBackfillReport>
                result =
                        NereusManagedLedgerStorage
                                .activateAfterSuccessfulBackfill(
                                        report,
                                        true,
                                        false,
                                        () -> activation,
                                        () -> CompletableFuture.completedFuture(
                                                physicalActivation()));

        assertThat(result).isNotDone();
        activation.complete(null);
        assertThat(result.join()).isSameAs(report);
    }

    @Test
    public void physicalDeletionActivationRunsOnlyAfterPublicationAndIsAwaited() {
        CompletableFuture<Void> publication = new CompletableFuture<>();
        CompletableFuture<ManagedLedgerPhysicalDeletionActivationResult>
                deletion = new CompletableFuture<>();
        AtomicInteger deletionCalls = new AtomicInteger();
        GenerationRegistrationBackfillReport report = report(0);

        CompletableFuture<GenerationRegistrationBackfillReport> result =
                NereusManagedLedgerStorage.activateAfterSuccessfulBackfill(
                        report,
                        true,
                        true,
                        () -> publication,
                        () -> {
                            deletionCalls.incrementAndGet();
                            return deletion;
                        });

        assertThat(result).isNotDone();
        assertThat(deletionCalls).hasValue(0);
        publication.complete(null);
        assertThat(deletionCalls).hasValue(1);
        assertThat(result).isNotDone();
        deletion.complete(physicalActivation());
        assertThat(result.join()).isSameAs(report);
    }

    @Test
    public void activationFailureFailsTheBackfillCompletionPromise() {
        CompletableFuture<GenerationRegistrationBackfillReport>
                result =
                        NereusManagedLedgerStorage
                                .activateAfterSuccessfulBackfill(
                                        report(0),
                                        true,
                                        false,
                                        () -> CompletableFuture
                                                .failedFuture(
                                                        new IllegalStateException(
                                                                "activation failed")),
                                        () -> CompletableFuture.completedFuture(
                                                physicalActivation()));

        assertThatThrownBy(result::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("activation failed");
    }

    @Test
    public void physicalDeletionFailureFailsTheBackfillCompletionPromise() {
        CompletableFuture<GenerationRegistrationBackfillReport> result =
                NereusManagedLedgerStorage.activateAfterSuccessfulBackfill(
                        report(0),
                        true,
                        true,
                        () -> CompletableFuture.completedFuture(null),
                        () -> CompletableFuture.failedFuture(
                                new IllegalStateException(
                                        "physical activation failed")));

        assertThatThrownBy(result::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("physical activation failed");
    }

    private static ManagedLedgerPhysicalDeletionActivationResult
            physicalActivation() {
        return new ManagedLedgerPhysicalDeletionActivationResult(
                "abcdefghijklmnopqrstuvwxyz",
                7,
                "33".repeat(32),
                "44".repeat(32),
                "55".repeat(32),
                9,
                Status.ACTIVATED);
    }

    private static GenerationRegistrationBackfillReport report(
            long failures) {
        return new GenerationRegistrationBackfillReport(
                "abcdefghijklmnopqrstuvwxyz",
                7,
                1,
                1,
                1,
                failures == 0 ? 1 : 0,
                0,
                failures,
                new Checksum(
                        ChecksumType.SHA256,
                        "11".repeat(32)),
                failures == 0
                        ? List.of()
                        : List.of(new BackfillFailure(
                                "22".repeat(32),
                                GenerationRegistrationBackfillStage
                                        .REGISTRATION_WRITE,
                                "WRITE_FAILED")));
    }
}
