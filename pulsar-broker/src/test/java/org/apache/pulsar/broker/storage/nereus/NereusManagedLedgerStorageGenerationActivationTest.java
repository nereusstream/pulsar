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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class NereusManagedLedgerStorageGenerationActivationTest {
    @Test
    public void disabledOrFailedBackfillNeverInvokesClusterActivation() {
        AtomicInteger activations = new AtomicInteger();

        GenerationRegistrationBackfillReport disabled =
                NereusManagedLedgerStorage
                        .activateAfterSuccessfulBackfill(
                                report(0),
                                false,
                                () -> {
                                    activations.incrementAndGet();
                                    return CompletableFuture
                                            .completedFuture(null);
                                })
                        .join();
        GenerationRegistrationBackfillReport failed =
                NereusManagedLedgerStorage
                        .activateAfterSuccessfulBackfill(
                                report(1),
                                true,
                                () -> {
                                    activations.incrementAndGet();
                                    return CompletableFuture
                                            .completedFuture(null);
                                })
                        .join();

        assertThat(disabled.failureCount()).isZero();
        assertThat(failed.failureCount()).isEqualTo(1);
        assertThat(activations).hasValue(0);
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
                                        () -> activation);

        assertThat(result).isNotDone();
        activation.complete(null);
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
                                        () -> CompletableFuture
                                                .failedFuture(
                                                        new IllegalStateException(
                                                                "activation failed")));

        assertThatThrownBy(result::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("activation failed");
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
