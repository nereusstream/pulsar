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

import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Control-path A/read/B installer for one allocation-free local ACTIVE word. */
public final class NereusPulsarAuthorityInstaller {
    public record Installation(
            NereusPulsarActiveFence.ValidWord word,
            NereusPulsarActiveFence fence,
            AutoCloseable invalidationRegistration)
            implements AutoCloseable {
        public Installation {
            Objects.requireNonNull(word, "word");
            Objects.requireNonNull(fence, "fence");
            Objects.requireNonNull(invalidationRegistration, "invalidationRegistration");
        }

        @Override
        public void close() throws Exception {
            fence.invalidateIfCurrent(word);
            invalidationRegistration.close();
        }
    }

    private final NereusAuthoritativeOwnershipWitnessProvider ownershipProvider;
    private final NereusPulsarBindingAuthorityProvider bindingProvider;
    private final NereusPulsarActiveFence fence;

    public NereusPulsarAuthorityInstaller(
            NereusAuthoritativeOwnershipWitnessProvider ownershipProvider,
            NereusPulsarBindingAuthorityProvider bindingProvider,
            NereusPulsarActiveFence fence) {
        this.ownershipProvider = Objects.requireNonNull(ownershipProvider, "ownershipProvider");
        this.bindingProvider = Objects.requireNonNull(bindingProvider, "bindingProvider");
        this.fence = Objects.requireNonNull(fence, "fence");
    }

    public CompletionStage<Installation> install(
            String serviceUnit, PulsarTopicIncarnationIdentity expectedIncarnation) {
        Objects.requireNonNull(serviceUnit, "serviceUnit");
        Objects.requireNonNull(expectedIncarnation, "expectedIncarnation");
        NereusPulsarActiveFence.Word initial = fence.current();
        if (!(initial instanceof NereusPulsarActiveFence.InvalidWord expectedInvalid)) {
            return CompletableFuture.failedFuture(new IllegalStateException("P1 ACTIVE fence is already installed"));
        }

        AutoCloseable registration;
        try {
            registration = bindingProvider.armInvalidation(
                    expectedIncarnation,
                    invalidationEpoch ->
                            fence.invalidateIfSequenceCurrent(expectedInvalid.sequence(), invalidationEpoch));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
        NereusContinuityPermit permit = bindingProvider.captureContinuityPermitOrNull();
        if (permit == null) {
            closeQuietly(registration);
            return CompletableFuture.failedFuture(new IllegalStateException("P1 continuity is not READY"));
        }

        CompletableFuture<NereusPulsarActiveFence.ValidWord> result = ownershipProvider.read(serviceUnit)
                .thenCompose(first -> requirePresent(first, "ownership witness A is absent"))
                .thenCompose(first -> bindingProvider.readActive(expectedIncarnation)
                        .thenCompose(binding -> requirePresent(binding, "ACTIVE binding authority is absent")
                                .thenCompose(active -> ownershipProvider.read(serviceUnit)
                                        .thenCompose(second -> requirePresent(
                                                        second, "ownership witness B is absent")
                                                .thenApply(last -> verifyAndInstall(
                                                        expectedInvalid,
                                                        expectedIncarnation,
                                                        permit,
                                                        first,
                                                        active,
                                                        last))))))
                .toCompletableFuture();
        return result.whenComplete((installed, failure) -> {
            if (failure != null) {
                closeQuietly(registration);
            }
        }).thenApply(word -> new Installation(word, fence, registration));
    }

    private NereusPulsarActiveFence.ValidWord verifyAndInstall(
            NereusPulsarActiveFence.InvalidWord expectedInvalid,
            PulsarTopicIncarnationIdentity expectedIncarnation,
            NereusContinuityPermit permit,
            NereusOwnershipWitness first,
            NereusPulsarBindingAuthority binding,
            NereusOwnershipWitness last) {
        if (!first.equals(last)) {
            throw new IllegalStateException("authoritative ownership changed across A/read/B");
        }
        if (!binding.incarnation().equals(expectedIncarnation)) {
            throw new IllegalStateException("ACTIVE binding authority does not match the expected incarnation");
        }
        if (!bindingProvider.isCurrent(permit)) {
            throw new IllegalStateException("continuity changed across A/read/B");
        }
        NereusPulsarActiveFence.ValidWord installed = fence.tryInstall(expectedInvalid, first, binding, permit);
        if (installed == null) {
            throw new IllegalStateException("stale P1 installer lost the local fence CAS");
        }
        return installed;
    }

    private static <T> CompletionStage<T> requirePresent(Optional<T> value, String message) {
        return value.<CompletionStage<T>>map(CompletableFuture::completedFuture)
                .orElseGet(() -> CompletableFuture.failedFuture(new IllegalStateException(message)));
    }

    private static void closeQuietly(AutoCloseable registration) {
        try {
            registration.close();
        } catch (Exception ignored) {
            // The authority word is already fail closed; registration cleanup is best effort here.
        }
    }
}
