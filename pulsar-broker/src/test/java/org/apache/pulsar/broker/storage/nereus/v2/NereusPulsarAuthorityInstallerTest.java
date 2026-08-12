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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.PulsarTopicName;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NereusPulsarAuthorityInstallerTest {
    private static final String SERVICE_UNIT = "tenant/ns/0x00000000_0xffffffff";
    private static final PulsarTopicIncarnationIdentity INCARNATION = new PulsarTopicIncarnationIdentity(
            PulsarPersistenceName.fromString("tenant/ns/persistent/orders"),
            PulsarTopicName.fromString("persistent://tenant/ns/orders"),
            new PulsarBindingGeneration(1));

    private NereusPulsarActiveFence fence;
    private FakeBindingProvider bindings;
    private ArrayDeque<NereusOwnershipWitness> witnesses;
    private NereusPulsarAuthorityInstaller installer;

    @BeforeMethod
    public void setUp() {
        fence = new NereusPulsarActiveFence();
        bindings = new FakeBindingProvider();
        witnesses = new ArrayDeque<>();
        witnesses.add(witness("11111111111111111111111111111111", 7));
        witnesses.add(witness("11111111111111111111111111111111", 7));
        installer = new NereusPulsarAuthorityInstaller(
                ignored -> CompletableFuture.completedFuture(Optional.ofNullable(witnesses.poll())),
                bindings,
                fence);
    }

    @Test
    public void exactAReadBInstallsOneReferenceHotPathFence() {
        var installation = installer.install(SERVICE_UNIT, INCARNATION).toCompletableFuture().join();
        var captured = fence.captureValidOrNull();

        assertThat(captured).isSameAs(installation.word());
        assertThat(fence.isCurrent(captured)).isTrue();
        bindings.invalidate();
        assertThat(fence.captureValidOrNull()).isNull();
        assertThat(fence.isCurrent(captured)).isFalse();
    }

    @Test
    public void closingInstallationInvalidatesItsWordBeforeClosingRegistration() throws Exception {
        var installation = installer.install(SERVICE_UNIT, INCARNATION).toCompletableFuture().join();

        installation.close();

        assertThat(fence.captureValidOrNull()).isNull();
        assertThat(bindings.closed).isTrue();
    }

    @Test
    public void closingStaleInstallationCannotInvalidateSuccessor() throws Exception {
        var first = installer.install(SERVICE_UNIT, INCARNATION).toCompletableFuture().join();
        bindings.invalidate();
        witnesses.add(witness("33333333333333333333333333333333", 8));
        witnesses.add(witness("33333333333333333333333333333333", 8));
        var successor = installer.install(SERVICE_UNIT, INCARNATION).toCompletableFuture().join();

        first.close();

        assertThat(fence.captureValidOrNull()).isSameAs(successor.word());
    }

    @Test
    public void changedOwnershipAcrossAReadBFailsClosed() {
        witnesses.clear();
        witnesses.add(witness("11111111111111111111111111111111", 7));
        witnesses.add(witness("33333333333333333333333333333333", 8));

        assertThatThrownBy(() -> installer.install(SERVICE_UNIT, INCARNATION).toCompletableFuture().join())
                .hasRootCauseMessage("authoritative ownership changed across A/read/B");
        assertThat(fence.captureValidOrNull()).isNull();
    }

    @Test
    public void invalidationDuringReadChangesContinuityAndFailsClosed() {
        bindings.invalidateDuringRead = true;

        assertThatThrownBy(() -> installer.install(SERVICE_UNIT, INCARNATION).toCompletableFuture().join())
                .hasRootCauseMessage("continuity changed across A/read/B");
        assertThat(fence.captureValidOrNull()).isNull();
    }

    @Test
    public void invalidationAfterContinuityCheckWinsTheSingleWordCas() {
        bindings.invalidateAfterCurrentCheck = true;

        assertThatThrownBy(() -> installer.install(SERVICE_UNIT, INCARNATION).toCompletableFuture().join())
                .hasRootCauseMessage("stale P1 installer lost the local fence CAS");
        assertThat(fence.captureValidOrNull()).isNull();
    }

    @Test
    public void continuityChangeAcrossReadFailsClosed() {
        bindings.current = false;

        assertThatThrownBy(() -> installer.install(SERVICE_UNIT, INCARNATION).toCompletableFuture().join())
                .hasRootCauseMessage("continuity changed across A/read/B");
        assertThat(fence.captureValidOrNull()).isNull();
    }

    @Test
    public void bindingProviderCannotInstallADifferentIncarnation() {
        bindings.authority = authority(new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromString("tenant/ns/persistent/other"),
                PulsarTopicName.fromString("persistent://tenant/ns/other"),
                new PulsarBindingGeneration(1)));

        assertThatThrownBy(() -> installer.install(SERVICE_UNIT, INCARNATION).toCompletableFuture().join())
                .hasRootCauseMessage("ACTIVE binding authority does not match the expected incarnation");
        assertThat(fence.captureValidOrNull()).isNull();
    }

    private static NereusOwnershipWitness witness(String acquisition, long version) {
        CanonicalBytes bytes = CanonicalBytes.copyOf("owned".getBytes(StandardCharsets.UTF_8));
        return new NereusOwnershipWitness(
                SERVICE_UNIT,
                "broker-1",
                new NereusOwnershipId("22222222222222222222222222222222"),
                new NereusOwnershipId(acquisition),
                bytes,
                Sha256Digest.hash(bytes),
                version);
    }

    private static NereusPulsarBindingAuthority authority() {
        return authority(INCARNATION);
    }

    private static NereusPulsarBindingAuthority authority(PulsarTopicIncarnationIdentity incarnation) {
        byte[] binding = new byte[32];
        byte[] epoch = new byte[32];
        binding[0] = 1;
        epoch[0] = 2;
        return new NereusPulsarBindingAuthority(
                incarnation,
                new TopicBindingId(Sha256Digest.copyOf(binding)),
                new StorageEpochId(Sha256Digest.copyOf(epoch)),
                3,
                4,
                Sha256Digest.copyOf(binding),
                Sha256Digest.copyOf(epoch));
    }

    private final class FakeBindingProvider implements NereusPulsarBindingAuthorityProvider {
        private Runnable invalidation;
        private boolean current = true;
        private boolean invalidateDuringRead;
        private boolean invalidateAfterCurrentCheck;
        private long epoch = 1;
        private NereusPulsarBindingAuthority authority = authority();
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public AutoCloseable armInvalidation(PulsarTopicIncarnationIdentity incarnation, Runnable callback) {
            invalidation = callback;
            return () -> closed.set(true);
        }

        @Override
        public NereusContinuityPermit captureContinuityPermitOrNull() {
            return new NereusContinuityPermit(1, epoch);
        }

        @Override
        public boolean isCurrent(NereusContinuityPermit permit) {
            boolean result = current && permit.invalidationEpoch() == epoch;
            if (result && invalidateAfterCurrentCheck) {
                invalidateAfterCurrentCheck = false;
                invalidate();
            }
            return result;
        }

        @Override
        public long currentInvalidationEpoch() {
            return epoch;
        }

        @Override
        public CompletionStage<Optional<NereusPulsarBindingAuthority>> readActive(
                PulsarTopicIncarnationIdentity incarnation) {
            if (invalidateDuringRead) {
                invalidate();
            }
            return CompletableFuture.completedFuture(Optional.of(authority));
        }

        private void invalidate() {
            epoch++;
            invalidation.run();
        }
    }
}
