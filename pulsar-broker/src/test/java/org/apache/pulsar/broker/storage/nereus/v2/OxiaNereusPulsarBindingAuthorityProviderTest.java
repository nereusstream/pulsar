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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.nereusstream.domain.aggregate.FrameEncodingPolicyValueV1;
import com.nereusstream.domain.aggregate.InitialStorageEpochV1;
import com.nereusstream.domain.aggregate.PolicyCatalogDigest;
import com.nereusstream.domain.aggregate.ProfileOriginV1;
import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.aggregate.TopicBindingV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.PulsarTopicName;
import com.nereusstream.metadata.spi.capability.PulsarTopicGenerationSelectorStore;
import com.nereusstream.metadata.spi.capability.TopicBindingAggregateReader;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import com.nereusstream.metadata.spi.model.VersionedAggregateSnapshot;
import com.nereusstream.metadata.spi.model.VersionedSelectorSnapshot;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OxiaNereusPulsarBindingAuthorityProviderTest {
    private static final PulsarTopicIncarnationIdentity INCARNATION = new PulsarTopicIncarnationIdentity(
            PulsarPersistenceName.fromString("tenant/ns/persistent/orders"),
            PulsarTopicName.fromString("persistent://tenant/ns/orders"),
            new PulsarBindingGeneration(7));

    private PulsarTopicGenerationSelectorStore selectors;
    private TopicBindingAggregateReader aggregates;
    private FakeContinuity continuity;
    private OxiaNereusPulsarBindingAuthorityProvider provider;

    @BeforeMethod
    public void setup() {
        selectors = mock(PulsarTopicGenerationSelectorStore.class);
        aggregates = mock(TopicBindingAggregateReader.class);
        continuity = new FakeContinuity();
        provider = new OxiaNereusPulsarBindingAuthorityProvider(selectors, aggregates, continuity);
    }

    @Test
    public void projectsExactActiveSelectorAndAggregate() {
        VersionedAggregateSnapshot aggregate = aggregateSnapshot();
        VersionedSelectorSnapshot selector = selectorSnapshot(
                PulsarTopicGenerationSelectorStateV1.ACTIVE,
                aggregate.binding().bindingId(),
                aggregate.canonicalStoredDigest());
        when(selectors.readSelector(INCARNATION.persistenceName()))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(selector)));
        when(aggregates.readAggregate(INCARNATION))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(aggregate)));

        NereusPulsarBindingAuthority authority = provider.readActive(INCARNATION)
                .toCompletableFuture()
                .join()
                .orElseThrow();

        assertThat(authority.incarnation()).isEqualTo(INCARNATION);
        assertThat(authority.bindingId()).isEqualTo(aggregate.binding().bindingId());
        assertThat(authority.storageEpochId()).isEqualTo(aggregate.initialEpoch().storageEpochId());
        assertThat(authority.selectorBackendVersion()).isEqualTo(selector.metadataVersion());
        assertThat(authority.aggregateBackendVersion()).isEqualTo(aggregate.metadataVersion());
        assertThat(authority.selectorDigest()).isEqualTo(selector.value().canonicalStoredDigest());
        assertThat(authority.aggregateDigest()).isEqualTo(aggregate.canonicalStoredDigest());
    }

    @Test
    public void nonActiveOrDifferentGenerationFailsClosedWithoutAggregateRead() {
        VersionedAggregateSnapshot aggregate = aggregateSnapshot();
        VersionedSelectorSnapshot reserved = selectorSnapshot(
                PulsarTopicGenerationSelectorStateV1.RESERVED,
                aggregate.binding().bindingId(),
                aggregate.canonicalStoredDigest());
        when(selectors.readSelector(INCARNATION.persistenceName()))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(reserved)));

        assertThat(provider.readActive(INCARNATION).toCompletableFuture().join()).isEmpty();

        PulsarTopicGenerationSelectorValueV1 differentGeneration = new PulsarTopicGenerationSelectorValueV1(
                INCARNATION.persistenceName(),
                new PulsarBindingGeneration(8),
                PulsarTopicGenerationSelectorStateV1.ACTIVE,
                aggregate.binding().bindingId(),
                aggregate.canonicalStoredDigest(),
                bytes("selector-8"),
                digest("selector-8"));
        when(selectors.readSelector(INCARNATION.persistenceName())).thenReturn(CompletableFuture.completedFuture(
                Optional.of(new VersionedSelectorSnapshot(differentGeneration, version(8)))));

        assertThat(provider.readActive(INCARNATION).toCompletableFuture().join()).isEmpty();
    }

    @Test
    public void selectorAggregateMismatchIsAnAuthorityFailure() {
        VersionedAggregateSnapshot aggregate = aggregateSnapshot();
        VersionedSelectorSnapshot selector = selectorSnapshot(
                PulsarTopicGenerationSelectorStateV1.ACTIVE,
                aggregate.binding().bindingId(),
                digest("different-aggregate"));
        when(selectors.readSelector(INCARNATION.persistenceName()))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(selector)));
        when(aggregates.readAggregate(INCARNATION))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(aggregate)));

        assertThatThrownBy(() -> provider.readActive(INCARNATION).toCompletableFuture().join())
                .hasRootCauseMessage("selector aggregate digest does not match the aggregate");
    }

    @Test
    public void bridgesInvalidationAndExactContinuityPermit() throws Exception {
        AtomicBoolean invalidated = new AtomicBoolean();
        AtomicLong invalidationEpoch = new AtomicLong();
        AutoCloseable registration = provider.armInvalidation(INCARNATION, epoch -> {
            invalidationEpoch.set(epoch);
            invalidated.set(true);
        });
        NereusContinuityPermit permit = provider.captureContinuityPermitOrNull();

        assertThat(permit).isEqualTo(new NereusContinuityPermit(3, 11));
        assertThat(provider.isCurrent(permit)).isTrue();

        continuity.invalidate();
        assertThat(invalidated).isTrue();
        assertThat(invalidationEpoch).hasValue(12);
        assertThat(provider.isCurrent(permit)).isFalse();
        registration.close();
    }

    private static VersionedAggregateSnapshot aggregateSnapshot() {
        TopicBindingId bindingId = new TopicBindingId(digest("binding"));
        StorageEpochId storageEpochId = new StorageEpochId(digest("storage-epoch"));
        PulsarProtocolCellIdentity cell = new PulsarProtocolCellIdentity(
                new DeploymentId(new Id128(1, 2)),
                new ReservationDomainId(new Id128(3, 4)),
                new PulsarCellId(new Id128(5, 6)));
        TopicBindingV1 binding = new TopicBindingV1(ProtocolKindV1.PULSAR, bindingId, cell, INCARNATION);
        InitialStorageEpochV1 epoch = new InitialStorageEpochV1(
                storageEpochId,
                0,
                StorageProfileV1.BOOKKEEPER_WAL_ONLY,
                ProfileOriginV1.TOPIC_EXPLICIT,
                new PolicyCatalogDigest(digest("catalog")),
                FrameEncodingPolicyValueV1.none());
        TopicBindingAggregateV1 value = new TopicBindingAggregateV1(
                TopicBindingAggregateV1.SCHEMA_VERSION, binding, epoch);
        CanonicalBytes stored = bytes("aggregate");
        return new VersionedAggregateSnapshot(value, stored, Sha256Digest.hash(stored), version(19));
    }

    private static VersionedSelectorSnapshot selectorSnapshot(
            PulsarTopicGenerationSelectorStateV1 state,
            TopicBindingId bindingId,
            Sha256Digest aggregateDigest) {
        CanonicalBytes stored = bytes("selector");
        PulsarTopicGenerationSelectorValueV1 value = new PulsarTopicGenerationSelectorValueV1(
                INCARNATION.persistenceName(),
                INCARNATION.bindingGeneration(),
                state,
                bindingId,
                aggregateDigest,
                stored,
                Sha256Digest.hash(stored));
        return new VersionedSelectorSnapshot(value, version(17));
    }

    private static MetadataVersion version(int value) {
        return new MetadataVersion(CanonicalBytes.copyOf(new byte[] {(byte) value}));
    }

    private static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(bytes(value));
    }

    private static final class FakeContinuity
            implements OxiaNereusPulsarBindingAuthorityProvider.ContinuityBridge {
        private long epoch = 11;
        private LongConsumer invalidation = ignored -> {};

        @Override
        public AutoCloseable arm(PulsarTopicIncarnationIdentity incarnation, LongConsumer callback) {
            assertThat(incarnation).isEqualTo(INCARNATION);
            invalidation = callback;
            return () -> invalidation = ignored -> {};
        }

        @Override
        public NereusContinuityPermit captureOrNull() {
            return new NereusContinuityPermit(3, epoch);
        }

        @Override
        public boolean isCurrent(NereusContinuityPermit permit) {
            return permit.equals(new NereusContinuityPermit(3, epoch));
        }

        private void invalidate() {
            epoch++;
            invalidation.accept(epoch);
        }
    }
}
