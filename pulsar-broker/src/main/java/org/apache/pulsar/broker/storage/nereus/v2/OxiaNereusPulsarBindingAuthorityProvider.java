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

import com.nereusstream.domain.aggregate.TopicBindingV1;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.metadata.oxia.v2.OxiaV2CapabilityStore;
import com.nereusstream.metadata.oxia.v2.continuity.InstallPermit;
import com.nereusstream.metadata.spi.capability.PulsarTopicGenerationSelectorStore;
import com.nereusstream.metadata.spi.capability.TopicBindingAggregateReader;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.VersionedAggregateSnapshot;
import com.nereusstream.metadata.spi.model.VersionedSelectorSnapshot;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.LongConsumer;

/** Exact P1 bridge from the source-qualified Oxia capability to the native ACTIVE installer. */
public final class OxiaNereusPulsarBindingAuthorityProvider
        implements NereusPulsarBindingAuthorityProvider {
    interface ContinuityBridge {
        AutoCloseable arm(PulsarTopicIncarnationIdentity incarnation, LongConsumer invalidation);

        NereusContinuityPermit captureOrNull();

        boolean isCurrent(NereusContinuityPermit permit);

    }

    private final PulsarTopicGenerationSelectorStore selectorStore;
    private final TopicBindingAggregateReader aggregateReader;
    private final ContinuityBridge continuity;

    public OxiaNereusPulsarBindingAuthorityProvider(OxiaV2CapabilityStore store) {
        this(
                Objects.requireNonNull(store, "store").selectorStore(),
                store.aggregateReader(),
                new StoreContinuityBridge(store));
    }

    OxiaNereusPulsarBindingAuthorityProvider(
            PulsarTopicGenerationSelectorStore selectorStore,
            TopicBindingAggregateReader aggregateReader,
            ContinuityBridge continuity) {
        this.selectorStore = Objects.requireNonNull(selectorStore, "selectorStore");
        this.aggregateReader = Objects.requireNonNull(aggregateReader, "aggregateReader");
        this.continuity = Objects.requireNonNull(continuity, "continuity");
    }

    @Override
    public AutoCloseable armInvalidation(
            PulsarTopicIncarnationIdentity incarnation, LongConsumer invalidation) {
        return continuity.arm(incarnation, invalidation);
    }

    @Override
    public NereusContinuityPermit captureContinuityPermitOrNull() {
        return continuity.captureOrNull();
    }

    @Override
    public boolean isCurrent(NereusContinuityPermit permit) {
        return continuity.isCurrent(permit);
    }

    @Override
    public CompletionStage<Optional<NereusPulsarBindingAuthority>> readActive(
            PulsarTopicIncarnationIdentity incarnation) {
        if (incarnation == null) {
            return CompletableFuture.failedFuture(new NullPointerException("incarnation"));
        }
        return selectorStore.readSelector(incarnation.persistenceName()).thenCompose(optionalSelector -> {
            if (optionalSelector.isEmpty()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            VersionedSelectorSnapshot selector = optionalSelector.orElseThrow();
            if (!selector.value().persistenceName().equals(incarnation.persistenceName())) {
                return failedAuthority("selector key identity does not match the requested persistence name");
            }
            if (selector.value().state() != PulsarTopicGenerationSelectorStateV1.ACTIVE
                    || !selector.value().generation().equals(incarnation.bindingGeneration())) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return aggregateReader.readAggregate(incarnation).thenApply(optionalAggregate ->
                    optionalAggregate.map(aggregate -> project(incarnation, selector, aggregate)));
        });
    }

    private static NereusPulsarBindingAuthority project(
            PulsarTopicIncarnationIdentity incarnation,
            VersionedSelectorSnapshot selector,
            VersionedAggregateSnapshot aggregate) {
        TopicBindingV1 binding = aggregate.binding();
        if (binding.protocolKind() != ProtocolKindV1.PULSAR
                || !binding.incarnationIdentity().equals(incarnation)) {
            throw new IllegalStateException("aggregate incarnation does not match the ACTIVE selector");
        }
        if (!selector.value().aggregateBindingId().equals(binding.bindingId())) {
            throw new IllegalStateException("selector binding ID does not match the aggregate");
        }
        if (!selector.value().aggregateCanonicalStoredDigest().equals(aggregate.canonicalStoredDigest())) {
            throw new IllegalStateException("selector aggregate digest does not match the aggregate");
        }
        return new NereusPulsarBindingAuthority(
                incarnation,
                binding.bindingId(),
                aggregate.initialEpoch().storageEpochId(),
                selector.metadataVersion(),
                aggregate.metadataVersion(),
                selector.value().canonicalStoredDigest(),
                aggregate.canonicalStoredDigest());
    }

    private static CompletionStage<Optional<NereusPulsarBindingAuthority>> failedAuthority(String message) {
        return CompletableFuture.failedFuture(new IllegalStateException(message));
    }

    private static final class StoreContinuityBridge implements ContinuityBridge {
        private final OxiaV2CapabilityStore store;

        private StoreContinuityBridge(OxiaV2CapabilityStore store) {
            this.store = store;
        }

        @Override
        public AutoCloseable arm(
                PulsarTopicIncarnationIdentity incarnation, LongConsumer invalidation) {
            return store.registerPulsarAuthorityInvalidation(incarnation, invalidation);
        }

        @Override
        public NereusContinuityPermit captureOrNull() {
            return store.capturePulsarInstallPermit()
                    .map(permit -> new NereusContinuityPermit(
                            permit.clientGeneration(), permit.invalidationEpoch()))
                    .orElse(null);
        }

        @Override
        public boolean isCurrent(NereusContinuityPermit permit) {
            return store.isCurrent(new InstallPermit(
                    permit.clientGeneration(), permit.invalidationEpoch()));
        }
    }
}
