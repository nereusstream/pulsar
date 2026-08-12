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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateMetadataStoreTableViewImpl;
import org.apache.pulsar.metadata.api.MetadataStore;

/** MetadataStore direct-get implementation of the P1 authoritative A/B witness. */
public final class MetadataStoreNereusOwnershipWitnessProvider
        implements NereusAuthoritativeOwnershipWitnessProvider {
    private final MetadataStore store;
    private final String localBrokerId;
    private final NereusOwnershipStateCodec codec;

    public MetadataStoreNereusOwnershipWitnessProvider(MetadataStore store, String localBrokerId) {
        this(store, localBrokerId, new NereusOwnershipStateCodec());
    }

    MetadataStoreNereusOwnershipWitnessProvider(
            MetadataStore store, String localBrokerId, NereusOwnershipStateCodec codec) {
        this.store = Objects.requireNonNull(store, "store");
        if (localBrokerId == null || localBrokerId.isBlank()) {
            throw new IllegalArgumentException("local broker ID must be present");
        }
        this.localBrokerId = localBrokerId;
        this.codec = Objects.requireNonNull(codec, "codec");
    }

    @Override
    public CompletionStage<Optional<NereusOwnershipWitness>> read(String serviceUnit) {
        if (serviceUnit == null || serviceUnit.isBlank() || serviceUnit.startsWith("/")) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                    new IllegalArgumentException("service unit must be a non-empty relative authority key"));
        }
        String path = ServiceUnitStateMetadataStoreTableViewImpl.PATH_PREFIX + "/" + serviceUnit;
        return store.get(path).thenApply(result -> result.flatMap(value -> decode(serviceUnit, value)));
    }

    private Optional<NereusOwnershipWitness> decode(
            String serviceUnit, org.apache.pulsar.metadata.api.GetResult result) {
        byte[] storedBytes = result.getValue();
        ServiceUnitStateData state = codec.decode(storedBytes);
        if (!Arrays.equals(storedBytes, codec.encode(state))
                || state.state() != ServiceUnitState.Owned
                || !localBrokerId.equals(state.dstBroker())
                || !state.hasNereusOwnershipIdentity()) {
            return Optional.empty();
        }
        CanonicalBytes canonical = CanonicalBytes.copyOf(storedBytes);
        return Optional.of(new NereusOwnershipWitness(
                serviceUnit,
                localBrokerId,
                new NereusOwnershipId(state.brokerIncarnationId()),
                new NereusOwnershipId(state.acquisitionId()),
                canonical,
                Sha256Digest.hash(canonical),
                result.getStat().getVersion()));
    }
}
