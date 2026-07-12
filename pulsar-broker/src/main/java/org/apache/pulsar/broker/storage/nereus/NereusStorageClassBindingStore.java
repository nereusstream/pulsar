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

import com.nereusstream.managedledger.integration.NereusCreationGuard;
import com.nereusstream.managedledger.integration.NereusCreationPermit;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/** Single-key Nereus storage-class claim used before projection publication. */
public final class NereusStorageClassBindingStore implements AutoCloseable {
    private final MetadataStoreExtended metadataStore;
    private final ManagedLedgerFactory bookkeeperFactory;
    private final Duration timeout;
    private final StorageClassBindingKeyspace keyspace = new StorageClassBindingKeyspace();
    private final StorageClassBindingCodec codec = new StorageClassBindingCodec();
    private final AtomicBoolean closed = new AtomicBoolean();

    public NereusStorageClassBindingStore(
            MetadataStoreExtended metadataStore,
            ManagedLedgerFactory bookkeeperFactory,
            Duration timeout) {
        this.metadataStore = java.util.Objects.requireNonNull(metadataStore, "metadataStore");
        this.bookkeeperFactory = java.util.Objects.requireNonNull(bookkeeperFactory, "bookkeeperFactory");
        this.timeout = java.util.Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }

    public NereusCreationGuard creationGuard() {
        return this::acquire;
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private CompletableFuture<NereusCreationPermit> acquire(String persistenceName) {
        if (persistenceName == null || persistenceName.isBlank()) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("persistenceName cannot be blank"));
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Nereus binding store is closed"));
        }
        TopicName topicName = TopicName.get(TopicName.fromPersistenceNamingEncoding(persistenceName));
        String path = keyspace.bindingKey(topicName.getNamespaceObject(), persistenceName);
        StorageClassBindingRecord candidate = StorageClassBindingRecord.claimed(
                persistenceName,
                StorageClassBindingRecord.NEREUS,
                1,
                System.currentTimeMillis());
        CompletableFuture<NereusCreationPermit> operation = metadataStore.sync(path)
                .thenCompose(ignored -> metadataStore.get(path)).thenCompose(existing -> {
            if (existing.isPresent()) {
                StorageClassBindingRecord binding = decode(existing.orElseThrow(), persistenceName);
                return CompletableFuture.completedFuture(permit(path, binding));
            }
            return bookkeeperFactory.asyncExists(persistenceName).thenCompose(bookkeeperExists -> {
                if (bookkeeperExists) {
                    return CompletableFuture.failedFuture(new IllegalStateException(
                            "existing BookKeeper storage cannot be opened as Nereus"));
                }
                return metadataStore.put(path, codec.encode(candidate), Optional.of(-1L))
                    .thenApply(stat -> permit(path, candidate.withMetadataVersion(stat.getVersion())))
                    .exceptionallyCompose(failure -> {
                        if (!isBadVersion(failure)) {
                            return CompletableFuture.failedFuture(unwrap(failure));
                        }
                        return metadataStore.get(path).thenApply(raced -> {
                            GetResult result = raced.orElseThrow(() ->
                                    new IllegalStateException("binding disappeared after create conflict"));
                            return permit(path, decode(result, persistenceName));
                        });
                    });
            });
        });
        return operation.orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    private NereusCreationPermit permit(String path, StorageClassBindingRecord binding) {
        requireNereusOpenState(binding);
        return new NereusCreationPermit() {
            @Override
            public String persistenceName() {
                return binding.persistenceName();
            }

            @Override
            public long bindingGeneration() {
                return binding.bindingGeneration();
            }

            @Override
            public CompletableFuture<Void> validateBeforeProjectionPublish() {
                if (closed.get()) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Nereus binding store is closed"));
                }
                return metadataStore.sync(path).thenCompose(ignored -> metadataStore.get(path)).thenCombine(
                        bookkeeperFactory.asyncExists(binding.persistenceName()),
                        (current, bookkeeperExists) -> {
                    if (bookkeeperExists) {
                        throw new IllegalStateException(
                                "BookKeeper storage appeared before Nereus projection publish");
                    }
                    GetResult result = current.orElseThrow(() ->
                            new IllegalStateException("Nereus binding disappeared before projection publish"));
                    StorageClassBindingRecord actual = decode(result, binding.persistenceName());
                    if (!actual.equals(binding)) {
                        throw new IllegalStateException("Nereus binding changed before projection publish");
                    }
                    return (Void) null;
                }).orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
            }
        };
    }

    private StorageClassBindingRecord decode(GetResult result, String expectedName) {
        StorageClassBindingRecord binding = codec.decode(result.getValue(), result.getStat().getVersion());
        if (!binding.persistenceName().equals(expectedName)) {
            throw new IllegalStateException("Nereus storage-class binding hash collision");
        }
        return binding;
    }

    private static void requireNereusOpenState(StorageClassBindingRecord binding) {
        if (!StorageClassBindingRecord.NEREUS.equals(binding.storageClass())) {
            throw new IllegalStateException("storage-class migration is required before Nereus open");
        }
        if (binding.state() != StorageClassBindingState.CLAIMED
                && binding.state() != StorageClassBindingState.ACTIVE) {
            throw new IllegalStateException("Nereus binding is not openable");
        }
    }

    private static boolean isBadVersion(Throwable failure) {
        return unwrap(failure) instanceof MetadataStoreException.BadVersionException;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
