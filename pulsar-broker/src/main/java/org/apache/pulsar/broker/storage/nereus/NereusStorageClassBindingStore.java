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

import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.managedledger.integration.NereusCreationGuard;
import com.nereusstream.managedledger.integration.NereusCreationPermit;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/** Single-key Nereus storage-class claim used before projection publication. */
public final class NereusStorageClassBindingStore implements AutoCloseable {
    private static final byte[] MAGIC = new byte[] {'N', 'S', 'B', '1'};
    private static final String ROOT = "/managed-ledger-storage-bindings/v1/";
    private final MetadataStoreExtended metadataStore;
    private final Duration timeout;
    private final AtomicBoolean closed = new AtomicBoolean();

    public NereusStorageClassBindingStore(MetadataStoreExtended metadataStore, Duration timeout) {
        this.metadataStore = java.util.Objects.requireNonNull(metadataStore, "metadataStore");
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
        String path = path(persistenceName);
        byte[] candidate = encode(persistenceName, 1);
        CompletableFuture<NereusCreationPermit> operation = metadataStore.get(path).thenCompose(existing -> {
            if (existing.isPresent()) {
                Binding binding = decode(existing.orElseThrow().getValue(), persistenceName);
                return CompletableFuture.completedFuture(permit(path, binding));
            }
            return metadataStore.put(path, candidate, Optional.of(-1L))
                    .thenApply(ignored -> permit(path, new Binding(persistenceName, 1)))
                    .exceptionallyCompose(failure -> {
                        if (!isBadVersion(failure)) {
                            return CompletableFuture.failedFuture(unwrap(failure));
                        }
                        return metadataStore.get(path).thenApply(raced -> {
                            GetResult result = raced.orElseThrow(() ->
                                    new IllegalStateException("binding disappeared after create conflict"));
                            return permit(path, decode(result.getValue(), persistenceName));
                        });
                    });
        });
        return operation.orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    private NereusCreationPermit permit(String path, Binding binding) {
        return new NereusCreationPermit() {
            @Override
            public String persistenceName() {
                return binding.persistenceName();
            }

            @Override
            public long bindingGeneration() {
                return binding.generation();
            }

            @Override
            public CompletableFuture<Void> validateBeforeProjectionPublish() {
                if (closed.get()) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Nereus binding store is closed"));
                }
                return metadataStore.get(path).thenApply(current -> {
                    GetResult result = current.orElseThrow(() ->
                            new IllegalStateException("Nereus binding disappeared before projection publish"));
                    Binding actual = decode(result.getValue(), binding.persistenceName());
                    if (!actual.equals(binding)) {
                        throw new IllegalStateException("Nereus binding changed before projection publish");
                    }
                    return (Void) null;
                }).orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
            }
        };
    }

    private static String path(String persistenceName) {
        return ROOT + DeterministicIds.stableHashComponent(
                "pulsar-storage-binding-v1\0" + persistenceName);
    }

    private static byte[] encode(String persistenceName, long generation) {
        byte[] name = persistenceName.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(MAGIC.length + Long.BYTES + Integer.BYTES + name.length);
        buffer.put(MAGIC).putLong(generation).putInt(name.length).put(name);
        return buffer.array();
    }

    private static Binding decode(byte[] bytes, String expectedName) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(bytes).asReadOnlyBuffer();
            byte[] magic = new byte[MAGIC.length];
            buffer.get(magic);
            if (!Arrays.equals(magic, MAGIC)) {
                throw new IllegalArgumentException("invalid Nereus binding magic");
            }
            long generation = buffer.getLong();
            int length = buffer.getInt();
            if (generation <= 0 || length < 0 || length != buffer.remaining()) {
                throw new IllegalArgumentException("invalid Nereus binding fields");
            }
            byte[] name = new byte[length];
            buffer.get(name);
            String persistenceName = new String(name, StandardCharsets.UTF_8);
            if (!persistenceName.equals(expectedName)) {
                throw new IllegalStateException("Nereus binding hash collision");
            }
            return new Binding(persistenceName, generation);
        } catch (RuntimeException failure) {
            throw new IllegalStateException("corrupt Nereus storage-class binding", failure);
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

    private record Binding(String persistenceName, long generation) {
    }
}
