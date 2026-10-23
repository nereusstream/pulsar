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
package org.apache.bookkeeper.mledger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import org.apache.bookkeeper.client.api.ReadHandle;

/**
 * Optional sealed-ledger offloader contract for whole-range dual-source reads and verified BookKeeper deletion.
 */
public interface SourceSafeLedgerOffloader extends LedgerOffloader {
    /** Owner-serialized admission; actualCleanup remains pending through SDK, delivered buffers and callback return. */
    @FunctionalInterface
    interface NativeReadAdmission {
        CompletionStage<Void> admitRead(long ledgerId, long firstEntry, long lastEntry,
                                       CompletionStage<Void> actualCleanup);

        default CompletionStage<Void> admitOwnedRead(long ledgerId, long firstEntry, long lastEntry,
                                                   long maximumEntryBytes, long maximumBatchBytes,
                                                   CompletionStage<Void> actualCleanup) {
            return admitRead(ledgerId, firstEntry, lastEntry, actualCleanup);
        }

        default CompletionStage<Void> admitVerification(long ledgerId, CompletionStage<Void> actualCleanup) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("verification not admitted"));
        }

        /** Cached selected-source routing only; checked before opening an original BookKeeper handle. */
        default boolean hasPreferredReadSource(long ledgerId) {
            return false;
        }

        default int maximumReadEntries() {
            return Integer.MAX_VALUE;
        }

        /** Called after this owner's ordinary admission; retained segments use the supplied original Native reader. */
        default PreferredReadOperation readPreferred(long ledgerId, long firstEntry, long lastEntry,
                                                    NativeReadRange retained) {
            throw new UnsupportedOperationException("preferred Native read is unsupported");
        }

        /** Cached owner/source gate, checked again before each physical primary/fallback attempt. */
        default boolean sourceUseOpen() {
            return true;
        }
    }

    @FunctionalInterface
    interface NativeReadRange {
        CompletionStage<List<Entry>> read(long firstEntry, long lastEntry);
    }

    /** Entries transfer to the callback; drain tracks original SDK cleanup independently of observer completion. */
    record PreferredReadOperation(CompletionStage<List<Entry>> result, CompletionStage<Void> drained) {
        public PreferredReadOperation {
            result = Objects.requireNonNull(result, "preferred result").toCompletableFuture().minimalCompletionStage();
            drained = Objects.requireNonNull(drained, "preferred SDK drain")
                    .toCompletableFuture().minimalCompletionStage();
        }
    }

    /** Provider-observed read token; this alone grants no immutable physical namespace or deletion authority. */
    record OffloadedObjectReadIdentity(String key, String version, long length, String sha256) {
        public OffloadedObjectReadIdentity {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(version, "version");
            Objects.requireNonNull(sha256, "sha256");
            if (key.isBlank() || version.isBlank() || length < 0 || !sha256.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("offloaded Object read identity is incomplete");
            }
        }
    }

    /** Facts from the actual opened sealed source, independent of the ManagedLedger metadata version. */
    record OffloadedReadSourceIdentity(long ledgerId, UUID attemptUuid,
                                       OffloadedObjectReadIdentity root, OffloadedObjectReadIdentity data) {
        public OffloadedReadSourceIdentity {
            Objects.requireNonNull(attemptUuid, "attemptUuid");
            Objects.requireNonNull(root, "root");
            Objects.requireNonNull(data, "data");
            if (ledgerId < 0 || root.key().equals(data.key())) {
                throw new IllegalArgumentException("offloaded source identity is outside its exact domain");
            }
        }
    }

    /**
     * Reads immutable facts from this provider's existing handle, without opening another source or doing IO.
     * Providers that cannot expose their actual selected identities retain ordinary read support.
     */
    default OffloadedReadSourceIdentity describeOffloadedReadSource(ReadHandle handle) {
        throw new UnsupportedOperationException("opened offload source identity is unsupported");
    }

    /** Upper bounds from the actual opened, verified Object root; no current HEAD or caller-supplied estimate. */
    record OffloadedReadMemoryBounds(long maximumEncodedBlockBytes, long maximumDecodedBlockBytes,
                                     int maximumBlockEntries) {
        public OffloadedReadMemoryBounds {
            if (maximumEncodedBlockBytes < 0 || maximumDecodedBlockBytes < 0 || maximumBlockEntries < 0) {
                throw new IllegalArgumentException("negative opened Object memory bounds");
            }
        }
    }

    default OffloadedReadMemoryBounds describeOffloadedReadMemoryBounds(ReadHandle handle) {
        throw new UnsupportedOperationException("opened Object memory bounds unsupported");
    }

    default CompletableFuture<Void> verifyOpenedOffloadedSource(ReadHandle handle, long lastAddConfirmed,
            long entryCount, long logicalLength, BooleanSupplier sourceUseOpen) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("opened verification unsupported"));
    }

    /** Opens the published immutable versions; unsupported providers cannot substitute their current HEAD. */
    default CompletableFuture<ReadHandle> readOffloadedCaptured(long ledgerId, UUID attemptUuid,
            Map<String, String> driverMetadata, OffloadedReadSourceIdentity expected) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("published versions unsupported"));
    }

    enum ReadFailureKind {
        NOT_FOUND,
        TIMEOUT,
        UNAVAILABLE,
        SHORT_READ,
        INTEGRITY,
        FORMAT,
        INVALID_RANGE,
        CANCELLED,
        CLOSED,
        UNSUPPORTED,
        OTHER
    }

    enum RetentionClass {
        RETAIN_BK,
        DELETE_AFTER_VERIFIED
    }

    RetentionClass getBookKeeperRetentionClass();

    ReadFailureKind classifyOffloadedReadFailure(Throwable failure);

    default void recordOffloadedReadIntegrityFailure(long ledgerId, UUID attemptUuid, Throwable failure) {
    }

    /**
     * Revalidates the exact persisted pair and production read path before the native delete-intent CAS.
     */
    CompletableFuture<Void> revalidateOffloadedForSourceDeletion(
            long ledgerId,
            UUID attemptUuid,
            Map<String, String> offloadDriverMetadata,
            long lastAddConfirmed,
            long entryCount,
            long logicalLength);
}
