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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Optional sealed-ledger offloader contract for whole-range dual-source reads and verified BookKeeper deletion.
 */
public interface SourceSafeLedgerOffloader extends LedgerOffloader {
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
