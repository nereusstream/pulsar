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
package org.apache.bookkeeper.mledger.impl;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.mledger.OffloadReadSourceState;

/** Shared per-ledger source pins used by cached handles and the native deletion cut. */
final class LedgerReadSourcePinRegistry {
    enum Source {
        OBJECT,
        BOOKKEEPER
    }

    private boolean objectFenced;
    private boolean bookKeeperFenced;
    private int objectPins;
    private int bookKeeperPins;
    private CompletableFuture<Void> objectDrain;
    private CompletableFuture<Void> bookKeeperDrain;

    synchronized Pin acquire(Source source, OffloadReadSourceState state) {
        if ((source == Source.OBJECT && objectFenced)
                || (source == Source.BOOKKEEPER && bookKeeperFenced)) {
            throw new SourcePinFencedException(source + " source-pin admission is fenced");
        }
        if (source == Source.OBJECT) {
            objectPins++;
        } else {
            bookKeeperPins++;
        }
        return new Pin(this, source, state.metadataVersion(), state.attemptUuid());
    }

    synchronized CompletableFuture<Void> fence(Source source) {
        if (source == Source.OBJECT) {
            objectFenced = true;
            if (objectPins == 0) {
                return CompletableFuture.completedFuture(null);
            }
            if (objectDrain == null) {
                objectDrain = new CompletableFuture<>();
            }
            return objectDrain;
        }
        bookKeeperFenced = true;
        if (bookKeeperPins == 0) {
            return CompletableFuture.completedFuture(null);
        }
        if (bookKeeperDrain == null) {
            bookKeeperDrain = new CompletableFuture<>();
        }
        return bookKeeperDrain;
    }

    synchronized CompletableFuture<Void> fenceBoth() {
        return CompletableFuture.allOf(fence(Source.OBJECT), fence(Source.BOOKKEEPER));
    }

    synchronized boolean unfenceBookKeeper() {
        if (bookKeeperPins != 0 || objectFenced) {
            return false;
        }
        bookKeeperFenced = false;
        bookKeeperDrain = null;
        return true;
    }

    synchronized int pinCount() {
        return objectPins + bookKeeperPins;
    }

    private synchronized void release(Source source) {
        if (source == Source.OBJECT) {
            if (objectPins <= 0) {
                throw new IllegalStateException("Object source-pin underflow");
            }
            objectPins--;
            if (objectPins == 0 && objectDrain != null) {
                objectDrain.complete(null);
            }
        } else {
            if (bookKeeperPins <= 0) {
                throw new IllegalStateException("BookKeeper source-pin underflow");
            }
            bookKeeperPins--;
            if (bookKeeperPins == 0 && bookKeeperDrain != null) {
                bookKeeperDrain.complete(null);
            }
        }
    }

    static final class Pin implements AutoCloseable {
        private final LedgerReadSourcePinRegistry owner;
        private final Source source;
        private final long metadataVersion;
        private final UUID attemptUuid;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Pin(
                LedgerReadSourcePinRegistry owner,
                Source source,
                long metadataVersion,
                UUID attemptUuid) {
            this.owner = owner;
            this.source = source;
            this.metadataVersion = metadataVersion;
            this.attemptUuid = attemptUuid;
        }

        long metadataVersion() {
            return metadataVersion;
        }

        UUID attemptUuid() {
            return attemptUuid;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                owner.release(source);
            }
        }
    }

    static final class SourcePinFencedException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        SourcePinFencedException(String message) {
            super(message);
        }
    }
}
