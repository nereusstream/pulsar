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

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.OffloadReadSourceState;
import org.apache.bookkeeper.mledger.OffloadedLedgerHandle;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.ReadFailureKind;
import org.apache.bookkeeper.mledger.impl.LedgerReadSourcePinRegistry.Pin;
import org.apache.bookkeeper.mledger.impl.LedgerReadSourcePinRegistry.Source;
import org.apache.bookkeeper.mledger.impl.LedgerReadSourcePinRegistry.SourcePinFencedException;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;

/** Cached sealed-ledger handle that returns each range wholly from one native-authorized source. */
final class DualSourceReadHandle implements ReadHandle, OffloadedLedgerHandle {
    private final long ledgerId;
    private final SourceSafeLedgerOffloader offloader;
    private final Supplier<OffloadReadSourceState> stateSupplier;
    private final OffloadedReadPriority priority;
    private final LedgerReadSourcePinRegistry pins;
    private final LazyChild objectChild;
    private final LazyChild bookKeeperChild;
    private final LedgerMetadata ledgerMetadata;
    private final long lastAddConfirmed;
    private final long length;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object closeMutex = new Object();
    private CompletableFuture<Void> closeFuture;
    private volatile long lastAccessTimestamp = System.currentTimeMillis();

    static CompletableFuture<ReadHandle> open(
            long ledgerId,
            SourceSafeLedgerOffloader offloader,
            Supplier<OffloadReadSourceState> stateSupplier,
            OffloadedReadPriority priority,
            LedgerReadSourcePinRegistry pins,
            Supplier<CompletableFuture<ReadHandle>> objectFactory,
            Supplier<CompletableFuture<ReadHandle>> bookKeeperFactory) {
        Objects.requireNonNull(offloader);
        Objects.requireNonNull(stateSupplier);
        Objects.requireNonNull(priority);
        Objects.requireNonNull(pins);
        LazyChild object = new LazyChild(Source.OBJECT, objectFactory);
        LazyChild bookKeeper = new LazyChild(Source.BOOKKEEPER, bookKeeperFactory);
        OffloadReadSourceState state;
        try {
            state = Objects.requireNonNull(stateSupplier.get());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        Source primary;
        try {
            primary = selectPrimary(state, priority);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        LazyChild eager = primary == Source.OBJECT ? object : bookKeeper;
        LazyChild alternate = primary == Source.OBJECT ? bookKeeper : object;
        return openMetadataHandle(primary, state, offloader, eager, alternate).thenApply(handle -> {
            if (handle.getId() != ledgerId || !handle.isClosed()) {
                throw new OffloadReadIntegrityException("eager child does not represent the sealed ledger");
            }
            return (ReadHandle) new DualSourceReadHandle(
                    ledgerId,
                    offloader,
                    stateSupplier,
                    priority,
                    pins,
                    object,
                    bookKeeper,
                    handle.getLedgerMetadata(),
                    handle.getLastAddConfirmed(),
                    handle.getLength());
        });
    }

    private static CompletableFuture<ReadHandle> openMetadataHandle(
            Source primary,
            OffloadReadSourceState state,
            SourceSafeLedgerOffloader offloader,
            LazyChild eager,
            LazyChild alternate) {
        return eager.get().handle((handle, openFailure) -> {
            if (openFailure == null) {
                return CompletableFuture.completedFuture(handle);
            }
            Throwable primaryFailure = unwrap(openFailure);
            Source secondary = alternate(primary);
            if (!eligible(state, secondary)
                    || !fallbackAllowedDuringOpen(primary, primaryFailure, offloader)) {
                return CompletableFuture.<ReadHandle>failedFuture(primaryFailure);
            }
            return alternate.get().handle((secondaryHandle, secondaryFailure) -> {
                if (secondaryFailure == null) {
                    return secondaryHandle;
                }
                Throwable unwrappedSecondary = unwrap(secondaryFailure);
                if (unwrappedSecondary != primaryFailure) {
                    primaryFailure.addSuppressed(unwrappedSecondary);
                }
                throw new CompletionException(primaryFailure);
            });
        }).thenCompose(future -> future);
    }

    private static boolean fallbackAllowedDuringOpen(
            Source source, Throwable failure, SourceSafeLedgerOffloader offloader) {
        if (source == Source.BOOKKEEPER) {
            int code = BKException.getExceptionCode(failure);
            return code == BKException.Code.NoSuchLedgerExistsException
                    || code == BKException.Code.NoSuchLedgerExistsOnMetadataServerException;
        }
        return switch (offloader.classifyOffloadedReadFailure(failure)) {
            case NOT_FOUND, TIMEOUT, UNAVAILABLE, SHORT_READ, INTEGRITY, FORMAT -> true;
            default -> false;
        };
    }

    private DualSourceReadHandle(
            long ledgerId,
            SourceSafeLedgerOffloader offloader,
            Supplier<OffloadReadSourceState> stateSupplier,
            OffloadedReadPriority priority,
            LedgerReadSourcePinRegistry pins,
            LazyChild objectChild,
            LazyChild bookKeeperChild,
            LedgerMetadata ledgerMetadata,
            long lastAddConfirmed,
            long length) {
        this.ledgerId = ledgerId;
        this.offloader = offloader;
        this.stateSupplier = stateSupplier;
        this.priority = priority;
        this.pins = pins;
        this.objectChild = objectChild;
        this.bookKeeperChild = bookKeeperChild;
        this.ledgerMetadata = ledgerMetadata;
        this.lastAddConfirmed = lastAddConfirmed;
        this.length = length;
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        if (firstEntry < 0 || lastEntry < firstEntry || lastEntry > lastAddConfirmed) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("invalid sealed-ledger range"));
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("dual-source read handle is closed"));
        }
        OffloadReadSourceState state;
        try {
            state = Objects.requireNonNull(stateSupplier.get());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return readFrom(selectPrimary(state, priority), state, firstEntry, lastEntry, true, null);
    }

    private CompletableFuture<LedgerEntries> readFrom(
            Source source,
            OffloadReadSourceState state,
            long firstEntry,
            long lastEntry,
            boolean allowFallback,
            Throwable primaryFailure) {
        Pin pin;
        try {
            if (!eligible(state, source) || closed.get()) {
                throw new SourcePinFencedException("source is not native-authorized");
            }
            pin = pins.acquire(source, state);
            OffloadReadSourceState afterPin = Objects.requireNonNull(stateSupplier.get());
            if (!state.equals(afterPin) || !eligible(afterPin, source)) {
                pin.close();
                throw new SourcePinFencedException("native source state changed during pin admission");
            }
        } catch (SourcePinFencedException failure) {
            Source alternate = alternate(source);
            OffloadReadSourceState current = stateSupplier.get();
            if (allowFallback && current != null && eligible(current, alternate)) {
                return readFrom(alternate, current, firstEntry, lastEntry, false, primaryFailure);
            }
            return CompletableFuture.failedFuture(primaryFailure == null ? failure : primaryFailure);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(primaryFailure == null ? failure : primaryFailure);
        }

        CompletableFuture<LedgerEntries> result = new CompletableFuture<>();
        child(source).get().thenCompose(handle -> handle.readAsync(firstEntry, lastEntry))
                .whenComplete((entries, readFailure) -> {
                    if (readFailure == null) {
                        try {
                            validate(entries, source, firstEntry, lastEntry);
                            lastAccessTimestamp = System.currentTimeMillis();
                            result.complete(new PinnedLedgerEntries(entries, pin));
                        } catch (Throwable invalidRange) {
                            closeEntriesAndPin(entries, pin);
                            handleFailure(
                                    result,
                                    source,
                                    state,
                                    firstEntry,
                                    lastEntry,
                                    allowFallback,
                                    primaryFailure,
                                    invalidRange);
                        }
                    } else {
                        pin.close();
                        handleFailure(
                                result,
                                source,
                                state,
                                firstEntry,
                                lastEntry,
                                allowFallback,
                                primaryFailure,
                                unwrap(readFailure));
                    }
                });
        return result;
    }

    private void handleFailure(
            CompletableFuture<LedgerEntries> result,
            Source source,
            OffloadReadSourceState originalState,
            long firstEntry,
            long lastEntry,
            boolean allowFallback,
            Throwable existingPrimary,
            Throwable failure) {
        Throwable primary = existingPrimary == null ? failure : existingPrimary;
        if (existingPrimary != null && failure != existingPrimary) {
            primary.addSuppressed(failure);
        }
        if (source == Source.OBJECT) {
            ReadFailureKind kind = classifyObjectFailure(failure);
            if (kind == ReadFailureKind.INTEGRITY || kind == ReadFailureKind.FORMAT) {
                offloader.recordOffloadedReadIntegrityFailure(ledgerId, originalState.attemptUuid(), failure);
            }
        }
        OffloadReadSourceState current;
        try {
            current = Objects.requireNonNull(stateSupplier.get());
        } catch (Throwable stateFailure) {
            primary.addSuppressed(stateFailure);
            result.completeExceptionally(primary);
            return;
        }
        if (!allowFallback || !fallbackAllowed(source, failure, originalState, current)) {
            result.completeExceptionally(primary);
            return;
        }
        readFrom(alternate(source), current, firstEntry, lastEntry, false, primary)
                .whenComplete((entries, secondaryFailure) -> {
                    if (secondaryFailure == null) {
                        result.complete(entries);
                    } else {
                        Throwable secondary = unwrap(secondaryFailure);
                        if (secondary != primary) {
                            primary.addSuppressed(secondary);
                        }
                        result.completeExceptionally(primary);
                    }
                });
    }

    private boolean fallbackAllowed(
            Source source,
            Throwable failure,
            OffloadReadSourceState original,
            OffloadReadSourceState current) {
        if (!Objects.equals(original.attemptUuid(), current.attemptUuid())
                || !current.objectEligible()
                || !current.bookKeeperEligible()) {
            return false;
        }
        if (source == Source.BOOKKEEPER) {
            int code = BKException.getExceptionCode(failure);
            return code == BKException.Code.NoSuchLedgerExistsException
                    || code == BKException.Code.NoSuchLedgerExistsOnMetadataServerException;
        }
        return switch (classifyObjectFailure(failure)) {
            case NOT_FOUND, TIMEOUT, UNAVAILABLE, SHORT_READ, INTEGRITY, FORMAT -> true;
            default -> false;
        };
    }

    private ReadFailureKind classifyObjectFailure(Throwable failure) {
        if (failure instanceof OffloadReadIntegrityException) {
            return ReadFailureKind.INTEGRITY;
        }
        if (failure instanceof OffloadReadShortException) {
            return ReadFailureKind.SHORT_READ;
        }
        return offloader.classifyOffloadedReadFailure(failure);
    }

    private void validate(LedgerEntries entries, Source source, long firstEntry, long lastEntry) {
        if (entries == null) {
            throw new OffloadReadIntegrityException(source + " returned null entries");
        }
        long expected = firstEntry;
        for (LedgerEntry entry : entries) {
            if (entry.getLedgerId() != ledgerId || entry.getEntryId() != expected) {
                throw new OffloadReadIntegrityException(source + " returned mixed or non-contiguous entries");
            }
            expected = Math.addExact(expected, 1);
        }
        if (expected != Math.addExact(lastEntry, 1)) {
            throw new OffloadReadShortException(source + " returned a short range");
        }
    }

    CompletableFuture<Void> closeBookKeeperSource() {
        return pins.fence(Source.BOOKKEEPER).thenCompose(ignored -> bookKeeperChild.close());
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (closeMutex) {
            if (closeFuture != null) {
                return closeFuture;
            }
            closed.set(true);
            closeFuture = pins.fenceBoth()
                    .thenCompose(ignored -> CompletableFuture.allOf(objectChild.close(), bookKeeperChild.close()));
            return closeFuture;
        }
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        return readAsync(firstEntry, lastEntry);
    }

    @Override
    public CompletableFuture<Long> readLastAddConfirmedAsync() {
        return CompletableFuture.completedFuture(lastAddConfirmed);
    }

    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
        return CompletableFuture.completedFuture(lastAddConfirmed);
    }

    @Override
    public long getLastAddConfirmed() {
        return lastAddConfirmed;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public boolean isClosed() {
        return ledgerMetadata.isClosed();
    }

    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(
            long entryId, long timeOutInMillis, boolean parallel) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("sealed dual-source long poll"));
    }

    @Override
    public long getId() {
        return ledgerId;
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return ledgerMetadata;
    }

    @Override
    public long lastAccessTimestamp() {
        return lastAccessTimestamp;
    }

    @Override
    public int getPendingRead() {
        return pins.pinCount();
    }

    private LazyChild child(Source source) {
        return source == Source.OBJECT ? objectChild : bookKeeperChild;
    }

    private static Source selectPrimary(OffloadReadSourceState state, OffloadedReadPriority priority) {
        Source preferred = priority == OffloadedReadPriority.BOOKKEEPER_FIRST ? Source.BOOKKEEPER : Source.OBJECT;
        if (eligible(state, preferred)) {
            return preferred;
        }
        Source alternate = alternate(preferred);
        if (eligible(state, alternate)) {
            return alternate;
        }
        throw new SourcePinFencedException("native metadata authorizes no read source");
    }

    private static boolean eligible(OffloadReadSourceState state, Source source) {
        return source == Source.OBJECT ? state.objectEligible() : state.bookKeeperEligible();
    }

    private static Source alternate(Source source) {
        return source == Source.OBJECT ? Source.BOOKKEEPER : Source.OBJECT;
    }

    private static void closeEntriesAndPin(LedgerEntries entries, Pin pin) {
        try {
            if (entries != null) {
                entries.close();
            }
        } finally {
            pin.close();
        }
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }

    private static final class LazyChild {
        private final Source source;
        private final Supplier<CompletableFuture<ReadHandle>> factory;
        private CompletableFuture<ReadHandle> handle;
        private CompletableFuture<Void> closeFuture;
        private boolean closing;

        private LazyChild(Source source, Supplier<CompletableFuture<ReadHandle>> factory) {
            this.source = source;
            this.factory = Objects.requireNonNull(factory);
        }

        private synchronized CompletableFuture<ReadHandle> get() {
            if (closing) {
                return CompletableFuture.failedFuture(new IllegalStateException(source + " child is closing"));
            }
            if (handle == null) {
                try {
                    handle = Objects.requireNonNull(factory.get());
                } catch (Throwable failure) {
                    handle = CompletableFuture.failedFuture(failure);
                }
            }
            return handle;
        }

        private synchronized CompletableFuture<Void> close() {
            if (closeFuture != null) {
                return closeFuture;
            }
            closing = true;
            closeFuture = handle == null
                    ? CompletableFuture.completedFuture(null)
                    : handle.thenCompose(ReadHandle::closeAsync);
            return closeFuture;
        }
    }

    private static final class PinnedLedgerEntries implements LedgerEntries {
        private final LedgerEntries delegate;
        private final Pin pin;
        private final AtomicBoolean closed = new AtomicBoolean();

        private PinnedLedgerEntries(LedgerEntries delegate, Pin pin) {
            this.delegate = delegate;
            this.pin = pin;
        }

        @Override
        public LedgerEntry getEntry(long entryId) {
            return delegate.getEntry(entryId);
        }

        @Override
        public Iterator<LedgerEntry> iterator() {
            return delegate.iterator();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                try {
                    delegate.close();
                } finally {
                    pin.close();
                }
            }
        }
    }

    static final class OffloadReadIntegrityException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private OffloadReadIntegrityException(String message) {
            super(message);
        }
    }

    static final class OffloadReadShortException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private OffloadReadShortException(String message) {
            super(message);
        }
    }
}
