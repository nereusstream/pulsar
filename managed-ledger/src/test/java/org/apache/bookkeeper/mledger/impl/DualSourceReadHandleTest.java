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

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.OffloadReadSourceState;
import org.apache.bookkeeper.mledger.OffloadReadSourceState.BookKeeperDeleteState;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.ReadFailureKind;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.RetentionClass;
import org.apache.bookkeeper.mledger.impl.LedgerReadSourcePinRegistry.Source;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.testng.annotations.Test;

public class DualSourceReadHandleTest {
    private static final long LEDGER_ID = 42;
    private static final UUID ATTEMPT = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

    @Test
    public void objectFirstReturnsOneExactObjectRange() {
        Harness harness = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        LedgerEntries entries = harness.handle.readAsync(1, 3).join();

        assertEntryIds(entries, 1, 2, 3);
        assertEquals(harness.object.reads, 1);
        assertEquals(harness.bookKeeper.reads, 0);
        entries.close();
    }

    @Test
    public void objectOpenFailureUsesBookKeeperMetadataAndPreservesWholeRangeFallback() {
        AtomicReference<OffloadReadSourceState> state = new AtomicReference<>(none());
        FakeOffloader offloader = new FakeOffloader();
        TaggedFailure objectFailure = new TaggedFailure(ReadFailureKind.UNAVAILABLE);
        FakeReadHandle bookKeeper = new FakeReadHandle();
        ReadHandle handle = DualSourceReadHandle.open(
                        LEDGER_ID,
                        offloader,
                        state::get,
                        OffloadedReadPriority.TIERED_STORAGE_FIRST,
                        new LedgerReadSourcePinRegistry(),
                        () -> CompletableFuture.failedFuture(objectFailure),
                        () -> CompletableFuture.completedFuture(bookKeeper))
                .join();

        LedgerEntries entries = handle.readAsync(0, 2).join();

        assertEntryIds(entries, 0, 1, 2);
        assertEquals(bookKeeper.reads, 1);
        entries.close();
    }

    @Test
    public void missingBookKeeperAtOpenUsesObjectMetadata() {
        AtomicReference<OffloadReadSourceState> state = new AtomicReference<>(none());
        FakeOffloader offloader = new FakeOffloader();
        FakeReadHandle object = new FakeReadHandle();
        Throwable missing = BKException.create(BKException.Code.NoSuchLedgerExistsException);
        ReadHandle handle = DualSourceReadHandle.open(
                        LEDGER_ID,
                        offloader,
                        state::get,
                        OffloadedReadPriority.BOOKKEEPER_FIRST,
                        new LedgerReadSourcePinRegistry(),
                        () -> CompletableFuture.completedFuture(object),
                        () -> CompletableFuture.failedFuture(missing))
                .join();

        LedgerEntries entries = handle.readAsync(0, 2).join();

        assertEntryIds(entries, 0, 1, 2);
        assertEquals(object.reads, 1);
        entries.close();
    }

    @Test
    public void objectTimeoutReleasesPrimaryAndRetriesWholeRangeFromBookKeeper() {
        Harness harness = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        harness.object.failure = new TaggedFailure(ReadFailureKind.TIMEOUT);

        LedgerEntries entries = harness.handle.readAsync(0, 3).join();

        assertEntryIds(entries, 0, 1, 2, 3);
        assertEquals(harness.object.reads, 1);
        assertEquals(harness.bookKeeper.reads, 1);
        entries.close();
    }

    @Test
    public void bookKeeperFirstFallsBackOnlyForNoSuchLedger() {
        Harness missing = harness(OffloadedReadPriority.BOOKKEEPER_FIRST, none());
        missing.bookKeeper.failure = BKException.create(BKException.Code.NoSuchLedgerExistsException);
        LedgerEntries entries = missing.handle.readAsync(0, 1).join();
        assertEquals(missing.object.reads, 1);
        entries.close();

        Harness transientFailure = harness(OffloadedReadPriority.BOOKKEEPER_FIRST, none());
        transientFailure.bookKeeper.failure = BKException.create(BKException.Code.ReadException);
        assertSame(failure(transientFailure.handle.readAsync(0, 1)), transientFailure.bookKeeper.failure);
        assertEquals(transientFailure.object.reads, 0);
    }

    @Test
    public void bothSourcesFailWithPrimaryAndSuppressedSecondary() {
        Harness harness = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        harness.object.failure = new TaggedFailure(ReadFailureKind.UNAVAILABLE);
        harness.bookKeeper.failure = BKException.create(BKException.Code.ReadException);

        Throwable failure = failure(harness.handle.readAsync(0, 1));

        assertSame(failure, harness.object.failure);
        assertEquals(failure.getSuppressed().length, 1);
    }

    @Test
    public void deleteIntentUsesObjectEvenWhenBookKeeperStillOpens() {
        Harness harness = harness(OffloadedReadPriority.BOOKKEEPER_FIRST, intent());
        LedgerEntries entries = harness.handle.readAsync(0, 1).join();

        assertEquals(harness.object.reads, 1);
        assertEquals(harness.bookKeeper.reads, 0);
        entries.close();
    }

    @Test
    public void bookKeeperFenceDrainsAcceptedRangeAndReroutesNewRead() {
        Harness harness = harness(OffloadedReadPriority.BOOKKEEPER_FIRST, none());
        LedgerEntries accepted = harness.handle.readAsync(0, 1).join();

        CompletableFuture<Void> drain = harness.pins.fence(Source.BOOKKEEPER);
        assertFalse(drain.isDone());
        LedgerEntries rerouted = harness.handle.readAsync(2, 3).join();
        assertEquals(harness.object.reads, 1);

        accepted.close();
        assertTrue(drain.isDone());
        rerouted.close();
    }

    @Test
    public void closeWaitsForRangePinAndClosesInitializedChildExactlyOnce() {
        Harness harness = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        LedgerEntries accepted = harness.handle.readAsync(0, 1).join();

        CompletableFuture<Void> firstClose = harness.handle.closeAsync();
        assertSame(harness.handle.closeAsync(), firstClose);
        assertFalse(firstClose.isDone());
        accepted.close();
        firstClose.join();

        assertEquals(harness.object.closes, 1);
        assertEquals(harness.bookKeeper.closes, 0);
    }

    @Test
    public void invalidObjectRangeIsClosedQuarantinedAndRetriedWithoutMixing() {
        Harness harness = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        harness.object.entryIdDelta = 1;

        LedgerEntries entries = harness.handle.readAsync(0, 2).join();

        assertEntryIds(entries, 0, 1, 2);
        assertEquals(harness.object.closedRanges, 1);
        assertEquals(harness.offloader.integrityFailures, 1);
        entries.close();
    }

    @Test
    public void cancelledObjectReadDoesNotFallback() {
        Harness harness = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        harness.object.failure = new TaggedFailure(ReadFailureKind.CANCELLED);

        assertSame(failure(harness.handle.readAsync(0, 1)), harness.object.failure);
        assertEquals(harness.bookKeeper.reads, 0);
    }

    @Test
    public void metadataAttemptChangeAfterPrimaryFailureForbidsFallback() {
        Harness harness = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        harness.object.failure = new TaggedFailure(ReadFailureKind.TIMEOUT);
        harness.object.onRead = () -> harness.state.set(new OffloadReadSourceState(
                8,
                UUID.fromString("223e4567-e89b-12d3-a456-426614174000"),
                true,
                BookKeeperDeleteState.NONE));

        assertSame(failure(harness.handle.readAsync(0, 1)), harness.object.failure);
        assertEquals(harness.bookKeeper.reads, 0);
    }

    private static Harness harness(OffloadedReadPriority priority, OffloadReadSourceState initial) {
        AtomicReference<OffloadReadSourceState> state = new AtomicReference<>(initial);
        FakeOffloader offloader = new FakeOffloader();
        FakeReadHandle object = new FakeReadHandle();
        FakeReadHandle bookKeeper = new FakeReadHandle();
        LedgerReadSourcePinRegistry pins = new LedgerReadSourcePinRegistry();
        ReadHandle handle = DualSourceReadHandle.open(
                        LEDGER_ID,
                        offloader,
                        state::get,
                        priority,
                        pins,
                        () -> CompletableFuture.completedFuture(object),
                        () -> CompletableFuture.completedFuture(bookKeeper))
                .join();
        return new Harness(state, (DualSourceReadHandle) handle, pins, offloader, object, bookKeeper);
    }

    private static OffloadReadSourceState none() {
        return new OffloadReadSourceState(7, ATTEMPT, true, BookKeeperDeleteState.NONE);
    }

    private static OffloadReadSourceState intent() {
        return new OffloadReadSourceState(8, ATTEMPT, true, BookKeeperDeleteState.INTENT);
    }

    private static void assertEntryIds(LedgerEntries entries, long... expected) {
        List<Long> actual = new ArrayList<>();
        entries.forEach(entry -> actual.add(entry.getEntryId()));
        List<Long> expectedList = new ArrayList<>();
        for (long value : expected) {
            expectedList.add(value);
        }
        assertEquals(actual, expectedList);
    }

    private static Throwable failure(CompletableFuture<?> future) {
        try {
            future.join();
            fail("expected read failure");
            return null;
        } catch (CompletionException failure) {
            return failure.getCause();
        }
    }

    private record Harness(
            AtomicReference<OffloadReadSourceState> state,
            DualSourceReadHandle handle,
            LedgerReadSourcePinRegistry pins,
            FakeOffloader offloader,
            FakeReadHandle object,
            FakeReadHandle bookKeeper) {
    }

    private static final class FakeReadHandle implements ReadHandle {
        private final LedgerMetadata metadata = mock(LedgerMetadata.class);
        private Throwable failure;
        private long entryIdDelta;
        private Runnable onRead = () -> { };
        private int reads;
        private int closes;
        private int closedRanges;

        @Override
        public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
            reads++;
            onRead.run();
            if (failure != null) {
                return CompletableFuture.failedFuture(failure);
            }
            List<LedgerEntry> entries = new ArrayList<>();
            for (long entryId = firstEntry; entryId <= lastEntry; entryId++) {
                long returnedId = entryId + entryIdDelta;
                entries.add(LedgerEntryImpl.create(
                        LEDGER_ID, returnedId, 1, Unpooled.wrappedBuffer(new byte[] {(byte) returnedId})));
            }
            LedgerEntries delegate = LedgerEntriesImpl.create(entries);
            return CompletableFuture.completedFuture(new TrackingEntries(delegate, () -> closedRanges++));
        }

        @Override
        public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
            return readAsync(firstEntry, lastEntry);
        }

        @Override
        public CompletableFuture<Long> readLastAddConfirmedAsync() {
            return CompletableFuture.completedFuture(9L);
        }

        @Override
        public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
            return readLastAddConfirmedAsync();
        }

        @Override
        public long getLastAddConfirmed() {
            return 9;
        }

        @Override
        public long getLength() {
            return 10;
        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(
                long entryId, long timeOutInMillis, boolean parallel) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public long getId() {
            return LEDGER_ID;
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            closes++;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public LedgerMetadata getLedgerMetadata() {
            return metadata;
        }
    }

    private static final class TrackingEntries implements LedgerEntries {
        private final LedgerEntries delegate;
        private final Runnable onClose;

        private TrackingEntries(LedgerEntries delegate, Runnable onClose) {
            this.delegate = delegate;
            this.onClose = onClose;
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
            delegate.close();
            onClose.run();
        }
    }

    private static final class TaggedFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final ReadFailureKind kind;

        private TaggedFailure(ReadFailureKind kind) {
            super(kind.name());
            this.kind = kind;
        }
    }

    private static final class FakeOffloader implements SourceSafeLedgerOffloader {
        private int integrityFailures;

        @Override
        public RetentionClass getBookKeeperRetentionClass() {
            return RetentionClass.DELETE_AFTER_VERIFIED;
        }

        @Override
        public ReadFailureKind classifyOffloadedReadFailure(Throwable failure) {
            return failure instanceof TaggedFailure tagged ? tagged.kind : ReadFailureKind.OTHER;
        }

        @Override
        public void recordOffloadedReadIntegrityFailure(long ledgerId, UUID attemptUuid, Throwable failure) {
            integrityFailures++;
        }

        @Override
        public CompletableFuture<Void> revalidateOffloadedForSourceDeletion(
                long ledgerId,
                UUID attemptUuid,
                Map<String, String> offloadDriverMetadata,
                long lastAddConfirmed,
                long entryCount,
                long logicalLength) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public String getOffloadDriverName() {
            return "test";
        }

        @Override
        public CompletableFuture<Void> offload(
                ReadHandle ledger, UUID uid, Map<String, String> extraMetadata) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<ReadHandle> readOffloaded(
                long ledgerId, UUID uid, Map<String, String> offloadDriverMetadata) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Void> deleteOffloaded(
                long ledgerId, UUID uid, Map<String, String> offloadDriverMetadata) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public OffloadPolicies getOffloadPolicies() {
            return null;
        }

        @Override
        public void close() {
        }
    }
}
