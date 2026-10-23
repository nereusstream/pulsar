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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
    public void objectOnlyOverridesBookKeeperPriorityAndNeverOpensFallback() {
        AtomicReference<OffloadReadSourceState> state = new AtomicReference<>(none());
        FakeReadHandle object = new FakeReadHandle();
        AtomicInteger bkOpens = new AtomicInteger();
        var pins = new LedgerReadSourcePinRegistry();
        ReadHandle handle = DualSourceReadHandle.open(LEDGER_ID, new FakeOffloader(), state::get,
                OffloadedReadPriority.BOOKKEEPER_FIRST, pins, () -> CompletableFuture.completedFuture(object),
                () -> {
                    bkOpens.incrementAndGet();
                    return CompletableFuture.completedFuture(new FakeReadHandle());
                }, true).join();
        try (LedgerEntries entries = handle.readAsync(0, 1).join()) {
            assertEntryIds(entries, 0, 1);
        }
        TaggedFailure failedObject = new TaggedFailure(ReadFailureKind.TIMEOUT);
        object.failure = failedObject;
        assertSame(failure(handle.readAsync(0, 1)), failedObject);
        assertEquals(bkOpens.get(), 0);
        assertEquals(pins.pinCount(), 0);
        handle.closeAsync().join();
        assertEquals(object.closes, 1);
    }

    @Test
    public void objectOnlyOpenFailureCannotOpenBookKeeper() {
        TaggedFailure failedObject = new TaggedFailure(ReadFailureKind.UNAVAILABLE);
        AtomicInteger bkOpens = new AtomicInteger();
        var opened = DualSourceReadHandle.open(LEDGER_ID, new FakeOffloader(),
                DualSourceReadHandleTest::none, OffloadedReadPriority.BOOKKEEPER_FIRST,
                new LedgerReadSourcePinRegistry(), () -> CompletableFuture.failedFuture(failedObject),
                () -> {
                    bkOpens.incrementAndGet();
                    return CompletableFuture.completedFuture(new FakeReadHandle());
                }, true);
        assertSame(failure(opened), failedObject);
        assertEquals(bkOpens.get(), 0);
    }

    @Test
    public void unsupportedIdentityCaptureReleasesPinAndRetainsOrdinaryReads() {
        Harness harness = harness(OffloadedReadPriority.BOOKKEEPER_FIRST, none());

        assertTrue(failure(harness.handle.captureOffloadedReadSourceIdentity(none(), () -> true))
                instanceof UnsupportedOperationException);
        assertEquals(harness.object.reads, 0);
        assertEquals(harness.bookKeeper.reads, 0);
        assertEquals(harness.pins.pinCount(), 0);
        try (LedgerEntries entries = harness.handle.readAsync(0, 0).join()) {
            assertEntryIds(entries, 0);
        }
        harness.handle.closeAsync().join();
        assertEquals(harness.object.closes, 1);
        assertEquals(harness.bookKeeper.closes, 1);
    }

    @Test
    public void identityCaptureGateFailureAfterPinReleasesAdmission() {
        Harness harness = harness(OffloadedReadPriority.BOOKKEEPER_FIRST, none());
        AtomicInteger checks = new AtomicInteger();
        IllegalStateException failedGate = new IllegalStateException("identity gate failed after pin admission");

        assertSame(failure(harness.handle.captureOffloadedReadSourceIdentity(none(), () -> {
            if (checks.incrementAndGet() == 2) {
                throw failedGate;
            }
            return true;
        })), failedGate);
        assertEquals(harness.pins.pinCount(), 0);
        assertEquals(harness.object.reads, 0);
        assertEquals(harness.bookKeeper.reads, 0);
        harness.handle.closeAsync().join();
        assertEquals(harness.object.closes, 0);
    }

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
    public void failedDeletionFenceUnfencesOnlyAfterLateBookKeeperPinDrains() {
        LedgerReadSourcePinRegistry pins = new LedgerReadSourcePinRegistry();
        LedgerReadSourcePinRegistry.Pin accepted = pins.acquire(Source.BOOKKEEPER, none());

        CompletableFuture<Void> unfenced = pins.unfenceBookKeeperAfterDrain();

        assertFalse(unfenced.isDone());
        accepted.close();
        unfenced.join();
        LedgerReadSourcePinRegistry.Pin admittedAgain = pins.acquire(Source.BOOKKEEPER, none());
        admittedAgain.close();
    }

    @Test
    public void closeWaitsForRangePinAndClosesInitializedChildExactlyOnce() {
        Harness harness = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        LedgerEntries accepted = harness.handle.readAsync(0, 1).join();

        CompletableFuture<Void> firstClose = harness.handle.closeAsync();
        CompletableFuture<Void> repeatedClose = harness.handle.closeAsync();
        assertTrue(firstClose != repeatedClose);
        assertFalse(firstClose.isDone());
        assertFalse(repeatedClose.isDone());
        assertTrue(harness.handle.closeAsync().cancel(false));
        assertTrue(harness.handle.closeAsync().complete(null));
        CompletableFuture<Void> afterChangedObservers = harness.handle.closeAsync();
        assertFalse(afterChangedObservers.isDone());
        accepted.close();
        firstClose.join();
        repeatedClose.join();
        afterChangedObservers.join();

        assertEquals(harness.object.closes, 1);
        assertEquals(harness.bookKeeper.closes, 0);
    }

    @Test
    public void cancelledObserverReleasesUndeliverablePrimaryRange() {
        verifyUndeliverableRange(false, true);
    }

    @Test
    public void completedObserverReleasesUndeliverablePrimaryRange() {
        verifyUndeliverableRange(false, false);
    }

    @Test
    public void cancelledObserverReleasesUndeliverableFallbackRange() {
        verifyUndeliverableRange(true, true);
    }

    private static void verifyUndeliverableRange(boolean fallback, boolean cancelObserver) {
        Harness harness = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        FakeReadHandle held = fallback ? harness.bookKeeper : harness.object;
        held.readGate = new CompletableFuture<>();
        if (fallback) {
            harness.object.failure = new TaggedFailure(ReadFailureKind.UNAVAILABLE);
        }
        CompletableFuture<LedgerEntries> read = harness.handle.readAsync(0, 1);
        try {
            assertFalse(read.isDone());
            assertEquals(harness.pins.pinCount(), 1);
            if (cancelObserver) {
                assertTrue(read.cancel(false));
            } else {
                assertTrue(read.completeExceptionally(new IllegalStateException("observer completed externally")));
            }
            CompletableFuture<Void> close = harness.handle.closeAsync();
            assertFalse(close.isDone());
            assertEquals(harness.pins.pinCount(), 1);
            held.readGate.complete(null);
            assertEquals(held.closedRanges, 1);
            assertEquals(harness.pins.pinCount(), 0);
            assertTrue(close.isDone());
            close.join();
            assertEquals(held.closes, 1);
            assertEquals(harness.object.reads, 1);
            assertEquals(harness.bookKeeper.reads, fallback ? 1 : 0);
        } finally {
            held.readGate.complete(null);
            if (held.lastRange != null && held.closedRanges == 0) {
                held.lastRange.close();
            }
        }
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

    @Test
    public void capturedReadRejectsChangedVersionBeforeSourceUse() {
        Harness harness = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        harness.state.set(new OffloadReadSourceState(8, ATTEMPT, true, BookKeeperDeleteState.NONE));

        assertTrue(failure(harness.handle.readCapturedAsync(0, 1, none()))
                instanceof LedgerReadSourcePinRegistry.SourcePinFencedException);
        assertEquals(harness.object.reads, 0);
        assertEquals(harness.bookKeeper.reads, 0);
        harness.handle.closeAsync().join();
    }

    @Test
    public void capturedReadDoesNotRecaptureVersionAfterPrimaryFailure() {
        Harness captured = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        captured.object.failure = new TaggedFailure(ReadFailureKind.TIMEOUT);
        captured.object.onRead = () -> captured.state.set(
                new OffloadReadSourceState(8, ATTEMPT, true, BookKeeperDeleteState.NONE));

        Throwable primary = failure(captured.handle.readCapturedAsync(0, 1, none()));

        assertSame(primary, captured.object.failure);
        assertEquals(primary.getSuppressed().length, 1);
        assertTrue(primary.getSuppressed()[0] instanceof LedgerReadSourcePinRegistry.SourcePinFencedException);
        assertEquals(captured.object.reads, 1);
        assertEquals(captured.bookKeeper.reads, 0);
        captured.handle.closeAsync().join();

        Harness ordinary = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        ordinary.object.failure = new TaggedFailure(ReadFailureKind.TIMEOUT);
        ordinary.object.onRead = () -> ordinary.state.set(
                new OffloadReadSourceState(8, ATTEMPT, true, BookKeeperDeleteState.NONE));
        try (LedgerEntries entries = ordinary.handle.readAsync(0, 1).join()) {
            assertEntryIds(entries, 0, 1);
            assertEquals(ordinary.bookKeeper.reads, 1);
        }
        ordinary.handle.closeAsync().join();
    }

    @Test
    public void capturedReadKeepsWholeRangeFallbackWhenStateIsUnchanged() {
        Harness harness = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        harness.object.failure = new TaggedFailure(ReadFailureKind.TIMEOUT);

        try (LedgerEntries entries = harness.handle.readCapturedAsync(0, 3, none()).join()) {
            assertEntryIds(entries, 0, 1, 2, 3);
            assertEquals(harness.object.reads, 1);
            assertEquals(harness.bookKeeper.reads, 1);
        }
        harness.handle.closeAsync().join();
    }

    @Test
    public void capturedReadDoesNotRetargetFencedBookKeeperSource() {
        Harness harness = harness(OffloadedReadPriority.BOOKKEEPER_FIRST, none());
        harness.pins.fence(Source.BOOKKEEPER).join();

        assertTrue(failure(harness.handle.readCapturedAsync(0, 1, none()))
                instanceof LedgerReadSourcePinRegistry.SourcePinFencedException);
        assertEquals(harness.object.reads, 0);
        assertEquals(harness.bookKeeper.reads, 0);
        harness.handle.closeAsync().join();
    }

    @Test
    public void cancelledCapturedReadDrainsPrimaryWithoutStartingFallback() {
        Harness harness = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        harness.object.readGate = new CompletableFuture<>();
        CompletableFuture<LedgerEntries> reading = harness.handle.readCapturedAsync(0, 1, none());
        assertTrue(reading.cancel(false));

        harness.object.readGate.completeExceptionally(new TaggedFailure(ReadFailureKind.TIMEOUT));

        assertEquals(harness.object.reads, 1);
        assertEquals(harness.bookKeeper.reads, 0);
        assertTrue(reading.isCancelled());
        harness.handle.closeAsync().join();
        assertEquals(harness.object.closes, 1);
        assertEquals(harness.bookKeeper.closes, 0);
    }

    @Test
    public void capturedSourceGateRejectsBeforeProviderUse() {
        Harness harness = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());

        assertTrue(failure(harness.handle.readCapturedAsync(0, 1, none(), () -> false))
                instanceof LedgerReadSourcePinRegistry.SourcePinFencedException);
        assertEquals(harness.object.reads, 0);
        assertEquals(harness.bookKeeper.reads, 0);
        harness.handle.closeAsync().join();
    }

    @Test
    public void closedCapturedSourceGateWaitsForPrimaryAndRejectsNewFallback() {
        Harness harness = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        AtomicBoolean sourceUseOpen = new AtomicBoolean(true);
        harness.object.readGate = new CompletableFuture<>();
        CompletableFuture<LedgerEntries> reading =
                harness.handle.readCapturedAsync(0, 1, none(), sourceUseOpen::get);
        sourceUseOpen.set(false);
        CompletableFuture<Void> drain = harness.pins.fence(Source.OBJECT);
        assertFalse(reading.isDone());
        assertFalse(drain.isDone());
        TaggedFailure primary = new TaggedFailure(ReadFailureKind.TIMEOUT);

        harness.object.readGate.completeExceptionally(primary);

        assertSame(failure(reading), primary);
        assertEquals(harness.bookKeeper.reads, 0);
        drain.join();
        harness.handle.closeAsync().join();
    }

    @Test
    public void capturedGateFailureAfterPinReleasesAdmission() {
        Harness harness = harness(OffloadedReadPriority.TIERED_STORAGE_FIRST, none());
        AtomicInteger checks = new AtomicInteger();
        IllegalStateException failedGate = new IllegalStateException("source gate failed after pin admission");

        Throwable failure = failure(harness.handle.readCapturedAsync(0, 1, none(), () -> {
            if (checks.incrementAndGet() == 3) {
                throw failedGate;
            }
            return true;
        }));

        assertSame(failure, failedGate);
        assertEquals(harness.object.reads, 0);
        assertEquals(harness.bookKeeper.reads, 0);
        harness.handle.closeAsync().join();
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
        private CompletableFuture<Void> readGate = CompletableFuture.completedFuture(null);
        private LedgerEntries lastRange;
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
            return readGate.thenApply(ignored -> {
                List<LedgerEntry> entries = new ArrayList<>();
                for (long entryId = firstEntry; entryId <= lastEntry; entryId++) {
                    long returnedId = entryId + entryIdDelta;
                    entries.add(LedgerEntryImpl.create(
                            LEDGER_ID, returnedId, 1, Unpooled.wrappedBuffer(new byte[] {(byte) returnedId})));
                }
                LedgerEntries delegate = LedgerEntriesImpl.create(entries);
                lastRange = new TrackingEntries(delegate, () -> closedRanges++);
                return lastRange;
            });
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
