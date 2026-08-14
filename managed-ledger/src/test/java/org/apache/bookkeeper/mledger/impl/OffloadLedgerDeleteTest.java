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

import static org.apache.bookkeeper.mledger.impl.OffloadPrefixTest.assertEventuallyTrue;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.OffloadReadSourceState;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader;
import org.apache.bookkeeper.mledger.proto.BookKeeperDeleteState;
import org.apache.bookkeeper.mledger.proto.BookKeeperRetentionClass;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.OffloadContext;
import org.apache.bookkeeper.mledger.util.MockClock;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

@CustomLog
public class OffloadLedgerDeleteTest extends MockedBookKeeperTestCase {


    static class MockFileSystemLedgerOffloader implements LedgerOffloader {
        interface InjectAfterOffload {
            void call();
        }

        private String storageBasePath = "/Users/pulsar_filesystem_offloader";

        private static String getStoragePath(String storageBasePath, String managedLedgerName) {
            return storageBasePath == null ? managedLedgerName + "/" : storageBasePath + "/" + managedLedgerName + "/";
        }

        private static String getDataFilePath(String storagePath, long ledgerId, UUID uuid) {
            return storagePath + ledgerId + "-" + uuid.toString();
        }

        ConcurrentHashMap<Long, String> offloads = new ConcurrentHashMap<Long, String>();
        ConcurrentHashMap<Long, String> deletes = new ConcurrentHashMap<Long, String>();
        OffloadPrefixTest.MockLedgerOffloader.InjectAfterOffload inject = null;

        Set<Long> offloadedLedgers() {
            return offloads.keySet();
        }

        Set<Long> deletedOffloads() {
            return deletes.keySet();
        }

        OffloadPoliciesImpl offloadPolicies = OffloadPoliciesImpl.create("filesystem", "", "", "",
                null, null,
                null, null,
                OffloadPoliciesImpl.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_READ_BUFFER_SIZE_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_OFFLOAD_THRESHOLD_IN_SECONDS,
                OffloadPoliciesImpl.DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS,
                OffloadPoliciesImpl.DEFAULT_OFFLOADED_READ_PRIORITY);

        @Override
        public String getOffloadDriverName() {
            return "mockfilesystem";
        }

        @Override
        public CompletableFuture<Void> offload(ReadHandle ledger,
                                               UUID uuid,
                                               Map<String, String> extraMetadata) {
            Assert.assertNotNull(extraMetadata.get("ManagedLedgerName"));
            String storagePath = getStoragePath(storageBasePath, extraMetadata.get("ManagedLedgerName"));
            String dataFilePath = getDataFilePath(storagePath, ledger.getId(), uuid);
            CompletableFuture<Void> promise = new CompletableFuture<>();
            if (offloads.putIfAbsent(ledger.getId(), dataFilePath) == null) {
                promise.complete(null);
            } else {
                promise.completeExceptionally(new Exception("Already exists exception"));
            }

            if (inject != null) {
                inject.call();
            }
            return promise;
        }

        @Override
        public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uuid,
                                                           Map<String, String> offloadDriverMetadata) {
            CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
            promise.completeExceptionally(new UnsupportedOperationException());
            return promise;
        }

        @Override
        public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uuid,
                                                       Map<String, String> offloadDriverMetadata) {
            Assert.assertNotNull(offloadDriverMetadata.get("ManagedLedgerName"));
            String storagePath = getStoragePath(storageBasePath, offloadDriverMetadata.get("ManagedLedgerName"));
            String dataFilePath = getDataFilePath(storagePath, ledgerId, uuid);
            CompletableFuture<Void> promise = new CompletableFuture<>();
            if (offloads.remove(ledgerId, dataFilePath)) {
                deletes.put(ledgerId, dataFilePath);
                promise.complete(null);
            } else {
                promise.completeExceptionally(new Exception("Not found"));
            }
            return promise;
        };

        @Override
        public OffloadPoliciesImpl getOffloadPolicies() {
            return offloadPolicies;
        }

        @Override
        public void close() {
        }
    }

    static class MockSourceSafeLedgerOffloader extends OffloadPrefixTest.MockLedgerOffloader
            implements SourceSafeLedgerOffloader {
        private final RetentionClass retentionClass;
        private final AtomicInteger revalidations = new AtomicInteger();
        private volatile Throwable revalidationFailure;

        MockSourceSafeLedgerOffloader(RetentionClass retentionClass) {
            this.retentionClass = retentionClass;
        }

        @Override
        public RetentionClass getBookKeeperRetentionClass() {
            return retentionClass;
        }

        @Override
        public ReadFailureKind classifyOffloadedReadFailure(Throwable failure) {
            return ReadFailureKind.OTHER;
        }

        @Override
        public CompletableFuture<Void> revalidateOffloadedForSourceDeletion(
                long ledgerId,
                UUID attemptUuid,
                Map<String, String> offloadDriverMetadata,
                long lastAddConfirmed,
                long entryCount,
                long logicalLength) {
            revalidations.incrementAndGet();
            Assert.assertEquals(offloads.get(ledgerId), attemptUuid);
            Assert.assertNotNull(offloadDriverMetadata.get("ManagedLedgerName"));
            Assert.assertEquals(lastAddConfirmed, entryCount - 1);
            Assert.assertTrue(entryCount > 0);
            Assert.assertTrue(logicalLength > 0);
            return revalidationFailure == null
                    ? CompletableFuture.completedFuture(null)
                    : CompletableFuture.failedFuture(revalidationFailure);
        }
    }

    @Test
    public void testSourceSafeDeletePersistsIntentDeletesAndProvesDone() throws Exception {
        MockSourceSafeLedgerOffloader offloader =
                new MockSourceSafeLedgerOffloader(SourceSafeLedgerOffloader.RetentionClass.DELETE_AFTER_VERIFIED);
        ManagedLedgerConfig config = sourceSafeConfig(offloader);
        MockClock clock = (MockClock) config.getClock();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("source-safe-delete", config);
        long firstLedgerId = writeAndOffloadFirstLedger(ledger);

        OffloadContext completed = ledger.getLedgerInfo(firstLedgerId).join().getOffloadContext();
        Assert.assertEquals(
                completed.getBookkeeperRetentionClass(), BookKeeperRetentionClass.DELETE_AFTER_VERIFIED);
        Assert.assertEquals(completed.getBookkeeperDeleteState(), BookKeeperDeleteState.BK_DELETE_NONE);
        Assert.assertFalse(completed.isBookkeeperDeleted());

        clock.advance(1, TimeUnit.SECONDS);
        CompletableFuture<Void> trim = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(trim);
        trim.join();

        OffloadContext deleted = ledger.getLedgerInfo(firstLedgerId).join().getOffloadContext();
        Assert.assertEquals(deleted.getBookkeeperDeleteState(), BookKeeperDeleteState.BK_DELETE_DONE);
        Assert.assertTrue(deleted.isBookkeeperDeleted());
        Assert.assertEquals(offloader.revalidations.get(), 1);
        Assert.assertFalse(bkc.getLedgers().contains(firstLedgerId));
    }

    @Test
    public void testSourceSafeRetainClassNeverDeletesBookKeeper() throws Exception {
        MockSourceSafeLedgerOffloader offloader =
                new MockSourceSafeLedgerOffloader(SourceSafeLedgerOffloader.RetentionClass.RETAIN_BK);
        ManagedLedgerConfig config = sourceSafeConfig(offloader);
        MockClock clock = (MockClock) config.getClock();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("source-safe-retain", config);
        long firstLedgerId = writeAndOffloadFirstLedger(ledger);

        clock.advance(1, TimeUnit.SECONDS);
        CompletableFuture<Void> trim = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(trim);
        trim.join();

        OffloadContext retained = ledger.getLedgerInfo(firstLedgerId).join().getOffloadContext();
        Assert.assertEquals(retained.getBookkeeperRetentionClass(), BookKeeperRetentionClass.RETAIN_BK);
        Assert.assertEquals(retained.getBookkeeperDeleteState(), BookKeeperDeleteState.BK_DELETE_NONE);
        Assert.assertEquals(offloader.revalidations.get(), 0);
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));
    }

    @Test
    public void testSourceSafeRevalidationFailureLeavesNoneAndUnfencesBookKeeper() throws Exception {
        MockSourceSafeLedgerOffloader offloader =
                new MockSourceSafeLedgerOffloader(SourceSafeLedgerOffloader.RetentionClass.DELETE_AFTER_VERIFIED);
        offloader.revalidationFailure = new IllegalStateException("revalidation failed");
        ManagedLedgerConfig config = sourceSafeConfig(offloader);
        MockClock clock = (MockClock) config.getClock();
        ManagedLedgerImpl ledger =
                (ManagedLedgerImpl) factory.open("source-safe-revalidation-failure", config);
        long firstLedgerId = writeAndOffloadFirstLedger(ledger);
        OffloadContext completed = ledger.getLedgerInfo(firstLedgerId).join().getOffloadContext();
        UUID attemptUuid = new UUID(completed.getUidMsb(), completed.getUidLsb());

        clock.advance(1, TimeUnit.SECONDS);
        CompletableFuture<Void> trim = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(trim);
        Assert.assertThrows(java.util.concurrent.CompletionException.class, trim::join);

        OffloadContext unchanged = ledger.getLedgerInfo(firstLedgerId).join().getOffloadContext();
        Assert.assertEquals(unchanged.getBookkeeperDeleteState(), BookKeeperDeleteState.BK_DELETE_NONE);
        Assert.assertFalse(unchanged.isBookkeeperDeleted());
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));
        LedgerReadSourcePinRegistry registry = ledger.offloadReadSourcePins.get(firstLedgerId);
        LedgerReadSourcePinRegistry.Pin pin = registry.acquire(
                LedgerReadSourcePinRegistry.Source.BOOKKEEPER,
                new OffloadReadSourceState(
                        0, attemptUuid, true, OffloadReadSourceState.BookKeeperDeleteState.NONE));
        pin.close();
    }

    @Test
    public void testSourceSafeDrainTimeoutDoesNotDeleteAndUnfencesAfterLateRelease() throws Exception {
        MockSourceSafeLedgerOffloader offloader =
                new MockSourceSafeLedgerOffloader(SourceSafeLedgerOffloader.RetentionClass.DELETE_AFTER_VERIFIED);
        ManagedLedgerConfig config = sourceSafeConfig(offloader).setMetadataOperationsTimeoutSeconds(1);
        MockClock clock = (MockClock) config.getClock();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("source-safe-drain-timeout", config);
        long firstLedgerId = writeAndOffloadFirstLedger(ledger);
        OffloadContext completed = ledger.getLedgerInfo(firstLedgerId).join().getOffloadContext();
        UUID attemptUuid = new UUID(completed.getUidMsb(), completed.getUidLsb());
        LedgerReadSourcePinRegistry registry = ledger.offloadReadSourcePins.computeIfAbsent(
                firstLedgerId, ignored -> new LedgerReadSourcePinRegistry());
        LedgerReadSourcePinRegistry.Pin accepted = registry.acquire(
                LedgerReadSourcePinRegistry.Source.BOOKKEEPER,
                new OffloadReadSourceState(
                        0, attemptUuid, true, OffloadReadSourceState.BookKeeperDeleteState.NONE));

        clock.advance(1, TimeUnit.SECONDS);
        CompletableFuture<Void> trim = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(trim);
        Assert.assertThrows(java.util.concurrent.CompletionException.class, trim::join);

        OffloadContext unchanged = ledger.getLedgerInfo(firstLedgerId).join().getOffloadContext();
        Assert.assertEquals(unchanged.getBookkeeperDeleteState(), BookKeeperDeleteState.BK_DELETE_NONE);
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));
        Assert.assertEquals(offloader.revalidations.get(), 0);
        accepted.close();
        LedgerReadSourcePinRegistry.Pin admittedAgain = registry.acquire(
                LedgerReadSourcePinRegistry.Source.BOOKKEEPER,
                new OffloadReadSourceState(
                        0, attemptUuid, true, OffloadReadSourceState.BookKeeperDeleteState.NONE));
        admittedAgain.close();
    }

    @Test
    public void testSourceSafeIntentResumesWithoutRepeatingRevalidation() throws Exception {
        MockSourceSafeLedgerOffloader offloader =
                new MockSourceSafeLedgerOffloader(SourceSafeLedgerOffloader.RetentionClass.DELETE_AFTER_VERIFIED);
        ManagedLedgerConfig config = sourceSafeConfig(offloader);
        MockClock clock = (MockClock) config.getClock();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("source-safe-intent-resume", config);
        long firstLedgerId = writeAndOffloadFirstLedger(ledger);
        LedgerInfo completed = ledger.getLedgerInfo(firstLedgerId).join();
        LedgerInfo intent = new LedgerInfo();
        intent.copyFrom(completed);
        intent.setOffloadContext()
                .setBookkeeperDeleted(true)
                .setBookkeeperDeleteState(BookKeeperDeleteState.BK_DELETE_INTENT);
        ledger.ledgers.put(firstLedgerId, intent);

        clock.advance(1, TimeUnit.SECONDS);
        CompletableFuture<Void> trim = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(trim);
        trim.join();

        OffloadContext done = ledger.getLedgerInfo(firstLedgerId).join().getOffloadContext();
        Assert.assertEquals(done.getBookkeeperDeleteState(), BookKeeperDeleteState.BK_DELETE_DONE);
        Assert.assertTrue(done.isBookkeeperDeleted());
        Assert.assertEquals(offloader.revalidations.get(), 0);
        Assert.assertFalse(bkc.getLedgers().contains(firstLedgerId));
    }

    @Test
    public void testSourceSafeDeleteEligibilityRequiresConsistentCompatibilityFence() throws Exception {
        MockSourceSafeLedgerOffloader offloader =
                new MockSourceSafeLedgerOffloader(SourceSafeLedgerOffloader.RetentionClass.DELETE_AFTER_VERIFIED);
        ManagedLedgerImpl ledger =
                (ManagedLedgerImpl) factory.open("source-safe-delete-eligibility", sourceSafeConfig(offloader));
        long timestamp = ledger.getConfig().getClock().millis() - 1;

        OffloadContext context = new OffloadContext()
                .setTimestamp(timestamp)
                .setComplete(true)
                .setBookkeeperRetentionClass(BookKeeperRetentionClass.DELETE_AFTER_VERIFIED)
                .setBookkeeperDeleteState(BookKeeperDeleteState.BK_DELETE_NONE)
                .setBookkeeperDeleted(false);
        Assert.assertTrue(ledger.isOffloadedNeedsDelete(context, Optional.of(offloader.getOffloadPolicies())));

        context.setBookkeeperDeleteState(BookKeeperDeleteState.BK_DELETE_INTENT).setBookkeeperDeleted(true);
        Assert.assertTrue(ledger.isOffloadedNeedsDelete(context, Optional.of(offloader.getOffloadPolicies())));

        context.setBookkeeperDeleteState(BookKeeperDeleteState.BK_DELETE_DONE).setBookkeeperDeleted(true);
        Assert.assertFalse(ledger.isOffloadedNeedsDelete(context, Optional.of(offloader.getOffloadPolicies())));

        context.setBookkeeperDeleteState(BookKeeperDeleteState.BK_DELETE_NONE).setBookkeeperDeleted(true);
        Assert.assertFalse(ledger.isOffloadedNeedsDelete(context, Optional.of(offloader.getOffloadPolicies())));

        context.setBookkeeperDeleteState(BookKeeperDeleteState.BK_DELETE_INTENT).setBookkeeperDeleted(false);
        Assert.assertFalse(ledger.isOffloadedNeedsDelete(context, Optional.of(offloader.getOffloadPolicies())));
    }

    @Test
    public void testSourceSafeDeleteCompletesBeforeLogicalRetentionTrim() throws Exception {
        MockSourceSafeLedgerOffloader offloader =
                new MockSourceSafeLedgerOffloader(SourceSafeLedgerOffloader.RetentionClass.DELETE_AFTER_VERIFIED);
        ManagedLedgerConfig config = sourceSafeConfig(offloader).setRetentionTime(0, TimeUnit.MILLISECONDS);
        MockClock clock = (MockClock) config.getClock();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("source-safe-full-trim", config);
        long firstLedgerId = writeAndOffloadFirstLedger(ledger);

        clock.advance(1, TimeUnit.SECONDS);
        CompletableFuture<Void> firstTrim = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(firstTrim);
        firstTrim.join();

        OffloadContext done = ledger.getLedgerInfo(firstLedgerId).join().getOffloadContext();
        Assert.assertEquals(done.getBookkeeperDeleteState(), BookKeeperDeleteState.BK_DELETE_DONE);
        Assert.assertTrue(done.isBookkeeperDeleted());
        Assert.assertFalse(bkc.getLedgers().contains(firstLedgerId));

        CompletableFuture<Void> secondTrim = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(secondTrim);
        secondTrim.join();
        Assert.assertNull(ledger.getLedgerInfo(firstLedgerId).join());
    }

    private static ManagedLedgerConfig sourceSafeConfig(MockSourceSafeLedgerOffloader offloader) {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setClock(new MockClock());
        offloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(0L);
        offloader.getOffloadPolicies().setManagedLedgerOffloadedReadPriority(OffloadedReadPriority.BOOKKEEPER_FIRST);
        config.setLedgerOffloader(offloader);
        return config;
    }

    private static long writeAndOffloadFirstLedger(ManagedLedgerImpl ledger) throws Exception {
        for (int i = 0; i < 15; i++) {
            ledger.addEntry(("entry-" + i).getBytes());
        }
        long firstLedgerId = ledger.getLedgersInfoAsList().get(0).getLedgerId();
        ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        return firstLedgerId;
    }

    @Test
    public void testLaggedDelete() throws Exception {
        OffloadPrefixTest.MockLedgerOffloader offloader = new OffloadPrefixTest.MockLedgerOffloader();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        MockClock clock = new MockClock();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        offloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(300000L);
        config.setLedgerOffloader(offloader);
        config.setClock(clock);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);
        int i = 0;
        for (; i < 15; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        long firstLedgerId = ledger.getLedgersInfoAsList().get(0).getLedgerId();

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().isComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(2, TimeUnit.MINUTES);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise);
        promise.join();
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(5, TimeUnit.MINUTES);
        CompletableFuture<Void> promise2 = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise2);
        promise2.join();

        // assert bk ledger is deleted
        assertEventuallyTrue(() -> !bkc.getLedgers().contains(firstLedgerId));

        // ledger still exists in list
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().isComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());

        // move past retention, should be deleted from offloaded also
        clock.advance(5, TimeUnit.MINUTES);
        CompletableFuture<Void> promise3 = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise3);
        promise3.join();

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 1);
        assertEventuallyTrue(() -> offloader.deletedOffloads().contains(firstLedgerId));
    }

    @Test
    public void testGetReadLedgerHandleAfterTrimOffloadedLedgers() throws Exception {
        // Create managed ledger.
        final long offloadThresholdSeconds = 5;
        final long offloadDeletionLagInSeconds = 1;
        OffloadPrefixTest.MockLedgerOffloader offloader = new OffloadPrefixTest.MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        offloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(offloadDeletionLagInSeconds * 1000);
        offloader.getOffloadPolicies().setManagedLedgerOffloadThresholdInSeconds(offloadThresholdSeconds);
        offloader.getOffloadPolicies().setManagedLedgerOffloadedReadPriority(OffloadedReadPriority.BOOKKEEPER_FIRST);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ml =
                (ManagedLedgerImpl) factory.open("testGetReadLedgerHandleAfterTrimOffloadedLedgers", config);
        ml.openCursor("c1");

        // Write entries.
        int i = 0;
        for (; i < 35; i++) {
            String content = "entry-" + i;
            ml.addEntry(content.getBytes());
        }
        Assert.assertEquals(ml.getLedgersInfoAsList().size(), 4);
        long ledger1 = ml.getLedgersInfoAsList().get(0).getLedgerId();
        long ledger2 = ml.getLedgersInfoAsList().get(1).getLedgerId();
        long ledger3 = ml.getLedgersInfoAsList().get(2).getLedgerId();
        long ledger4 = ml.getLedgersInfoAsList().get(3).getLedgerId();

        // Offload ledgers.
        Thread.sleep(offloadThresholdSeconds * 2 * 1000);
        CompletableFuture<Position> offloadFuture = new CompletableFuture<Position>();
        ml.maybeOffloadInBackground(offloadFuture);
        offloadFuture.join();

        // Cache ledger handle.
        CountDownLatch readCountDownLatch = new CountDownLatch(4);
        AsyncCallbacks.ReadEntryCallback readCb = new AsyncCallbacks.ReadEntryCallback(){

            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                readCountDownLatch.countDown();
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                readCountDownLatch.countDown();
            }
        };
        ml.asyncReadEntry(PositionFactory.create(ledger1, 0), readCb, null);
        ml.asyncReadEntry(PositionFactory.create(ledger2, 0), readCb, null);
        ml.asyncReadEntry(PositionFactory.create(ledger3, 0), readCb, null);
        ml.asyncReadEntry(PositionFactory.create(ledger4, 0), readCb, null);
        readCountDownLatch.await();
        ReadHandle originalReadHandle4 = ml.getLedgerHandle(ledger4).join();

        // Trim offloaded BK ledger handles.
        Thread.sleep(offloadDeletionLagInSeconds * 2 * 1000);
        CompletableFuture<Position> trimLedgerFuture = new CompletableFuture<Position>();
        ml.internalTrimLedgers(false, trimLedgerFuture);
        trimLedgerFuture.join();
        LedgerInfo ledgerInfo1 = ml.getLedgerInfo(ledger1).get();
        LedgerInfo ledgerInfo2 = ml.getLedgerInfo(ledger2).get();
        LedgerInfo ledgerInfo3 = ml.getLedgerInfo(ledger3).get();
        LedgerInfo ledgerInfo4 = ml.getLedgerInfo(ledger4).get();
        Assert.assertTrue(ledgerInfo1.hasOffloadContext() && ledgerInfo1.getOffloadContext().isBookkeeperDeleted());
        Assert.assertTrue(ledgerInfo2.hasOffloadContext() && ledgerInfo2.getOffloadContext().isBookkeeperDeleted());
        Assert.assertTrue(ledgerInfo3.hasOffloadContext() && ledgerInfo3.getOffloadContext().isBookkeeperDeleted());
        Assert.assertFalse(ledgerInfo4.hasOffloadContext() || ledgerInfo4.getOffloadContext().isBookkeeperDeleted());

        Awaitility.await().untilAsserted(() -> {
            try {
                factory.getBookKeeper().get().openLedger(ledger3, ml.digestType, ml.config.getPassword());
                Assert.fail("Should fail: the ledger has been deleted");
            } catch (BKException.BKNoSuchLedgerExistsException ex) {
                // Expected.
            }
            try {
                factory.getBookKeeper().get().openLedger(ledger2, ml.digestType, ml.config.getPassword());
                Assert.fail("Should fail: the ledger has been deleted");
            } catch (BKException.BKNoSuchLedgerExistsException ex) {
                // Expected.
            }
            try {
                factory.getBookKeeper().get().openLedger(ledger1, ml.digestType, ml.config.getPassword());
                Assert.fail("Should fail: the ledger has been deleted");
            } catch (BKException.BKNoSuchLedgerExistsException ex) {
                // Expected.
            }
        });

        // Verify: "ml.getLedgerHandle" returns a correct ledger handle.
        ReadHandle currentReadHandle4 = ml.getLedgerHandle(ledger4).join();
        Assert.assertEquals(currentReadHandle4, originalReadHandle4);
        try {
            ml.getLedgerHandle(ledger3).join();
            Assert.fail("should get a failure: MockLedgerOffloader does not support read");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause().getCause().getMessage()
                    .contains("MockLedgerOffloader does not support read"));
        }
        try {
            ml.getLedgerHandle(ledger2).join();
            Assert.fail("should get a failure: MockLedgerOffloader does not support read");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause().getCause().getMessage()
                    .contains("MockLedgerOffloader does not support read"));
        }
        try {
            ml.getLedgerHandle(ledger1).join();
            Assert.fail("should get a failure: MockLedgerOffloader does not support read");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause().getCause().getMessage()
                    .contains("MockLedgerOffloader does not support read"));
        }
    }

    @Test(timeOut = 5000)
    public void testFileSystemOffloadDeletePath() throws Exception {
        MockFileSystemLedgerOffloader offloader = new MockFileSystemLedgerOffloader();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        MockClock clock = new MockClock();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(3, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        offloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(300000L);
        config.setLedgerOffloader(offloader);
        config.setClock(clock);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger_filesystem", config);
        int i = 0;
        for (; i < 15; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        long firstLedgerId = ledger.getLedgersInfoAsList().get(0).getLedgerId();

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                        .filter(e -> e.getOffloadContext().isComplete())
                        .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                offloader.offloadedLedgers());
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        // ledger still exists in list
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                        .filter(e -> e.getOffloadContext().isComplete())
                        .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                offloader.offloadedLedgers());

        // move past retention, should be deleted from offloaded also
        clock.advance(5, TimeUnit.MINUTES);
        CompletableFuture<Void> promise3 = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise3);
        promise3.join();

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 1);
        assertEventuallyTrue(() -> offloader.deletedOffloads().contains(firstLedgerId));
    }

    @Test
    public void testLaggedDeleteRetentionSetLower() throws Exception {
        OffloadPrefixTest.MockLedgerOffloader offloader = new OffloadPrefixTest.MockLedgerOffloader();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        MockClock clock = new MockClock();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(5, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        offloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(600000L);
        config.setLedgerOffloader(offloader);
        config.setClock(clock);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);
        int i = 0;
        for (; i < 15; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        long firstLedgerId = ledger.getLedgersInfoAsList().get(0).getLedgerId();

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().isComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(2, TimeUnit.MINUTES);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise);
        promise.join();
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(5, TimeUnit.MINUTES);
        CompletableFuture<Void> promise2 = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise2);
        promise2.join();

        // ensure it gets deleted from both bookkeeper and offloader
        assertEventuallyTrue(() -> !bkc.getLedgers().contains(firstLedgerId));
        assertEventuallyTrue(() -> offloader.deletedOffloads().contains(firstLedgerId));
    }

    @Test
    public void testLaggedDeleteSlowConsumer() throws Exception {
        OffloadPrefixTest.MockLedgerOffloader offloader = new OffloadPrefixTest.MockLedgerOffloader();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        MockClock clock = new MockClock();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        offloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(300000L);
        config.setLedgerOffloader(offloader);
        config.setClock(clock);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("sub1");

        for (int i = 0; i < 15; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        long firstLedgerId = ledger.getLedgersInfoAsList().get(0).getLedgerId();

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().isComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(2, TimeUnit.MINUTES);

        CompletableFuture<Void> promise = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise);
        promise.join();
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(5, TimeUnit.MINUTES);
        CompletableFuture<Void> promise2 = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise2);
        promise2.join();

        // assert bk ledger is deleted
        assertEventuallyTrue(() -> !bkc.getLedgers().contains(firstLedgerId));

        // ledger still exists in list
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().isComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
    }

    @Test
    public void isOffloadedNeedsDeleteTest() throws Exception {
        OffloadPoliciesImpl offloadPolicies = new OffloadPoliciesImpl();
        LedgerOffloader ledgerOffloader = Mockito.mock(LedgerOffloader.class);
        Mockito.when(ledgerOffloader.getOffloadPolicies()).thenReturn(offloadPolicies);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        MockClock clock = new MockClock();
        config.setLedgerOffloader(ledgerOffloader);
        config.setClock(clock);

        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) factory.open("isOffloadedNeedsDeleteTest", config);

        OffloadContext offloadContext = new OffloadContext()
                .setTimestamp(config.getClock().millis() - 1000)
                .setComplete(true)
                .setBookkeeperDeleted(false);

        boolean needsDelete = managedLedger.isOffloadedNeedsDelete(offloadContext, Optional.of(offloadPolicies));
        Assert.assertFalse(needsDelete);

        offloadPolicies.setManagedLedgerOffloadDeletionLagInMillis(500L);
        needsDelete = managedLedger.isOffloadedNeedsDelete(offloadContext, Optional.of(offloadPolicies));
        Assert.assertTrue(needsDelete);

        offloadPolicies.setManagedLedgerOffloadDeletionLagInMillis(-1L);
        needsDelete = managedLedger.isOffloadedNeedsDelete(offloadContext, Optional.of(offloadPolicies));
        Assert.assertFalse(needsDelete);

        offloadPolicies.setManagedLedgerOffloadDeletionLagInMillis(1000L * 2);
        needsDelete = managedLedger.isOffloadedNeedsDelete(offloadContext, Optional.of(offloadPolicies));
        Assert.assertFalse(needsDelete);

        offloadContext = new OffloadContext()
                .setTimestamp(config.getClock().millis() - 1000)
                .setComplete(false)
                .setBookkeeperDeleted(false);
        needsDelete = managedLedger.isOffloadedNeedsDelete(offloadContext, Optional.of(offloadPolicies));
        Assert.assertFalse(needsDelete);

        offloadContext = new OffloadContext()
                .setTimestamp(config.getClock().millis() - 1000)
                .setComplete(true)
                .setBookkeeperDeleted(true);
        needsDelete = managedLedger.isOffloadedNeedsDelete(offloadContext, Optional.of(offloadPolicies));
        Assert.assertFalse(needsDelete);

    }
}
