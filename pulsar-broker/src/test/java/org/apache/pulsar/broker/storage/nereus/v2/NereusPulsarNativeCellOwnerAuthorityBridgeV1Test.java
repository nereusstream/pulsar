/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pulsar.broker.storage.nereus.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunReference;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NereusPulsarNativeCellOwnerAuthorityBridgeV1Test {
    private static final PulsarProtocolCellIdentity CELL = new PulsarProtocolCellIdentity(
            new DeploymentId(new Id128(1, 2)),
            new ReservationDomainId(new Id128(3, 4)),
            new PulsarCellId(new Id128(5, 6)));
    private static final WalRunReference ROOT = new WalRunReference(
            WalRunControlKeys.rootKey(2, 7), digest(9), 2, 7);

    @Test
    public void executesExactlyOnceUnderTheExactNativeCellOwnerCut() throws Exception {
        FakeNativeAuthority nativeAuthority = new FakeNativeAuthority(11);
        var bridge = new NereusPulsarNativeCellOwnerAuthorityBridgeV1(nativeAuthority);
        AtomicInteger callbacks = new AtomicInteger();
        WalRunObjectSession expected = mock(WalRunObjectSession.class);

        WalRunObjectSession actual = bridge.withAllBindingsActiveMonotonicFence(CELL, ROOT, observed -> {
            callbacks.incrementAndGet();
            assertThat(nativeAuthority.lock.getReadHoldCount()).isOne();
            assertThat(nativeAuthority.lock.writeLock().tryLock()).isFalse();
            assertThat(observed.protocolCell()).isEqualTo(CELL);
            assertThat(observed.rootReference()).isEqualTo(ROOT);
            assertThat(observed.ownerEpoch()).isEqualTo(11);
            assertThat(observed.ownerFenceCommitment()).isEqualTo(digest(11));
            return expected;
        });

        assertThat(actual).isSameAs(expected);
        assertThat(callbacks).hasValue(1);
    }

    @Test
    public void propagatesProviderIoFailureAndReleasesTheNativeGuard() throws Exception {
        FakeNativeAuthority nativeAuthority = new FakeNativeAuthority(11);
        var bridge = new NereusPulsarNativeCellOwnerAuthorityBridgeV1(nativeAuthority);
        IOException expected = new IOException("provider unavailable");

        assertThatThrownBy(() -> bridge.withAllBindingsActiveMonotonicFence(CELL, ROOT, ignored -> {
                    throw expected;
                }))
                .isSameAs(expected);

        WalRunObjectSession recovered = mock(WalRunObjectSession.class);
        assertThat(bridge.withAllBindingsActiveMonotonicFence(CELL, ROOT, ignored -> recovered))
                .isSameAs(recovered);
    }

    @Test
    public void rejectsMissingOrSubstitutedCallbackResult() {
        WalRunObjectSession expected = mock(WalRunObjectSession.class);
        var missing = new NereusPulsarNativeCellOwnerAuthorityBridgeV1(
                (cell, root, callback) -> expected);
        assertThatThrownBy(() -> missing.withAllBindingsActiveMonotonicFence(CELL, ROOT, ignored -> expected))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("without owner-open recovery");

        var substituted = new NereusPulsarNativeCellOwnerAuthorityBridgeV1((cell, root, callback) -> {
            callback.execute(new NereusPulsarNativeCellOwnerAuthorityBridgeV1.NativeCellOwnerObservation(
                    11, digest(11)));
            return mock(WalRunObjectSession.class);
        });
        assertThatThrownBy(() -> substituted.withAllBindingsActiveMonotonicFence(CELL, ROOT, ignored -> expected))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("substituted");
    }

    @Test
    public void blocksCellOwnerSupersedeForTheCompleteProviderCallback() throws Exception {
        FakeNativeAuthority nativeAuthority = new FakeNativeAuthority(11);
        var bridge = new NereusPulsarNativeCellOwnerAuthorityBridgeV1(nativeAuthority);
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        CountDownLatch takeoverAttempted = new CountDownLatch(1);
        CountDownLatch takeoverBlocked = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<WalRunObjectSession> ownerOpen = executor.submit(() ->
                    bridge.withAllBindingsActiveMonotonicFence(CELL, ROOT, ignored -> {
                        callbackEntered.countDown();
                        await(releaseCallback);
                        return mock(WalRunObjectSession.class);
                    }));
            assertThat(callbackEntered.await(5, TimeUnit.SECONDS)).isTrue();

            Future<?> takeover = executor.submit(() -> {
                takeoverAttempted.countDown();
                if (!nativeAuthority.lock.writeLock().tryLock()) {
                    takeoverBlocked.countDown();
                    nativeAuthority.lock.writeLock().lock();
                }
                try {
                    nativeAuthority.ownerEpoch.incrementAndGet();
                } finally {
                    nativeAuthority.lock.writeLock().unlock();
                }
            });
            assertThat(takeoverAttempted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(takeoverBlocked.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(nativeAuthority.ownerEpoch).hasValue(11);

            releaseCallback.countDown();
            ownerOpen.get(5, TimeUnit.SECONDS);
            takeover.get(5, TimeUnit.SECONDS);
            assertThat(nativeAuthority.ownerEpoch).hasValue(12);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void stockPulsarPositionsPreserveTheCompleteFixedSliceLedgerDomain() {
        long first = PulsarVirtualLedgerChainControllerV1.RESERVED_START_INCLUSIVE;
        long last = Math.addExact(first, PulsarVirtualLedgerChainControllerV1.SLICE_SIZE - 1);
        var slice = new PulsarVirtualLedgerChainControllerV1.FixedSlice(first, last);
        slice.requireContains(first);
        slice.requireContains(last);

        Position position = PositionFactory.create(last, 37);
        MessageIdImpl messageId = new MessageIdImpl(last, 37, -1);

        assertThat(position.getLedgerId()).isEqualTo(last);
        assertThat(position.getEntryId()).isEqualTo(37);
        assertThat(messageId.getLedgerId()).isEqualTo(last);
        assertThat(messageId.getEntryId()).isEqualTo(37);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for deterministic test latch");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for deterministic test latch", failure);
        }
    }

    private static Sha256Digest digest(int marker) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        java.util.Arrays.fill(bytes, (byte) marker);
        return Sha256Digest.copyOf(bytes);
    }

    private static final class FakeNativeAuthority
            implements NereusPulsarNativeCellOwnerAuthorityBridgeV1.NativeCellOwnerMaintenanceAuthority {
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private final AtomicLong ownerEpoch;

        private FakeNativeAuthority(long ownerEpoch) {
            this.ownerEpoch = new AtomicLong(ownerEpoch);
        }

        @Override
        public WalRunObjectSession executeWhileCurrentOwner(
                PulsarProtocolCellIdentity exactProtocolCell,
                WalRunReference exactRootReference,
                NereusPulsarNativeCellOwnerAuthorityBridgeV1.NativeCellOwnerCallback callback)
                throws IOException {
            assertThat(exactProtocolCell).isEqualTo(CELL);
            assertThat(exactRootReference).isEqualTo(ROOT);
            lock.readLock().lock();
            try {
                long observedEpoch = ownerEpoch.get();
                WalRunObjectSession result = callback.execute(
                        new NereusPulsarNativeCellOwnerAuthorityBridgeV1.NativeCellOwnerObservation(
                                observedEpoch, digest(Math.toIntExact(observedEpoch))));
                assertThat(ownerEpoch).hasValue(observedEpoch);
                return result;
            } finally {
                lock.readLock().unlock();
            }
        }
    }
}
