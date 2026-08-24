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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunReference;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Translates Pulsar's Cell-wide maintenance authority into the exact synchronous Nereus Object-WAL owner fence.
 *
 * <p>The injected native authority owns the actual all-Binding read/maintenance lock and its durable ACTIVE
 * observation. It must block ownership replacement and prior-owner dispatch until its callback returns. This bridge
 * deliberately performs no broker lifecycle activation; M6 remains responsible for installing the authority.
 */
public final class NereusPulsarNativeCellOwnerAuthorityBridgeV1
        implements PulsarObjectWalBridgeV1.PulsarActiveOwnerFenceAuthority {
    private final NativeCellOwnerMaintenanceAuthority nativeAuthority;
    private final AtomicBoolean executing = new AtomicBoolean();

    public NereusPulsarNativeCellOwnerAuthorityBridgeV1(
            NativeCellOwnerMaintenanceAuthority nativeAuthority) {
        this.nativeAuthority = Objects.requireNonNull(nativeAuthority, "nativeAuthority");
    }

    @Override
    public WalRunObjectSession withAllBindingsActiveMonotonicFence(
            PulsarProtocolCellIdentity exactProtocolCell,
            WalRunReference exactRootReference,
            PulsarObjectWalBridgeV1.PulsarFencedRecoveryAction callback)
            throws IOException {
        Objects.requireNonNull(exactProtocolCell, "exactProtocolCell");
        Objects.requireNonNull(exactRootReference, "exactRootReference");
        Objects.requireNonNull(callback, "callback");
        if (!executing.compareAndSet(false, true)) {
            throw new IllegalStateException("Pulsar native Cell owner callback is already executing");
        }
        Thread callingThread = Thread.currentThread();
        AtomicBoolean observedOnce = new AtomicBoolean();
        AtomicReference<WalRunObjectSession> exactResult = new AtomicReference<>();
        try {
            WalRunObjectSession result = nativeAuthority.executeWhileCurrentOwner(
                    exactProtocolCell,
                    exactRootReference,
                    observation -> {
                        if (Thread.currentThread() != callingThread) {
                            throw new IllegalStateException(
                                    "Pulsar native Cell authority changed thread before owner-open recovery");
                        }
                        if (!observedOnce.compareAndSet(false, true)) {
                            throw new IllegalStateException(
                                    "Pulsar native Cell authority invoked owner-open recovery more than once");
                        }
                        WalRunObjectSession recovered = Objects.requireNonNull(
                                callback.recover(new PulsarObjectWalBridgeV1.ObservedPulsarOwnerFence(
                                        exactProtocolCell,
                                        exactRootReference,
                                        observation.ownerEpoch(),
                                        observation.ownerFenceCommitment())),
                                "Pulsar native owner-open recovery result");
                        if (Thread.currentThread() != callingThread) {
                            throw new IllegalStateException(
                                    "Pulsar native owner-open recovery changed thread");
                        }
                        exactResult.set(recovered);
                        return recovered;
                    });
            if (!observedOnce.get()) {
                throw new IllegalStateException(
                        "Pulsar native Cell authority returned without owner-open recovery");
            }
            if (result != exactResult.get()) {
                throw new IllegalStateException(
                        "Pulsar native Cell authority substituted the owner-open recovery result");
            }
            return result;
        } finally {
            executing.set(false);
        }
    }

    /** Exact durable ACTIVE Cell observation made while the native all-Binding maintenance lock is held. */
    public record NativeCellOwnerObservation(long ownerEpoch, Sha256Digest ownerFenceCommitment) {
        public NativeCellOwnerObservation {
            Objects.requireNonNull(ownerFenceCommitment, "ownerFenceCommitment");
            if (ownerEpoch <= 0 || ownerFenceCommitment.isZero()) {
                throw new IllegalArgumentException("Pulsar native Cell owner observation is invalid");
            }
        }
    }

    @FunctionalInterface
    public interface NativeCellOwnerCallback {
        WalRunObjectSession execute(NativeCellOwnerObservation exactObservation) throws IOException;
    }

    /**
     * Stock/runtime seam for the one Cell-wide maintenance lock. The implementation must invoke the callback
     * synchronously on the calling thread and retain the exact same ownership cut until the callback returns.
     */
    @FunctionalInterface
    public interface NativeCellOwnerMaintenanceAuthority {
        WalRunObjectSession executeWhileCurrentOwner(
                PulsarProtocolCellIdentity exactProtocolCell,
                WalRunReference exactRootReference,
                NativeCellOwnerCallback callback)
                throws IOException;
    }
}
