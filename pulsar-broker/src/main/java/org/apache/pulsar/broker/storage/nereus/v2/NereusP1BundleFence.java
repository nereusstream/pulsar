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
package org.apache.pulsar.broker.storage.nereus.v2;

import java.util.HashSet;
import java.util.Set;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.metadata.api.extended.NereusOwnershipMetadataStore;

/** Orders every local topic installation against revocation of one exact native Owned state. */
public final class NereusP1BundleFence {
    private final ServiceUnitStateData owner;
    private final NereusP1OwnershipTransitions transitions;
    private final NereusOwnershipMetadataStore.Continuity cut;
    private final Set<Runnable> invalidations = new HashSet<>();
    private boolean revoked;

    public NereusP1BundleFence(ServiceUnitStateData owner, NereusP1OwnershipTransitions transitions) {
        this.owner = owner;
        this.transitions = transitions;
        this.cut = transitions.continuity();
    }

    public boolean matches(ServiceUnitStateData state) {
        return owner.equals(state);
    }

    public boolean matches(NereusOwnershipWitness witness) {
        return owner.brokerIncarnationId().equals(witness.brokerIncarnationId().value())
                && owner.acquisitionId().equals(witness.acquisitionId().value());
    }

    public synchronized boolean isRevoked() {
        return revoked;
    }

    public synchronized boolean isCurrent() {
        return !revoked && cut.equals(transitions.continuity()) && transitions.isLocal(owner);
    }

    public synchronized AutoCloseable arm(Runnable invalidation) {
        if (!isCurrent()) {
            throw new IllegalStateException("native P1 bundle ownership is not current");
        }
        invalidations.add(invalidation);
        try {
            AutoCloseable continuity = transitions.onInvalidation(invalidation);
            return () -> {
                synchronized (this) {
                    invalidations.remove(invalidation);
                }
                continuity.close();
            };
        } catch (RuntimeException failure) {
            invalidations.remove(invalidation);
            throw failure;
        }
    }

    public synchronized void revoke() {
        if (!revoked) {
            revoked = true;
            invalidations.forEach(Runnable::run);
            invalidations.clear();
        }
    }
}
