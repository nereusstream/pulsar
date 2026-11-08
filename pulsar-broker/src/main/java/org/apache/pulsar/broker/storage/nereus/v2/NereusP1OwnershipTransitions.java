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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.metadata.api.extended.NereusOwnershipMetadataStore;

/** Selects acquisition identities for writes issued by the native service-unit channel. */
public final class NereusP1OwnershipTransitions implements AutoCloseable {
    public static final String INCARNATION_PROPERTY = "nereus.p1.broker.incarnation";

    public record Prepared(ServiceUnitStateData state, NereusOwnershipMetadataStore.Continuity continuity) {
    }

    private final NereusOwnershipMetadataStore store;
    private final AutoCloseable invalidation;
    private final CopyOnWriteArrayList<Runnable> listeners = new CopyOnWriteArrayList<>();
    private NereusOwnershipIdentityGenerator identities;
    private volatile NereusOwnershipMetadataStore.Continuity registered;

    public NereusP1OwnershipTransitions(NereusOwnershipMetadataStore store) {
        this.store = store;
        invalidation = store.onOwnershipInvalidated(this::invalidateRegistration);
    }

    public void invalidateRegistration() {
        registered = null;
        listeners.forEach(Runnable::run);
    }

    public synchronized void loseRegistration() {
        invalidateRegistration();
        identities = null;
    }

    public NereusOwnershipMetadataStore.Continuity continuity() {
        return store.ownershipContinuity();
    }

    public AutoCloseable onInvalidation(Runnable listener) {
        listeners.add(listener);
        if (!isRegistered()) {
            listener.run();
        }
        return () -> listeners.remove(listener);
    }

    public boolean isRegistered() {
        var cut = registered;
        return cut != null && cut.equals(store.ownershipContinuity());
    }

    private NereusOwnershipIdentityGenerator identities() {
        if (identities == null) {
            identities = new NereusOwnershipIdentityGenerator();
        }
        return identities;
    }

    private synchronized NereusOwnershipId localIncarnation() {
        return identities().brokerIncarnationId();
    }

    private synchronized NereusOwnershipId newAcquisition() {
        return identities().newAcquisitionId();
    }

    public Map<String, String> registrationProperties(
            Map<String, String> configured, NereusOwnershipMetadataStore.Continuity cut, boolean existing) {
        requireCurrent(cut);
        if (!existing) {
            loseRegistration();
        }
        var properties = new HashMap<>(configured);
        properties.put(INCARNATION_PROPERTY, localIncarnation().value());
        requireCurrent(cut);
        return Map.copyOf(properties);
    }

    public synchronized void registered(NereusOwnershipMetadataStore.Continuity cut, String incarnation) {
        requireCurrent(cut);
        if (!localIncarnation().value().equals(incarnation)) {
            throw new IllegalStateException("native P1 registration incarnation changed");
        }
        registered = cut;
        requireCurrent(cut);
    }

    public CompletableFuture<Prepared> prepare(
            ServiceUnitStateData previous, ServiceUnitStateData candidate,
            Function<String, CompletableFuture<Optional<NereusOwnershipId>>> authoritativeBrokerLookup) {
        return CompletableFuture.completedFuture(store.ownershipContinuity()).thenCompose(cut -> {
            requireCurrent(cut);
            if (!cut.equals(registered)) {
                throw new IllegalStateException("local native P1 broker is not registered on the current continuity");
            }
            if (candidate.state() == ServiceUnitState.Assigning) {
                return authoritativeBrokerLookup.apply(candidate.dstBroker()).thenApply(incarnation -> {
                    requireCurrent(cut);
                    var target = incarnation.orElseThrow(
                            () -> new IllegalStateException("target broker has no qualified P1 incarnation"));
                    var acquisition = newAcquisition();
                    requireCurrent(cut);
                    return new Prepared(validate(previous, withIdentity(candidate, target, acquisition)), cut);
                });
            }
            if (previous == null || !previous.hasNereusOwnershipIdentity()) {
                throw new IllegalStateException("P1 transition has no qualified predecessor");
            }
            var broker = new NereusOwnershipId(previous.brokerIncarnationId());
            var acquisition = new NereusOwnershipId(previous.acquisitionId());
            if (candidate.state() == ServiceUnitState.Owned && !localIncarnation().equals(broker)) {
                throw new IllegalStateException("P1 assignment belongs to a previous or foreign broker incarnation");
            }
            requireCurrent(cut);
            return CompletableFuture.completedFuture(
                    new Prepared(validate(previous, withIdentity(candidate, broker, acquisition)), cut));
        });
    }

    public <T> CompletableFuture<T> publish(
            Prepared prepared, Function<ServiceUnitStateData, CompletableFuture<T>> write) {
        requireCurrent(prepared.continuity());
        if (!prepared.continuity().equals(registered)) {
            throw new IllegalStateException("native P1 registration changed before state publication");
        }
        return write.apply(prepared.state());
    }

    public boolean isLocal(ServiceUnitStateData state) {
        var cut = store.ownershipContinuity();
        return state.hasNereusOwnershipIdentity()
                && cut.ready()
                && cut.equals(registered)
                && localIncarnation().value().equals(state.brokerIncarnationId())
                && cut.equals(store.ownershipContinuity());
    }

    @Override
    public void close() throws Exception {
        invalidateRegistration();
        invalidation.close();
    }

    private void requireCurrent(NereusOwnershipMetadataStore.Continuity cut) {
        if (!cut.ready() || !cut.equals(store.ownershipContinuity())) {
            throw new IllegalStateException("native P1 ownership continuity changed");
        }
    }

    private static ServiceUnitStateData validate(ServiceUnitStateData before, ServiceUnitStateData after) {
        if (NereusOwnershipTransitionValidator.shouldReject(before, after)) {
            throw new IllegalStateException("unqualified native P1 service-unit transition");
        }
        return after;
    }

    private static ServiceUnitStateData withIdentity(
            ServiceUnitStateData candidate, NereusOwnershipId broker, NereusOwnershipId acquisition) {
        return new ServiceUnitStateData(candidate.state(), candidate.dstBroker(), candidate.sourceBroker(),
                candidate.splitServiceUnitToDestBroker(), candidate.force(), candidate.timestamp(),
                candidate.versionId(), broker, acquisition);
    }
}
