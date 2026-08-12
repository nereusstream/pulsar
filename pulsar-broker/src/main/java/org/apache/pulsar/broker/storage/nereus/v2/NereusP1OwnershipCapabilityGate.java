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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfiguration.ServiceUnitTableViewSyncerType;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateMetadataStoreTableViewImpl;

/**
 * Fail-closed P1 startup admission over exact native ownership capabilities.
 *
 * <p>This control-path gate does not activate V2 topics. Configuration alone cannot manufacture the
 * native-hook evidence; M6 must bind that evidence to installed transition and lifecycle hooks.
 */
public final class NereusP1OwnershipCapabilityGate {
    enum NativeHookRequirement {
        AUTHORITATIVE_A_B_READ,
        INVALIDATE_BEFORE_OWNERSHIP_MUTATION_OR_LOSS,
        IDENTITY_AWARE_ALL_WRITERS,
        GAP_AND_SESSION_LIFECYCLE
    }

    public enum Failure {
        EXTENSIBLE_LOAD_MANAGER_REQUIRED,
        METADATA_STORE_TABLE_VIEW_REQUIRED,
        TABLE_VIEW_SYNCER_MUST_BE_DISABLED,
        AUTHORITATIVE_A_B_READ_MISSING,
        ORDERED_INVALIDATION_HOOKS_MISSING,
        IDENTITY_AWARE_WRITER_SET_INCOMPLETE,
        GAP_OR_SESSION_LIFECYCLE_UNQUALIFIED
    }

    public record Decision(boolean qualified, List<Failure> failures) {
        public Decision {
            failures = List.copyOf(Objects.requireNonNull(failures, "failures"));
            if (qualified != failures.isEmpty()) {
                throw new IllegalArgumentException("qualified must be equivalent to an empty failure list");
            }
        }
    }

    /** Opaque proof input; only package-owned native integration can construct it. */
    public static final class NativeHookEvidence {
        private final EnumSet<NativeHookRequirement> verified;

        private NativeHookEvidence(EnumSet<NativeHookRequirement> verified) {
            this.verified = EnumSet.copyOf(verified);
        }

        private boolean verifies(NativeHookRequirement requirement) {
            return verified.contains(requirement);
        }
    }

    private NereusP1OwnershipCapabilityGate() {}

    public static Decision evaluate(
            ServiceConfiguration configuration, NativeHookEvidence evidence) {
        Objects.requireNonNull(configuration, "configuration");
        List<Failure> failures = new ArrayList<>();
        if (!ExtensibleLoadManagerImpl.class.getName().equals(configuration.getLoadManagerClassName())) {
            failures.add(Failure.EXTENSIBLE_LOAD_MANAGER_REQUIRED);
        }
        if (!ServiceUnitStateMetadataStoreTableViewImpl.class.getName()
                .equals(configuration.getLoadManagerServiceUnitStateTableViewClassName())) {
            failures.add(Failure.METADATA_STORE_TABLE_VIEW_REQUIRED);
        }
        if (configuration.getLoadBalancerServiceUnitTableViewSyncer() != ServiceUnitTableViewSyncerType.None) {
            failures.add(Failure.TABLE_VIEW_SYNCER_MUST_BE_DISABLED);
        }
        if (!verifies(evidence, NativeHookRequirement.AUTHORITATIVE_A_B_READ)) {
            failures.add(Failure.AUTHORITATIVE_A_B_READ_MISSING);
        }
        if (!verifies(evidence, NativeHookRequirement.INVALIDATE_BEFORE_OWNERSHIP_MUTATION_OR_LOSS)) {
            failures.add(Failure.ORDERED_INVALIDATION_HOOKS_MISSING);
        }
        if (!verifies(evidence, NativeHookRequirement.IDENTITY_AWARE_ALL_WRITERS)) {
            failures.add(Failure.IDENTITY_AWARE_WRITER_SET_INCOMPLETE);
        }
        if (!verifies(evidence, NativeHookRequirement.GAP_AND_SESSION_LIFECYCLE)) {
            failures.add(Failure.GAP_OR_SESSION_LIFECYCLE_UNQUALIFIED);
        }
        return new Decision(failures.isEmpty(), failures);
    }

    public static void requireQualified(
            ServiceConfiguration configuration, NativeHookEvidence evidence) {
        Decision decision = evaluate(configuration, evidence);
        if (!decision.qualified()) {
            throw new IllegalStateException("Nereus P1 ownership capability is not qualified: "
                    + decision.failures());
        }
    }

    static NativeHookEvidence verifiedNativeHooks(EnumSet<NativeHookRequirement> requirements) {
        return new NativeHookEvidence(Objects.requireNonNull(requirements, "requirements"));
    }

    private static boolean verifies(NativeHookEvidence evidence, NativeHookRequirement requirement) {
        return evidence != null && evidence.verifies(requirement);
    }
}
