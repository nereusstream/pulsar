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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.EnumSet;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfiguration.ServiceUnitTableViewSyncerType;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateMetadataStoreTableViewImpl;
import org.apache.pulsar.broker.storage.nereus.v2.NereusP1OwnershipCapabilityGate.Failure;
import org.apache.pulsar.broker.storage.nereus.v2.NereusP1OwnershipCapabilityGate.NativeHookRequirement;
import org.testng.annotations.Test;

public class NereusP1OwnershipCapabilityGateTest {
    @Test
    public void exactMetadataStoreElmAndAllNativeHooksQualify() {
        ServiceConfiguration configuration = qualifiedConfiguration();
        var evidence = NereusP1OwnershipCapabilityGate.verifiedNativeHooks(
                EnumSet.allOf(NativeHookRequirement.class));

        var decision = NereusP1OwnershipCapabilityGate.evaluate(configuration, evidence);

        assertThat(decision.qualified()).isTrue();
        assertThat(decision.failures()).isEmpty();
        NereusP1OwnershipCapabilityGate.requireQualified(configuration, evidence);
    }

    @Test
    public void stockDefaultsAndAbsentEvidenceFailClosed() {
        var decision = NereusP1OwnershipCapabilityGate.evaluate(new ServiceConfiguration(), null);

        assertThat(decision.qualified()).isFalse();
        assertThat(decision.failures())
                .containsExactly(
                        Failure.EXTENSIBLE_LOAD_MANAGER_REQUIRED,
                        Failure.METADATA_STORE_TABLE_VIEW_REQUIRED,
                        Failure.AUTHORITATIVE_A_B_READ_MISSING,
                        Failure.ORDERED_INVALIDATION_HOOKS_MISSING,
                        Failure.IDENTITY_AWARE_WRITER_SET_INCOMPLETE,
                        Failure.GAP_OR_SESSION_LIFECYCLE_UNQUALIFIED);
    }

    @Test
    public void anyMissingNativeHookFailsClosed() {
        for (NativeHookRequirement missing : NativeHookRequirement.values()) {
            EnumSet<NativeHookRequirement> requirements = EnumSet.allOf(NativeHookRequirement.class);
            requirements.remove(missing);
            var decision = NereusP1OwnershipCapabilityGate.evaluate(
                    qualifiedConfiguration(),
                    NereusP1OwnershipCapabilityGate.verifiedNativeHooks(requirements));
            assertThat(decision.qualified()).as("missing %s", missing).isFalse();
            assertThat(decision.failures()).as("missing %s", missing).hasSize(1);
        }
    }

    @Test
    public void syncerOrNonMetadataTableViewCannotBeOverriddenByEvidence() {
        ServiceConfiguration configuration = qualifiedConfiguration();
        configuration.setLoadBalancerServiceUnitTableViewSyncer(
                ServiceUnitTableViewSyncerType.MetadataStoreToSystemTopicSyncer);
        configuration.setLoadManagerServiceUnitStateTableViewClassName("example.EventualTableView");
        var evidence = NereusP1OwnershipCapabilityGate.verifiedNativeHooks(
                EnumSet.allOf(NativeHookRequirement.class));

        var decision = NereusP1OwnershipCapabilityGate.evaluate(configuration, evidence);

        assertThat(decision.failures())
                .containsExactly(
                        Failure.METADATA_STORE_TABLE_VIEW_REQUIRED,
                        Failure.TABLE_VIEW_SYNCER_MUST_BE_DISABLED);
        assertThatThrownBy(() -> NereusP1OwnershipCapabilityGate.requireQualified(configuration, evidence))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("METADATA_STORE_TABLE_VIEW_REQUIRED")
                .hasMessageContaining("TABLE_VIEW_SYNCER_MUST_BE_DISABLED");
    }

    private static ServiceConfiguration qualifiedConfiguration() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        configuration.setLoadManagerServiceUnitStateTableViewClassName(
                ServiceUnitStateMetadataStoreTableViewImpl.class.getName());
        configuration.setLoadBalancerServiceUnitTableViewSyncer(ServiceUnitTableViewSyncerType.None);
        return configuration;
    }
}
