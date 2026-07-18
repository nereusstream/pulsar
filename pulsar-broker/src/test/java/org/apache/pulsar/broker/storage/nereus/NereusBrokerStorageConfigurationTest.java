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
package org.apache.pulsar.broker.storage.nereus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.pulsar.NereusProcessIdentity;
import java.security.SecureRandom;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.testng.annotations.Test;

public class NereusBrokerStorageConfigurationTest {
    @Test
    public void mapsAllRuntimeIdentityAndCapacityBoundaries() {
        ServiceConfiguration broker = validConfiguration();
        NereusProcessIdentity identity = NereusProcessIdentity.generate(new SecureRandom());

        var checked = new NereusBrokerStorageConfiguration(broker);
        var runtime = checked.runtimeConfiguration(identity);

        assertThat(runtime.streamStorage().cluster()).isEqualTo("test-cluster");
        assertThat(runtime.streamStorage().processRunId()).isEqualTo(identity.processRunId());
        assertThat(runtime.streamStorage().writerId()).isEqualTo(identity.writerId());
        assertThat(runtime.oxia().maxCommitChainScan())
                .isEqualTo(runtime.streamStorage().maxCommitChainScan());
        assertThat(runtime.managedLedger().maxRetainedAppendAttempts())
                .isEqualTo(runtime.streamStorage().maxRetainedAppendAttempts());
        assertThat(runtime.projectionMetadata().maxPendingOperations())
                .isLessThanOrEqualTo(runtime.oxia().maxPendingOperations());
        assertThat(runtime.managedLedger().defaultStorageProfile())
                .isEqualTo(StorageProfile.OBJECT_WAL_SYNC_OBJECT);
        assertThat(runtime.materialization().committedPolicy().minMergeSourceRanges())
                .isEqualTo(2);
        assertThat(runtime.materialization().maxConcurrentWorkers())
                .isEqualTo(8);
        assertThat(runtime.materialization().lagThrottleDelay())
                .isEqualTo(java.time.Duration.ofMillis(25));
        assertThat(runtime.materialization().stagingDirectory().getFileName().toString())
                .isEqualTo(identity.processRunId());
        assertThat(runtime.retention().statsScanPageSize()).isEqualTo(512);
        assertThat(runtime.retention().maxConcurrentPlans()).isEqualTo(4);
        assertThat(runtime.retention().maxQueuedPlans()).isEqualTo(1024);
        assertThat(runtime.retention().operationTimeout())
                .isEqualTo(java.time.Duration.ofSeconds(60));
        assertThat(runtime.retention().closeTimeout())
                .isEqualTo(java.time.Duration.ofSeconds(120));
        assertThat(runtime.physicalGc().enabled()).isFalse();
        assertThat(runtime.physicalGc().dryRun()).isTrue();
        assertThat(runtime.physicalGc().metadataScanPageSize()).isEqualTo(1000);
        assertThat(runtime.physicalGc().objectListPageSize()).isEqualTo(1000);
        assertThat(runtime.physicalGc().maxConcurrentDeletes()).isEqualTo(4);
        assertThat(runtime.physicalGc().maxStreamsPerCandidate()).isEqualTo(1024);
        assertThat(runtime.physicalGc().maxAuthoritiesPerDomainSnapshot()).isEqualTo(100000);
        assertThat(runtime.physicalGc().maxReferencesPerDomainSnapshot()).isEqualTo(100000);
        assertThat(runtime.physicalGc().scanInterval())
                .isEqualTo(java.time.Duration.ofSeconds(60));
        assertThat(runtime.physicalGc().readerLeaseDuration())
                .isEqualTo(java.time.Duration.ofSeconds(120));
        assertThat(runtime.physicalGc().readerLeaseRenewInterval())
                .isEqualTo(java.time.Duration.ofSeconds(30));
        assertThat(runtime.physicalGc().maximumClockSkew())
                .isEqualTo(java.time.Duration.ofSeconds(5));
        assertThat(runtime.physicalGc().drainGrace())
                .isEqualTo(java.time.Duration.ofSeconds(300));
        assertThat(runtime.physicalGc().pendingProtectionDuration())
                .isEqualTo(java.time.Duration.ofSeconds(300));
        assertThat(runtime.physicalGc().orphanGrace())
                .isEqualTo(java.time.Duration.ofDays(1));
        assertThat(runtime.physicalGc().tombstoneAuditGrace())
                .isEqualTo(java.time.Duration.ofDays(7));
        assertThat(runtime.physicalGc().operationTimeout())
                .isEqualTo(java.time.Duration.ofSeconds(60));
        assertThat(runtime.physicalGc().closeTimeout())
                .isEqualTo(java.time.Duration.ofSeconds(300));
        assertThat(checked.generationRegistrationBackfillConcurrency())
                .isEqualTo(16);
        assertThat(checked.generationRegistrationBackfillTimeout())
                .isEqualTo(java.time.Duration.ofHours(1));
        assertThat(checked.generationProtocolEnabled()).isFalse();
        assertThat(checked.generationRegistrationBackfillMaxTopicsPerNamespace())
                .isEqualTo(broker.getNereusMaxNamespaceBindingScanEntries());

        broker.setNereusGenerationProtocolEnabled(true);
        assertThat(checked.generationProtocolEnabled()).isTrue();

        broker.setNereusPhysicalGcEnabled(true);
        broker.setNereusPhysicalGcDryRun(false);
        assertThat(checked.runtimeConfiguration(identity).physicalGc().mutationsAllowed())
                .isTrue();

        broker.setNereusDefaultStorageProfile(
                StorageProfile.OBJECT_WAL_ASYNC_OBJECT.name());
        assertThat(checked.runtimeConfiguration(identity)
                        .managedLedger()
                        .defaultStorageProfile())
                .isEqualTo(StorageProfile.OBJECT_WAL_ASYNC_OBJECT);
    }

    @Test
    public void rejectsInvalidCrossConfigBeforeClientConstruction() {
        ServiceConfiguration disabled = validConfiguration();
        disabled.setNereusEnabled(false);
        assertThatThrownBy(() -> new NereusBrokerStorageConfiguration(disabled)
                .runtimeConfiguration(NereusProcessIdentity.generate(new SecureRandom())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nereusEnabled");

        ServiceConfiguration persistentTopicsDisabled = validConfiguration();
        persistentTopicsDisabled.setEnablePersistentTopics(false);
        assertThatThrownBy(() -> new NereusBrokerStorageConfiguration(persistentTopicsDisabled)
                .runtimeConfiguration(NereusProcessIdentity.generate(new SecureRandom())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("enablePersistentTopics");

        ServiceConfiguration legacyLoadManager = validConfiguration();
        legacyLoadManager.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        assertThatThrownBy(() -> new NereusBrokerStorageConfiguration(legacyLoadManager)
                .runtimeConfiguration(NereusProcessIdentity.generate(new SecureRandom())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ExtensibleLoadManagerImpl");

        ServiceConfiguration undersizedCache = validConfiguration();
        undersizedCache.setNereusMaxCachedStreams(9_999);
        assertThatThrownBy(() -> new NereusBrokerStorageConfiguration(undersizedCache)
                .runtimeConfiguration(NereusProcessIdentity.generate(new SecureRandom())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxCachedStreams");

        ServiceConfiguration invalidBackfill = validConfiguration();
        invalidBackfill.setNereusGenerationRegistrationBackfillConcurrency(0);
        assertThatThrownBy(() -> new NereusBrokerStorageConfiguration(invalidBackfill)
                .generationRegistrationBackfillConcurrency())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BackfillConcurrency");

        ServiceConfiguration oversizedBackfill = validConfiguration();
        oversizedBackfill.setNereusGenerationRegistrationBackfillConcurrency(
                GenerationRegistrationBackfillRequest.MAX_CONCURRENCY + 1);
        assertThatThrownBy(() -> new NereusBrokerStorageConfiguration(oversizedBackfill)
                .generationRegistrationBackfillConcurrency())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 1024");

        ServiceConfiguration aliasedProfile = validConfiguration();
        aliasedProfile.setNereusDefaultStorageProfile("OBJECT_WAL");
        assertThatThrownBy(() -> new NereusBrokerStorageConfiguration(
                        aliasedProfile)
                .runtimeConfiguration(
                        NereusProcessIdentity.generate(
                                new SecureRandom())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact Object-WAL profile");

        ServiceConfiguration invalidLag = validConfiguration();
        invalidLag.setNereusMaterializationLagRejectRecords(100);
        invalidLag.setNereusMaterializationLagThrottleRecords(100);
        assertThatThrownBy(() -> new NereusBrokerStorageConfiguration(
                        invalidLag)
                .runtimeConfiguration(
                        NereusProcessIdentity.generate(
                                new SecureRandom())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("record lag");

        ServiceConfiguration oversizedRegistryPage = validConfiguration();
        oversizedRegistryPage.setNereusMaterializationRegistryScanPageSize(257);
        assertThatThrownBy(() -> new NereusBrokerStorageConfiguration(
                        oversizedRegistryPage)
                .runtimeConfiguration(
                        NereusProcessIdentity.generate(
                                new SecureRandom())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RegistryScanPageSize");

        ServiceConfiguration oversizedRetentionPage = validConfiguration();
        oversizedRetentionPage.setNereusRetentionStatsScanPageSize(513);
        assertThatThrownBy(() -> new NereusBrokerStorageConfiguration(
                        oversizedRetentionPage)
                .runtimeConfiguration(
                        NereusProcessIdentity.generate(
                                new SecureRandom())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RetentionStatsScanPageSize");

        ServiceConfiguration invalidRetentionDeadline = validConfiguration();
        invalidRetentionDeadline.setNereusRetentionOperationTimeoutSeconds(121);
        assertThatThrownBy(() -> new NereusBrokerStorageConfiguration(
                        invalidRetentionDeadline)
                .runtimeConfiguration(
                        NereusProcessIdentity.generate(
                                new SecureRandom())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retention operation timeout");

        ServiceConfiguration oversizedGcPage = validConfiguration();
        oversizedGcPage.setNereusGcMetadataScanPageSize(1001);
        assertThatThrownBy(() -> new NereusBrokerStorageConfiguration(
                        oversizedGcPage)
                .runtimeConfiguration(
                        NereusProcessIdentity.generate(
                                new SecureRandom())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nereusGcMetadataScanPageSize");

        ServiceConfiguration invalidGcLease = validConfiguration();
        invalidGcLease.setNereusGcOperationTimeoutSeconds(115);
        assertThatThrownBy(() -> new NereusBrokerStorageConfiguration(
                        invalidGcLease)
                .runtimeConfiguration(
                        NereusProcessIdentity.generate(
                                new SecureRandom())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shorter than readerLeaseDuration");
    }

    private static ServiceConfiguration validConfiguration() {
        ServiceConfiguration broker = new ServiceConfiguration();
        broker.setNereusEnabled(true);
        broker.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        broker.setClusterName("test-cluster");
        broker.setNereusOxiaServiceAddress("localhost:6648");
        broker.setNereusOxiaNamespace("nereus/test-cluster");
        broker.setNereusObjectStoreEndpoint("http://127.0.0.1:4566");
        broker.setNereusObjectStoreRegion("us-east-1");
        broker.setNereusObjectStoreBucket("nereus-test");
        broker.setNereusObjectStorePrefix("test-cluster");
        return broker;
    }
}
