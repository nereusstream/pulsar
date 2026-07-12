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

import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.managedledger.NereusManagedLedgerFactoryConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.ProjectionMetadataStoreConfig;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import com.nereusstream.pulsar.NereusProcessIdentity;
import com.nereusstream.pulsar.NereusRuntimeConfiguration;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.protocol.Commands;

/** Checked one-time mapping from broker fields into product-owned immutable configuration. */
public final class NereusBrokerStorageConfiguration {
    private final ServiceConfiguration broker;

    public NereusBrokerStorageConfiguration(ServiceConfiguration broker) {
        this.broker = java.util.Objects.requireNonNull(broker, "broker");
    }

    public NereusRuntimeConfiguration runtimeConfiguration(NereusProcessIdentity identity) {
        if (!broker.isNereusEnabled()) {
            throw new IllegalArgumentException("nereusEnabled must be true for Nereus hybrid storage");
        }
        Duration metadataTimeout = seconds(broker.getNereusMetadataTimeoutSeconds(), "nereusMetadataTimeoutSeconds");
        Duration appendTimeout = seconds(broker.getNereusAppendTimeoutSeconds(), "nereusAppendTimeoutSeconds");
        Duration recoveryTimeout = seconds(
                broker.getNereusAppendRecoveryTimeoutSeconds(), "nereusAppendRecoveryTimeoutSeconds");
        Duration readTimeout = seconds(broker.getNereusReadTimeoutSeconds(), "nereusReadTimeoutSeconds");
        Duration closeTimeout = seconds(broker.getNereusCloseTimeoutSeconds(), "nereusCloseTimeoutSeconds");
        Duration attemptTimeout = seconds(
                broker.getNereusAppendRecoveryAttemptTimeoutSeconds(),
                "nereusAppendRecoveryAttemptTimeoutSeconds");
        if (attemptTimeout.compareTo(recoveryTimeout) > 0) {
            throw new IllegalArgumentException("append recovery attempt timeout exceeds callback recovery timeout");
        }
        int maxEntryBytes = broker.getNereusMaxEntryBytes() == 0
                ? Math.addExact(broker.getMaxMessageSize(), Commands.MESSAGE_SIZE_FRAME_PADDING)
                : positive(broker.getNereusMaxEntryBytes(), "nereusMaxEntryBytes");
        OxiaClientConfiguration oxia = new OxiaClientConfiguration(
                text(broker.getNereusOxiaServiceAddress(), "nereusOxiaServiceAddress"),
                text(broker.getNereusOxiaNamespace(), "nereusOxiaNamespace"),
                metadataTimeout,
                seconds(broker.getNereusOxiaSessionTimeoutSeconds(), "nereusOxiaSessionTimeoutSeconds"),
                positive(broker.getNereusMaxCommitChainScan(), "nereusMaxCommitChainScan"),
                positive(broker.getNereusMaxOxiaPendingOperations(), "nereusMaxOxiaPendingOperations"));
        ObjectStoreConfiguration objectStore = new ObjectStoreConfiguration(
                text(broker.getNereusObjectStoreProviderClassName(), "nereusObjectStoreProviderClassName"),
                URI.create(text(broker.getNereusObjectStoreEndpoint(), "nereusObjectStoreEndpoint")),
                text(broker.getNereusObjectStoreRegion(), "nereusObjectStoreRegion"),
                text(broker.getNereusObjectStoreBucket(), "nereusObjectStoreBucket"),
                text(broker.getNereusObjectStorePrefix(), "nereusObjectStorePrefix"),
                broker.isNereusObjectStorePathStyleAccess(),
                seconds(broker.getNereusObjectStoreRequestTimeoutSeconds(),
                        "nereusObjectStoreRequestTimeoutSeconds"),
                positive(broker.getNereusObjectStoreMaxConnections(), "nereusObjectStoreMaxConnections"),
                optional(broker.getNereusObjectStoreAccessKeySecretRef()),
                optional(broker.getNereusObjectStoreSecretKeySecretRef()),
                optional(broker.getNereusObjectStoreSessionTokenSecretRef()));
        StreamStorageConfig stream = new StreamStorageConfig(
                text(broker.getClusterName(), "clusterName"),
                identity.writerId(),
                seconds(broker.getNereusAppendSessionTtlSeconds(), "nereusAppendSessionTtlSeconds"),
                seconds(broker.getNereusAppendSessionRenewBeforeSeconds(),
                        "nereusAppendSessionRenewBeforeSeconds"),
                seconds(broker.getNereusAppendSessionMinCommitRemainingSeconds(),
                        "nereusAppendSessionMinCommitRemainingSeconds"),
                appendTimeout,
                readTimeout,
                closeTimeout,
                positive(broker.getNereusMaxResolveRanges(), "nereusMaxResolveRanges"),
                positive(broker.getNereusMaxCommitChainScan(), "nereusMaxCommitChainScan"),
                positive(broker.getNereusMaxDerivedIndexRepairCommitsPerCall(),
                        "nereusMaxDerivedIndexRepairCommitsPerCall"),
                positive(broker.getNereusMaxCachedStreams(), "nereusMaxCachedStreams"),
                positive(broker.getNereusMaxInFlightAppends(), "nereusMaxInFlightAppends"),
                positive(broker.getNereusMaxBufferedBytes(), "nereusMaxBufferedBytes"),
                positive(broker.getNereusMaxConcurrentObjectReads(), "nereusMaxConcurrentObjectReads"),
                positive(broker.getNereusMaxReadBufferBytes(), "nereusMaxReadBufferBytes"),
                positive(broker.getNereusMaxObjectBytes(), "nereusMaxObjectBytes"),
                1,
                seconds(broker.getNereusOffsetIndexCacheTtlSeconds(), "nereusOffsetIndexCacheTtlSeconds"),
                true,
                broker.isNereusEnableMetadataWatch(),
                broker.isNereusEnableOffsetIndexCache(),
                identity.processRunId(),
                attemptTimeout,
                millis(broker.getNereusAppendRecoveryBackoffMinMillis(),
                        "nereusAppendRecoveryBackoffMinMillis"),
                seconds(broker.getNereusAppendRecoveryBackoffMaxSeconds(),
                        "nereusAppendRecoveryBackoffMaxSeconds"),
                seconds(broker.getNereusAppendRecoveryTerminalTtlSeconds(),
                        "nereusAppendRecoveryTerminalTtlSeconds"),
                positive(broker.getNereusMaxRetainedAppendAttempts(), "nereusMaxRetainedAppendAttempts"),
                positive(broker.getNereusMaxAppendRecoveryTerminals(), "nereusMaxAppendRecoveryTerminals"));
        NereusManagedLedgerFactoryConfig managedLedger = new NereusManagedLedgerFactoryConfig(
                NereusManagedLedgerFactoryConfig.STORAGE_CLASS_NAME,
                metadataTimeout,
                appendTimeout,
                recoveryTimeout,
                readTimeout,
                closeTimeout,
                millis(broker.getNereusTailPollIntervalMillis(), "nereusTailPollIntervalMillis"),
                maxEntryBytes,
                positive(broker.getNereusMaxReadEntries(), "nereusMaxReadEntries"),
                positive(broker.getNereusMaxOpenLedgers(), "nereusMaxOpenLedgers"),
                positive(broker.getNereusMaxPendingCallbacks(), "nereusMaxPendingCallbacks"),
                positive(broker.getNereusMaxRetainedAppendAttempts(), "nereusMaxRetainedAppendAttempts"),
                positive(broker.getNereusMaxScanEntries(), "nereusMaxScanEntries"));
        ProjectionMetadataStoreConfig projection = new ProjectionMetadataStoreConfig(
                metadataTimeout,
                positive(broker.getNereusMaxProjectionMetadataPendingOperations(),
                        "nereusMaxProjectionMetadataPendingOperations"),
                positive(broker.getNereusProjectionMetadataMaxValueBytes(),
                        "nereusProjectionMetadataMaxValueBytes"));
        return new NereusRuntimeConfiguration(oxia, objectStore, stream, managedLedger, projection);
    }

    public String runtimeProviderClassName() {
        return text(broker.getNereusRuntimeProviderClassName(), "nereusRuntimeProviderClassName");
    }

    public String secretResolverClassName() {
        return text(broker.getNereusObjectStoreSecretResolverClassName(),
                "nereusObjectStoreSecretResolverClassName");
    }

    private static Optional<String> optional(String value) {
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value);
    }

    private static String text(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return value;
    }

    private static int positive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static long positive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static Duration seconds(long value, String name) {
        return Duration.ofSeconds(positive(value, name));
    }

    private static Duration millis(long value, String name) {
        return Duration.ofMillis(positive(value, name));
    }
}
