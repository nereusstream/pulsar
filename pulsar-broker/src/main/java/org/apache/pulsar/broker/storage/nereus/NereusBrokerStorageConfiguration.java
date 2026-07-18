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

import com.nereusstream.api.StorageProfile;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.managedledger.NereusManagedLedgerFactoryConfig;
import com.nereusstream.managedledger.cursor.CursorStorageConfig;
import com.nereusstream.managedledger.retention.NereusRetentionConfig;
import com.nereusstream.materialization.MaterializationConfig;
import com.nereusstream.materialization.MaterializationPolicyFactory;
import com.nereusstream.metadata.oxia.CursorMetadataStoreConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.ProjectionMetadataStoreConfig;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import com.nereusstream.pulsar.NereusProcessIdentity;
import com.nereusstream.pulsar.NereusRuntimeConfiguration;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
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
        requireBrokerIntegration();
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
                positive(broker.getNereusMaxScanEntries(), "nereusMaxScanEntries"),
                storageProfile(broker.getNereusDefaultStorageProfile()));
        ProjectionMetadataStoreConfig projection = new ProjectionMetadataStoreConfig(
                metadataTimeout,
                positive(broker.getNereusMaxProjectionMetadataPendingOperations(),
                        "nereusMaxProjectionMetadataPendingOperations"),
                positive(broker.getNereusProjectionMetadataMaxValueBytes(),
                        "nereusProjectionMetadataMaxValueBytes"));
        CursorMetadataStoreConfig cursorMetadata = new CursorMetadataStoreConfig(
                metadataTimeout,
                positive(broker.getNereusMaxCursorMetadataPendingOperations(),
                        "nereusMaxCursorMetadataPendingOperations"),
                positive(broker.getNereusCursorMetadataMaxValueBytes(),
                        "nereusCursorMetadataMaxValueBytes"),
                positive(broker.getNereusCursorMetadataMaxScanPageSize(),
                        "nereusCursorMetadataMaxScanPageSize"));
        CursorStorageConfig cursorStorage = new CursorStorageConfig(
                positive(broker.getNereusCursorMetadataMaxValueBytes(),
                        "nereusCursorMetadataMaxValueBytes"),
                positive(broker.getNereusCursorMetadataSafetyMarginBytes(),
                        "nereusCursorMetadataSafetyMarginBytes"),
                positive(broker.getNereusCursorInlineAckMaxBytes(), "nereusCursorInlineAckMaxBytes"),
                positive(broker.getNereusCursorInlineDeltaMaxCount(), "nereusCursorInlineDeltaMaxCount"),
                positive(broker.getNereusCursorNameMaxUtf8Bytes(), "nereusCursorNameMaxUtf8Bytes"),
                positive(broker.getNereusCursorPositionPropertiesMaxBytes(),
                        "nereusCursorPositionPropertiesMaxBytes"),
                positive(broker.getNereusCursorPropertiesMaxBytes(), "nereusCursorPropertiesMaxBytes"),
                positive(broker.getNereusCursorSnapshotMaxBytes(), "nereusCursorSnapshotMaxBytes"),
                positive(broker.getNereusCursorAckPositionsPerRequestMax(),
                        "nereusCursorAckPositionsPerRequestMax"),
                positive(broker.getNereusCursorBatchIndexesMax(), "nereusCursorBatchIndexesMax"),
                positive(broker.getNereusCursorProtectionIntentMaxBytes(),
                        "nereusCursorProtectionIntentMaxBytes"),
                positive(broker.getNereusCursorTrimReasonMaxUtf8Bytes(),
                        "nereusCursorTrimReasonMaxUtf8Bytes"),
                positive(broker.getNereusCursorScanPageSize(), "nereusCursorScanPageSize"),
                positive(broker.getNereusCursorRecordsPerStreamMax(), "nereusCursorRecordsPerStreamMax"),
                positive(broker.getNereusCursorOwnerClaimConcurrency(),
                        "nereusCursorOwnerClaimConcurrency"),
                positive(broker.getNereusCursorMutationQueueMax(), "nereusCursorMutationQueueMax"),
                positive(broker.getNereusCursorMaxCasAttempts(), "nereusCursorMaxCasAttempts"),
                positive(broker.getNereusCursorHydrationMaxAttempts(), "nereusCursorHydrationMaxAttempts"),
                positive(broker.getNereusCursorSnapshotIdMaxAttempts(),
                        "nereusCursorSnapshotIdMaxAttempts"),
                metadataTimeout,
                seconds(broker.getNereusCursorSnapshotOperationTimeoutSeconds(),
                        "nereusCursorSnapshotOperationTimeoutSeconds"));
        Path stagingBase = absolutePath(
                broker.getNereusMaterializationStagingDirectory(),
                "nereusMaterializationStagingDirectory");
        MaterializationConfig materialization = new MaterializationConfig(
                MaterializationPolicyFactory.losslessCommitted(
                        positive(broker.getNereusMaterializationMinMergeSourceRanges(),
                                "nereusMaterializationMinMergeSourceRanges"),
                        positive(broker.getNereusMaterializationMaxSourceRanges(),
                                "nereusMaterializationMaxSourceRanges"),
                        positive(broker.getNereusMaterializationMaxRangeRecords(),
                                "nereusMaterializationMaxRangeRecords"),
                        positive(broker.getNereusMaterializationTargetObjectBytes(),
                                "nereusMaterializationTargetObjectBytes"),
                        positive(broker.getNereusMaterializationTargetRowGroupRecords(),
                                "nereusMaterializationTargetRowGroupRecords"),
                        text(broker.getNereusMaterializationCompression(),
                                "nereusMaterializationCompression")),
                positiveAtMost(
                        broker.getNereusMaterializationRegistryScanPageSize(),
                        256,
                        "nereusMaterializationRegistryScanPageSize"),
                seconds(broker.getNereusMaterializationRegistryScanIntervalSeconds(),
                        "nereusMaterializationRegistryScanIntervalSeconds"),
                positiveAtMost(
                        broker.getNereusMaterializationPlannerPageSize(),
                        512,
                        "nereusMaterializationPlannerPageSize"),
                positiveAtMost(
                        broker.getNereusMaterializationTaskScanPageSize(),
                        256,
                        "nereusMaterializationTaskScanPageSize"),
                positive(broker.getNereusMaterializationMaxTasksPerPlan(),
                        "nereusMaterializationMaxTasksPerPlan"),
                positive(broker.getNereusMaterializationMaxWorkers(),
                        "nereusMaterializationMaxWorkers"),
                positive(broker.getNereusMaterializationMaxWorkersPerStream(),
                        "nereusMaterializationMaxWorkersPerStream"),
                positive(broker.getNereusMaterializationSourceReadPageRecords(),
                        "nereusMaterializationSourceReadPageRecords"),
                positive(broker.getNereusMaterializationSourceReadPageBytes(),
                        "nereusMaterializationSourceReadPageBytes"),
                stagingBase.resolve(identity.processRunId()).normalize(),
                positive(broker.getNereusMaterializationMaxStagingBytes(),
                        "nereusMaterializationMaxStagingBytes"),
                positive(broker.getNereusObjectUploadChunkBytes(),
                        "nereusObjectUploadChunkBytes"),
                seconds(broker.getNereusMaterializationWorkerClaimSeconds(),
                        "nereusMaterializationWorkerClaimSeconds"),
                seconds(broker.getNereusMaterializationWorkerRenewSeconds(),
                        "nereusMaterializationWorkerRenewSeconds"),
                nonNegativeSeconds(broker.getNereusMaximumClockSkewSeconds(),
                        "nereusMaximumClockSkewSeconds"),
                seconds(broker.getNereusMaterializationOperationTimeoutSeconds(),
                        "nereusMaterializationOperationTimeoutSeconds"),
                seconds(broker.getNereusMaterializationCloseTimeoutSeconds(),
                        "nereusMaterializationCloseTimeoutSeconds"),
                millis(broker.getNereusMaterializationRetryMinMillis(),
                        "nereusMaterializationRetryMinMillis"),
                millis(broker.getNereusMaterializationRetryMaxMillis(),
                        "nereusMaterializationRetryMaxMillis"),
                positive(broker.getNereusMaterializationMaxTaskAttempts(),
                        "nereusMaterializationMaxTaskAttempts"),
                nonNegative(broker.getNereusMaterializationLagThrottleRecords(),
                        "nereusMaterializationLagThrottleRecords"),
                nonNegative(broker.getNereusMaterializationLagRejectRecords(),
                        "nereusMaterializationLagRejectRecords"),
                nonNegative(broker.getNereusMaterializationLagThrottleBytes(),
                        "nereusMaterializationLagThrottleBytes"),
                nonNegative(broker.getNereusMaterializationLagRejectBytes(),
                        "nereusMaterializationLagRejectBytes"),
                nonNegativeSeconds(broker.getNereusMaterializationLagRejectAgeSeconds(),
                        "nereusMaterializationLagRejectAgeSeconds"),
                millis(broker.getNereusMaterializationLagThrottleDelayMillis(),
                        "nereusMaterializationLagThrottleDelayMillis"),
                seconds(broker.getNereusSourceRetirementGraceSeconds(),
                        "nereusSourceRetirementGraceSeconds"),
                seconds(broker.getNereusAppendReplayGraceSeconds(),
                        "nereusAppendReplayGraceSeconds"),
                seconds(broker.getNereusMaterializationMetadataAuditGraceSeconds(),
                        "nereusMaterializationMetadataAuditGraceSeconds"),
                positive(broker.getNereusRecoveryCheckpointMaxEntries(),
                        "nereusRecoveryCheckpointMaxEntries"),
                positive(broker.getNereusRecoveryCheckpointMaxBytes(),
                        "nereusRecoveryCheckpointMaxBytes"));
        NereusRetentionConfig retention = new NereusRetentionConfig(
                positiveAtMost(
                        broker.getNereusRetentionStatsScanPageSize(),
                        NereusRetentionConfig.MAX_STATS_SCAN_PAGE_SIZE,
                        "nereusRetentionStatsScanPageSize"),
                positive(
                        broker.getNereusRetentionMaxConcurrentPlans(),
                        "nereusRetentionMaxConcurrentPlans"),
                positive(
                        broker.getNereusRetentionMaxQueuedPlans(),
                        "nereusRetentionMaxQueuedPlans"),
                seconds(
                        broker.getNereusRetentionOperationTimeoutSeconds(),
                        "nereusRetentionOperationTimeoutSeconds"),
                seconds(
                        broker.getNereusRetentionCloseTimeoutSeconds(),
                        "nereusRetentionCloseTimeoutSeconds"));
        return new NereusRuntimeConfiguration(
                oxia,
                objectStore,
                stream,
                managedLedger,
                projection,
                cursorMetadata,
                cursorStorage,
                materialization,
                retention);
    }

    public String runtimeProviderClassName() {
        return text(broker.getNereusRuntimeProviderClassName(), "nereusRuntimeProviderClassName");
    }

    public String secretResolverClassName() {
        return text(broker.getNereusObjectStoreSecretResolverClassName(),
                "nereusObjectStoreSecretResolverClassName");
    }

    public int generationRegistrationBackfillConcurrency() {
        return positive(
                broker.getNereusGenerationRegistrationBackfillConcurrency(),
                "nereusGenerationRegistrationBackfillConcurrency");
    }

    public Duration generationRegistrationBackfillTimeout() {
        return seconds(
                broker.getNereusGenerationRegistrationBackfillTimeoutSeconds(),
                "nereusGenerationRegistrationBackfillTimeoutSeconds");
    }

    public boolean generationProtocolEnabled() {
        return broker.isNereusGenerationProtocolEnabled();
    }

    public int generationRegistrationBackfillMaxTopicsPerNamespace() {
        return positive(
                broker.getNereusMaxNamespaceBindingScanEntries(),
                "nereusMaxNamespaceBindingScanEntries");
    }

    private void requireBrokerIntegration() {
        if (!broker.isEnablePersistentTopics()) {
            throw new IllegalArgumentException("enablePersistentTopics must be true for Nereus hybrid storage");
        }
        String className = text(broker.getLoadManagerClassName(), "loadManagerClassName");
        Class<?> loadManagerClass;
        try {
            loadManagerClass = Class.forName(
                    className, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException error) {
            throw new IllegalArgumentException("configured loadManagerClassName cannot be loaded", error);
        }
        if (!ExtensibleLoadManagerImpl.class.isAssignableFrom(loadManagerClass)) {
            throw new IllegalArgumentException(
                    "Nereus hybrid storage requires ExtensibleLoadManagerImpl");
        }
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

    private static int positiveAtMost(
            int value,
            int maximum,
            String name) {
        int exact = positive(value, name);
        if (exact > maximum) {
            throw new IllegalArgumentException(
                    name + " must be at most " + maximum);
        }
        return exact;
    }

    private static long positive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static long nonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private static Duration seconds(long value, String name) {
        return Duration.ofSeconds(positive(value, name));
    }

    private static Duration millis(long value, String name) {
        return Duration.ofMillis(positive(value, name));
    }

    private static Duration nonNegativeSeconds(long value, String name) {
        return Duration.ofSeconds(nonNegative(value, name));
    }

    private static StorageProfile storageProfile(String value) {
        String exact = text(value, "nereusDefaultStorageProfile");
        final StorageProfile profile;
        try {
            profile = StorageProfile.valueOf(exact);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "nereusDefaultStorageProfile is unknown",
                    failure);
        }
        if (profile != StorageProfile.OBJECT_WAL_SYNC_OBJECT
                && profile
                        != StorageProfile.OBJECT_WAL_ASYNC_OBJECT) {
            throw new IllegalArgumentException(
                    "nereusDefaultStorageProfile must be an exact Object-WAL profile");
        }
        return profile;
    }

    private static Path absolutePath(String value, String name) {
        Path path = Path.of(text(value, name))
                .normalize();
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException(
                    name + " must be absolute");
        }
        return path;
    }
}
