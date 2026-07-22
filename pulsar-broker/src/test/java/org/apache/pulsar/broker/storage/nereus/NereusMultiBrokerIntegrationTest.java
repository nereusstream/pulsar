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
import com.nereusstream.api.StorageProfile;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivation;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationUpdate;
import com.nereusstream.managedledger.NereusManagedLedger;
import com.nereusstream.managedledger.generation.ManagedLedgerPhysicalDeletionActivationRequest;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import com.nereusstream.objectstore.S3ObjectKeyMapper;
import com.nereusstream.pulsar.BookKeeperPrimaryWalAdministration;
import com.nereusstream.pulsar.NereusProcessIdentity;
import com.nereusstream.pulsar.NereusRuntimeConfiguration;
import com.nereusstream.pulsar.Phase4ObjectWalRuntime;
import com.nereusstream.pulsar.Phase4PhysicalGcRuntime;
import io.oxia.testcontainers.OxiaContainer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.metadata.bookkeeper.BKCluster;
import org.apache.pulsar.tests.ThreadLeakDetectorListener;
import org.awaitility.Awaitility;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** Real Oxia/S3 broker ownership, failover, and process-runtime restart acceptance for Nereus. */
@Test(groups = "broker-isolated")
public class NereusMultiBrokerIntegrationTest {
    private static final int NUM_BROKERS = 2;
    private static final String CLUSTER = "nereus-multi-broker";
    private static final String TENANT = "nereus-e2e";
    private static final String NAMESPACE = TENANT + "/phase2";
    private static final String NEREUS_TOPIC = "persistent://" + NAMESPACE + "/nereus";
    private static final String BOOKKEEPER_TOPIC = "persistent://" + NAMESPACE + "/bookkeeper";
    private static final String BUCKET = "nereus-phase2";
    private static final String OBJECT_PREFIX = "broker-e2e";
    private static final String BOOKKEEPER_DEPLOYMENT = "nereus-bk-e2e";
    private static final String BOOKKEEPER_PROVIDER_SCOPE = "11".repeat(32);
    private static final String BOOKKEEPER_RESERVATION = "nereus-bk-e2e-reservation";
    private static final String BOOKKEEPER_PASSWORD_REFERENCE = "bookkeeper/password";
    private static final DockerImageName LOCALSTACK_IMAGE =
            DockerImageName.parse("localstack/localstack:4.14.0");

    private final boolean generationProtocolEnabled;
    private final boolean physicalGcEnabled;
    private final boolean bookKeeperGcEnabled;
    private final StorageProfile defaultStorageProfile;
    private final List<StorageProfile> brokerDefaultStorageProfiles;
    private final List<Boolean> brokerBookKeeperPrimaryWalEnabled;
    private final List<PulsarService> brokers = new ArrayList<>(
            Collections.nCopies(NUM_BROKERS, null));
    private final List<PulsarAdmin> admins = new ArrayList<>(
            Collections.nCopies(NUM_BROKERS, null));
    private OxiaContainer oxia;
    private LocalStackContainer localstack;
    private BKCluster bookkeeper;
    private String metadataStoreUrl;

    public NereusMultiBrokerIntegrationTest() {
        this(false);
    }

    NereusMultiBrokerIntegrationTest(boolean physicalGcEnabled) {
        this(
                physicalGcEnabled,
                physicalGcEnabled,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                false);
    }

    NereusMultiBrokerIntegrationTest(
            boolean generationProtocolEnabled,
            boolean physicalGcEnabled,
            StorageProfile defaultStorageProfile) {
        this(
                generationProtocolEnabled,
                physicalGcEnabled,
                defaultStorageProfile,
                false);
    }

    NereusMultiBrokerIntegrationTest(
            boolean generationProtocolEnabled,
            boolean physicalGcEnabled,
            StorageProfile defaultStorageProfile,
            boolean bookKeeperGcEnabled) {
        if (physicalGcEnabled && !generationProtocolEnabled) {
            throw new IllegalArgumentException(
                    "physical GC requires the generation protocol");
        }
        this.generationProtocolEnabled = generationProtocolEnabled;
        this.physicalGcEnabled = physicalGcEnabled;
        this.bookKeeperGcEnabled = bookKeeperGcEnabled;
        this.defaultStorageProfile = java.util.Objects.requireNonNull(
                defaultStorageProfile, "defaultStorageProfile");
        if (defaultStorageProfile != defaultStorageProfile.canonical()) {
            throw new IllegalArgumentException("the broker fixture requires an exact non-legacy profile");
        }
        if ((defaultStorageProfile.asyncObjectMaterialization()
                        || defaultStorageProfile
                                == StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT)
                && !generationProtocolEnabled) {
            throw new IllegalArgumentException("object materialization requires the generation protocol");
        }
        this.brokerDefaultStorageProfiles = new ArrayList<>(
                Collections.nCopies(NUM_BROKERS, defaultStorageProfile));
        this.brokerBookKeeperPrimaryWalEnabled = new ArrayList<>(
                Collections.nCopies(
                        NUM_BROKERS, defaultStorageProfile.usesBookKeeperWal()));
    }

    @BeforeClass(alwaysRun = true)
    public void startCluster() throws Exception {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            throw new SkipException("Docker is required for the Nereus multi-broker integration gate");
        }
        try {
            oxia = new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME);
            oxia.start();
            metadataStoreUrl = "oxia://" + oxia.getServiceAddress();

            localstack = new LocalStackContainer(LOCALSTACK_IMAGE)
                    .withServices(LocalStackContainer.Service.S3);
            localstack.start();
            Container.ExecResult createBucket = localstack.execInContainer(
                    "awslocal", "s3api", "create-bucket", "--bucket", BUCKET);
            assertThat(createBucket.getExitCode())
                    .withFailMessage(createBucket.getStderr())
                    .isZero();
            LocalStackSecretResolver.install(localstack.getAccessKey(), localstack.getSecretKey());

            bookkeeper = startBookKeeper();
            if (defaultStorageProfile.usesBookKeeperWal()) {
                provisionBookKeeperPrimaryWal();
            }
            startBroker(0);
            PulsarAdmin bootstrap = admins.get(0);
            bootstrap.clusters().createCluster(CLUSTER, ClusterData.builder()
                    .serviceUrl(brokers.get(0).getWebServiceAddress())
                    .brokerServiceUrl(brokers.get(0).getBrokerServiceUrl())
                    .build());
            bootstrap.tenants().createTenant(TENANT, TenantInfo.builder()
                    .allowedClusters(Set.of(CLUSTER))
                    .build());
            bootstrap.namespaces().createNamespace(NAMESPACE, NUM_BROKERS * 2);
            startBroker(1);
            awaitCapabilityConvergence();
            ThreadLeakDetectorListener.resetCapturedThreads();
        } catch (Exception | Error startupFailure) {
            closeCluster();
            throw startupFailure;
        }
    }

    @AfterClass(alwaysRun = true)
    public void closeCluster() throws Exception {
        Exception failure = null;
        for (int index = NUM_BROKERS - 1; index >= 0; index--) {
            try {
                stopBroker(index);
            } catch (Exception closeFailure) {
                failure = accumulate(failure, closeFailure);
            }
        }
        if (bookkeeper != null) {
            try {
                bookkeeper.close();
            } catch (Exception closeFailure) {
                failure = accumulate(failure, closeFailure);
            } finally {
                bookkeeper = null;
            }
        }
        if (localstack != null) {
            try {
                localstack.close();
            } catch (Exception closeFailure) {
                failure = accumulate(failure, closeFailure);
            } finally {
                localstack = null;
            }
        }
        if (oxia != null) {
            try {
                oxia.close();
            } catch (Exception closeFailure) {
                failure = accumulate(failure, closeFailure);
            } finally {
                oxia = null;
            }
        }
        LocalStackSecretResolver.clear();
        if (failure != null) {
            throw failure;
        }
    }

    @Test(timeOut = 600_000)
    public void preservesPositionsAcrossUnloadOwnerFailoverAndRuntimeRestart() throws Exception {
        PersistencePolicies nereusPolicy = new PersistencePolicies(
                1, 1, 1, 0, StorageClassBindingRecord.NEREUS);
        admins.get(0).topicPolicies().setPersistence(NEREUS_TOPIC, nereusPolicy);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(admins.get(0).topicPolicies().getPersistence(NEREUS_TOPIC))
                        .isEqualTo(nereusPolicy));
        assertThat(admins.get(0).topics().getList(NAMESPACE)).doesNotContain(NEREUS_TOPIC);

        List<ExpectedMessage> expected = new ArrayList<>();
        List<ExpectedMessage> bookkeeperExpected = new ArrayList<>();
        int stoppedOwner;
        int survivor;
        try (PulsarClient client = multiBrokerClient();
                Producer<byte[]> producer = singleEntryProducer(client, NEREUS_TOPIC)) {
            appendSingles(producer, expected, "initial", 4);
            appendBatch(client, expected, "batch", 3);
            appendBookKeeperControl(client, bookkeeperExpected, "bookkeeper-before");
            assertFacadeEntriesReadable(admins.get(0), NEREUS_TOPIC, 5);
            assertReadAll(client, NEREUS_TOPIC, expected);
            assertSavedMessageIdStarts(client, expected);
            assertReadAll(client, BOOKKEEPER_TOPIC, bookkeeperExpected);
            assertNereusOwnerAndPosition(admins.get(0), expected);

            admins.get(0).topics().unload(NEREUS_TOPIC);
            assertReadAll(client, NEREUS_TOPIC, expected);
            assertSavedMessageIdStarts(client, expected);
            appendSingles(producer, expected, "after-unload", 2);
            assertReadAll(client, NEREUS_TOPIC, expected);

            stoppedOwner = assertNereusOwnerAndPosition(admins.get(0), expected);
            survivor = otherBroker(stoppedOwner);
            stopBroker(stoppedOwner);
            awaitOwner(admins.get(survivor), NEREUS_TOPIC, survivor);

            try (PulsarClient survivorClient = PulsarClient.builder()
                    .serviceUrl(brokers.get(survivor).getBrokerServiceUrl())
                    .build();
                    Producer<byte[]> survivorProducer = singleEntryProducer(survivorClient, NEREUS_TOPIC)) {
                appendSingles(survivorProducer, expected, "after-owner-failover", 3);
                assertReadAll(survivorClient, NEREUS_TOPIC, expected);
                assertSavedMessageIdStarts(survivorClient, expected);
                assertNereusOwnerAndPosition(admins.get(survivor), expected);
                assertActiveBinding(brokers.get(survivor), NEREUS_TOPIC, StorageClassBindingRecord.NEREUS);
            }

            startBroker(stoppedOwner);
            awaitCapabilityConvergence();
        }

        stopBroker(survivor);
        awaitOwner(admins.get(stoppedOwner), NEREUS_TOPIC, stoppedOwner);
        try (PulsarClient restartedClient = PulsarClient.builder()
                .serviceUrl(brokers.get(stoppedOwner).getBrokerServiceUrl())
                .build();
                Producer<byte[]> restartedProducer = singleEntryProducer(restartedClient, NEREUS_TOPIC)) {
            appendSingles(restartedProducer, expected, "after-runtime-restart", 2);
            appendBookKeeperControl(restartedClient, bookkeeperExpected, "bookkeeper-after");
            assertReadAll(restartedClient, NEREUS_TOPIC, expected);
            assertSavedMessageIdStarts(restartedClient, expected);
            assertReadAll(restartedClient, BOOKKEEPER_TOPIC, bookkeeperExpected);
        }

        assertNereusOwnerAndPosition(admins.get(stoppedOwner), expected);
        assertActiveBinding(brokers.get(stoppedOwner), NEREUS_TOPIC, StorageClassBindingRecord.NEREUS);
        assertActiveBinding(brokers.get(stoppedOwner), BOOKKEEPER_TOPIC, StorageClassBindingRecord.BOOKKEEPER);
        assertLoadedLedgerType(brokers.get(stoppedOwner), NEREUS_TOPIC, NereusManagedLedger.class);
        assertLoadedLedgerType(brokers.get(stoppedOwner), BOOKKEEPER_TOPIC, ManagedLedgerImpl.class);
        assertVirtualLedgerAbsentFromBookKeeper(brokers.get(stoppedOwner));
        assertThat(objectCount()).isPositive();
    }

    private BKCluster startBookKeeper() throws Exception {
        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setProperty("dbStorage_writeCacheMaxSizeMb", 32);
        configuration.setProperty("dbStorage_readAheadCacheMaxSizeMb", 4);
        configuration.setProperty("dbStorage_rocksDB_writeBufferSizeMB", 4);
        configuration.setProperty("dbStorage_rocksDB_blockCacheSize", 4 * 1024 * 1024);
        configuration.setJournalSyncData(false);
        configuration.setJournalWriteData(false);
        configuration.setProperty("journalMaxGroupWaitMSec", 0L);
        configuration.setProperty("journalPreAllocSizeMB", 1);
        configuration.setFlushInterval(60_000);
        configuration.setGcWaitTime(60_000);
        configuration.setAllowLoopback(true);
        configuration.setAdvertisedAddress("127.0.0.1");
        configuration.setAllowEphemeralPorts(true);
        configuration.setNumAddWorkerThreads(0);
        configuration.setNumReadWorkerThreads(0);
        configuration.setNumHighPriorityWorkerThreads(0);
        configuration.setNumJournalCallbackThreads(0);
        configuration.setServerNumIOThreads(1);
        configuration.setNumLongPollWorkerThreads(1);
        configuration.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
        configuration.setLedgerStorageClass("org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage");
        configuration.setDiskUsageThreshold(0.999F);
        configuration.setDiskUsageWarnThreshold(0.99F);
        return BKCluster.builder()
                .baseServerConfiguration(configuration)
                .metadataServiceUri(metadataStoreUrl)
                .numBookies(1)
                .clearOldData(true)
                .build();
    }

    private void provisionBookKeeperPrimaryWal() throws Exception {
        ServiceConfiguration bootstrap = brokerConfiguration(0);
        NereusRuntimeConfiguration runtime = new NereusBrokerStorageConfiguration(bootstrap)
                .runtimeConfiguration(NereusProcessIdentity.generate(new SecureRandom()));
        try (SharedOxiaClientRuntime shared =
                SharedOxiaClientRuntime.connect(runtime.oxia(), Clock.systemUTC())) {
            BookKeeperPrimaryWalAdministration administration =
                    BookKeeperPrimaryWalAdministration.usingSharedRuntime(
                            runtime.bookKeeper().orElseThrow(),
                            runtime.oxia(),
                            shared,
                            Clock.systemUTC());
            administration.provisionNamespace("22".repeat(32), Duration.ofSeconds(30)).join();
            BookKeeperProtocolActivation prepared = administration
                    .prepareActivation(1, "33".repeat(32), Duration.ofSeconds(30))
                    .join();
            administration.activate(
                            BookKeeperProtocolActivationUpdate.publications(
                                    1,
                                    "33".repeat(32),
                                    true,
                                    true,
                                    prepared.metadataVersion()),
                            Duration.ofSeconds(30))
                    .join();
        }
    }

    void startBroker(int index) throws Exception {
        assertThat(brokers.get(index)).isNull();
        ServiceConfiguration configuration = brokerConfiguration(index);
        PulsarService broker = new PulsarService(configuration);
        try {
            broker.start();
            PulsarAdmin admin = PulsarAdmin.builder()
                    .serviceHttpUrl(broker.getWebServiceAddress())
                    .build();
            brokers.set(index, broker);
            admins.set(index, admin);
        } catch (Exception | Error startupFailure) {
            broker.close();
            throw startupFailure;
        }
    }

    private ServiceConfiguration brokerConfiguration(int brokerIndex) {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setMetadataStoreUrl(metadataStoreUrl);
        configuration.setConfigurationMetadataStoreUrl(metadataStoreUrl);
        configuration.setClusterName(CLUSTER);
        configuration.setAdvertisedAddress("localhost");
        configuration.setBrokerServicePort(Optional.of(0));
        configuration.setWebServicePort(Optional.of(0));
        configuration.setManagedLedgerDefaultEnsembleSize(1);
        configuration.setManagedLedgerDefaultWriteQuorum(1);
        configuration.setManagedLedgerDefaultAckQuorum(1);
        configuration.setDefaultNumberOfNamespaceBundles(NUM_BROKERS * 2);
        configuration.setBrokerShutdownTimeoutMs(0L);
        configuration.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        configuration.setNumExecutorThreadPoolSize(5);
        configuration.setManagedLedgerCacheSizeMB(8);
        configuration.setActiveConsumerFailoverDelayTimeMillis(0);
        configuration.setBookkeeperNumberOfChannelsPerBookie(1);
        configuration.setBookkeeperClientExposeStatsToPrometheus(false);
        configuration.setDispatcherRetryBackoffInitialTimeInMs(50);
        configuration.setDispatcherRetryBackoffMaxTimeInMs(500);
        configuration.setForceDeleteNamespaceAllowed(true);
        configuration.setForceDeleteTenantAllowed(true);
        configuration.setBrokerDeleteInactiveTopicsEnabled(false);
        configuration.setBrokerDeduplicationEnabled(false);
        configuration.setTransactionCoordinatorEnabled(false);
        configuration.setNumIOThreads(2);
        configuration.setNumOrderedExecutorThreads(2);
        configuration.setNumHttpServerThreads(4);
        configuration.setBookkeeperClientNumWorkerThreads(2);
        configuration.setBookkeeperClientNumIoThreads(2);
        configuration.setNumCacheExecutorThreadPoolSize(1);
        configuration.setManagedLedgerNumSchedulerThreads(2);
        configuration.setTopicOrderedExecutorThreadNum(4);
        configuration.setLoadBalancerEnabled(true);
        configuration.setLoadBalancerSheddingEnabled(false);
        configuration.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());

        configuration.setManagedLedgerStorageClassName(NereusManagedLedgerStorage.class.getName());
        configuration.setNereusEnabled(true);
        configuration.setNereusOxiaServiceAddress(oxia.getServiceAddress());
        configuration.setNereusOxiaNamespace("default");
        configuration.setNereusObjectStoreEndpoint(
                localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString());
        configuration.setNereusObjectStoreRegion(localstack.getRegion());
        configuration.setNereusObjectStoreBucket(BUCKET);
        configuration.setNereusObjectStorePrefix(OBJECT_PREFIX);
        configuration.setNereusObjectStorePathStyleAccess(true);
        configuration.setNereusObjectStoreAccessKeySecretRef(LocalStackSecretResolver.ACCESS_REFERENCE);
        configuration.setNereusObjectStoreSecretKeySecretRef(LocalStackSecretResolver.SECRET_REFERENCE);
        configuration.setNereusObjectStoreSecretResolverClassName(LocalStackSecretResolver.class.getName());
        configuration.setNereusTailPollIntervalMillis(50);
        configuration.setNereusAppendSessionTtlSeconds(3);
        configuration.setNereusAppendSessionRenewBeforeSeconds(1);
        configuration.setNereusAppendSessionMinCommitRemainingSeconds(1);
        StorageProfile brokerProfile = brokerDefaultStorageProfiles.get(brokerIndex);
        boolean bookKeeperPrimaryWalEnabled =
                brokerBookKeeperPrimaryWalEnabled.get(brokerIndex);
        configuration.setNereusDefaultStorageProfile(brokerProfile.name());
        if (bookKeeperPrimaryWalEnabled) {
            configuration.setNereusBookKeeperPrimaryWalEnabled(true);
            configuration.setNereusBookKeeperDeploymentId(BOOKKEEPER_DEPLOYMENT);
            configuration.setNereusBookKeeperProviderScopeSha256(BOOKKEEPER_PROVIDER_SCOPE);
            configuration.setNereusBookKeeperLedgerIdNamespaceReservationId(
                    BOOKKEEPER_RESERVATION);
            configuration.setNereusBookKeeperEnsembleSize(1);
            configuration.setNereusBookKeeperWriteQuorumSize(1);
            configuration.setNereusBookKeeperAckQuorumSize(1);
            configuration.setNereusBookKeeperPasswordSecretRef(
                    BOOKKEEPER_PASSWORD_REFERENCE);
            configuration.setNereusBookKeeperPasswordIdentityVersion("v1");
            configuration.setNereusBookKeeperOperationTimeoutSeconds(10);
            configuration.setNereusBookKeeperAllocationTimeoutSeconds(10);
            configuration.setNereusBookKeeperSealTimeoutSeconds(10);
            configuration.setNereusBookKeeperDeleteTimeoutSeconds(10);
            if (bookKeeperGcEnabled) {
                configuration.setNereusBookKeeperGcEnabled(true);
                configuration.setNereusBookKeeperGcDryRun(false);
                configuration.setNereusBookKeeperRetentionScanIntervalSeconds(3600);
            }
        }
        if (generationProtocolEnabled) {
            configuration.setNereusGenerationProtocolEnabled(true);
            configuration.setNereusGenerationRegistrationBackfillConcurrency(2);
            configuration.setNereusGenerationRegistrationBackfillTimeoutSeconds(60);
            configuration.setNereusObjectStoreRequestTimeoutSeconds(5);
            configuration.setNereusAppendTimeoutSeconds(15);
            configuration.setNereusAppendRecoveryTimeoutSeconds(10);
            configuration.setNereusAppendRecoveryAttemptTimeoutSeconds(2);
            configuration.setNereusAppendRecoveryBackoffMaxSeconds(1);
            configuration.setNereusAppendRecoveryTerminalTtlSeconds(3);
            configuration.setNereusReadTimeoutSeconds(10);
            configuration.setNereusCursorSnapshotOperationTimeoutSeconds(5);
            configuration.setNereusMaterializationRegistryScanIntervalSeconds(3600);
            configuration.setNereusMaterializationWorkerClaimSeconds(7);
            configuration.setNereusMaterializationWorkerRenewSeconds(2);
            configuration.setNereusMaterializationOperationTimeoutSeconds(5);
            configuration.setNereusMaterializationCloseTimeoutSeconds(30);
            configuration.setNereusMaterializationRetryMinMillis(100);
            configuration.setNereusMaterializationRetryMaxMillis(1000);
            configuration.setNereusMaximumClockSkewSeconds(0);
            configuration.setNereusRetentionOperationTimeoutSeconds(20);
            configuration.setNereusRetentionCloseTimeoutSeconds(30);
        }
        if (physicalGcEnabled) {
            configuration.setNereusPhysicalGcEnabled(true);
            configuration.setNereusPhysicalGcDryRun(false);
            configuration.setNereusReaderLeaseSeconds(15);
            configuration.setNereusReaderLeaseRenewSeconds(5);
            configuration.setNereusGcScanIntervalSeconds(3600);
            configuration.setNereusGcOperationTimeoutSeconds(5);
            configuration.setNereusGcCloseTimeoutSeconds(30);
            configuration.setNereusGcDrainGraceSeconds(15);
            configuration.setNereusPendingProtectionSeconds(6);
            configuration.setNereusSourceRetirementGraceSeconds(1);
            configuration.setNereusAppendReplayGraceSeconds(1);
            configuration.setNereusMaterializationMetadataAuditGraceSeconds(1);
            configuration.setNereusOrphanGraceSeconds(7);
            configuration.setNereusGcTombstoneAuditGraceSeconds(20);
        }
        return configuration;
    }

    void stopBroker(int index) throws Exception {
        PulsarAdmin admin = admins.set(index, null);
        PulsarService broker = brokers.set(index, null);
        Exception failure = null;
        if (admin != null) {
            try {
                admin.close();
            } catch (Exception closeFailure) {
                failure = closeFailure;
            }
        }
        if (broker != null) {
            try {
                broker.close();
            } catch (Exception closeFailure) {
                failure = accumulate(failure, closeFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    PulsarClient multiBrokerClient() throws Exception {
        return PulsarClient.builder()
                .serviceUrl(brokers.stream()
                        .filter(java.util.Objects::nonNull)
                        .map(PulsarService::getBrokerServiceUrl)
                        .reduce((left, right) -> left + "," + right)
                        .orElseThrow())
                .build();
    }

    private static Producer<byte[]> singleEntryProducer(PulsarClient client, String topic) throws Exception {
        return client.newProducer()
                .topic(topic)
                .enableBatching(false)
                .sendTimeout(30, TimeUnit.SECONDS)
                .create();
    }

    private static void appendSingles(
            Producer<byte[]> producer, List<ExpectedMessage> expected, String prefix, int count) throws Exception {
        for (int index = 0; index < count; index++) {
            String value = prefix + "-" + index;
            expected.add(ExpectedMessage.from(value, producer.send(bytes(value))));
        }
    }

    private static void appendBatch(
            PulsarClient client, List<ExpectedMessage> expected, String prefix, int count) throws Exception {
        try (Producer<byte[]> producer = client.newProducer()
                .topic(NEREUS_TOPIC)
                .enableBatching(true)
                .batchingMaxMessages(count)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .sendTimeout(30, TimeUnit.SECONDS)
                .create()) {
            List<String> values = new ArrayList<>();
            List<CompletableFuture<MessageId>> sends = new ArrayList<>();
            for (int index = 0; index < count; index++) {
                String value = prefix + "-" + index;
                values.add(value);
                sends.add(producer.sendAsync(bytes(value)));
            }
            producer.flush();
            for (int index = 0; index < count; index++) {
                expected.add(ExpectedMessage.from(values.get(index), sends.get(index).get()));
            }
            List<MessageIdAdv> ids = expected.subList(expected.size() - count, expected.size()).stream()
                    .map(ExpectedMessage::messageId)
                    .toList();
            assertThat(ids).extracting(MessageIdAdv::getLedgerId).containsOnly(ids.get(0).getLedgerId());
            assertThat(ids).extracting(MessageIdAdv::getEntryId).containsOnly(ids.get(0).getEntryId());
            assertThat(ids).extracting(MessageIdAdv::getBatchIndex)
                    .containsExactly(0, 1, 2);
        }
    }

    private static void appendBookKeeperControl(
            PulsarClient client, List<ExpectedMessage> expected, String value) throws Exception {
        try (Producer<byte[]> producer = singleEntryProducer(client, BOOKKEEPER_TOPIC)) {
            expected.add(ExpectedMessage.from(value, producer.send(bytes(value))));
        }
    }

    private void assertReadAll(
            PulsarClient client, String topic, List<ExpectedMessage> expected) throws Exception {
        try (Reader<byte[]> reader = client.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .create()) {
            for (ExpectedMessage expectedMessage : expected) {
                Message<byte[]> actual = reader.readNext(30, TimeUnit.SECONDS);
                assertThat(actual).withFailMessage(() -> cursorDiagnostics(topic)).isNotNull();
                assertThat(new String(actual.getData(), StandardCharsets.UTF_8))
                        .isEqualTo(expectedMessage.value());
                expectedMessage.assertSamePosition(actual.getMessageId());
            }
            assertThat(reader.readNext(250, TimeUnit.MILLISECONDS)).isNull();
        }
    }

    private void assertSavedMessageIdStarts(
            PulsarClient client, List<ExpectedMessage> expected) throws Exception {
        assertThat(expected).hasSizeGreaterThanOrEqualTo(7);
        assertReaderStartsAt(client, expected.get(0), false, expected.get(1));
        assertReaderStartsAt(client, expected.get(0), true, expected.get(0));
        assertReaderStartsAt(client, expected.get(1), false, expected.get(2));
        assertReaderStartsAt(client, expected.get(1), true, expected.get(1));
        assertReaderStartsAt(client, expected.get(5), false, expected.get(6));
        assertReaderStartsAt(client, expected.get(5), true, expected.get(5));
    }

    private void assertReaderStartsAt(
            PulsarClient client,
            ExpectedMessage start,
            boolean inclusive,
            ExpectedMessage expectedFirst) throws Exception {
        ReaderBuilder<byte[]> builder = client.newReader()
                .topic(NEREUS_TOPIC)
                .startMessageId(start.messageId());
        if (inclusive) {
            builder.startMessageIdInclusive();
        }
        try (Reader<byte[]> reader = builder.create()) {
            Message<byte[]> actual = reader.readNext(30, TimeUnit.SECONDS);
            assertThat(actual)
                    .withFailMessage(() -> "start=" + start.messageId()
                            + ", inclusive=" + inclusive + ", " + cursorDiagnostics(NEREUS_TOPIC))
                    .isNotNull();
            assertThat(new String(actual.getData(), StandardCharsets.UTF_8))
                    .isEqualTo(expectedFirst.value());
            expectedFirst.assertSamePosition(actual.getMessageId());
        }
    }

    void assertFacadeEntriesReadable(String topicName, int expectedEntries) throws Exception {
        assertFacadeEntriesReadable(admins.get(0), topicName, expectedEntries);
    }

    private void assertFacadeEntriesReadable(
            PulsarAdmin admin, String topicName, int expectedEntries) throws Exception {
        int owner = awaitOwner(admin, topicName, null);
        PersistentTopic topic = (PersistentTopic) brokers.get(owner).getBrokerService()
                .getTopicReference(topicName)
                .orElseThrow();
        assertThat(topic.getManagedLedger().getLastConfirmedEntry().getEntryId())
                .isEqualTo(expectedEntries - 1L);
        ManagedCursor cursor = topic.getManagedLedger().newNonDurableCursor(
                PositionFactory.EARLIEST, "facade-probe");
        try {
            assertThat(cursor.hasMoreEntries()).isTrue();
            List<Entry> entries = cursor.readEntries(expectedEntries);
            try {
                assertThat(entries).hasSize(expectedEntries);
                assertThat(entries).allSatisfy(entry -> assertThat(entry.getMessageMetadata()).isNotNull());
            } finally {
                entries.forEach(Entry::release);
            }
        } finally {
            cursor.close();
        }
    }

    private String cursorDiagnostics(String topicName) {
        return brokers.stream()
                .filter(java.util.Objects::nonNull)
                .map(broker -> broker.getBrokerService().getTopicReference(topicName).orElse(null))
                .filter(PersistentTopic.class::isInstance)
                .map(PersistentTopic.class::cast)
                .flatMap(topic -> {
                    List<String> cursors = new ArrayList<>();
                    topic.getManagedLedger().getCursors().forEach(cursor -> cursors.add(
                            cursor.getName() + " read=" + cursor.getReadPosition()
                                    + " markDelete=" + cursor.getMarkDeletedPosition()
                                    + " more=" + cursor.hasMoreEntries()
                                    + " closed=" + cursor.isClosed()));
                    return cursors.stream();
                })
                .collect(java.util.stream.Collectors.joining("; ", "Nereus cursors: ", ""));
    }

    private int assertNereusOwnerAndPosition(PulsarAdmin admin, List<ExpectedMessage> expected) {
        int owner = awaitOwner(admin, NEREUS_TOPIC, null);
        AtomicReference<ManagedLedger> ledger = new AtomicReference<>();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            PersistentTopic topic = (PersistentTopic) brokers.get(owner).getBrokerService()
                    .getTopicReference(NEREUS_TOPIC)
                    .orElseThrow();
            assertThat(topic.getManagedLedger()).isInstanceOf(NereusManagedLedger.class);
            ledger.set(topic.getManagedLedger());
        });
        Position lastConfirmed = ledger.get().getLastConfirmedEntry();
        assertThat(lastConfirmed.getLedgerId())
                .isEqualTo(expected.get(expected.size() - 1).messageId().getLedgerId());
        assertThat(lastConfirmed.getEntryId())
                .isEqualTo(expected.get(expected.size() - 1).messageId().getEntryId());
        return owner;
    }

    int awaitOwner(PulsarAdmin admin, String topic, Integer expectedIndex) {
        AtomicReference<Integer> owner = new AtomicReference<>();
        Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    int actual = brokerIndex(admin.lookups().lookupTopic(topic));
                    if (expectedIndex != null) {
                        assertThat(actual).isEqualTo(expectedIndex);
                    }
                    owner.set(actual);
                });
        return owner.get();
    }

    private int brokerIndex(String serviceUrl) {
        int port = URI.create(serviceUrl).getPort();
        for (int index = 0; index < brokers.size(); index++) {
            PulsarService broker = brokers.get(index);
            if (broker != null && URI.create(broker.getBrokerServiceUrl()).getPort() == port) {
                return index;
            }
        }
        throw new AssertionError("Lookup returned an unknown broker: " + serviceUrl);
    }

    private void assertActiveBinding(PulsarService broker, String topic, String storageClass) {
        NereusManagedLedgerStorage storage = (NereusManagedLedgerStorage) broker.getManagedLedgerStorage();
        String persistenceName = TopicName.get(topic).getPersistenceNamingEncoding();
        List<StorageClassBindingRecord> matches = storage.bindingStore()
                .listNonDeletedBindings(NamespaceName.get(NAMESPACE), 100, 8)
                .join().stream()
                .filter(binding -> binding.persistenceName().equals(persistenceName))
                .toList();
        assertThat(matches).singleElement().satisfies(binding -> {
            assertThat(binding.storageClass()).isEqualTo(storageClass);
            assertThat(binding.bindingGeneration()).isEqualTo(1);
            assertThat(binding.state()).isEqualTo(StorageClassBindingState.ACTIVE);
        });
    }

    private static void assertLoadedLedgerType(PulsarService broker, String topic, Class<?> expectedType) {
        PersistentTopic loaded = (PersistentTopic) broker.getBrokerService()
                .getTopicReference(topic)
                .orElseThrow();
        assertThat(loaded.getManagedLedger()).isInstanceOf(expectedType);
    }

    private static void assertVirtualLedgerAbsentFromBookKeeper(PulsarService broker) throws Exception {
        PersistentTopic loaded = (PersistentTopic) broker.getBrokerService()
                .getTopicReference(NEREUS_TOPIC)
                .orElseThrow();
        long virtualLedgerId = ((NereusManagedLedger) loaded.getManagedLedger())
                .projection().virtualLedgerId();
        try (BookKeeperAdmin admin = new BookKeeperAdmin(broker.getBookKeeperClient())) {
            List<Long> bookKeeperLedgerIds = new ArrayList<>();
            admin.listLedgers().forEach(bookKeeperLedgerIds::add);
            assertThat(bookKeeperLedgerIds).isNotEmpty();
            assertThat(bookKeeperLedgerIds).doesNotContain(virtualLedgerId);
        }
    }

    void awaitCapabilityConvergence() {
        awaitBaseCapabilityConvergence();
        Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    boolean everyLiveBrokerBookKeeperCapable = true;
                    for (int index = 0; index < brokers.size(); index++) {
                        if (brokers.get(index) != null
                                && !brokerBookKeeperPrimaryWalEnabled.get(index)) {
                            everyLiveBrokerBookKeeperCapable = false;
                            break;
                        }
                    }
                    for (PulsarService broker : brokers) {
                        if (broker == null) {
                            continue;
                        }
                        NereusManagedLedgerStorage storage =
                                (NereusManagedLedgerStorage) broker.getManagedLedgerStorage();
                        if (everyLiveBrokerBookKeeperCapable
                                && defaultStorageProfile.usesBookKeeperWal()) {
                            storage.capabilityCoordinator()
                                    .requireStorageProfileReady(defaultStorageProfile)
                                    .join();
                        }
                    }
                });
    }

    void awaitBaseCapabilityConvergence() {
        Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    for (PulsarService broker : brokers) {
                        if (broker == null) {
                            continue;
                        }
                        NereusManagedLedgerStorage storage =
                                (NereusManagedLedgerStorage) broker.getManagedLedgerStorage();
                        storage.capabilityCoordinator().requireClusterReady().join();
                        storage.capabilityCoordinator().requireCursorClusterReady().join();
                    }
                });
    }

    void setBrokerDefaultStorageProfile(int index, StorageProfile profile) {
        assertThat(brokers.get(index)).isNull();
        StorageProfile exact = java.util.Objects.requireNonNull(profile, "profile");
        assertThat(exact.canonical()).isEqualTo(exact);
        brokerDefaultStorageProfiles.set(index, exact);
    }

    void setBrokerBookKeeperPrimaryWalEnabled(int index, boolean enabled) {
        assertThat(brokers.get(index)).isNull();
        brokerBookKeeperPrimaryWalEnabled.set(index, enabled);
        if (!enabled && brokerDefaultStorageProfiles.get(index).usesBookKeeperWal()) {
            brokerDefaultStorageProfiles.set(
                    index, StorageProfile.OBJECT_WAL_SYNC_OBJECT);
        }
    }

    long objectCount() throws Exception {
        Container.ExecResult listing = localstack.execInContainer(
                "awslocal", "s3", "ls", "s3://" + BUCKET, "--recursive");
        assertThat(listing.getExitCode()).withFailMessage(listing.getStderr()).isZero();
        return listing.getStdout().lines().filter(line -> !line.isBlank()).count();
    }

    Set<String> logicalObjectKeys() throws Exception {
        Container.ExecResult listing = localstack.execInContainer(
                "awslocal",
                "s3api",
                "list-objects-v2",
                "--bucket",
                BUCKET,
                "--prefix",
                OBJECT_PREFIX + "/objects/v1/",
                "--query",
                "Contents[].Key",
                "--output",
                "text");
        assertThat(listing.getExitCode()).withFailMessage(listing.getStderr()).isZero();
        String output = listing.getStdout().trim();
        if (output.isEmpty() || output.equals("None")) {
            return Set.of();
        }
        S3ObjectKeyMapper mapper = new S3ObjectKeyMapper(OBJECT_PREFIX);
        java.util.LinkedHashSet<String> logical = new java.util.LinkedHashSet<>();
        for (String physical : output.split("\\s+")) {
            assertThat(logical.add(mapper.unmap(physical).value()))
                    .withFailMessage("duplicate mapped object key %s", physical)
                    .isTrue();
        }
        return Set.copyOf(logical);
    }

    NereusManagedLedger loadedNereusLedger(String topicName) {
        return (NereusManagedLedger) loadedPersistentTopic(topicName)
                .getManagedLedger();
    }

    PersistentTopic loadedPersistentTopic(String topicName) {
        AtomicReference<PersistentTopic> loaded = new AtomicReference<>();
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            List<PersistentTopic> liveReferences = brokers.stream()
                    .filter(java.util.Objects::nonNull)
                    .map(PulsarService::getBrokerService)
                    .map(service -> service.getTopicReference(topicName))
                    .flatMap(Optional::stream)
                    .map(PersistentTopic.class::cast)
                    .toList();
            assertThat(liveReferences).hasSize(1);
            PersistentTopic topic = liveReferences.get(0);
            assertThat(topic.getManagedLedger()).isInstanceOf(NereusManagedLedger.class);
            loaded.set(topic);
        });
        return loaded.get();
    }

    void awaitTopicUnloaded(String topicName) {
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            for (PulsarService broker : brokers) {
                if (broker != null) {
                    assertThat(broker.getBrokerService()
                                    .getTopicReference(topicName))
                            .isEmpty();
                }
            }
        });
    }

    Phase4ObjectWalRuntime materializationRuntime(String topicName) {
        return (Phase4ObjectWalRuntime) loadedNereusLedger(topicName)
                .runtime()
                .materializationRuntime();
    }

    Phase4PhysicalGcRuntime physicalGcRuntime(String topicName) {
        return (Phase4PhysicalGcRuntime) loadedNereusLedger(topicName)
                .runtime()
                .physicalGcRuntime();
    }

    GenerationRegistrationBackfillReport activateOrRolloverPhysicalDeletion(
            int brokerIndex, String runId) throws Exception {
        return activateOrRolloverGeneration(brokerIndex, runId);
    }

    GenerationRegistrationBackfillReport activateOrRolloverGeneration(
            int brokerIndex, String runId) throws Exception {
        NereusManagedLedgerStorage storage =
                (NereusManagedLedgerStorage) brokers.get(brokerIndex).getManagedLedgerStorage();
        AtomicReference<GenerationRegistrationBackfillReport> completed = new AtomicReference<>();
        Awaitility.await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> {
                    final GenerationRegistrationBackfillReport report;
                    try {
                        storage.capabilityCoordinator()
                                .requireGenerationReadiness()
                                .get(60, TimeUnit.SECONDS);
                        report = storage.runGenerationRegistrationBackfill(runId)
                                .get(120, TimeUnit.SECONDS);
                    } catch (Exception transientFailure) {
                        throw new AssertionError(
                                "generation readiness/backfill has not converged",
                                transientFailure);
                    }
                    assertThat(report.failureCount())
                            .withFailMessage(
                                    "generation registration backfill failures: %s",
                                    report.boundedFailures())
                            .isZero();
                    assertThat(report.boundedFailures()).isEmpty();
                    completed.set(report);
                });
        return completed.get();
    }

    void startPhysicalDeletionLifecycleOnEveryBroker(
            String runId, int concurrency, Duration timeout) throws Exception {
        for (PulsarService broker : brokers) {
            if (broker == null) {
                continue;
            }
            NereusManagedLedgerStorage storage =
                    (NereusManagedLedgerStorage) broker.getManagedLedgerStorage();
            storage.activatePhysicalDeletion(
                            new ManagedLedgerPhysicalDeletionActivationRequest(
                                    runId, concurrency, timeout))
                    .get(120, TimeUnit.SECONDS);
        }
    }

    void awaitGenerationCapabilityConvergence() {
        Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    for (PulsarService broker : brokers) {
                        if (broker == null) {
                            continue;
                        }
                        NereusManagedLedgerStorage storage =
                                (NereusManagedLedgerStorage) broker.getManagedLedgerStorage();
                        storage.capabilityCoordinator().requireGenerationReadiness().join();
                    }
                });
    }

    private static int otherBroker(int index) {
        return index == 0 ? 1 : 0;
    }

    PulsarService broker(int index) {
        return brokers.get(index);
    }

    PulsarAdmin admin(int index) {
        return admins.get(index);
    }

    String namespace() {
        return NAMESPACE;
    }

    String clusterName() {
        return CLUSTER;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static Exception accumulate(Exception existing, Exception added) {
        if (existing == null) {
            return added;
        }
        existing.addSuppressed(added);
        return existing;
    }

    private record ExpectedMessage(String value, MessageIdAdv messageId) {
        private static ExpectedMessage from(String value, MessageId messageId) {
            assertThat(messageId).isInstanceOf(MessageIdAdv.class);
            return new ExpectedMessage(value, (MessageIdAdv) messageId);
        }

        private void assertSamePosition(MessageId actual) {
            assertThat(actual).isInstanceOf(MessageIdAdv.class);
            MessageIdAdv actualPosition = (MessageIdAdv) actual;
            assertThat(actualPosition.getLedgerId()).isEqualTo(messageId.getLedgerId());
            assertThat(actualPosition.getEntryId()).isEqualTo(messageId.getEntryId());
            assertThat(actualPosition.getPartitionIndex()).isEqualTo(messageId.getPartitionIndex());
            assertThat(actualPosition.getBatchIndex()).isEqualTo(messageId.getBatchIndex());
        }
    }

    /** Test-only explicit secret resolver; each resolve returns fresh memory that the provider may zero. */
    public static final class LocalStackSecretResolver implements ObjectStoreSecretResolver {
        static final String ACCESS_REFERENCE = "localstack/access";
        static final String SECRET_REFERENCE = "localstack/secret";
        static final String BOOKKEEPER_PASSWORD_REFERENCE =
                NereusMultiBrokerIntegrationTest.BOOKKEEPER_PASSWORD_REFERENCE;
        private static volatile String accessKey;
        private static volatile String secretKey;

        static void install(String access, String secret) {
            accessKey = access;
            secretKey = secret;
        }

        static void clear() {
            accessKey = null;
            secretKey = null;
        }

        @Override
        public Optional<char[]> resolve(String secretReference) {
            return switch (secretReference) {
                case ACCESS_REFERENCE -> Optional.ofNullable(accessKey).map(String::toCharArray);
                case SECRET_REFERENCE -> Optional.ofNullable(secretKey).map(String::toCharArray);
                case BOOKKEEPER_PASSWORD_REFERENCE -> Optional.of("nereus-bk-e2e".toCharArray());
                default -> Optional.empty();
            };
        }
    }
}
