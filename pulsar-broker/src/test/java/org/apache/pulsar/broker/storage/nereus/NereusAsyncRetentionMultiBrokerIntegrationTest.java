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
import com.nereusstream.managedledger.NereusManagedLedger;
import com.nereusstream.managedledger.retention.RetentionPolicySnapshot;
import com.nereusstream.objectstore.wal.WalObjectKeys;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** Real async Object-WAL, cold registration, logical retention, and capability-rollover acceptance. */
@Test(groups = "broker-isolated")
public class NereusAsyncRetentionMultiBrokerIntegrationTest {
    private static final String INITIAL_ROLLOUT_RUN =
            "phasefourmfiveinitialrollout";
    private static final String FAILOVER_ROLLOUT_RUN =
            "phasefourmfivefailoverrollout";
    private static final String REJOIN_ROLLOUT_RUN =
            "phasefourmfiverejoinrollout";
    private static final String FINAL_ROLLOUT_RUN =
            "phasefourmfivefinalrollout";
    private static final String SUBSCRIPTION = "phase4-m5-backlog";

    private final NereusMultiBrokerIntegrationTest cluster =
            new NereusMultiBrokerIntegrationTest(
                    true,
                    false,
                    StorageProfile.OBJECT_WAL_ASYNC_OBJECT);

    @BeforeClass(alwaysRun = true)
    public void startCluster() throws Exception {
        cluster.startCluster();
    }

    @AfterClass(alwaysRun = true)
    public void closeCluster() throws Exception {
        cluster.closeCluster();
    }

    @Test(timeOut = 900_000)
    public void repairsAsyncHistoryAndLogicallyTrimsEvictedBacklogAcrossOwnershipCuts()
            throws Exception {
        String topic = "persistent://" + cluster.namespace()
                + "/phase4-async-retention";
        String bookKeeperTopic = "persistent://" + cluster.namespace()
                + "/phase4-async-retention-bookkeeper";
        PersistencePolicies nereusPolicy = new PersistencePolicies(
                1, 1, 1, 0, StorageClassBindingRecord.NEREUS);
        cluster.admin(0).topicPolicies().setPersistence(topic, nereusPolicy);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(cluster.admin(0).topicPolicies().getPersistence(topic))
                        .isEqualTo(nereusPolicy));
        cluster.admin(0).topics().createNonPartitionedTopic(topic);

        cluster.awaitGenerationCapabilityConvergence();
        try (PulsarClient client = cluster.multiBrokerClient();
                Producer<byte[]> ignored = client.newProducer()
                        .topic(topic)
                        .enableBatching(false)
                        .create()) {
            assertThat(cluster.loadedNereusLedger(topic)
                            .currentMetadata()
                            .profile())
                    .isEqualTo(StorageProfile.OBJECT_WAL_ASYNC_OBJECT);
        }
        cluster.admin(0).topics().unload(topic);
        cluster.awaitTopicUnloaded(topic);
        String persistenceName = TopicName.get(topic)
                .getPersistenceNamingEncoding();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(((NereusManagedLedgerStorage) cluster.broker(0)
                                .getManagedLedgerStorage())
                                .bindingStore()
                                .listNonDeletedBindings(
                                        NamespaceName.get(cluster.namespace()),
                                        100,
                                        8)
                                .join())
                        .anySatisfy(binding -> {
                            assertThat(binding.persistenceName())
                                    .isEqualTo(persistenceName);
                            assertThat(binding.storageClass())
                                    .isEqualTo(StorageClassBindingRecord.NEREUS);
                            assertThat(binding.state())
                                    .isEqualTo(StorageClassBindingState.ACTIVE);
                        }));

        GenerationRegistrationBackfillReport initial =
                cluster.activateOrRolloverGeneration(
                        0, INITIAL_ROLLOUT_RUN);
        assertThat(initial.failureCount()).isZero();
        assertThat(initial.nereusProjectionsRegistered())
                .withFailMessage("cold-topic backfill report: %s", initial)
                .isPositive();

        List<ExpectedMessage> original = new ArrayList<>();
        List<ExpectedMessage> bookKeeper = new ArrayList<>();
        int stoppedOwner;
        int survivor;
        try (PulsarClient client = cluster.multiBrokerClient();
                Consumer<byte[]> consumer = client.newConsumer()
                        .topic(topic)
                        .subscriptionName(SUBSCRIPTION)
                        .subscriptionInitialPosition(
                                SubscriptionInitialPosition.Earliest)
                        .subscribe()) {
            appendSingles(client, topic, original, "async-before-failover", 4);
            appendCompressedBatch(client, topic, original, "async-batch", 3);
            appendSingles(client, bookKeeperTopic, bookKeeper, "bookkeeper-before", 1);
            assertConsumedWithoutAcknowledgement(consumer, original);
            assertReadAll(client, topic, original);
            assertReadAll(client, bookKeeperTopic, bookKeeper);

            NereusManagedLedger ledger = cluster.loadedNereusLedger(topic);
            assertThat(ledger.currentMetadata().profile())
                    .isEqualTo(StorageProfile.OBJECT_WAL_ASYNC_OBJECT);
            assertThat(ledger.currentMetadata().committedEndOffset())
                    .isEqualTo(5);
            assertThat(requireCursor(ledger).getNumberOfEntriesInBacklog(false))
                    .isEqualTo(5);
            stoppedOwner = cluster.awaitOwner(cluster.admin(0), topic, null);
            survivor = otherBroker(stoppedOwner);
        }

        cluster.stopBroker(stoppedOwner);
        cluster.awaitGenerationCapabilityConvergence();
        cluster.activateOrRolloverGeneration(
                survivor, FAILOVER_ROLLOUT_RUN);
        cluster.awaitOwner(cluster.admin(survivor), topic, survivor);
        try (PulsarClient survivorClient = PulsarClient.builder()
                .serviceUrl(cluster.broker(survivor).getBrokerServiceUrl())
                .build()) {
            assertReadAll(survivorClient, topic, original);
            assertReadAll(survivorClient, bookKeeperTopic, bookKeeper);
        }

        cluster.startBroker(stoppedOwner);
        cluster.awaitCapabilityConvergence();
        cluster.awaitGenerationCapabilityConvergence();
        cluster.activateOrRolloverGeneration(
                survivor, REJOIN_ROLLOUT_RUN);

        BacklogQuota quota = BacklogQuota.builder()
                .limitSize(1)
                .limitTime(-1)
                .retentionPolicy(
                        BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        cluster.admin(survivor).topicPolicies().setRetention(
                topic, new RetentionPolicies(0, 0));
        cluster.admin(survivor).topicPolicies().setBacklogQuota(
                topic, quota, BacklogQuotaType.destination_storage);
        RetentionPolicySnapshot noPostConsumeRetention =
                RetentionPolicySnapshot
                        .fromCanonicalMinutesAndMebibytes(0, 0);
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            PersistentTopic loaded = cluster.loadedPersistentTopic(topic);
            BacklogQuota installed = loaded.getBacklogQuota(
                    BacklogQuotaType.destination_storage);
            assertThat(installed.getLimitSize()).isEqualTo(1);
            assertThat(installed.getLimitTime()).isEqualTo(-1);
            assertThat(installed.getPolicy())
                    .isEqualTo(BacklogQuota.RetentionPolicy
                            .consumer_backlog_eviction);
            assertThat(((NereusManagedLedger) loaded.getManagedLedger())
                            .retentionPolicy())
                    .contains(noPostConsumeRetention);
        });

        int evictionOwner = cluster.awaitOwner(
                cluster.admin(survivor), topic, null);
        PersistentTopic evictionTopic = cluster.loadedPersistentTopic(topic);
        NereusManagedLedger evictionLedger =
                (NereusManagedLedger) evictionTopic.getManagedLedger();
        ManagedCursor evictedCursor = requireCursor(evictionLedger);
        assertThat(evictionLedger.getEstimatedBacklogSize()).isPositive();
        assertThat(evictedCursor.getNumberOfEntriesInBacklog(false))
                .isPositive();
        cluster.broker(evictionOwner)
                .getBrokerService()
                .getBacklogQuotaManager()
                .handleExceededBacklogQuota(
                        evictionTopic,
                        BacklogQuotaType.destination_storage,
                        false);
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(evictedCursor.getNumberOfEntriesInBacklog(false))
                    .isZero();
            assertThat(evictedCursor.getMarkDeletedPosition().getEntryId())
                    .isEqualTo(4);
        });

        long trimTarget = evictionLedger.refreshMetadata()
                .get(30, TimeUnit.SECONDS)
                .committedEndOffset();
        assertThat(trimTarget).isEqualTo(5);
        String walPrefix = WalObjectKeys.prefix(
                cluster.clusterName()).value();
        Set<String> retainedWal = cluster.logicalObjectKeys().stream()
                .filter(key -> key.startsWith(walPrefix))
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        assertThat(retainedWal).hasSizeGreaterThanOrEqualTo(5);

        cluster.admin(evictionOwner).topics().unload(topic);
        cluster.awaitTopicUnloaded(topic);
        cluster.admin(evictionOwner).topics().trimTopic(topic);
        NereusManagedLedger trimmedLedger = cluster.loadedNereusLedger(topic);
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(trimmedLedger.refreshMetadata()
                                .get(30, TimeUnit.SECONDS)
                                .trimOffset())
                        .isEqualTo(trimTarget));
        assertThat(cluster.logicalObjectKeys()).containsAll(retainedWal);

        List<ExpectedMessage> retained = new ArrayList<>();
        try (PulsarClient client = cluster.multiBrokerClient()) {
            appendSingles(client, topic, retained, "after-logical-trim", 2);
            appendCompressedBatch(client, topic, retained, "retained-batch", 3);
            assertReadAll(client, topic, retained);
            assertReadAll(client, bookKeeperTopic, bookKeeper);
        }

        int finalStoppedOwner = cluster.awaitOwner(
                cluster.admin(evictionOwner), topic, null);
        int finalSurvivor = otherBroker(finalStoppedOwner);
        cluster.stopBroker(finalStoppedOwner);
        cluster.awaitGenerationCapabilityConvergence();
        cluster.activateOrRolloverGeneration(
                finalSurvivor, FINAL_ROLLOUT_RUN);
        cluster.awaitOwner(
                cluster.admin(finalSurvivor), topic, finalSurvivor);
        try (PulsarClient finalClient = PulsarClient.builder()
                .serviceUrl(cluster.broker(finalSurvivor)
                        .getBrokerServiceUrl())
                .build()) {
            assertReadAll(finalClient, topic, retained);
            appendSingles(
                    finalClient,
                    bookKeeperTopic,
                    bookKeeper,
                    "bookkeeper-after",
                    1);
            assertReadAll(finalClient, bookKeeperTopic, bookKeeper);
        }
        assertThat(cluster.logicalObjectKeys()).containsAll(retainedWal);
    }

    private static void appendSingles(
            PulsarClient client,
            String topic,
            List<ExpectedMessage> expected,
            String prefix,
            int count) throws Exception {
        try (Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .enableBatching(false)
                .sendTimeout(30, TimeUnit.SECONDS)
                .create()) {
            for (int index = 0; index < count; index++) {
                String value = prefix + "-" + index;
                Map<String, String> properties = Map.of(
                        "kind", "single", "sequence", Integer.toString(index));
                MessageId id = producer.newMessage()
                        .properties(properties)
                        .value(bytes(value))
                        .send();
                expected.add(ExpectedMessage.from(value, properties, id));
            }
        }
    }

    private static void appendCompressedBatch(
            PulsarClient client,
            String topic,
            List<ExpectedMessage> expected,
            String prefix,
            int count) throws Exception {
        try (Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .enableBatching(true)
                .compressionType(CompressionType.LZ4)
                .batchingMaxMessages(count)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .sendTimeout(30, TimeUnit.SECONDS)
                .create()) {
            List<String> values = new ArrayList<>();
            List<Map<String, String>> properties = new ArrayList<>();
            List<CompletableFuture<MessageId>> sends = new ArrayList<>();
            for (int index = 0; index < count; index++) {
                String value = prefix + "-" + index;
                Map<String, String> messageProperties = Map.of(
                        "kind", "compressed-batch",
                        "sequence", Integer.toString(index));
                values.add(value);
                properties.add(messageProperties);
                sends.add(producer.newMessage()
                        .properties(messageProperties)
                        .value(bytes(value))
                        .sendAsync());
            }
            producer.flush();
            for (int index = 0; index < count; index++) {
                expected.add(ExpectedMessage.from(
                        values.get(index),
                        properties.get(index),
                        sends.get(index).get()));
            }
            List<MessageIdAdv> ids = expected
                    .subList(expected.size() - count, expected.size())
                    .stream()
                    .map(ExpectedMessage::messageId)
                    .toList();
            assertThat(ids).extracting(MessageIdAdv::getLedgerId)
                    .containsOnly(ids.get(0).getLedgerId());
            assertThat(ids).extracting(MessageIdAdv::getEntryId)
                    .containsOnly(ids.get(0).getEntryId());
            assertThat(ids).extracting(MessageIdAdv::getBatchIndex)
                    .containsExactly(0, 1, 2);
        }
    }

    private static void assertConsumedWithoutAcknowledgement(
            Consumer<byte[]> consumer,
            List<ExpectedMessage> expected) throws Exception {
        for (ExpectedMessage expectedMessage : expected) {
            Message<byte[]> actual = consumer.receive(30, TimeUnit.SECONDS);
            assertExact(actual, expectedMessage);
        }
        assertThat(consumer.receive(250, TimeUnit.MILLISECONDS)).isNull();
    }

    private static void assertReadAll(
            PulsarClient client,
            String topic,
            List<ExpectedMessage> expected) throws Exception {
        try (Reader<byte[]> reader = client.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .create()) {
            for (ExpectedMessage expectedMessage : expected) {
                assertExact(
                        reader.readNext(30, TimeUnit.SECONDS),
                        expectedMessage);
            }
            assertThat(reader.readNext(250, TimeUnit.MILLISECONDS)).isNull();
        }
    }

    private static void assertExact(
            Message<byte[]> actual,
            ExpectedMessage expected) {
        assertThat(actual).isNotNull();
        assertThat(new String(actual.getData(), StandardCharsets.UTF_8))
                .isEqualTo(expected.value());
        assertThat(actual.getProperties()).isEqualTo(expected.properties());
        expected.assertSamePosition(actual.getMessageId());
    }

    private static ManagedCursor requireCursor(
            NereusManagedLedger ledger) {
        for (ManagedCursor cursor : ledger.getCursors()) {
            if (SUBSCRIPTION.equals(cursor.getName())) {
                return cursor;
            }
        }
        throw new AssertionError(
                "durable backlog cursor is not loaded: " + SUBSCRIPTION);
    }

    private static int otherBroker(int index) {
        return index == 0 ? 1 : 0;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private record ExpectedMessage(
            String value,
            Map<String, String> properties,
            MessageIdAdv messageId) {
        private ExpectedMessage {
            properties = Map.copyOf(properties);
        }

        private static ExpectedMessage from(
                String value,
                Map<String, String> properties,
                MessageId messageId) {
            assertThat(messageId).isInstanceOf(MessageIdAdv.class);
            return new ExpectedMessage(
                    value, properties, (MessageIdAdv) messageId);
        }

        private void assertSamePosition(MessageId actual) {
            assertThat(actual).isInstanceOf(MessageIdAdv.class);
            MessageIdAdv exact = (MessageIdAdv) actual;
            assertThat(exact.getLedgerId()).isEqualTo(messageId.getLedgerId());
            assertThat(exact.getEntryId()).isEqualTo(messageId.getEntryId());
            assertThat(exact.getPartitionIndex())
                    .isEqualTo(messageId.getPartitionIndex());
            assertThat(exact.getBatchIndex())
                    .isEqualTo(messageId.getBatchIndex());
        }
    }
}
