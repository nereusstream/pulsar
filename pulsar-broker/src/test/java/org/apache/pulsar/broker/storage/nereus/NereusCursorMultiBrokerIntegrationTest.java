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

import static org.apache.bookkeeper.mledger.ManagedCursor.CURSOR_INTERNAL_PROPERTY_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.managedledger.NereusManagedLedger;
import com.nereusstream.managedledger.projection.VirtualLedgerProjection;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.TopicPoliciesService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** Real two-broker durable cursor recovery and storage-class coexistence acceptance for Nereus. */
@Test(groups = "broker-isolated")
public class NereusCursorMultiBrokerIntegrationTest {
    private static final int RECEIVE_TIMEOUT_SECONDS = 30;
    private static final int NO_MESSAGE_TIMEOUT_MILLIS = 500;

    private final NereusMultiBrokerIntegrationTest cluster = new NereusMultiBrokerIntegrationTest();

    @BeforeClass(alwaysRun = true)
    public void startCluster() throws Exception {
        cluster.startCluster();
    }

    @AfterClass(alwaysRun = true)
    public void closeCluster() throws Exception {
        cluster.closeCluster();
    }

    @Test(timeOut = 900_000)
    public void preservesDurableCursorTruthAcrossUnloadFailoverRestartExpiryAndBookKeeper() throws Exception {
        String runId = UUID.randomUUID().toString();
        assertExclusiveEarliestLatestAndUnload(topic(runId, "exclusive"));
        assertFailoverRedeliversIdenticalMessageIds(topic(runId, "failover"));
        assertSharedKeepsExactIndividualAckHoles(topic(runId, "shared"));
        assertPartialBatchSurvivesFailoverAndRuntimeRestart(topic(runId, "batch"));
        assertExpiryIsCursorOnly(topic(runId, "expiry"));
        assertBookKeeperSubscriptionRemainsStock(topic(runId, "bookkeeper"));
    }

    @Test(timeOut = 900_000)
    public void preservesMessageIdsPropertiesAndIncarnationAcrossCompatibilityCuts() throws Exception {
        String runId = UUID.randomUUID().toString();
        assertMessageIdsSurviveSeekHistoryUnloadFailoverAndRestart(topic(runId, "message-id"));
        assertCursorPropertiesSurvivePeerOwnershipAndRestart(topic(runId, "properties"));
        assertLoadedUnloadedAndNamespaceAdminRoutes(topic(runId, "admin"), topic(runId, "shadow"));
        assertPartitionedAdminRoutes(topic(runId, "partitioned-admin"), topic(runId, "partitioned-shadow"));
        assertTopicRecreationUsesFreshProjectionAndCursorTruth(topic(runId, "recreate"));
    }

    private void assertMessageIdsSurviveSeekHistoryUnloadFailoverAndRestart(String topic) throws Exception {
        configureNereus(topic);
        ExpectedMessage ordinary;
        ExpectedMessage middleBatch;
        try (PulsarClient client = cluster.multiBrokerClient();
                Producer<byte[]> producer = producer(client, topic)) {
            ordinary = appendSingles(producer, "message-id-single", 4).get(1);
            middleBatch = appendBatch(client, topic, "message-id-batch", 3).get(1);
            assertHistoryAndSeek(client, topic, "initial", ordinary, middleBatch);
        }

        cluster.admin(0).topics().unload(topic);
        try (PulsarClient client = cluster.multiBrokerClient()) {
            assertHistoryAndSeek(client, topic, "unload", ordinary, middleBatch);
        }

        try {
            int stoppedOwner = stopTopicOwner(topic);
            int survivor = otherBroker(stoppedOwner);
            try (PulsarClient client = clientForBroker(survivor)) {
                assertHistoryAndSeek(client, topic, "failover", ordinary, middleBatch);
            }

            ensureBrokerRunning(stoppedOwner);
            cluster.awaitCapabilityConvergence();
            cluster.stopBroker(survivor);
            cluster.awaitOwner(cluster.admin(stoppedOwner), topic, stoppedOwner);
            try (PulsarClient client = clientForBroker(stoppedOwner)) {
                assertHistoryAndSeek(client, topic, "restart", ordinary, middleBatch);
            }
        } finally {
            ensureBrokerRunning(0);
            ensureBrokerRunning(1);
        }
        cluster.awaitCapabilityConvergence();
    }

    private void assertHistoryAndSeek(
            PulsarClient client,
            String topic,
            String checkpoint,
            ExpectedMessage ordinary,
            ExpectedMessage middleBatch) throws Exception {
        assertReaderStartsAt(client, topic, ordinary);
        assertReaderStartsAt(client, topic, middleBatch);
        try (Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .subscriptionName("m6-seek-" + checkpoint)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .startMessageIdInclusive()
                .enableBatchIndexAcknowledgment(true)
                .isAckReceiptEnabled(true)
                .subscribe()) {
            consumer.seek(ordinary.messageId());
            ordinary.assertSame(receive(consumer));
            consumer.seek(middleBatch.messageId());
            middleBatch.assertSame(receive(consumer));
        }
    }

    private static void assertReaderStartsAt(
            PulsarClient client, String topic, ExpectedMessage expected) throws Exception {
        try (Reader<byte[]> reader = client.newReader()
                .topic(topic)
                .startMessageId(expected.messageId())
                .startMessageIdInclusive()
                .create()) {
            expected.assertSame(receive(reader));
        }
    }

    private void assertCursorPropertiesSurvivePeerOwnershipAndRestart(String topic) throws Exception {
        configureNereus(topic);
        String subscription = "m6-properties";
        String internalKey = CURSOR_INTERNAL_PROPERTY_PREFIX + "m6-owner";
        try (PulsarClient client = cluster.multiBrokerClient();
                Consumer<byte[]> ignored = consumer(
                        client,
                        topic,
                        subscription,
                        SubscriptionType.Exclusive,
                        SubscriptionInitialPosition.Earliest)) {
            ManagedCursor cursor = loadedCursor(topic, subscription);
            cursor.putCursorProperty(internalKey, "internal-one").join();
            cursor.putCursorProperty("obsolete", "remove-me").join();
            cursor.setCursorProperties(Map.of("external", "one")).join();
            assertThat(cursor.getCursorProperties()).containsExactlyInAnyOrderEntriesOf(Map.of(
                    internalKey, "internal-one", "external", "one"));
            assertThatThrownBy(() -> cursor.setCursorProperties(
                            Map.of(internalKey, "forbidden", "external", "corrupt"))
                    .join())
                    .hasRootCauseInstanceOf(IllegalArgumentException.class)
                    .hasRootCauseMessage("external replacement cannot contain internal cursor keys");
            assertThat(cursor.getCursorProperties()).containsExactlyInAnyOrderEntriesOf(Map.of(
                    internalKey, "internal-one", "external", "one"));
        }

        try {
            int stoppedOwner = stopTopicOwner(topic);
            int survivor = otherBroker(stoppedOwner);
            try (PulsarClient client = clientForBroker(survivor);
                    Consumer<byte[]> ignored = consumer(
                            client,
                            topic,
                            subscription,
                            SubscriptionType.Exclusive,
                            SubscriptionInitialPosition.Latest)) {
                ManagedCursor cursor = loadedCursor(survivor, topic, subscription);
                assertThat(cursor.getCursorProperties()).containsExactlyInAnyOrderEntriesOf(Map.of(
                        internalKey, "internal-one", "external", "one"));
                cursor.setCursorProperties(Map.of("external", "two")).join();
                assertThat(cursor.getCursorProperties()).containsExactlyInAnyOrderEntriesOf(Map.of(
                        internalKey, "internal-one", "external", "two"));
            }

            ensureBrokerRunning(stoppedOwner);
            cluster.awaitCapabilityConvergence();
            cluster.stopBroker(survivor);
            cluster.awaitOwner(cluster.admin(stoppedOwner), topic, stoppedOwner);
            try (PulsarClient client = clientForBroker(stoppedOwner);
                    Consumer<byte[]> ignored = consumer(
                            client,
                            topic,
                            subscription,
                            SubscriptionType.Exclusive,
                            SubscriptionInitialPosition.Latest)) {
                assertThat(loadedCursor(stoppedOwner, topic, subscription).getCursorProperties())
                        .containsExactlyInAnyOrderEntriesOf(Map.of(
                                internalKey, "internal-one", "external", "two"));
            }
        } finally {
            ensureBrokerRunning(0);
            ensureBrokerRunning(1);
        }
        cluster.awaitCapabilityConvergence();
    }

    private void assertLoadedUnloadedAndNamespaceAdminRoutes(String sourceTopic, String shadowTopic)
            throws Exception {
        configureNereus(sourceTopic);
        try (PulsarClient client = cluster.multiBrokerClient();
                Producer<byte[]> source = producer(client, sourceTopic);
                Producer<byte[]> shadow = producer(client, shadowTopic)) {
            source.send(bytes("admin-source"));
            shadow.send(bytes("admin-shadow"));
        }

        assertAdminRejected(
                () -> cluster.admin(0).topics().triggerCompaction(sourceTopic),
                NereusAdminOperation.TRIGGER_COMPACTION);

        cluster.admin(0).topics().unload(sourceTopic);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            for (int index = 0; index < 2; index++) {
                assertThat(cluster.broker(index).getBrokerService().getTopicReference(sourceTopic)).isEmpty();
            }
        });
        assertAdminRejected(
                () -> cluster.admin(0).topics().setShadowTopics(sourceTopic, List.of(shadowTopic)),
                NereusAdminOperation.SET_SHADOW_TOPICS);
        try (PulsarClient client = cluster.multiBrokerClient();
                Producer<byte[]> reopened = producer(client, sourceTopic)) {
            reopened.send(bytes("admin-reopened"));
        }
        assertThat(cluster.broker(0).getTopicPoliciesService()
                        .getTopicPoliciesAsync(
                                TopicName.get(sourceTopic), TopicPoliciesService.GetType.LOCAL_ONLY)
                        .join()
                        .orElseThrow()
                        .getShadowTopics())
                .isNullOrEmpty();

        cluster.admin(0).namespaces().clearNamespaceBacklog(cluster.namespace());
    }

    private static void assertAdminRejected(
            AdminOperation operation, NereusAdminOperation expected) {
        assertThatThrownBy(operation::run)
                .isInstanceOf(PulsarAdminException.class)
                .hasMessageContaining("NEREUS_UNSUPPORTED_ADMIN_OPERATION:" + expected.name());
    }

    private void assertPartitionedAdminRoutes(String sourceTopic, String shadowTopic) throws Exception {
        cluster.admin(0).topics().createPartitionedTopic(sourceTopic, 2);
        configureNereus(sourceTopic);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            for (int partition = 0; partition < 2; partition++) {
                TopicName exact = TopicName.get(sourceTopic).getPartition(partition);
                for (int broker = 0; broker < 2; broker++) {
                    assertThat(cluster.broker(broker).getBrokerService()
                                    .getManagedLedgerConfig(exact)
                                    .join()
                                    .getStorageClassName())
                            .isEqualTo(StorageClassBindingRecord.NEREUS);
                }
            }
        });
        try (PulsarClient client = cluster.multiBrokerClient();
                Producer<byte[]> first = producer(client, TopicName.get(sourceTopic).getPartition(0).toString());
                Producer<byte[]> second = producer(client, TopicName.get(sourceTopic).getPartition(1).toString());
                Producer<byte[]> shadow = producer(client, shadowTopic)) {
            first.send(bytes("partitioned-admin-0"));
            second.send(bytes("partitioned-admin-1"));
            shadow.send(bytes("partitioned-shadow"));
        }

        assertAdminRejected(
                () -> cluster.admin(0).topics().triggerCompaction(sourceTopic),
                NereusAdminOperation.TRIGGER_COMPACTION);

        cluster.admin(0).topics().unload(sourceTopic);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            for (int partition = 0; partition < 2; partition++) {
                String exact = TopicName.get(sourceTopic).getPartition(partition).toString();
                for (int broker = 0; broker < 2; broker++) {
                    assertThat(cluster.broker(broker).getBrokerService().getTopicReference(exact)).isEmpty();
                }
            }
        });
        assertAdminRejected(
                () -> cluster.admin(0).topics().setShadowTopics(sourceTopic, List.of(shadowTopic)),
                NereusAdminOperation.SET_SHADOW_TOPICS);
        assertThat(cluster.broker(0).getTopicPoliciesService()
                        .getTopicPoliciesAsync(
                                TopicName.get(sourceTopic), TopicPoliciesService.GetType.LOCAL_ONLY)
                        .join()
                        .map(policies -> policies.getShadowTopics())
                        .orElse(null))
                .isNullOrEmpty();
    }

    private void assertTopicRecreationUsesFreshProjectionAndCursorTruth(String topic) throws Exception {
        configureNereus(topic);
        String subscription = "m6-recreated-subscription";
        ExpectedMessage oldMessage;
        VirtualLedgerProjection oldProjection;
        try (PulsarClient client = cluster.multiBrokerClient();
                Producer<byte[]> producer = producer(client, topic);
                Consumer<byte[]> consumer = consumer(
                        client,
                        topic,
                        subscription,
                        SubscriptionType.Exclusive,
                        SubscriptionInitialPosition.Earliest)) {
            oldMessage = appendSingles(producer, "old-incarnation", 1).get(0);
            oldMessage.assertSame(receive(consumer));
            oldProjection = ((NereusManagedLedger) loadedTopic(topic).getManagedLedger()).projection();
        }

        cluster.admin(0).topics().delete(topic, true);
        configureNereus(topic);

        VirtualLedgerProjection newProjection;
        try (PulsarClient client = cluster.multiBrokerClient();
                Consumer<byte[]> consumer = consumer(
                        client,
                        topic,
                        subscription,
                        SubscriptionType.Exclusive,
                        SubscriptionInitialPosition.Earliest);
                Producer<byte[]> producer = producer(client, topic)) {
            assertThat(consumer.receive(NO_MESSAGE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)).isNull();
            ExpectedMessage newMessage = appendSingles(producer, "new-incarnation", 1).get(0);
            newMessage.assertSame(receive(consumer));
            assertThat(newMessage.messageId()).isNotEqualTo(oldMessage.messageId());
            newProjection = ((NereusManagedLedger) loadedTopic(topic).getManagedLedger()).projection();
        }

        assertThat(newProjection.incarnation()).isEqualTo(oldProjection.incarnation() + 1);
        assertThat(newProjection.storageClassBindingGeneration())
                .isEqualTo(oldProjection.storageClassBindingGeneration() + 1);
        assertThat(newProjection.streamId()).isNotEqualTo(oldProjection.streamId());
        assertThat(newProjection.virtualLedgerId()).isNotEqualTo(oldProjection.virtualLedgerId());
    }

    private void assertExclusiveEarliestLatestAndUnload(String topic) throws Exception {
        configureNereus(topic);
        List<ExpectedMessage> initial;
        try (PulsarClient client = cluster.multiBrokerClient();
                Producer<byte[]> producer = producer(client, topic)) {
            initial = appendSingles(producer, "exclusive", 6);
            try (Consumer<byte[]> consumer = consumer(
                    client,
                    topic,
                    "exclusive-earliest",
                    SubscriptionType.Exclusive,
                    SubscriptionInitialPosition.Earliest)) {
                Message<byte[]> first = receive(consumer);
                Message<byte[]> second = receive(consumer);
                Message<byte[]> third = receive(consumer);
                initial.get(0).assertSame(first);
                initial.get(1).assertSame(second);
                initial.get(2).assertSame(third);
                consumer.acknowledgeCumulative(third);
            }

            cluster.admin(0).topics().unload(topic);
            try (Consumer<byte[]> reopened = consumer(
                    client,
                    topic,
                    "exclusive-earliest",
                    SubscriptionType.Exclusive,
                    SubscriptionInitialPosition.Latest)) {
                initial.get(3).assertSame(receive(reopened));
            }

            try (Consumer<byte[]> latest = consumer(
                    client,
                    topic,
                    "exclusive-latest",
                    SubscriptionType.Exclusive,
                    SubscriptionInitialPosition.Latest)) {
                ExpectedMessage appended = appendSingles(producer, "exclusive-latest", 1).get(0);
                Message<byte[]> received = receive(latest);
                appended.assertSame(received);
                latest.acknowledgeCumulative(received);
            }
        }

        PersistentTopic loaded = loadedTopic(topic);
        assertThat(loaded.getManagedLedger()).isInstanceOf(NereusManagedLedger.class);
        assertThat(loaded.getManagedLedger().getCursors())
                .extracting(ManagedCursor::getName)
                .contains("exclusive-earliest", "exclusive-latest");
    }

    private void assertFailoverRedeliversIdenticalMessageIds(String topic) throws Exception {
        configureNereus(topic);
        List<ExpectedMessage> expected;
        int stoppedOwner = -1;
        try (PulsarClient client = cluster.multiBrokerClient();
                Consumer<byte[]> first = consumer(
                        client,
                        topic,
                        "failover-subscription",
                        SubscriptionType.Failover,
                        SubscriptionInitialPosition.Latest);
                Consumer<byte[]> second = consumer(
                        client,
                        topic,
                        "failover-subscription",
                        SubscriptionType.Failover,
                        SubscriptionInitialPosition.Latest);
                Producer<byte[]> producer = producer(client, topic)) {
            expected = appendSingles(producer, "failover", 3);
            List<Delivery> initial = receiveEither(List.of(first, second), expected.size());
            assertDeliveries(expected, initial);
        }

        try {
            stoppedOwner = stopTopicOwner(topic);
            int survivor = otherBroker(stoppedOwner);
            try (PulsarClient client = clientForBroker(survivor);
                    Consumer<byte[]> first = consumer(
                            client,
                            topic,
                            "failover-subscription",
                            SubscriptionType.Failover,
                            SubscriptionInitialPosition.Latest);
                    Consumer<byte[]> second = consumer(
                            client,
                            topic,
                            "failover-subscription",
                            SubscriptionType.Failover,
                            SubscriptionInitialPosition.Latest)) {
                List<Delivery> redelivered = receiveEither(List.of(first, second), expected.size());
                assertDeliveries(expected, redelivered);
                for (Delivery delivery : redelivered) {
                    delivery.consumer().acknowledge(delivery.message());
                }
            }
        } finally {
            if (stoppedOwner >= 0) {
                ensureBrokerRunning(stoppedOwner);
            }
        }
        cluster.awaitCapabilityConvergence();
    }

    private void assertSharedKeepsExactIndividualAckHoles(String topic) throws Exception {
        configureNereus(topic);
        List<ExpectedMessage> expected;
        int stoppedOwner;
        try (PulsarClient client = cluster.multiBrokerClient();
                Consumer<byte[]> first = consumer(
                        client,
                        topic,
                        "shared-subscription",
                        SubscriptionType.Shared,
                        SubscriptionInitialPosition.Latest);
                Consumer<byte[]> second = consumer(
                        client,
                        topic,
                        "shared-subscription",
                        SubscriptionType.Shared,
                        SubscriptionInitialPosition.Latest);
                Producer<byte[]> producer = producer(client, topic)) {
            expected = appendSingles(producer, "shared", 8);
            List<Delivery> deliveries = receiveEither(List.of(first, second), expected.size());
            assertDeliveries(expected, deliveries);
            for (Delivery delivery : deliveries) {
                int index = Integer.parseInt(value(delivery.message()).substring("shared-".length()));
                if ((index & 1) == 0) {
                    delivery.consumer().acknowledge(delivery.message());
                }
            }
        }

        stoppedOwner = stopTopicOwner(topic);
        int survivor = otherBroker(stoppedOwner);
        try (PulsarClient client = clientForBroker(survivor);
                Consumer<byte[]> reopened = consumer(
                        client,
                        topic,
                        "shared-subscription",
                        SubscriptionType.Shared,
                        SubscriptionInitialPosition.Latest)) {
            List<Message<byte[]>> remaining = receive(reopened, 4);
            assertThat(remaining).extracting(NereusCursorMultiBrokerIntegrationTest::value)
                    .containsExactlyInAnyOrder("shared-1", "shared-3", "shared-5", "shared-7");
            assertThat(messageIds(remaining)).containsExactlyInAnyOrderElementsOf(Set.of(
                    expected.get(1).messageId(),
                    expected.get(3).messageId(),
                    expected.get(5).messageId(),
                    expected.get(7).messageId()));
            for (Message<byte[]> message : remaining) {
                reopened.acknowledge(message);
            }
            assertThat(reopened.receive(NO_MESSAGE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)).isNull();
        } finally {
            ensureBrokerRunning(stoppedOwner);
        }
        cluster.awaitCapabilityConvergence();
    }

    private void assertPartialBatchSurvivesFailoverAndRuntimeRestart(String topic) throws Exception {
        configureNereus(topic);
        List<ExpectedMessage> expected;
        int firstOwner;
        try (PulsarClient client = cluster.multiBrokerClient();
                Consumer<byte[]> consumer = consumer(
                        client,
                        topic,
                        "batch-subscription",
                        SubscriptionType.Shared,
                        SubscriptionInitialPosition.Latest)) {
            expected = appendBatch(client, topic, "batch", 5);
            List<Message<byte[]>> batch = receive(consumer, expected.size());
            assertMessages(expected, batch);
            for (Message<byte[]> message : batch) {
                int batchIndex = ((MessageIdAdv) message.getMessageId()).getBatchIndex();
                if ((batchIndex & 1) == 0) {
                    consumer.acknowledge(message);
                }
            }
        }

        firstOwner = stopTopicOwner(topic);
        int survivor = otherBroker(firstOwner);
        try (PulsarClient client = clientForBroker(survivor);
                Consumer<byte[]> afterFailover = consumer(
                        client,
                        topic,
                        "batch-subscription",
                        SubscriptionType.Shared,
                        SubscriptionInitialPosition.Latest)) {
            assertRemainingBatch(expected, receive(afterFailover, 2));
        }

        ensureBrokerRunning(firstOwner);
        cluster.awaitCapabilityConvergence();
        cluster.stopBroker(survivor);
        cluster.awaitOwner(cluster.admin(firstOwner), topic, firstOwner);
        try (PulsarClient client = clientForBroker(firstOwner);
                Consumer<byte[]> afterRestart = consumer(
                        client,
                        topic,
                        "batch-subscription",
                        SubscriptionType.Shared,
                        SubscriptionInitialPosition.Latest)) {
            List<Message<byte[]>> remaining = receive(afterRestart, 2);
            assertRemainingBatch(expected, remaining);
            for (Message<byte[]> message : remaining) {
                afterRestart.acknowledge(message);
            }
        } finally {
            ensureBrokerRunning(survivor);
        }
        cluster.awaitCapabilityConvergence();
    }

    private void assertExpiryIsCursorOnly(String topic) throws Exception {
        configureNereus(topic);
        List<ExpectedMessage> expected;
        try (PulsarClient client = cluster.multiBrokerClient();
                Producer<byte[]> producer = producer(client, topic);
                Consumer<byte[]> consumer = consumer(
                        client,
                        topic,
                        "ttl-subscription",
                        SubscriptionType.Exclusive,
                        SubscriptionInitialPosition.Earliest)) {
            expected = appendSingles(producer, "expiry", 3);
        }

        long objectsBefore = cluster.objectCount();
        cluster.admin(0).namespaces().setNamespaceMessageTTL(cluster.namespace(), 1);
        try {
            PersistentTopic loaded = loadedTopic(topic);
            Awaitility.await()
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> {
                        if (loaded.getSubscription("ttl-subscription")
                                .getNumberOfEntriesInBacklog(false) == 0) {
                            return true;
                        }
                        try {
                            cluster.admin(0).topics().expireMessages(topic, "ttl-subscription", 0);
                        } catch (PulsarAdminException.ConflictException ignored) {
                            // Applying the namespace TTL starts an asynchronous expiry check. A concurrent manual
                            // request is allowed to return 409 until that check releases the subscription monitor.
                        }
                        return loaded.getSubscription("ttl-subscription")
                                .getNumberOfEntriesInBacklog(false) == 0;
                    });
            try (PulsarClient client = cluster.multiBrokerClient();
                    Consumer<byte[]> reopened = consumer(
                            client,
                            topic,
                            "ttl-subscription",
                            SubscriptionType.Exclusive,
                            SubscriptionInitialPosition.Earliest)) {
                assertThat(reopened.receive(NO_MESSAGE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)).isNull();
            }
        } finally {
            cluster.admin(0).namespaces().removeNamespaceMessageTTL(cluster.namespace());
        }
        assertThat(expected).hasSize(3);
        assertThat(cluster.objectCount()).isEqualTo(objectsBefore);

        cluster.admin(0).topics().createSubscription(topic, "inactive-subscription", MessageId.latest);
        PersistentTopic loaded = loadedTopic(topic);
        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            ManagedCursor cursor = loaded.getSubscription("inactive-subscription").getCursor();
            assertThat(System.currentTimeMillis() - cursor.getLastActive()).isGreaterThan(1);
        });
        loaded.checkInactiveSubscriptions(1);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(loaded.getSubscription("inactive-subscription")).isNull());
        assertThat(cluster.objectCount()).isEqualTo(objectsBefore);
    }

    private void assertBookKeeperSubscriptionRemainsStock(String topic) throws Exception {
        long objectsBefore = cluster.objectCount();
        try (PulsarClient client = cluster.multiBrokerClient();
                Producer<byte[]> producer = producer(client, topic);
                Consumer<byte[]> consumer = consumer(
                        client,
                        topic,
                        "bookkeeper-subscription",
                        SubscriptionType.Exclusive,
                        SubscriptionInitialPosition.Earliest)) {
            List<ExpectedMessage> expected = appendSingles(producer, "bookkeeper", 2);
            Message<byte[]> first = receive(consumer);
            Message<byte[]> second = receive(consumer);
            expected.get(0).assertSame(first);
            expected.get(1).assertSame(second);
            consumer.acknowledgeCumulative(second);
        }

        cluster.admin(0).topics().unload(topic);
        try (PulsarClient client = cluster.multiBrokerClient();
                Consumer<byte[]> reopened = consumer(
                        client,
                        topic,
                        "bookkeeper-subscription",
                        SubscriptionType.Exclusive,
                        SubscriptionInitialPosition.Earliest)) {
            assertThat(reopened.receive(NO_MESSAGE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)).isNull();
        }
        assertThat(loadedTopic(topic).getManagedLedger()).isInstanceOf(ManagedLedgerImpl.class);
        assertThat(cluster.objectCount()).isEqualTo(objectsBefore);
    }

    private void configureNereus(String topic) throws Exception {
        PersistencePolicies policy = new PersistencePolicies(1, 1, 1, 0, StorageClassBindingRecord.NEREUS);
        cluster.admin(0).topicPolicies().setPersistence(topic, policy);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(cluster.admin(0).topicPolicies().getPersistence(topic)).isEqualTo(policy));
    }

    private int stopTopicOwner(String topic) throws Exception {
        int owner = cluster.awaitOwner(cluster.admin(0), topic, null);
        int survivor = otherBroker(owner);
        cluster.stopBroker(owner);
        cluster.awaitOwner(cluster.admin(survivor), topic, survivor);
        return owner;
    }

    private void ensureBrokerRunning(int index) throws Exception {
        if (cluster.broker(index) == null) {
            cluster.startBroker(index);
        }
    }

    private PulsarClient clientForBroker(int index) throws Exception {
        return PulsarClient.builder().serviceUrl(cluster.broker(index).getBrokerServiceUrl()).build();
    }

    private PersistentTopic loadedTopic(String topic) {
        int owner = cluster.awaitOwner(cluster.admin(0), topic, null);
        return loadedTopic(owner, topic);
    }

    private PersistentTopic loadedTopic(int owner, String topic) {
        return (PersistentTopic) cluster.broker(owner).getBrokerService()
                .getTopicReference(topic)
                .orElseThrow();
    }

    private ManagedCursor loadedCursor(String topic, String subscription) {
        int owner = cluster.awaitOwner(cluster.admin(0), topic, null);
        return loadedCursor(owner, topic, subscription);
    }

    private ManagedCursor loadedCursor(int owner, String topic, String subscription) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(loadedTopic(owner, topic).getSubscription(subscription)).isNotNull());
        return loadedTopic(owner, topic).getSubscription(subscription).getCursor();
    }

    private String topic(String runId, String suffix) {
        return "persistent://" + cluster.namespace() + "/cursor-m5-" + runId + "-" + suffix;
    }

    private static Producer<byte[]> producer(PulsarClient client, String topic) throws Exception {
        return client.newProducer()
                .topic(topic)
                .enableBatching(false)
                .sendTimeout(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .create();
    }

    private static Consumer<byte[]> consumer(
            PulsarClient client,
            String topic,
            String subscription,
            SubscriptionType type,
            SubscriptionInitialPosition initialPosition) throws Exception {
        return client.newConsumer()
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionType(type)
                .subscriptionInitialPosition(initialPosition)
                .enableBatchIndexAcknowledgment(true)
                .isAckReceiptEnabled(true)
                .subscribe();
    }

    private static List<ExpectedMessage> appendSingles(
            Producer<byte[]> producer, String prefix, int count) throws Exception {
        List<ExpectedMessage> expected = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            String value = prefix + "-" + index;
            expected.add(ExpectedMessage.from(value, producer.send(bytes(value))));
        }
        return List.copyOf(expected);
    }

    private static List<ExpectedMessage> appendBatch(
            PulsarClient client, String topic, String prefix, int count) throws Exception {
        try (Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .enableBatching(true)
                .batchingMaxMessages(count)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .sendTimeout(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .create()) {
            List<CompletableFuture<MessageId>> sends = new ArrayList<>();
            for (int index = 0; index < count; index++) {
                sends.add(producer.sendAsync(bytes(prefix + "-" + index)));
            }
            producer.flush();
            List<ExpectedMessage> expected = new ArrayList<>();
            for (int index = 0; index < count; index++) {
                expected.add(ExpectedMessage.from(prefix + "-" + index, sends.get(index).get()));
            }
            return List.copyOf(expected);
        }
    }

    private static Message<byte[]> receive(Consumer<byte[]> consumer) throws Exception {
        Message<byte[]> message = consumer.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(message).isNotNull();
        return message;
    }

    private static Message<byte[]> receive(Reader<byte[]> reader) throws Exception {
        Message<byte[]> message = reader.readNext(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(message).isNotNull();
        return message;
    }

    private static List<Message<byte[]>> receive(Consumer<byte[]> consumer, int count) throws Exception {
        List<Message<byte[]>> messages = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            messages.add(receive(consumer));
        }
        return List.copyOf(messages);
    }

    private static List<Delivery> receiveEither(List<Consumer<byte[]>> consumers, int count) throws Exception {
        List<Delivery> deliveries = new ArrayList<>();
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(RECEIVE_TIMEOUT_SECONDS);
        while (deliveries.size() < count && System.nanoTime() < deadlineNanos) {
            for (Consumer<byte[]> consumer : consumers) {
                Message<byte[]> message = consumer.receive(200, TimeUnit.MILLISECONDS);
                if (message != null) {
                    deliveries.add(new Delivery(consumer, message));
                    if (deliveries.size() == count) {
                        break;
                    }
                }
            }
        }
        assertThat(deliveries).hasSize(count);
        return List.copyOf(deliveries);
    }

    private static void assertDeliveries(List<ExpectedMessage> expected, List<Delivery> deliveries) {
        Map<String, MessageIdAdv> actual = new LinkedHashMap<>();
        deliveries.forEach(delivery -> actual.put(
                value(delivery.message()), requireMessageId(delivery.message().getMessageId())));
        assertThat(actual.keySet()).containsExactlyInAnyOrderElementsOf(
                expected.stream().map(ExpectedMessage::value).toList());
        expected.forEach(message -> assertThat(actual.get(message.value())).isEqualTo(message.messageId()));
    }

    private static void assertMessages(List<ExpectedMessage> expected, List<Message<byte[]>> actual) {
        Map<String, Message<byte[]>> byValue = new LinkedHashMap<>();
        actual.forEach(message -> byValue.put(value(message), message));
        expected.forEach(message -> message.assertSame(byValue.get(message.value())));
    }

    private static void assertRemainingBatch(
            List<ExpectedMessage> expected, List<Message<byte[]>> remaining) {
        assertThat(remaining).extracting(message -> requireMessageId(message.getMessageId()).getBatchIndex())
                .containsExactlyInAnyOrder(1, 3);
        assertThat(messageIds(remaining)).containsExactlyInAnyOrder(
                expected.get(1).messageId(), expected.get(3).messageId());
    }

    private static Set<MessageIdAdv> messageIds(List<Message<byte[]>> messages) {
        Set<MessageIdAdv> ids = new HashSet<>();
        messages.forEach(message -> ids.add(requireMessageId(message.getMessageId())));
        return Set.copyOf(ids);
    }

    private static MessageIdAdv requireMessageId(MessageId messageId) {
        assertThat(messageId).isInstanceOf(MessageIdAdv.class);
        return (MessageIdAdv) messageId;
    }

    private static String value(Message<byte[]> message) {
        return new String(message.getData(), StandardCharsets.UTF_8);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static int otherBroker(int index) {
        return index == 0 ? 1 : 0;
    }

    private record Delivery(Consumer<byte[]> consumer, Message<byte[]> message) {
    }

    @FunctionalInterface
    private interface AdminOperation {
        void run() throws Exception;
    }

    private record ExpectedMessage(String value, MessageIdAdv messageId) {
        private static ExpectedMessage from(String value, MessageId messageId) {
            return new ExpectedMessage(value, requireMessageId(messageId));
        }

        private void assertSame(Message<byte[]> actual) {
            assertThat(actual).isNotNull();
            assertThat(NereusCursorMultiBrokerIntegrationTest.value(actual)).isEqualTo(value);
            assertThat(requireMessageId(actual.getMessageId())).isEqualTo(messageId);
        }
    }
}
