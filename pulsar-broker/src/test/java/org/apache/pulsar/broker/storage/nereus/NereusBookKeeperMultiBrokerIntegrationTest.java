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
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.StorageProfile;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** Real BookKeeper-primary WAL ownership transfer and stock-BookKeeper coexistence acceptance. */
@Test(groups = "broker-isolated")
public class NereusBookKeeperMultiBrokerIntegrationTest {
    private static final String TOPIC_SUFFIX = "/bookkeeper-primary-wal";
    private static final String STOCK_TOPIC_SUFFIX = "/bookkeeper-stock-control";

    private final NereusMultiBrokerIntegrationTest cluster =
            new NereusMultiBrokerIntegrationTest(
                    false,
                    false,
                    StorageProfile.BOOKKEEPER_WAL_ONLY);

    @BeforeClass(alwaysRun = true)
    public void startCluster() throws Exception {
        cluster.startCluster();
    }

    @AfterClass(alwaysRun = true)
    public void closeCluster() throws Exception {
        cluster.closeCluster();
    }

    @Test(timeOut = 900_000)
    public void preservesOwnershipProjectionAndStockIsolationAcrossBothTakeovers()
            throws Exception {
        String topic = "persistent://" + cluster.namespace() + TOPIC_SUFFIX;
        String stockTopic = "persistent://" + cluster.namespace() + STOCK_TOPIC_SUFFIX;
        PersistencePolicies nereusPolicy = new PersistencePolicies(
                1, 1, 1, 0, StorageClassBindingRecord.NEREUS);
        cluster.admin(0).topicPolicies().setPersistence(topic, nereusPolicy);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(cluster.admin(0).topicPolicies().getPersistence(topic))
                        .isEqualTo(nereusPolicy));

        List<ExpectedMessage> expected = new ArrayList<>();
        List<ExpectedMessage> stockExpected = new ArrayList<>();
        int firstOwner;
        int survivor;
        try (PulsarClient client = cluster.multiBrokerClient()) {
            append(client, topic, expected, "initial", 4);
            appendCompressedBatch(client, topic, expected, "initial-batch", 3);
            append(client, stockTopic, stockExpected, "stock-before", 1);
            assertDirectNereusRead(topic);
            assertReadAll(client, topic, expected);
            assertReadAll(client, stockTopic, stockExpected);
            assertThat(cluster.loadedNereusLedger(topic).currentMetadata().profile())
                    .isEqualTo(StorageProfile.BOOKKEEPER_WAL_ONLY);

            cluster.admin(0).topics().unload(topic);
            assertReadAll(client, topic, expected);
            assertSeekSemantics(client, topic, expected, 2);
            append(client, topic, expected, "after-unload", 2);
            assertReadAll(client, topic, expected);

            firstOwner = cluster.awaitOwner(cluster.admin(0), topic, null);
            survivor = otherBroker(firstOwner);
            cluster.stopBroker(firstOwner);
            cluster.awaitOwner(cluster.admin(survivor), topic, survivor);
            try (PulsarClient survivorClient = clientForBroker(survivor)) {
                append(survivorClient, topic, expected, "after-failover", 3);
                assertReadAll(survivorClient, topic, expected);
                assertSeekSemantics(survivorClient, topic, expected, 5);
                append(survivorClient, stockTopic, stockExpected, "stock-during", 1);
                assertReadAll(survivorClient, stockTopic, stockExpected);
            }

            cluster.startBroker(firstOwner);
            cluster.awaitCapabilityConvergence();
        }

        cluster.stopBroker(survivor);
        cluster.awaitOwner(cluster.admin(firstOwner), topic, firstOwner);
        try (PulsarClient restartedClient = clientForBroker(firstOwner)) {
            append(restartedClient, topic, expected, "after-reverse-takeover", 2);
            append(restartedClient, stockTopic, stockExpected, "stock-after", 1);
            assertReadAll(restartedClient, topic, expected);
            assertReadAll(restartedClient, stockTopic, stockExpected);
        }

        assertThat(cluster.loadedNereusLedger(topic).currentMetadata().profile())
                .isEqualTo(StorageProfile.BOOKKEEPER_WAL_ONLY);
    }

    private PulsarClient clientForBroker(int broker) throws Exception {
        return PulsarClient.builder()
                .serviceUrl(cluster.broker(broker).getBrokerServiceUrl())
                .build();
    }

    private void assertDirectNereusRead(String topic) {
        var ledger = cluster.loadedNereusLedger(topic);
        var read = ledger.runtime().streamStorage().read(
                ledger.projection().streamId(),
                0,
                new ReadOptions(
                        1,
                        1024 * 1024,
                        ReadIsolation.COMMITTED,
                        Duration.ofSeconds(30)))
                .join();
        assertThat(read.batches()).hasSize(1);
    }

    private static void append(
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
                expected.add(new ExpectedMessage(
                        value,
                        requireMessageId(producer.send(value.getBytes(StandardCharsets.UTF_8)))));
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
                .batchingMaxMessages(count)
                .batchingMaxPublishDelay(1, TimeUnit.MINUTES)
                .compressionType(CompressionType.LZ4)
                .sendTimeout(30, TimeUnit.SECONDS)
                .create()) {
            List<String> values = new ArrayList<>();
            List<CompletableFuture<MessageId>> sends = new ArrayList<>();
            for (int index = 0; index < count; index++) {
                String value = prefix + "-" + index;
                values.add(value);
                sends.add(producer.sendAsync(value.getBytes(StandardCharsets.UTF_8)));
            }
            producer.flush();
            for (int index = 0; index < count; index++) {
                expected.add(new ExpectedMessage(
                        values.get(index),
                        requireMessageId(sends.get(index).get(30, TimeUnit.SECONDS))));
            }
        }
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
                Message<byte[]> actual = reader.readNext(30, TimeUnit.SECONDS);
                assertThat(actual).isNotNull();
                assertThat(new String(actual.getData(), StandardCharsets.UTF_8))
                        .isEqualTo(expectedMessage.value());
                expectedMessage.assertSame(actual.getMessageId());
            }
            assertThat(reader.readNext(250, TimeUnit.MILLISECONDS)).isNull();
        }
    }

    private static void assertSeekSemantics(
            PulsarClient client,
            String topic,
            List<ExpectedMessage> expected,
            int index) throws Exception {
        try (Reader<byte[]> reader = client.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .create()) {
            reader.seek(expected.get(index).messageId());
            Message<byte[]> actual = reader.readNext(30, TimeUnit.SECONDS);
            assertThat(actual).isNotNull();
            assertThat(new String(actual.getData(), StandardCharsets.UTF_8))
                    .isEqualTo(expected.get(index + 1).value());
            expected.get(index + 1).assertSame(actual.getMessageId());
        }
        try (Reader<byte[]> reader = client.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .startMessageIdInclusive()
                .create()) {
            reader.seek(expected.get(index).messageId());
            Message<byte[]> actual = reader.readNext(30, TimeUnit.SECONDS);
            assertThat(actual).isNotNull();
            assertThat(new String(actual.getData(), StandardCharsets.UTF_8))
                    .isEqualTo(expected.get(index).value());
            expected.get(index).assertSame(actual.getMessageId());
        }
    }

    private static MessageIdAdv requireMessageId(MessageId messageId) {
        assertThat(messageId).isInstanceOf(MessageIdAdv.class);
        return (MessageIdAdv) messageId;
    }

    private static int otherBroker(int broker) {
        return broker == 0 ? 1 : 0;
    }

    private record ExpectedMessage(String value, MessageIdAdv messageId) {
        private void assertSame(MessageId actual) {
            MessageIdAdv exact = requireMessageId(actual);
            assertThat(exact.getLedgerId()).isEqualTo(messageId.getLedgerId());
            assertThat(exact.getEntryId()).isEqualTo(messageId.getEntryId());
            assertThat(exact.getPartitionIndex()).isEqualTo(messageId.getPartitionIndex());
            assertThat(exact.getBatchIndex()).isEqualTo(messageId.getBatchIndex());
        }
    }
}
