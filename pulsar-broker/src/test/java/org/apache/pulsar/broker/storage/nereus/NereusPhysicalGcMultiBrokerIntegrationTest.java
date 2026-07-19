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
import com.nereusstream.api.ReadView;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV1;
import com.nereusstream.objectstore.wal.WalObjectKeys;
import com.nereusstream.pulsar.Phase4ObjectWalRuntime;
import com.nereusstream.pulsar.Phase4PhysicalGcRuntime;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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

/** Real two-broker generation replacement, source-byte deletion, ownership, and restart acceptance. */
@Test(groups = "broker-isolated")
public class NereusPhysicalGcMultiBrokerIntegrationTest {
    private static final String INITIAL_ACTIVATION_RUN =
            "phasefourphysicalgcinitialrun";
    private static final String FAILOVER_ROLLOVER_RUN =
            "phasefourphysicalgcfailoverrun";
    private static final String RESTART_ROLLOVER_RUN =
            "phasefourphysicalgcrestartrun";
    private static final String REVERSE_ROLLOVER_RUN =
            "phasefourphysicalgcreverserun";

    private final NereusMultiBrokerIntegrationTest cluster =
            new NereusMultiBrokerIntegrationTest(true);

    @BeforeClass(alwaysRun = true)
    public void startCluster() throws Exception {
        cluster.startCluster();
    }

    @AfterClass(alwaysRun = true)
    public void closeCluster() throws Exception {
        cluster.closeCluster();
    }

    @Test(timeOut = 900_000)
    public void deletesMaterializedWalSourcesAndPreservesMessageIdsAcrossOwnershipCuts()
            throws Exception {
        cluster.awaitGenerationCapabilityConvergence();
        cluster.activateOrRolloverPhysicalDeletion(0, INITIAL_ACTIVATION_RUN);
        cluster.startPhysicalDeletionLifecycleOnEveryBroker(
                INITIAL_ACTIVATION_RUN, 2, Duration.ofSeconds(60));

        String topic = "persistent://" + cluster.namespace() + "/phase4-physical-gc";
        String bookKeeperTopic = "persistent://" + cluster.namespace() + "/phase4-bookkeeper";
        PersistencePolicies nereusPolicy = new PersistencePolicies(
                1, 1, 1, 0, StorageClassBindingRecord.NEREUS);
        cluster.admin(0).topicPolicies().setPersistence(topic, nereusPolicy);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(cluster.admin(0).topicPolicies().getPersistence(topic))
                        .isEqualTo(nereusPolicy));

        List<ExpectedMessage> expected = new ArrayList<>();
        List<ExpectedMessage> bookKeeperExpected = new ArrayList<>();
        try (PulsarClient client = cluster.multiBrokerClient()) {
            appendSingles(client, topic, expected, "before-materialization", 4);
            appendBatch(client, topic, expected, "batch", 3);
            appendSingles(client, bookKeeperTopic, bookKeeperExpected, "bookkeeper-before", 1);
            cluster.assertFacadeEntriesReadable(topic, 5);
            assertReadAll(client, topic, expected);
            assertReadAll(client, bookKeeperTopic, bookKeeperExpected);

            String walPrefix = WalObjectKeys.prefix(cluster.clusterName()).value();
            AtomicReference<Set<String>> initialWal = new AtomicReference<>();
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                Set<String> keys = cluster.logicalObjectKeys().stream()
                        .filter(key -> key.startsWith(walPrefix))
                        .collect(java.util.stream.Collectors.toUnmodifiableSet());
                assertThat(keys).hasSizeGreaterThanOrEqualTo(5);
                initialWal.set(keys);
            });

            Phase4PhysicalGcRuntime gc = cluster.physicalGcRuntime(topic);
            AtomicReference<Object> lastGcPass = new AtomicReference<>();
            AtomicReference<Object> lastMaterializationPass = new AtomicReference<>();
            AtomicReference<Object> lastReferencedGcResults = new AtomicReference<>();
            lastGcPass.set(gc.lifecycleService().scanNow().get(120, TimeUnit.SECONDS));
            lastReferencedGcResults.set(gc.referencedGcResults());
            assertThat(cluster.logicalObjectKeys()).containsAll(initialWal.get());

            Phase4ObjectWalRuntime materialization = cluster.materializationRuntime(topic);
            var materialized = materialization.materializationService()
                    .scanNow()
                    .get(120, TimeUnit.SECONDS);
            lastMaterializationPass.set(materialized);
            assertThat(materialized.registrationsAdmitted()).isPositive();
            assertThat(materialized.plannedTasksConverged()).isPositive();

            String compactedPrefix = CompactedObjectFormatV1
                    .prefix(cluster.clusterName(), ReadView.COMMITTED)
                    .value();
            AtomicReference<Set<String>> compacted = new AtomicReference<>();
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                Set<String> keys = cluster.logicalObjectKeys().stream()
                        .filter(key -> key.startsWith(compactedPrefix))
                        .collect(java.util.stream.Collectors.toUnmodifiableSet());
                assertThat(keys).isNotEmpty();
                compacted.set(keys);
            });
            assertReadAll(client, topic, expected);

            Awaitility.await()
                    .atMost(Duration.ofSeconds(120))
                    .pollInterval(Duration.ofMillis(250))
                    .untilAsserted(() -> {
                        lastMaterializationPass.set(materialization.materializationService()
                                .scanNow()
                                .get(120, TimeUnit.SECONDS));
                        lastGcPass.set(gc.lifecycleService()
                                .scanNow()
                                .get(120, TimeUnit.SECONDS));
                        lastReferencedGcResults.set(gc.referencedGcResults());
                        Set<String> keys = cluster.logicalObjectKeys();
                        assertThat(keys)
                                .withFailMessage(
                                        "last materialization pass: %s%nlast physical-GC pass: %s%n"
                                                + "last referenced-GC results: %s",
                                        lastMaterializationPass.get(),
                                        lastGcPass.get(),
                                        lastReferencedGcResults.get())
                                .doesNotContainAnyElementsOf(initialWal.get());
                        assertThat(keys).containsAll(compacted.get());
                    });

            assertReadAll(client, topic, expected);
            cluster.admin(0).topics().unload(topic);
            assertReadAll(client, topic, expected);
        }

        int stoppedOwner = cluster.awaitOwner(cluster.admin(0), topic, null);
        int survivor = stoppedOwner == 0 ? 1 : 0;
        cluster.stopBroker(stoppedOwner);
        cluster.awaitGenerationCapabilityConvergence();
        cluster.activateOrRolloverPhysicalDeletion(survivor, FAILOVER_ROLLOVER_RUN);
        cluster.awaitOwner(cluster.admin(survivor), topic, survivor);

        try (PulsarClient survivorClient = PulsarClient.builder()
                .serviceUrl(cluster.broker(survivor).getBrokerServiceUrl())
                .build()) {
            assertReadAll(survivorClient, topic, expected);
            appendSingles(survivorClient, topic, expected, "after-owner-failover", 2);
            assertReadAll(survivorClient, topic, expected);
        }

        cluster.startBroker(stoppedOwner);
        cluster.awaitCapabilityConvergence();
        cluster.awaitGenerationCapabilityConvergence();
        cluster.activateOrRolloverPhysicalDeletion(stoppedOwner, RESTART_ROLLOVER_RUN);

        cluster.stopBroker(survivor);
        cluster.awaitGenerationCapabilityConvergence();
        cluster.activateOrRolloverPhysicalDeletion(stoppedOwner, REVERSE_ROLLOVER_RUN);
        cluster.awaitOwner(cluster.admin(stoppedOwner), topic, stoppedOwner);
        try (PulsarClient restartedClient = PulsarClient.builder()
                .serviceUrl(cluster.broker(stoppedOwner).getBrokerServiceUrl())
                .build()) {
            assertReadAll(restartedClient, topic, expected);
            appendSingles(restartedClient, bookKeeperTopic, bookKeeperExpected, "bookkeeper-after", 1);
            assertReadAll(restartedClient, bookKeeperTopic, bookKeeperExpected);
        }
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
                        .value(value.getBytes(StandardCharsets.UTF_8))
                        .send();
                expected.add(ExpectedMessage.from(value, properties, id));
            }
        }
    }

    private static void appendBatch(
            PulsarClient client,
            String topic,
            List<ExpectedMessage> expected,
            String prefix,
            int count) throws Exception {
        try (Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .enableBatching(true)
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
                        "kind", "batch", "sequence", Integer.toString(index));
                values.add(value);
                properties.add(messageProperties);
                sends.add(producer.newMessage()
                        .properties(messageProperties)
                        .value(value.getBytes(StandardCharsets.UTF_8))
                        .sendAsync());
            }
            producer.flush();
            for (int index = 0; index < count; index++) {
                expected.add(ExpectedMessage.from(
                        values.get(index), properties.get(index), sends.get(index).get()));
            }
            List<MessageIdAdv> ids = expected.subList(expected.size() - count, expected.size()).stream()
                    .map(ExpectedMessage::messageId)
                    .toList();
            assertThat(ids).extracting(MessageIdAdv::getLedgerId).containsOnly(ids.get(0).getLedgerId());
            assertThat(ids).extracting(MessageIdAdv::getEntryId).containsOnly(ids.get(0).getEntryId());
            assertThat(ids).extracting(MessageIdAdv::getBatchIndex).containsExactly(0, 1, 2);
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
                assertThat(actual.getProperties()).isEqualTo(expectedMessage.properties());
                expectedMessage.assertSamePosition(actual.getMessageId());
            }
            assertThat(reader.readNext(250, TimeUnit.MILLISECONDS)).isNull();
        }
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
            return new ExpectedMessage(value, properties, (MessageIdAdv) messageId);
        }

        private void assertSamePosition(MessageId actual) {
            assertThat(actual).isInstanceOf(MessageIdAdv.class);
            MessageIdAdv exact = (MessageIdAdv) actual;
            assertThat(exact.getLedgerId()).isEqualTo(messageId.getLedgerId());
            assertThat(exact.getEntryId()).isEqualTo(messageId.getEntryId());
            assertThat(exact.getPartitionIndex()).isEqualTo(messageId.getPartitionIndex());
            assertThat(exact.getBatchIndex()).isEqualTo(messageId.getBatchIndex());
        }
    }
}
