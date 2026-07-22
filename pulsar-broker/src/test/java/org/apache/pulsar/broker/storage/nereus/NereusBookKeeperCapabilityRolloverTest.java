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
import com.nereusstream.bookkeeper.BookKeeperProtocolActivation;
import com.nereusstream.pulsar.BookKeeperDeletionActivationRequest;
import com.nereusstream.pulsar.BookKeeperDeletionActivationResult;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** Real capability downgrade, owner exclusion, and readiness-proof rollover acceptance. */
@Test(groups = "broker-isolated")
public class NereusBookKeeperCapabilityRolloverTest {
    private static final String TOPIC_SUFFIX = "/bookkeeper-capability-rollover";
    private static final String REJECTED_TOPIC_SUFFIX = "/bookkeeper-capability-rejected-create";

    private final NereusMultiBrokerIntegrationTest cluster =
            new NereusMultiBrokerIntegrationTest(
                    false,
                    false,
                    StorageProfile.BOOKKEEPER_WAL_ONLY,
                    true);

    @BeforeClass(alwaysRun = true)
    public void startCluster() throws Exception {
        cluster.startCluster();
    }

    @AfterClass(alwaysRun = true)
    public void closeCluster() throws Exception {
        cluster.closeCluster();
    }

    @Test(priority = 1, timeOut = 900_000)
    public void excludesOldBroker() throws Exception {
        String topic = "persistent://" + cluster.namespace() + TOPIC_SUFFIX;
        selectNereusStorage(topic);
        MessageId first;
        try (PulsarClient client = cluster.multiBrokerClient();
                Producer<byte[]> producer = producer(client, topic)) {
            first = producer.send(bytes("before-downgrade"));
        }

        cluster.stopBroker(1);
        cluster.setBrokerBookKeeperPrimaryWalEnabled(1, false);
        cluster.startBroker(1);
        cluster.awaitBaseCapabilityConvergence();
        try {
            NereusManagedLedgerStorage incapable = storage(1);
            assertThatThrownBy(() -> incapable.capabilityCoordinator()
                            .requireLocalStorageProfileReady(
                                    StorageProfile.BOOKKEEPER_WAL_ONLY)
                            .join())
                    .hasRootCauseMessage(
                            "NEREUS_BOOKKEEPER_CAPABILITY_NOT_READY:LOCAL");

            cluster.admin(0).topics().unload(topic);
            cluster.awaitOwner(cluster.admin(0), topic, 0);
            assertNeverLoadedOnBroker(topic, 1);

            try (PulsarClient oldBrokerClient = PulsarClient.builder()
                    .serviceUrl(cluster.broker(1).getBrokerServiceUrl())
                    .build();
                    Producer<byte[]> producer = producer(oldBrokerClient, topic)) {
                MessageId second = producer.send(bytes("through-capable-owner"));
                assertThat(second).isNotEqualTo(first);
                assertRead(oldBrokerClient, topic, "before-downgrade", "through-capable-owner");
            }
            assertNeverLoadedOnBroker(topic, 1);

            String rejected = "persistent://" + cluster.namespace() + REJECTED_TOPIC_SUFFIX;
            selectNereusStorage(rejected);
            try (PulsarClient capableClient = PulsarClient.builder()
                    .serviceUrl(cluster.broker(0).getBrokerServiceUrl())
                    .build()) {
                assertThatThrownBy(() -> {
                    try (Producer<byte[]> producer = capableClient.newProducer()
                            .topic(rejected)
                            .enableBatching(false)
                            .sendTimeout(5, TimeUnit.SECONDS)
                            .create()) {
                        producer.send(bytes("must-not-commit"));
                    }
                });
            }
            assertThat(storage(0).hasActiveNereusBinding(TopicName.get(rejected)).join())
                    .isFalse();
        } finally {
            cluster.stopBroker(1);
            cluster.setBrokerBookKeeperPrimaryWalEnabled(1, true);
            cluster.setBrokerDefaultStorageProfile(
                    1, StorageProfile.BOOKKEEPER_WAL_ONLY);
            cluster.startBroker(1);
            cluster.awaitCapabilityConvergence();
        }
    }

    @Test(priority = 2, timeOut = 900_000)
    public void reestablishesExactAuthority() throws Exception {
        NereusManagedLedgerStorage storage = storage(0);
        BookKeeperProtocolActivation publication = storage.bookKeeperPrimaryWalAdministration()
                .readActivation(Duration.ofSeconds(30))
                .join()
                .orElseThrow();
        BookKeeperDeletionActivationResult initial = storage.activateBookKeeperLedgerDeletion(
                        new BookKeeperDeletionActivationRequest(
                                "bk-capability-epoch-initial",
                                publication.metadataVersion(),
                                Duration.ofMinutes(3)))
                .get(4, TimeUnit.MINUTES);
        assertThat(initial.activation().value().ledgerDeletionEnabled()).isTrue();

        cluster.stopBroker(1);
        BookKeeperDeletionActivationResult oneBroker = storage.activateBookKeeperLedgerDeletion(
                        new BookKeeperDeletionActivationRequest(
                                "bk-capability-epoch-one-broker",
                                initial.activation().metadataVersion(),
                                Duration.ofMinutes(3)))
                .get(4, TimeUnit.MINUTES);
        assertRebound(initial, oneBroker);
        assertThat(oneBroker.activation().value().brokerReadinessEpoch())
                .isNotEqualTo(initial.activation().value().brokerReadinessEpoch());

        cluster.startBroker(1);
        cluster.awaitCapabilityConvergence();
        BookKeeperDeletionActivationResult rejoined = storage.activateBookKeeperLedgerDeletion(
                        new BookKeeperDeletionActivationRequest(
                                "bk-capability-epoch-rejoined",
                                oneBroker.activation().metadataVersion(),
                                Duration.ofMinutes(3)))
                .get(4, TimeUnit.MINUTES);
        assertRebound(oneBroker, rejoined);
        assertThat(rejoined.activation().value().brokerReadinessEpoch())
                .isNotEqualTo(oneBroker.activation().value().brokerReadinessEpoch());
    }

    private static void assertRebound(
            BookKeeperDeletionActivationResult previous,
            BookKeeperDeletionActivationResult rebound) {
        assertThat(rebound.newlyActivated()).isTrue();
        assertThat(rebound.activation().metadataVersion())
                .isGreaterThan(previous.activation().metadataVersion());
        assertThat(rebound.activation().publicationActivationSha256())
                .isEqualTo(previous.activation().publicationActivationSha256());
        assertThat(rebound.activation().activationRecordSha256())
                .isNotEqualTo(previous.activation().activationRecordSha256());
        assertThat(rebound.activation().value().brokerReadinessSha256())
                .isNotEqualTo(previous.activation().value().brokerReadinessSha256());
    }

    private void selectNereusStorage(String topic) throws Exception {
        PersistencePolicies policy = new PersistencePolicies(
                1, 1, 1, 0, StorageClassBindingRecord.NEREUS);
        cluster.admin(0).topicPolicies().setPersistence(topic, policy);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(cluster.admin(0).topicPolicies().getPersistence(topic))
                        .isEqualTo(policy));
    }

    private NereusManagedLedgerStorage storage(int broker) {
        return (NereusManagedLedgerStorage) cluster.broker(broker)
                .getManagedLedgerStorage();
    }

    private void assertNeverLoadedOnBroker(String topic, int broker) {
        Awaitility.await().during(Duration.ofSeconds(2))
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(cluster.broker(broker)
                                .getBrokerService()
                                .getTopicReference(topic))
                        .isEmpty());
    }

    private static Producer<byte[]> producer(PulsarClient client, String topic) throws Exception {
        return client.newProducer()
                .topic(topic)
                .enableBatching(false)
                .sendTimeout(30, TimeUnit.SECONDS)
                .create();
    }

    private static void assertRead(
            PulsarClient client, String topic, String... expected) throws Exception {
        try (Reader<byte[]> reader = client.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .create()) {
            for (String value : expected) {
                Message<byte[]> actual = reader.readNext(30, TimeUnit.SECONDS);
                assertThat(actual).isNotNull();
                assertThat(new String(actual.getData(), StandardCharsets.UTF_8))
                        .isEqualTo(value);
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
