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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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

/** Real first-create profile rollout with mixed durable histories on both owners. */
@Test(groups = "broker-isolated")
public class NereusMixedPrimaryProfilesMultiBrokerTest {
    private static final String SYNC_SUFFIX = "/mixed-bk-sync";
    private static final String ASYNC_SUFFIX = "/mixed-bk-async";
    private static final String OBJECT_SUFFIX = "/mixed-object-sync";

    private final NereusMultiBrokerIntegrationTest cluster =
            new NereusMultiBrokerIntegrationTest(
                    true,
                    false,
                    StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT);

    @BeforeClass(alwaysRun = true)
    public void startCluster() throws Exception {
        cluster.startCluster();
    }

    @AfterClass(alwaysRun = true)
    public void closeCluster() throws Exception {
        cluster.closeCluster();
    }

    @Test(timeOut = 900_000)
    public void coexistsAcrossProfiles() throws Exception {
        String sync = topic(SYNC_SUFFIX);
        String async = topic(ASYNC_SUFFIX);
        String object = topic(OBJECT_SUFFIX);
        Map<String, ExpectedMessage> expected = new LinkedHashMap<>();

        createNereusTopic(sync);
        activateGeneration("mixedprofilesinitialsyncaa");
        expected.put(sync, append(sync, "bk-sync"));
        assertProfile(sync, StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT);

        restartBothWithDefault(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT);
        createNereusTopic(async);
        activateGeneration("mixedprofilesasyncrollouta");
        expected.put(async, append(async, "bk-async"));
        assertProfile(async, StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT);
        assertProfile(sync, StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT);

        restartBothWithDefault(StorageProfile.OBJECT_WAL_SYNC_OBJECT);
        createNereusTopic(object);
        activateGeneration("mixedprofilesobjectrollout");
        expected.put(object, append(object, "object-sync"));
        assertProfile(object, StorageProfile.OBJECT_WAL_SYNC_OBJECT);
        assertProfile(async, StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT);
        assertProfile(sync, StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT);
        assertAll(expected);

        cluster.stopBroker(1);
        for (String topic : expected.keySet()) {
            cluster.awaitOwner(cluster.admin(0), topic, 0);
        }
        assertAll(expected);

        cluster.startBroker(1);
        cluster.awaitCapabilityConvergence();
        cluster.awaitGenerationCapabilityConvergence();
        cluster.stopBroker(0);
        for (String topic : expected.keySet()) {
            cluster.awaitOwner(cluster.admin(1), topic, 1);
        }
        assertAll(expected);
    }

    private void restartBothWithDefault(StorageProfile profile) throws Exception {
        cluster.stopBroker(1);
        cluster.stopBroker(0);
        for (int index = 0; index < 2; index++) {
            cluster.setBrokerDefaultStorageProfile(index, profile);
        }
        cluster.startBroker(0);
        cluster.startBroker(1);
        cluster.awaitCapabilityConvergence();
        cluster.awaitGenerationCapabilityConvergence();
    }

    private void createNereusTopic(String topic) throws Exception {
        PersistencePolicies policy = new PersistencePolicies(
                1, 1, 1, 0, StorageClassBindingRecord.NEREUS);
        cluster.admin(0).topicPolicies().setPersistence(topic, policy);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(cluster.admin(0).topicPolicies().getPersistence(topic))
                        .isEqualTo(policy));
        cluster.admin(0).topics().createNonPartitionedTopic(topic);
    }

    private void activateGeneration(String runId) throws Exception {
        cluster.awaitGenerationCapabilityConvergence();
        GenerationRegistrationBackfillReport report =
                cluster.activateOrRolloverGeneration(0, runId);
        assertThat(report.failureCount()).isZero();
    }

    private ExpectedMessage append(String topic, String value) throws Exception {
        try (PulsarClient client = cluster.multiBrokerClient();
                Producer<byte[]> producer = client.newProducer()
                        .topic(topic)
                        .enableBatching(false)
                        .sendTimeout(60, TimeUnit.SECONDS)
                        .create()) {
            MessageId messageId = producer.send(bytes(value));
            assertThat(messageId).isInstanceOf(MessageIdAdv.class);
            return new ExpectedMessage(value, (MessageIdAdv) messageId);
        }
    }

    private void assertAll(Map<String, ExpectedMessage> expected) throws Exception {
        try (PulsarClient client = cluster.multiBrokerClient()) {
            for (Map.Entry<String, ExpectedMessage> entry : expected.entrySet()) {
                try (Reader<byte[]> reader = client.newReader()
                        .topic(entry.getKey())
                        .startMessageId(MessageId.earliest)
                        .create()) {
                    Message<byte[]> actual = reader.readNext(60, TimeUnit.SECONDS);
                    assertThat(actual).isNotNull();
                    entry.getValue().assertSame(actual);
                }
            }
        }
    }

    private void assertProfile(String topic, StorageProfile profile) throws Exception {
        try (PulsarClient client = cluster.multiBrokerClient();
                Reader<byte[]> ignored = client.newReader()
                        .topic(topic)
                        .startMessageId(MessageId.earliest)
                        .create()) {
            assertThat(cluster.loadedNereusLedger(topic).currentMetadata().profile())
                    .isEqualTo(profile);
        }
    }

    private String topic(String suffix) {
        return "persistent://" + cluster.namespace() + suffix;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private record ExpectedMessage(String value, MessageIdAdv messageId) {
        private void assertSame(Message<byte[]> actual) {
            assertThat(new String(actual.getData(), StandardCharsets.UTF_8))
                    .isEqualTo(value);
            assertThat(actual.getMessageId()).isInstanceOf(MessageIdAdv.class);
            MessageIdAdv actualId = (MessageIdAdv) actual.getMessageId();
            assertThat(actualId.getLedgerId()).isEqualTo(messageId.getLedgerId());
            assertThat(actualId.getEntryId()).isEqualTo(messageId.getEntryId());
            assertThat(actualId.getPartitionIndex()).isEqualTo(messageId.getPartitionIndex());
            assertThat(actualId.getBatchIndex()).isEqualTo(messageId.getBatchIndex());
        }
    }
}
