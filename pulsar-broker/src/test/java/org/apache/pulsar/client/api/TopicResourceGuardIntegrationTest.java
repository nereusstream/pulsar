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
package org.apache.pulsar.client.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.ValidatedTopicResourceGuard;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** Real broker/client coverage for guarded producer identity and SEND evidence. */
@Test(groups = "broker-api")
public class TopicResourceGuardIntegrationTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setBrokerEntryMetadataInterceptors(org.assertj.core.util.Sets.newTreeSet(
                "org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor",
                "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor"));
        conf.setExposingBrokerEntryMetadataToClientEnabled(true);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void guardedSendReturnsAttestedMessageIdAndEvidence() throws Exception {
        String topic = newTopicName();
        TopicResourceGuard guard = publishGuard(topic, incarnation((byte) 0x11), 100L);

        try (Producer<byte[]> producer = guardedProducer(topic, guard)) {
            MessageId messageId = producer.send("guarded-send".getBytes(StandardCharsets.UTF_8));

            assertThat(messageId).isInstanceOf(GuardedMessageId.class);
            GuardedMessageId guardedMessageId = (GuardedMessageId) messageId;
            assertThat(guardedMessageId.resourceGuard()).isEqualTo(guard);
            assertThat(guardedMessageId.physicalTopic()).isEqualTo(topic);
            assertThat(guardedMessageId.partition()).isZero();
            assertThat(guardedMessageId.brokerEntryTimestamp()).isGreaterThanOrEqualTo(0L);

            GuardedSendSuccessEvidence evidence = guardedMessageId.responseEvidence();
            assertThat(evidence.protocolVersion()).isGreaterThanOrEqualTo(22);
            assertThat(evidence.attestation())
                    .isEqualTo(new TopicResourceGuardAttestation(guard, topic, 0));
            assertThat(evidence.ledgerId()).isGreaterThanOrEqualTo(0L);
            assertThat(evidence.entryId()).isGreaterThanOrEqualTo(0L);
            assertThat(evidence.brokerEntryTimestamp())
                    .isEqualTo(guardedMessageId.brokerEntryTimestamp());
            assertThat(evidence.sendCommandSha256()).hasSize(32);
            assertThat(evidence.authenticatedResponseCommandSha256()).hasSize(32);
        }
    }

    @Test
    public void oldResourceIncarnationIsRejectedAfterDeleteAndRecreate() throws Exception {
        String topic = newTopicName();
        TopicResourceGuard oldGuard = publishGuard(topic, incarnation((byte) 0x22), 200L);

        try (Producer<byte[]> producer = guardedProducer(topic, oldGuard)) {
            assertThat(producer.send("before-recreate".getBytes(StandardCharsets.UTF_8)))
                    .isInstanceOf(GuardedMessageId.class);
        }

        admin.topics().delete(topic, false);
        admin.topics().createNonPartitionedTopic(topic);
        TopicResourceGuard newGuard = publishGuard(topic, incarnation((byte) 0x33), 300L);
        assertThat(newGuard).isNotEqualTo(oldGuard);

        Throwable failure = catchThrowable(() -> guardedProducer(topic, oldGuard));
        assertThat(failure).isNotNull();
        TopicResourceGuardException guardException = findGuardException(failure);
        assertThat(guardException).isNotNull();
        assertThat(guardException.expectedGuard()).isEqualTo(oldGuard);
        assertThat(guardException.definitelyNotPersisted()).isTrue();
        assertThat(guardException.responseEvidence()).isEmpty();

        try (Producer<byte[]> producer = guardedProducer(topic, newGuard)) {
            assertThat(producer.send("after-recreate".getBytes(StandardCharsets.UTF_8)))
                    .isInstanceOf(GuardedMessageId.class);
        }
    }

    private Producer<byte[]> guardedProducer(String topic, TopicResourceGuard guard)
            throws PulsarClientException {
        return pulsarClient.newProducer().topic(topic).resourceGuard(guard).create();
    }

    private TopicResourceGuard publishGuard(String topic, byte[] incarnation, long timestamp) throws Exception {
        if (!admin.topics().getList("my-property/my-ns").contains(topic)) {
            admin.topics().createNonPartitionedTopic(topic);
        }
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic)
                .orElseThrow(() -> new IllegalStateException("topic was not loaded: " + topic));
        Map<String, String> properties = Map.of(
                ValidatedTopicResourceGuard.VERSION_PROPERTY, "1",
                ValidatedTopicResourceGuard.INCARNATION_PROPERTY,
                Base64.getUrlEncoder().withoutPadding().encodeToString(incarnation),
                ValidatedTopicResourceGuard.CREATED_AT_PROPERTY, Long.toUnsignedString(timestamp));
        persistentTopic.updateTopicResourceGuardProperties(properties).get(10, TimeUnit.SECONDS);

        ValidatedTopicResourceGuard view = persistentTopic.getCurrentTopicResourceGuardView();
        assertThat(view.isValid()).as("resource guard view for %s", topic).isTrue();
        return view.guard();
    }

    private static byte[] incarnation(byte value) {
        byte[] incarnation = new byte[TopicResourceGuard.RESOURCE_INCARNATION_BYTES];
        Arrays.fill(incarnation, value);
        return incarnation;
    }

    private static TopicResourceGuardException findGuardException(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof TopicResourceGuardException) {
                return (TopicResourceGuardException) current;
            }
            current = current.getCause();
        }
        return null;
    }
}
