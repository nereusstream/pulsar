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
package org.apache.pulsar.common.protocol;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.TopicResourceGuardReceipt;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.testng.annotations.Test;

public class CommandsTopicResourceGuardTest {

    @Test
    public void producerGuardSurvivesWireRoundTrip() {
        TopicResourceGuard guard = guard();
        BaseCommand command = parseFrame(Commands.newProducer(
                "persistent://tenant/ns/topic-partition-0", 1L, 2L, "producer", false, Map.of(), null, 0L, false,
                ProducerAccessMode.Shared, Optional.empty(), false, null, guard));

        assertEquals(command.getType(), BaseCommand.Type.PRODUCER);
        org.apache.pulsar.common.api.proto.TopicResourceGuard wireGuard =
                command.getProducer().getResourceGuard();
        assertTrue(command.getProducer().hasResourceGuard());
        assertEquals(wireGuard.getGuardVersion(), 1);
        assertEquals(wireGuard.getAuthenticatedClusterId(), "cluster-a");
        assertEquals(wireGuard.getTopicCreationTimestamp(), Long.MIN_VALUE);
        assertEquals(wireGuard.getResourceIncarnation(), guard.resourceIncarnation());
        assertTrue(Commands.peerSupportsTopicResourceGuard(22));
        assertFalse(Commands.peerSupportsTopicResourceGuard(21));
    }

    @Test
    public void subscribeGuardSurvivesWireRoundTrip() {
        TopicResourceGuard guard = guard();
        BaseCommand command = parseFrame(Commands.newSubscribe(
                "persistent://tenant/ns/topic-partition-0", "source", 7L, 8L,
                org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Exclusive, 0, "consumer", true,
                null, Map.of(), false, false,
                org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition.Earliest, 0, null, false,
                null, Map.of(), 0L, guard));

        assertEquals(command.getType(), BaseCommand.Type.SUBSCRIBE);
        assertTrue(command.getSubscribe().hasResourceGuard());
        org.apache.pulsar.common.api.proto.TopicResourceGuard wireGuard =
                command.getSubscribe().getResourceGuard();
        assertEquals(wireGuard.getGuardVersion(), 1);
        assertEquals(wireGuard.getAuthenticatedClusterId(), "cluster-a");
        assertEquals(wireGuard.getTopicCreationTimestamp(), Long.MIN_VALUE);
        assertEquals(wireGuard.getResourceIncarnation(), guard.resourceIncarnation());
    }

    @Test
    public void successAttestationAndReceiptSurviveWireRoundTrip() {
        TopicResourceGuardAttestation attestation = new TopicResourceGuardAttestation(
                guard(), "persistent://tenant/ns/topic-partition-0", 0);
        BaseCommand success = Commands.newProducerSuccessCommand(2L, "producer", -1L, SchemaVersion.Empty,
                Optional.empty(), true, Optional.of(attestation));
        BaseCommand parsedSuccess = parseFrame(Commands.serializeWithSize(success));

        assertTrue(parsedSuccess.getProducerSuccess().hasResourceGuardAttestation());
        assertEquals(parsedSuccess.getProducerSuccess().getResourceGuardAttestation().getPhysicalTopic(),
                attestation.physicalTopic());
        assertEquals(parsedSuccess.getProducerSuccess().getResourceGuardAttestation().getPartition(), 0);

        org.apache.pulsar.common.api.proto.TopicResourceGuardAttestation wireAttestation =
                new org.apache.pulsar.common.api.proto.TopicResourceGuardAttestation()
                        .setGuardVersion(attestation.guardVersion())
                        .setAuthenticatedClusterId(attestation.authenticatedClusterId())
                        .setResourceIncarnation(attestation.resourceIncarnation())
                        .setTopicCreationTimestamp(attestation.topicCreationTimestamp())
                        .setPhysicalTopic(attestation.physicalTopic())
                        .setPartition(attestation.partition());
        TopicResourceGuardReceipt receipt = new TopicResourceGuardReceipt()
                .setBrokerEntryTimestamp(99L);
        receipt.setAttestation().copyFrom(wireAttestation);
        BaseCommand parsedReceipt = parseFrame(Commands.serializeWithSize(
                Commands.newSendReceiptCommand(1L, 2L, 2L, 3L, 4L, receipt)));

        assertTrue(parsedReceipt.getSendReceipt().hasResourceGuardReceipt());
        assertEquals(parsedReceipt.getSendReceipt().getResourceGuardReceipt().getBrokerEntryTimestamp(), 99L);
        assertEquals(parsedReceipt.getSendReceipt().getResourceGuardReceipt().getAttestation().getResourceIncarnation(),
                attestation.resourceIncarnation());
    }

    private static TopicResourceGuard guard() {
        byte[] incarnation = new byte[TopicResourceGuard.RESOURCE_INCARNATION_BYTES];
        for (int i = 0; i < incarnation.length; i++) {
            incarnation[i] = (byte) i;
        }
        return new TopicResourceGuard("cluster-a", incarnation, Long.MIN_VALUE);
    }

    private static BaseCommand parseFrame(ByteBuf frame) {
        try {
            frame.skipBytes(4);
            int commandSize = (int) frame.readUnsignedInt();
            BaseCommand command = new BaseCommand();
            command.parseFrom(frame, commandSize);
            command.materialize();
            return command;
        } finally {
            frame.release();
        }
    }
}
