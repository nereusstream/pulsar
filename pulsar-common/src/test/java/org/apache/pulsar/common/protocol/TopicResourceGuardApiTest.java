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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import java.util.Arrays;
import org.apache.pulsar.client.api.GuardedSendErrorEvidence;
import org.apache.pulsar.client.api.GuardedSendSuccessEvidence;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;
import org.testng.annotations.Test;

public class TopicResourceGuardApiTest {

    @Test
    public void valuesAreImmutableAndSupportUnsignedTimestamp() {
        byte[] incarnation = new byte[TopicResourceGuard.RESOURCE_INCARNATION_BYTES];
        Arrays.fill(incarnation, (byte) 0xA5);
        TopicResourceGuard guard = new TopicResourceGuard("cluster-a", incarnation, Long.MIN_VALUE);
        incarnation[0] = 0;

        assertEquals(guard.topicCreationTimestamp(), Long.MIN_VALUE);
        assertEquals(guard.resourceIncarnation()[0], (byte) 0xA5);

        byte[] returned = guard.resourceIncarnation();
        returned[1] = 0;
        assertEquals(guard.resourceIncarnation()[1], (byte) 0xA5);

        TopicResourceGuard equal = new TopicResourceGuard("cluster-a", guard.resourceIncarnation(), Long.MIN_VALUE);
        assertEquals(guard, equal);
        assertEquals(guard.hashCode(), equal.hashCode());
        assertNotEquals(guard, new TopicResourceGuard("cluster-b", guard.resourceIncarnation(), Long.MIN_VALUE));
    }

    @Test
    public void attestationAndEvidenceDefensivelyCopyBytes() {
        byte[] incarnation = new byte[TopicResourceGuard.RESOURCE_INCARNATION_BYTES];
        byte[] sendDigest = new byte[32];
        byte[] responseDigest = new byte[32];
        Arrays.fill(incarnation, (byte) 1);
        Arrays.fill(sendDigest, (byte) 2);
        Arrays.fill(responseDigest, (byte) 3);
        TopicResourceGuard guard = new TopicResourceGuard("cluster-a", incarnation, 7L);
        TopicResourceGuardAttestation attestation = new TopicResourceGuardAttestation(guard, "topic-partition-0", 0);
        GuardedSendSuccessEvidence evidence = new GuardedSendSuccessEvidence(
                22, 4L, 5L, 6L, attestation, 7L, 8L, 9L, sendDigest, responseDigest);

        incarnation[0] = 9;
        sendDigest[0] = 9;
        responseDigest[0] = 9;
        assertEquals(attestation.resourceIncarnation()[0], (byte) 1);
        assertEquals(evidence.sendCommandSha256()[0], (byte) 2);
        assertEquals(evidence.authenticatedResponseCommandSha256()[0], (byte) 3);
        assertEquals(Arrays.equals(evidence.sendCommandSha256(), evidence.sendCommandSha256()), true);
    }

    @Test
    public void rejectsMalformedValues() {
        assertThrows(IllegalArgumentException.class,
                () -> new TopicResourceGuard("cluster-a", new byte[31], 1L));
        assertThrows(IllegalArgumentException.class,
                () -> new GuardedSendErrorEvidence(21, 1L, 2L, 3L, 26, new byte[32], new byte[32]));
        assertThrows(IllegalArgumentException.class,
                () -> new GuardedSendSuccessEvidence(22, 1L, 2L, 3L,
                        new TopicResourceGuardAttestation(
                                new TopicResourceGuard("cluster-a", new byte[32], 1L), "topic", 0),
                        4L, 5L, 6L, new byte[31], new byte[32]));
    }
}
