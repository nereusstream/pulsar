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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.common.api.proto.TopicResourceGuard;
import org.testng.annotations.Test;

public class ValidatedTopicResourceGuardTest {

    @Test
    public void parsesCanonicalUnsignedPropertiesAndMatchesWireGuard() {
        byte[] incarnation = new byte[32];
        for (int i = 0; i < incarnation.length; i++) {
            incarnation[i] = (byte) (i + 1);
        }
        Map<String, String> properties = properties(incarnation, Long.MIN_VALUE);
        ValidatedTopicResourceGuard view = ValidatedTopicResourceGuard.fromProperties(
                "persistent://tenant/ns/topic-partition-0", "cluster-a", 0, properties);

        assertTrue(view.isValid());
        assertEquals(view.guard().topicCreationTimestamp(), Long.MIN_VALUE);
        assertEquals(view.attestation().physicalTopic(), "persistent://tenant/ns/topic-partition-0");
        assertEquals(view.attestation().partition(), 0);

        TopicResourceGuard wireGuard = new TopicResourceGuard()
                .setGuardVersion(1)
                .setAuthenticatedClusterId("cluster-a")
                .setResourceIncarnation(incarnation)
                .setTopicCreationTimestamp(Long.MIN_VALUE);
        assertTrue(view.matchesProtoGuard(wireGuard));
        wireGuard.setAuthenticatedClusterId("cluster-b");
        assertFalse(view.matchesProtoGuard(wireGuard));
    }

    @Test
    public void rejectsIncompleteNonCanonicalOrMalformedProperties() {
        byte[] incarnation = new byte[32];
        Map<String, String> properties = properties(incarnation, 1L);

        properties.remove(ValidatedTopicResourceGuard.VERSION_PROPERTY);
        assertFalse(ValidatedTopicResourceGuard.fromProperties("topic", "cluster-a", 0, properties).isValid());

        properties = properties(incarnation, 1L);
        properties.put(ValidatedTopicResourceGuard.INCARNATION_PROPERTY,
                Base64.getUrlEncoder().encodeToString(incarnation));
        assertFalse(ValidatedTopicResourceGuard.fromProperties("topic", "cluster-a", 0, properties).isValid());

        properties = properties(incarnation, 1L);
        properties.put(ValidatedTopicResourceGuard.CREATED_AT_PROPERTY, "not-uint64");
        assertFalse(ValidatedTopicResourceGuard.fromProperties("topic", "cluster-a", 0, properties).isValid());
    }

    private static Map<String, String> properties(byte[] incarnation, long timestamp) {
        Map<String, String> properties = new HashMap<>();
        properties.put(ValidatedTopicResourceGuard.VERSION_PROPERTY, "1");
        properties.put(ValidatedTopicResourceGuard.INCARNATION_PROPERTY,
                Base64.getUrlEncoder().withoutPadding().encodeToString(incarnation));
        properties.put(ValidatedTopicResourceGuard.CREATED_AT_PROPERTY, Long.toUnsignedString(timestamp));
        return properties;
    }
}
