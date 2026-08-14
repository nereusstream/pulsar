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

import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.TopicResourceGuard;
import org.apache.pulsar.common.naming.TopicName;

/** Validates a wire guard against one atomically published PersistentTopic view. */
public final class TopicResourceGuardValidator {
    private TopicResourceGuardValidator() {
    }

    public static ValidatedTopicResourceGuard load(PersistentTopic topic, String clusterId) {
        TopicName topicName = TopicName.get(topic.getName());
        return ValidatedTopicResourceGuard.fromProperties(topic.getName(), clusterId,
                Math.max(0, topicName.getPartitionIndex()), topic.getManagedLedger().getProperties());
    }

    public static boolean matches(ValidatedTopicResourceGuard current, TopicResourceGuard requested) {
        return current != null && current.matchesProtoGuard(requested);
    }
}
