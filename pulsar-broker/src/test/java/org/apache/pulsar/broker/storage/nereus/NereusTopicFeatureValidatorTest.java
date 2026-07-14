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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Set;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

public class NereusTopicFeatureValidatorTest {
    private final NereusTopicFeatureValidator validator = new NereusTopicFeatureValidator();

    @Test
    public void admitsTheExactF3SubscriptionMatrix() {
        for (SubType type : new SubType[] {SubType.Exclusive, SubType.Failover, SubType.Shared}) {
            assertThatCode(() -> validator.validateSubscribe(type, true, false, false, null))
                    .doesNotThrowAnyException();
            assertThatCode(() -> validator.validateSubscribe(type, false, false, false, new KeySharedMeta()))
                    .doesNotThrowAnyException();
        }
        assertThatThrownBy(() -> validator.validateSubscribe(
                SubType.Key_Shared, true, false, false, null))
                .hasMessage("NEREUS_UNSUPPORTED_SUBSCRIPTION:KEY_SHARED");
        assertThatThrownBy(() -> validator.validateSubscribe(
                SubType.Exclusive, true, true, false, null))
                .hasMessage("NEREUS_UNSUPPORTED_SUBSCRIPTION:READ_COMPACTED");
        assertThatThrownBy(() -> validator.validateSubscribe(
                SubType.Exclusive, true, false, true, null))
                .hasMessage("NEREUS_UNSUPPORTED_SUBSCRIPTION:REPLICATED");
    }

    @Test
    public void admitsCursorLifecyclePoliciesButNotF4PhysicalLifecyclePolicies() {
        TopicName topic = TopicName.get("persistent://tenant/ns/topic");
        NereusResolvedTopicFeatures ttlAndExpiration = new NereusResolvedTopicFeatures(
                Set.of(), false, 30, 60, 0, false, false, false, false, false, false);
        assertThatCode(() -> validator.validateTopicOpen(
                topic, new ManagedLedgerConfig(), ttlAndExpiration)).doesNotThrowAnyException();

        NereusResolvedTopicFeatures retention = new NereusResolvedTopicFeatures(
                Set.of(), false, 30, 60, 0, true, false, false, false, false, false);
        assertThatThrownBy(() -> validator.validateTopicOpen(
                topic, new ManagedLedgerConfig(), retention))
                .hasMessage("NEREUS_UNSUPPORTED_TOPIC_FEATURE:RETENTION");
    }
}
