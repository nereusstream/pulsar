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
package org.apache.pulsar.broker.service.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.nereusstream.managedledger.NereusManagedLedger;
import java.util.Set;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.apache.pulsar.broker.service.BacklogQuotaManager;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.storage.nereus.NereusAdminOperation;
import org.apache.pulsar.broker.storage.nereus.NereusResolvedTopicFeatures;
import org.apache.pulsar.broker.storage.nereus.NereusTopicOpenContext;
import org.testng.annotations.Test;

public class PersistentTopicNereusAdmissionTest {
    @Test
    public void gatesAdminOperationsOnlyForNereusLedger() throws Exception {
        BrokerService brokerService = brokerService();
        PersistentTopic nereusTopic = new PersistentTopic(
                "persistent://tenant/ns/nereus",
                brokerService,
                mock(NereusManagedLedger.class),
                mock(MessageDeduplication.class));
        nereusTopic.installNereusTopicOpenContext(new NereusTopicOpenContext(
                new ManagedLedgerConfig(),
                new NereusResolvedTopicFeatures(
                        Set.of(), false, 0, 0, 0, false, false, false, false, false, false)));

        assertThat(nereusTopic.isNereusManagedLedger()).isTrue();
        nereusTopic.validateNereusAdminOperation(NereusAdminOperation.TERMINATE_TOPIC).get();
        assertThatThrownBy(() -> nereusTopic
                .validateNereusAdminOperation(NereusAdminOperation.TRUNCATE_TOPIC).get())
                .hasRootCauseMessage("NEREUS_UNSUPPORTED_ADMIN_OPERATION:TRUNCATE_TOPIC");

        PersistentTopic bookKeeperTopic = new PersistentTopic(
                "persistent://tenant/ns/bookkeeper",
                brokerService,
                mock(ManagedLedger.class),
                mock(MessageDeduplication.class));
        assertThat(bookKeeperTopic.isNereusManagedLedger()).isFalse();
        bookKeeperTopic.validateNereusAdminOperation(NereusAdminOperation.TRUNCATE_TOPIC).get();
    }

    private static BrokerService brokerService() {
        BrokerService brokerService = mock(BrokerService.class);
        PulsarService pulsar = mock(PulsarService.class);
        when(brokerService.pulsar()).thenReturn(pulsar);
        when(brokerService.getPulsar()).thenReturn(pulsar);
        when(brokerService.getBacklogQuotaManager()).thenReturn(mock(BacklogQuotaManager.class));
        when(pulsar.getConfiguration()).thenReturn(new ServiceConfiguration());
        when(pulsar.getMonotonicClock()).thenReturn(AsyncTokenBucket.DEFAULT_SNAPSHOT_CLOCK);
        return brokerService;
    }
}
