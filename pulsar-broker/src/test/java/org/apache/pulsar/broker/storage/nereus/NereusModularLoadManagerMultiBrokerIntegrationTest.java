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
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** Real Oxia-backed Nereus broker registration smoke using the modular load manager. */
@Test(groups = "broker-isolated")
public class NereusModularLoadManagerMultiBrokerIntegrationTest {
    private final NereusMultiBrokerIntegrationTest cluster =
            new NereusMultiBrokerIntegrationTest(
                    false,
                    false,
                    StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                    false,
                    ModularLoadManagerImpl.class.getName());

    @BeforeClass(alwaysRun = true)
    public void startCluster() throws Exception {
        cluster.startCluster();
    }

    @AfterClass(alwaysRun = true)
    public void closeCluster() throws Exception {
        cluster.closeCluster();
    }

    @Test(timeOut = 600_000)
    public void startsBothBrokersAndConvergesCapabilitiesThroughMetadataStore() {
        cluster.awaitBaseCapabilityConvergence();
        for (int index = 0; index < 2; index++) {
            assertThat(cluster.broker(index).getLoadManager().get())
                    .isInstanceOf(ModularLoadManagerWrapper.class);
            NereusManagedLedgerStorage storage =
                    (NereusManagedLedgerStorage) cluster.broker(index)
                            .getManagedLedgerStorage();
            storage.capabilityCoordinator().requireClusterReady().join();
            storage.capabilityCoordinator().requireCursorClusterReady().join();
        }
    }
}
