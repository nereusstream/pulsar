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
import com.nereusstream.pulsar.NereusProcessIdentity;
import java.security.SecureRandom;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.annotations.Test;

public class NereusBrokerStorageConfigurationTest {
    @Test
    public void mapsAllRuntimeIdentityAndCapacityBoundaries() {
        ServiceConfiguration broker = validConfiguration();
        NereusProcessIdentity identity = NereusProcessIdentity.generate(new SecureRandom());

        var runtime = new NereusBrokerStorageConfiguration(broker).runtimeConfiguration(identity);

        assertThat(runtime.streamStorage().cluster()).isEqualTo("test-cluster");
        assertThat(runtime.streamStorage().processRunId()).isEqualTo(identity.processRunId());
        assertThat(runtime.streamStorage().writerId()).isEqualTo(identity.writerId());
        assertThat(runtime.oxia().maxCommitChainScan())
                .isEqualTo(runtime.streamStorage().maxCommitChainScan());
        assertThat(runtime.managedLedger().maxRetainedAppendAttempts())
                .isEqualTo(runtime.streamStorage().maxRetainedAppendAttempts());
        assertThat(runtime.projectionMetadata().maxPendingOperations())
                .isLessThanOrEqualTo(runtime.oxia().maxPendingOperations());
    }

    @Test
    public void rejectsInvalidCrossConfigBeforeClientConstruction() {
        ServiceConfiguration disabled = validConfiguration();
        disabled.setNereusEnabled(false);
        assertThatThrownBy(() -> new NereusBrokerStorageConfiguration(disabled)
                .runtimeConfiguration(NereusProcessIdentity.generate(new SecureRandom())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nereusEnabled");

        ServiceConfiguration undersizedCache = validConfiguration();
        undersizedCache.setNereusMaxCachedStreams(9_999);
        assertThatThrownBy(() -> new NereusBrokerStorageConfiguration(undersizedCache)
                .runtimeConfiguration(NereusProcessIdentity.generate(new SecureRandom())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxCachedStreams");
    }

    private static ServiceConfiguration validConfiguration() {
        ServiceConfiguration broker = new ServiceConfiguration();
        broker.setNereusEnabled(true);
        broker.setClusterName("test-cluster");
        broker.setNereusOxiaServiceAddress("localhost:6648");
        broker.setNereusOxiaNamespace("nereus/test-cluster");
        broker.setNereusObjectStoreEndpoint("http://127.0.0.1:4566");
        broker.setNereusObjectStoreRegion("us-east-1");
        broker.setNereusObjectStoreBucket("nereus-test");
        broker.setNereusObjectStorePrefix("test-cluster");
        return broker;
    }
}
