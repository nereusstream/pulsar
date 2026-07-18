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
import static org.mockito.Mockito.mock;
import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import com.nereusstream.managedledger.integration.NereusCreationGuard;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import com.nereusstream.pulsar.NereusProcessIdentity;
import com.nereusstream.pulsar.NereusRuntimeContext;
import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import java.security.SecureRandom;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.testng.annotations.Test;

public class NereusBrokerStorageConfigurationCursorTest {
    @Test
    public void mapsTypedCursorSettingsThroughTheCanonicalSevenArgumentConfiguration() {
        ServiceConfiguration broker = validConfiguration();
        broker.setNereusMaxOxiaPendingOperations(2_000);
        broker.setNereusMaxCursorMetadataPendingOperations(777);
        broker.setNereusCursorMetadataMaxScanPageSize(512);
        broker.setNereusCursorScanPageSize(384);
        broker.setNereusCursorAckPositionsPerRequestMax(321);
        broker.setNereusCursorMutationQueueMax(2_048);
        broker.setNereusCursorSnapshotOperationTimeoutSeconds(65);

        var runtime = new NereusBrokerStorageConfiguration(broker).runtimeConfiguration(
                NereusProcessIdentity.generate(new SecureRandom()));

        assertThat(runtime.cursorMetadata().maxPendingOperations()).isEqualTo(777);
        assertThat(runtime.cursorMetadata().maxScanPageSize()).isEqualTo(512);
        assertThat(runtime.cursorStorage().cursorScanPageSize()).isEqualTo(384);
        assertThat(runtime.cursorStorage().cursorAckPositionsPerRequestMax()).isEqualTo(321);
        assertThat(runtime.cursorStorage().cursorMutationQueueMax()).isEqualTo(2_048);
        assertThat(runtime.cursorStorage().cursorSnapshotOperationTimeout()).hasSeconds(65);
    }

    @Test
    public void rejectsCursorCrossConfigViolationsBeforeRuntimeCreation() {
        ServiceConfiguration scanOverflow = validConfiguration();
        scanOverflow.setNereusCursorMetadataMaxScanPageSize(32);
        scanOverflow.setNereusCursorScanPageSize(64);
        assertThatThrownBy(() -> runtime(scanOverflow))
                .hasMessageContaining("cursor scan page");

        ServiceConfiguration snapshotDeadline = validConfiguration();
        snapshotDeadline.setNereusCursorSnapshotOperationTimeoutSeconds(
                Math.addExact(snapshotDeadline.getNereusCloseTimeoutSeconds(), 1));
        assertThatThrownBy(() -> runtime(snapshotDeadline))
                .hasMessageContaining("snapshot timeout");

        ServiceConfiguration wireLimit = validConfiguration();
        wireLimit.setNereusCursorMetadataMaxValueBytes(65_535);
        assertThatThrownBy(() -> runtime(wireLimit))
                .hasMessageContaining("frozen F3 value");
    }

    @Test
    public void compatibilityContextRemainsFailClosedForCursorActivation() {
        NereusRuntimeContext compatibility = new NereusRuntimeContext(
                mock(EventLoopGroup.class),
                mock(OpenTelemetry.class),
                mock(NereusCreationGuard.class),
                mock(ObjectStoreSecretResolver.class),
                getClass().getClassLoader());

        assertThatThrownBy(() -> compatibility.cursorProtocolActivationGuard()
                .acquireFirstActivationPermit(mock(CursorLedgerIdentity.class)).join())
                .hasRootCauseMessage("NEREUS_CURSOR_CAPABILITY_NOT_READY");
    }

    private static Object runtime(ServiceConfiguration configuration) {
        return new NereusBrokerStorageConfiguration(configuration).runtimeConfiguration(
                NereusProcessIdentity.generate(new SecureRandom()));
    }

    private static ServiceConfiguration validConfiguration() {
        ServiceConfiguration broker = new ServiceConfiguration();
        broker.setNereusEnabled(true);
        broker.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
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
