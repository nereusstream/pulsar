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
package org.apache.pulsar.broker.storage.nereus.v2;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Optional;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateMetadataStoreTableViewImpl;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MetadataStoreNereusOwnershipWitnessProviderTest {
    private static final String SERVICE_UNIT = "tenant/ns/0x00000000_0xffffffff";
    private static final NereusOwnershipId BROKER =
            new NereusOwnershipId("11111111111111111111111111111111");
    private static final NereusOwnershipId ACQUISITION =
            new NereusOwnershipId("22222222222222222222222222222222");

    private MetadataStore store;
    private NereusOwnershipStateCodec codec;
    private MetadataStoreNereusOwnershipWitnessProvider provider;

    @BeforeMethod
    public void setUp() throws Exception {
        store = MetadataStoreFactory.create(
                "memory:p1-witness-" + java.util.UUID.randomUUID(), MetadataStoreConfig.builder().build());
        codec = new NereusOwnershipStateCodec();
        provider = new MetadataStoreNereusOwnershipWitnessProvider(store, "broker-1", codec);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        store.close();
    }

    @Test
    public void directReadReturnsExactVersionBoundLocalOwnedWitness() {
        ServiceUnitStateData owned = new ServiceUnitStateData(
                ServiceUnitState.Owned, "broker-1", null, null, false, 1, 1, BROKER, ACQUISITION);
        String path = ServiceUnitStateMetadataStoreTableViewImpl.PATH_PREFIX + "/" + SERVICE_UNIT;
        var stat = store.put(path, codec.encode(owned), Optional.of(-1L)).join();

        var witness = provider.read(SERVICE_UNIT).toCompletableFuture().join().orElseThrow();

        assertThat(witness.backendVersion()).isEqualTo(stat.getVersion());
        assertThat(witness.brokerIncarnationId()).isEqualTo(BROKER);
        assertThat(witness.acquisitionId()).isEqualTo(ACQUISITION);
        assertThat(witness.canonicalStoredDigest())
                .isEqualTo(com.nereusstream.domain.bytes.Sha256Digest.hash(witness.canonicalStoredBytes()));
    }

    @Test
    public void nonLocalLegacyAndNonCanonicalValuesFailClosed() {
        String path = ServiceUnitStateMetadataStoreTableViewImpl.PATH_PREFIX + "/" + SERVICE_UNIT;
        ServiceUnitStateData remote = new ServiceUnitStateData(
                ServiceUnitState.Owned, "broker-2", null, null, false, 1, 1, BROKER, ACQUISITION);
        var stat = store.put(path, codec.encode(remote), Optional.of(-1L)).join();
        assertThat(provider.read(SERVICE_UNIT).toCompletableFuture().join()).isEmpty();

        byte[] canonical = codec.encode(remote);
        byte[] padded = java.util.Arrays.copyOf(canonical, canonical.length + 1);
        padded[padded.length - 1] = ' ';
        store.put(path, padded, Optional.of(stat.getVersion())).join();
        assertThat(provider.read(SERVICE_UNIT).toCompletableFuture().join()).isEmpty();
    }
}
