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
import java.util.HexFormat;
import org.apache.pulsar.common.naming.NamespaceName;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StorageClassBindingCodecTest {
    private static final String PERSISTENCE_NAME = "tenant/ns/persistent/topic";
    private static final String TOPIC_NAME = "persistent://tenant/ns/topic";
    private final StorageClassBindingCodec codec = new StorageClassBindingCodec();

    @DataProvider
    public Object[][] states() {
        return new Object[][] {
            {StorageClassBindingState.CLAIMED,
                "4e5342310001000000000077000000010000001a74656e616e742f6e732f70657273697374656e742f746f706963"
                + "0000001c70657273697374656e743a2f2f74656e616e742f6e732f746f706963000000066e657265757300000000"
                + "0000000700000007434c41494d454400000000000004d2000000000000000500000000000000008b5c4392"},
            {StorageClassBindingState.ACTIVE,
                "4e5342310001000000000076000000010000001a74656e616e742f6e732f70657273697374656e742f746f706963"
                + "0000001c70657273697374656e743a2f2f74656e616e742f6e732f746f706963000000066e657265757300000000"
                + "000000070000000641435449564500000000000004d200000000000000050000000000000000732fba29"},
            {StorageClassBindingState.DELETING,
                "4e5342310001000000000078000000010000001a74656e616e742f6e732f70657273697374656e742f746f706963"
                + "0000001c70657273697374656e743a2f2f74656e616e742f6e732f746f706963000000066e657265757300000000"
                + "000000070000000844454c4554494e4700000000000004d2000000000000000500000000000000004c479c41"},
            {StorageClassBindingState.DELETED,
                "4e5342310001000000000077000000010000001a74656e616e742f6e732f70657273697374656e742f746f706963"
                + "0000001c70657273697374656e743a2f2f74656e616e742f6e732f746f706963000000066e657265757300000000"
                + "000000070000000744454c4554454400000000000004d20000000000000005000000000000000008a7620c"}
        };
    }

    @Test(dataProvider = "states")
    public void roundTripsEveryStateWithGoldenBytesAndHydratesBackendVersion(
            StorageClassBindingState state, String goldenBytes) {
        StorageClassBindingRecord input = record(state);

        byte[] encoded = codec.encode(input);
        StorageClassBindingRecord decoded = codec.decode(encoded, 37);

        assertThat(HexFormat.of().formatHex(encoded)).isEqualTo(goldenBytes);
        assertThat(decoded).isEqualTo(input.withMetadataVersion(37));
    }

    @Test
    public void rejectsChecksumDamageAndTrailingData() {
        byte[] damaged = codec.encode(record(StorageClassBindingState.ACTIVE));
        damaged[20] ^= 1;
        assertThatThrownBy(() -> codec.decode(damaged, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("storage binding checksum mismatch");

        byte[] trailing = java.util.Arrays.copyOf(
                codec.encode(record(StorageClassBindingState.ACTIVE)),
                codec.encode(record(StorageClassBindingState.ACTIVE)).length + 1);
        assertThatThrownBy(() -> codec.decode(trailing, 1))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void enforcesStateTransitionsAndGenerationRollover() {
        StorageClassBindingRecord claimed = record(StorageClassBindingState.CLAIMED);

        StorageClassBindingRecord deleted = claimed
                .transitionTo(StorageClassBindingState.ACTIVE)
                .transitionTo(StorageClassBindingState.DELETING)
                .transitionTo(StorageClassBindingState.DELETED);
        StorageClassBindingRecord next = deleted.nextGeneration(StorageClassBindingRecord.BOOKKEEPER, 2000);

        assertThat(deleted.stateVersion()).isEqualTo(8);
        assertThat(next.bindingGeneration()).isEqualTo(8);
        assertThat(next.state()).isEqualTo(StorageClassBindingState.CLAIMED);
        assertThat(next.stateVersion()).isZero();
        assertThat(next.storageClass()).isEqualTo(StorageClassBindingRecord.BOOKKEEPER);
        assertThatThrownBy(() -> claimed.transitionTo(StorageClassBindingState.DELETED))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void buildsNamespaceListableCollisionSeparatedKeys() {
        StorageClassBindingKeyspace keyspace = new StorageClassBindingKeyspace();
        NamespaceName namespace = NamespaceName.get("tenant/ns");

        String namespaceRoot = keyspace.namespaceRoot(namespace);
        String topicKey = keyspace.bindingKey(namespace, PERSISTENCE_NAME);

        assertThat(topicKey).startsWith(namespaceRoot);
        assertThat(topicKey.substring(namespaceRoot.length())).hasSize(52);
        assertThatThrownBy(() -> keyspace.bindingKey(
                NamespaceName.get("other/ns"), PERSISTENCE_NAME))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("persistence name does not belong to namespace");
    }

    private static StorageClassBindingRecord record(StorageClassBindingState state) {
        return new StorageClassBindingRecord(
                1,
                PERSISTENCE_NAME,
                TOPIC_NAME,
                StorageClassBindingRecord.NEREUS,
                7,
                state,
                1234,
                5,
                99);
    }
}
