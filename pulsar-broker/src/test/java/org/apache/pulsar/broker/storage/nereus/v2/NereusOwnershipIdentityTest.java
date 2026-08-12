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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.security.SecureRandom;
import java.util.HashSet;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NereusOwnershipIdentityTest {
    private static final NereusOwnershipId BROKER =
            new NereusOwnershipId("11111111111111111111111111111111");
    private static final NereusOwnershipId ACQUISITION =
            new NereusOwnershipId("22222222222222222222222222222222");
    private static final NereusOwnershipId NEXT_BROKER =
            new NereusOwnershipId("33333333333333333333333333333333");
    private static final NereusOwnershipId NEXT_ACQUISITION =
            new NereusOwnershipId("44444444444444444444444444444444");

    @Test
    public void rejectsNonCanonicalAndZeroIdentities() {
        assertThatThrownBy(() -> new NereusOwnershipId("ABCDEF00000000000000000000000000"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NereusOwnershipId("00000000000000000000000000000000"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ServiceUnitStateData(
                        ServiceUnitState.Owned, "broker", null, null, false, 1, 1,
                        BROKER.value(), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void generatorUsesOneProcessIncarnationAndUniqueAcquisitions() {
        NereusOwnershipIdentityGenerator generator = new NereusOwnershipIdentityGenerator(new SecureRandom());
        var identities = new HashSet<NereusOwnershipId>();
        identities.add(generator.brokerIncarnationId());
        for (int index = 0; index < 100; index++) {
            identities.add(generator.newAcquisitionId());
        }

        assertThat(identities).hasSize(101);
        assertThat(generator.brokerIncarnationId().value()).hasSize(32);
    }

    @Test
    public void closedTransitionRulesPreserveOrReplaceTheExactTuple() {
        ServiceUnitStateData assigning = state(ServiceUnitState.Assigning, BROKER, ACQUISITION);
        ServiceUnitStateData owned = state(ServiceUnitState.Owned, BROKER, ACQUISITION);
        ServiceUnitStateData releasing = state(ServiceUnitState.Releasing, BROKER, ACQUISITION);
        ServiceUnitStateData transferred = state(ServiceUnitState.Assigning, NEXT_BROKER, NEXT_ACQUISITION);

        assertThat(NereusOwnershipTransitionValidator.shouldReject(null, assigning)).isFalse();
        assertThat(NereusOwnershipTransitionValidator.shouldReject(assigning, owned)).isFalse();
        assertThat(NereusOwnershipTransitionValidator.shouldReject(
                        assigning, state(ServiceUnitState.Owned, BROKER, NEXT_ACQUISITION)))
                .isTrue();
        assertThat(NereusOwnershipTransitionValidator.shouldReject(owned, releasing)).isFalse();
        assertThat(NereusOwnershipTransitionValidator.shouldReject(releasing, transferred)).isFalse();
        assertThat(NereusOwnershipTransitionValidator.shouldReject(releasing, assigning)).isTrue();
        assertThat(NereusOwnershipTransitionValidator.shouldReject(owned,
                        new ServiceUnitStateData(ServiceUnitState.Releasing, "target", "broker", null, true, 1, 2)))
                .isTrue();
    }

    private static ServiceUnitStateData state(
            ServiceUnitState state, NereusOwnershipId broker, NereusOwnershipId acquisition) {
        String destination = switch (state) {
            case Releasing, Splitting, Deleted, Free -> null;
            default -> "broker";
        };
        String source = destination == null ? "broker" : null;
        return new ServiceUnitStateData(
                state, destination, source, null, false, 1, 1, broker, acquisition);
    }
}
