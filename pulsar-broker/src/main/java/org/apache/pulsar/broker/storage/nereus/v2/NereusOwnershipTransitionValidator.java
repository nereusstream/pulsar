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

import java.util.Objects;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;

/** Closed identity rules applied before the native force/business-edge decision. */
public final class NereusOwnershipTransitionValidator {
    private NereusOwnershipTransitionValidator() {
    }

    /** Returns true when an identity-bearing transition must fail closed. */
    public static boolean shouldReject(ServiceUnitStateData from, ServiceUnitStateData to) {
        Objects.requireNonNull(to, "to");
        boolean fromQualified = from != null && from.hasNereusOwnershipIdentity();
        boolean toQualified = to.hasNereusOwnershipIdentity();
        if (!fromQualified && !toQualified) {
            return false;
        }
        if (!toQualified) {
            return true;
        }

        ServiceUnitState fromState = ServiceUnitStateData.state(from);
        return switch (fromState) {
            case Init -> to.state() != ServiceUnitState.Assigning;
            case Free, Deleted -> to.state() != ServiceUnitState.Assigning || sameIdentity(from, to);
            case Assigning -> to.state() != ServiceUnitState.Owned || !sameIdentity(from, to);
            case Owned -> switch (to.state()) {
                case Releasing, Splitting -> !sameIdentity(from, to);
                default -> true;
            };
            case Releasing -> switch (to.state()) {
                case Assigning -> sameIdentity(from, to);
                case Free -> !sameIdentity(from, to);
                default -> true;
            };
            case Splitting -> to.state() != ServiceUnitState.Deleted || !sameIdentity(from, to);
        };
    }

    private static boolean sameIdentity(ServiceUnitStateData left, ServiceUnitStateData right) {
        return Objects.equals(left.brokerIncarnationId(), right.brokerIncarnationId())
                && Objects.equals(left.acquisitionId(), right.acquisitionId());
    }
}
