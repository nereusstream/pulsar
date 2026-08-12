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
package org.apache.pulsar.broker.loadbalance.extensions.channel;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.storage.nereus.v2.NereusOwnershipId;

/**
 * Defines data for the service unit state changes.
 * This data will be broadcast in ServiceUnitStateChannel.
 */

public record ServiceUnitStateData(
        ServiceUnitState state, String dstBroker, String sourceBroker,
        Map<String, Optional<String>> splitServiceUnitToDestBroker, boolean force, long timestamp, long versionId,
        String brokerIncarnationId, String acquisitionId) {

    public ServiceUnitStateData {
        Objects.requireNonNull(state);
        if (state != ServiceUnitState.Free && StringUtils.isBlank(dstBroker) && StringUtils.isBlank(sourceBroker)) {
            throw new IllegalArgumentException("Empty broker");
        }
        if ((brokerIncarnationId == null) != (acquisitionId == null)) {
            throw new IllegalArgumentException("broker incarnation and acquisition identities must appear together");
        }
        if (brokerIncarnationId != null) {
            new NereusOwnershipId(brokerIncarnationId);
            new NereusOwnershipId(acquisitionId);
        }
    }

    public ServiceUnitStateData(ServiceUnitState state, String dstBroker, String sourceBroker,
                                Map<String, Optional<String>> splitServiceUnitToDestBroker, boolean force,
                                long timestamp, long versionId) {
        this(state, dstBroker, sourceBroker, splitServiceUnitToDestBroker, force, timestamp, versionId,
                (String) null, (String) null);
    }

    public ServiceUnitStateData(ServiceUnitState state, String dstBroker, String sourceBroker,
                                Map<String, Optional<String>> splitServiceUnitToDestBroker, boolean force,
                                long timestamp, long versionId, NereusOwnershipId brokerIncarnationId,
                                NereusOwnershipId acquisitionId) {
        this(state, dstBroker, sourceBroker, splitServiceUnitToDestBroker, force, timestamp, versionId,
                Objects.requireNonNull(brokerIncarnationId, "brokerIncarnationId").value(),
                Objects.requireNonNull(acquisitionId, "acquisitionId").value());
    }

    public ServiceUnitStateData(ServiceUnitState state, String dstBroker, String sourceBroker,
                                Map<String, Optional<String>> splitServiceUnitToDestBroker, long versionId) {
        this(state, dstBroker, sourceBroker, splitServiceUnitToDestBroker, false,
                System.currentTimeMillis(), versionId);
    }

    public ServiceUnitStateData(ServiceUnitState state, String dstBroker, String sourceBroker,
                                Map<String, Optional<String>> splitServiceUnitToDestBroker, boolean force,
                                long versionId) {
        this(state, dstBroker, sourceBroker, splitServiceUnitToDestBroker, force,
                System.currentTimeMillis(), versionId);
    }

    public ServiceUnitStateData(ServiceUnitState state, String dstBroker, String sourceBroker, long versionId) {
        this(state, dstBroker, sourceBroker, null, false, System.currentTimeMillis(), versionId);
    }

    public ServiceUnitStateData(ServiceUnitState state, String dstBroker, String sourceBroker, boolean force,
                                long versionId) {
        this(state, dstBroker, sourceBroker, null, force,
                System.currentTimeMillis(), versionId);
    }



    public ServiceUnitStateData(ServiceUnitState state, String dstBroker, long versionId) {
        this(state, dstBroker, null, null, false, System.currentTimeMillis(), versionId);
    }

    public ServiceUnitStateData(ServiceUnitState state, String dstBroker, boolean force, long versionId) {
        this(state, dstBroker, null, null, force, System.currentTimeMillis(), versionId);
    }

    public static ServiceUnitState state(ServiceUnitStateData data) {
        return data == null ? ServiceUnitState.Init : data.state();
    }

    public boolean hasNereusOwnershipIdentity() {
        return brokerIncarnationId != null;
    }
}
