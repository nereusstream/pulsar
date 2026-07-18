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

import com.nereusstream.managedledger.retention.RetentionPolicySnapshot;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;

/** Managed-ledger config and admission features derived from the same policy input tuple. */
public record NereusTopicOpenContext(
        ManagedLedgerConfig managedLedgerConfig,
        NereusResolvedTopicFeatures features,
        RetentionPolicySnapshot retentionPolicy) {
    public NereusTopicOpenContext {
        java.util.Objects.requireNonNull(managedLedgerConfig, "managedLedgerConfig");
        java.util.Objects.requireNonNull(features, "features");
        java.util.Objects.requireNonNull(retentionPolicy, "retentionPolicy");
        RetentionPolicySnapshot expected = features.retention()
                .map(value -> RetentionPolicySnapshot.fromCanonicalMinutesAndMebibytes(
                        value.getRetentionTimeInMinutes(), value.getRetentionSizeInMB()))
                .orElseGet(() -> RetentionPolicySnapshot.fromCanonicalMinutesAndMebibytes(0, 0));
        if (!expected.equals(retentionPolicy)) {
            throw new IllegalArgumentException(
                    "retentionPolicy must be derived from the exact resolved Pulsar retention values");
        }
    }
}
