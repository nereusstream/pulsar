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

import java.util.Optional;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicPolicies;

/** One authoritative policy input tuple and its derived Nereus topic-open view. */
public record NereusTopicPolicySnapshot(
        NereusTopicOpenContext openContext,
        Policies namespacePolicies,
        Optional<TopicPolicies> localPolicies,
        Optional<TopicPolicies> globalPolicies) {
    public NereusTopicPolicySnapshot {
        java.util.Objects.requireNonNull(openContext, "openContext");
        java.util.Objects.requireNonNull(namespacePolicies, "namespacePolicies");
        localPolicies = clonePolicies(localPolicies, false);
        globalPolicies = clonePolicies(globalPolicies, true);
    }

    /** Compares the complete policy authority tuple while deliberately ignoring mutable config object identity. */
    public boolean hasSamePolicyInputs(NereusTopicPolicySnapshot other) {
        NereusTopicPolicySnapshot exact = java.util.Objects.requireNonNull(other, "other");
        return namespacePolicies.equals(exact.namespacePolicies)
                && localPolicies.equals(exact.localPolicies)
                && globalPolicies.equals(exact.globalPolicies)
                && openContext.features().equals(exact.openContext.features())
                && openContext.retentionPolicy().equals(exact.openContext.retentionPolicy())
                && java.util.Objects.equals(
                        openContext.managedLedgerConfig().getStorageClassName(),
                        exact.openContext.managedLedgerConfig().getStorageClassName());
    }

    private static Optional<TopicPolicies> clonePolicies(
            Optional<TopicPolicies> policies, boolean global) {
        java.util.Objects.requireNonNull(policies, "policies");
        return policies.map(value -> {
            TopicPolicies copy = value.clone();
            copy.setIsGlobal(global);
            return copy;
        });
    }
}
