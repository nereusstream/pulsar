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

import com.nereusstream.api.StorageProfile;
import com.nereusstream.pulsar.BookKeeperPrimaryWalCapabilityBinding;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Reserved lookup properties for the exact locally verified BookKeeper primary-WAL binding. */
public final class NereusBookKeeperPrimaryWalCapability {
    public static final String PROTOCOL_PROPERTY = "nereus.bookkeeper-primary-wal-protocol";
    public static final String CONFIGURATION_PROPERTY = "nereus.bookkeeper-primary-wal-config";
    public static final String NAMESPACE_PROPERTY = "nereus.bookkeeper-ledger-namespace";
    public static final String REQUIRED_OBJECT_GENERATION_PROPERTY =
            "nereus.bookkeeper-required-object-generation";

    private NereusBookKeeperPrimaryWalCapability() {
    }

    public static Map<String, String> properties(BookKeeperPrimaryWalCapabilityBinding binding) {
        java.util.Objects.requireNonNull(binding, "binding");
        return Map.of(
                PROTOCOL_PROPERTY, Integer.toString(binding.protocolVersion()),
                CONFIGURATION_PROPERTY, binding.configurationBindingSha256().value(),
                NAMESPACE_PROPERTY, binding.ledgerIdNamespaceSha256().value(),
                REQUIRED_OBJECT_GENERATION_PROPERTY,
                Integer.toString(binding.requiredObjectGenerationCompletionVersion()));
    }

    public static Map<String, String> requiredProperties(
            BookKeeperPrimaryWalCapabilityBinding binding,
            StorageProfile profile) {
        StorageProfile exact = java.util.Objects.requireNonNull(profile, "profile").canonical();
        if (!exact.usesBookKeeperWal()) {
            return Map.of();
        }
        Map<String, String> required = new HashMap<>(properties(binding));
        required.put(
                NereusStorageBindingCapability.PROPERTY,
                NereusStorageBindingCapability.VERSION);
        required.put(
                NereusCursorProtocolCapability.PROPERTY,
                NereusCursorProtocolCapability.VERSION);
        if (exact.objectMaterializationEnabled()) {
            required.put(
                    NereusGenerationProtocolCapability.PROPERTY,
                    NereusGenerationProtocolCapability.VERSION);
        }
        if (exact != StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT) {
            required.remove(REQUIRED_OBJECT_GENERATION_PROPERTY);
        }
        return Map.copyOf(required);
    }

    public static Map<String, String> requireUnreserved(Map<String, String> configuredProperties) {
        java.util.Objects.requireNonNull(configuredProperties, "configuredProperties");
        for (String property : List.of(
                PROTOCOL_PROPERTY,
                CONFIGURATION_PROPERTY,
                NAMESPACE_PROPERTY,
                REQUIRED_OBJECT_GENERATION_PROPERTY)) {
            if (configuredProperties.containsKey(property)) {
                throw new IllegalArgumentException(property + " is reserved by the broker");
            }
        }
        return Map.copyOf(configuredProperties);
    }
}
