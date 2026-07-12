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

/** Exact binding observation authorizing one managed-ledger open. */
public record StorageClassOpenPermit(
        String persistenceName,
        String storageClass,
        long bindingGeneration,
        long expectedMetadataVersion,
        boolean activationRequired) {
    public StorageClassOpenPermit {
        requireIdentity(persistenceName, storageClass, bindingGeneration, expectedMetadataVersion);
    }

    static void requireIdentity(
            String persistenceName,
            String storageClass,
            long bindingGeneration,
            long expectedMetadataVersion) {
        if (persistenceName == null || persistenceName.isBlank()) {
            throw new IllegalArgumentException("persistenceName cannot be blank");
        }
        if (!StorageClassBindingRecord.BOOKKEEPER.equals(storageClass)
                && !StorageClassBindingRecord.NEREUS.equals(storageClass)) {
            throw new IllegalArgumentException("unsupported storage class");
        }
        if (bindingGeneration <= 0 || expectedMetadataVersion < 0) {
            throw new IllegalArgumentException("invalid storage binding permit version");
        }
    }
}
