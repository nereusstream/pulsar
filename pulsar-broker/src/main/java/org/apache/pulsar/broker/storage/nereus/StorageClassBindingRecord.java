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

import java.util.Objects;
import org.apache.pulsar.common.naming.TopicName;

/** One immutable generation of the fork-owned topic storage-class binding. */
public record StorageClassBindingRecord(
        int formatVersion,
        String persistenceName,
        String canonicalTopicName,
        String storageClass,
        long bindingGeneration,
        StorageClassBindingState state,
        long createdAtMillis,
        long stateVersion,
        long metadataVersion) {
    public static final int FORMAT_VERSION = 1;
    public static final String BOOKKEEPER = "bookkeeper";
    public static final String NEREUS = "nereus";

    public StorageClassBindingRecord {
        if (formatVersion != FORMAT_VERSION) {
            throw new IllegalArgumentException("unsupported storage binding format version");
        }
        if (persistenceName == null || persistenceName.isBlank()) {
            throw new IllegalArgumentException("persistenceName cannot be blank");
        }
        if (canonicalTopicName == null || canonicalTopicName.isBlank()) {
            throw new IllegalArgumentException("canonicalTopicName cannot be blank");
        }
        String derivedTopicName = TopicName.get(
                TopicName.fromPersistenceNamingEncoding(persistenceName)).toString();
        if (!canonicalTopicName.equals(derivedTopicName)) {
            throw new IllegalArgumentException("canonicalTopicName does not match persistenceName");
        }
        if (!BOOKKEEPER.equals(storageClass) && !NEREUS.equals(storageClass)) {
            throw new IllegalArgumentException("unsupported storage class");
        }
        if (bindingGeneration <= 0) {
            throw new IllegalArgumentException("bindingGeneration must be positive");
        }
        Objects.requireNonNull(state, "state");
        if (createdAtMillis < 0 || stateVersion < 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("binding versions and timestamps cannot be negative");
        }
    }

    public static StorageClassBindingRecord claimed(
            String persistenceName,
            String storageClass,
            long bindingGeneration,
            long createdAtMillis) {
        String canonicalTopicName = TopicName.get(
                TopicName.fromPersistenceNamingEncoding(persistenceName)).toString();
        return new StorageClassBindingRecord(
                FORMAT_VERSION,
                persistenceName,
                canonicalTopicName,
                storageClass,
                bindingGeneration,
                StorageClassBindingState.CLAIMED,
                createdAtMillis,
                0,
                0);
    }

    public StorageClassBindingRecord transitionTo(StorageClassBindingState target) {
        Objects.requireNonNull(target, "target");
        if (target == state) {
            return this;
        }
        boolean allowed = switch (state) {
            case CLAIMED -> target == StorageClassBindingState.ACTIVE
                    || target == StorageClassBindingState.DELETING;
            case ACTIVE -> target == StorageClassBindingState.DELETING;
            case DELETING -> target == StorageClassBindingState.DELETED;
            case DELETED -> false;
        };
        if (!allowed) {
            throw new IllegalStateException("illegal storage binding state transition");
        }
        return new StorageClassBindingRecord(
                formatVersion,
                persistenceName,
                canonicalTopicName,
                storageClass,
                bindingGeneration,
                target,
                createdAtMillis,
                Math.addExact(stateVersion, 1),
                metadataVersion);
    }

    public StorageClassBindingRecord nextGeneration(String nextStorageClass, long nextCreatedAtMillis) {
        if (state != StorageClassBindingState.DELETED) {
            throw new IllegalStateException("only a deleted binding can start a new generation");
        }
        return claimed(persistenceName, nextStorageClass, Math.addExact(bindingGeneration, 1), nextCreatedAtMillis);
    }

    public StorageClassBindingRecord withMetadataVersion(long backendVersion) {
        return new StorageClassBindingRecord(
                formatVersion,
                persistenceName,
                canonicalTopicName,
                storageClass,
                bindingGeneration,
                state,
                createdAtMillis,
                stateVersion,
                backendVersion);
    }
}
