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

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.pulsar.common.naming.TopicName;

/** Applies topic-level storage policy mutations under the namespace first-create lock. */
public final class NereusStorageClassMigrationGuard {
    private final NamespaceStorageClassPolicyGuard namespaceGuard;

    public NereusStorageClassMigrationGuard(NamespaceStorageClassPolicyGuard namespaceGuard) {
        this.namespaceGuard = java.util.Objects.requireNonNull(namespaceGuard, "namespaceGuard");
    }

    public CompletableFuture<Void> updateTopicPersistence(
            TopicName topic,
            Supplier<CompletableFuture<String>> proposedStorageClassLoader,
            Supplier<CompletableFuture<Void>> policyMutation) {
        return namespaceGuard.updateTopicPersistence(topic, proposedStorageClassLoader, policyMutation);
    }
}
