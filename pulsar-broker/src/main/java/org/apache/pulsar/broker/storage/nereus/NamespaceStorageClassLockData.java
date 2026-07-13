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

/** Diagnostic value stored in a namespace storage-policy resource lock. */
public record NamespaceStorageClassLockData(
        String brokerId,
        String operationId,
        long acquiredAtMillis) {
    public NamespaceStorageClassLockData {
        if (brokerId == null || brokerId.isBlank()) {
            throw new IllegalArgumentException("brokerId cannot be blank");
        }
        if (operationId == null || operationId.isBlank()) {
            throw new IllegalArgumentException("operationId cannot be blank");
        }
        if (acquiredAtMillis < 0) {
            throw new IllegalArgumentException("acquiredAtMillis cannot be negative");
        }
    }
}
