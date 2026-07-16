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

/** Privacy-preserving bounded failure fact in canonical traversal order. */
public record BackfillFailure(
        String resourceIdentitySha256,
        GenerationRegistrationBackfillStage stage,
        String errorCode) {
    public BackfillFailure {
        resourceIdentitySha256 = requireSha256(resourceIdentitySha256);
        Objects.requireNonNull(stage, "stage");
        errorCode = requireErrorCode(errorCode);
    }

    private static String requireSha256(String value) {
        Objects.requireNonNull(value, "resourceIdentitySha256");
        if (value.length() != 64) {
            throw new IllegalArgumentException("resourceIdentitySha256 must be lowercase SHA-256");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f'))) {
                throw new IllegalArgumentException("resourceIdentitySha256 must be lowercase SHA-256");
            }
        }
        return value;
    }

    private static String requireErrorCode(String value) {
        Objects.requireNonNull(value, "errorCode");
        if (value.isEmpty() || value.length() > 64 || value.charAt(0) < 'A' || value.charAt(0) > 'Z') {
            throw new IllegalArgumentException("errorCode must be a bounded uppercase machine value");
        }
        for (int index = 1; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= 'A' && character <= 'Z')
                    || (character >= '0' && character <= '9')
                    || character == '_')) {
                throw new IllegalArgumentException("errorCode must be a bounded uppercase machine value");
            }
        }
        return value;
    }
}
