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

import java.util.HexFormat;

/** Canonical non-zero 128-bit broker-incarnation or ownership-acquisition identity. */
public record NereusOwnershipId(String value) {
    public static final int ENCODED_LENGTH = 32;

    public NereusOwnershipId {
        if (value == null || value.length() != ENCODED_LENGTH || !value.matches("[0-9a-f]{32}")) {
            throw new IllegalArgumentException("ownership identity must be 32 lowercase hexadecimal characters");
        }
        if (value.chars().allMatch(character -> character == '0')) {
            throw new IllegalArgumentException("ownership identity must be non-zero");
        }
    }

    public static NereusOwnershipId fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length != 16) {
            throw new IllegalArgumentException("ownership identity input must be exactly 16 bytes");
        }
        return new NereusOwnershipId(HexFormat.of().formatHex(bytes));
    }
}
