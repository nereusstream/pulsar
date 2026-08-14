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
package org.apache.pulsar.client.api;

import java.util.Arrays;
import java.util.Objects;

/** Evidence for a guarded SEND that returned a validated persistent position. */
public record GuardedSendSuccessEvidence(
        int protocolVersion,
        long connectionGeneration,
        long producerId,
        long sequenceId,
        TopicResourceGuardAttestation attestation,
        long ledgerId,
        long entryId,
        long brokerEntryTimestamp,
        byte[] sendCommandSha256,
        byte[] authenticatedResponseCommandSha256)
        implements TopicResourceGuardResponseEvidence {

    public GuardedSendSuccessEvidence {
        if (protocolVersion < 22) {
            throw new IllegalArgumentException("Guarded SEND evidence requires protocol v22 or newer");
        }
        if (connectionGeneration < 0 || producerId < 0 || sequenceId < 0) {
            throw new IllegalArgumentException("Guarded SEND identity values must be non-negative");
        }
        Objects.requireNonNull(attestation, "attestation");
        if (ledgerId < 0 || entryId < 0 || brokerEntryTimestamp < 0) {
            throw new IllegalArgumentException("Guarded SEND success position values must be non-negative");
        }
        sendCommandSha256 = copyDigest(sendCommandSha256, "sendCommandSha256");
        authenticatedResponseCommandSha256 = copyDigest(authenticatedResponseCommandSha256,
                "authenticatedResponseCommandSha256");
    }

    @Override
    public byte[] sendCommandSha256() {
        return sendCommandSha256.clone();
    }

    @Override
    public byte[] authenticatedResponseCommandSha256() {
        return authenticatedResponseCommandSha256.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GuardedSendSuccessEvidence)) {
            return false;
        }
        GuardedSendSuccessEvidence that = (GuardedSendSuccessEvidence) other;
        return protocolVersion == that.protocolVersion
                && connectionGeneration == that.connectionGeneration
                && producerId == that.producerId
                && sequenceId == that.sequenceId
                && ledgerId == that.ledgerId
                && entryId == that.entryId
                && brokerEntryTimestamp == that.brokerEntryTimestamp
                && attestation.equals(that.attestation)
                && Arrays.equals(sendCommandSha256, that.sendCommandSha256)
                && Arrays.equals(authenticatedResponseCommandSha256, that.authenticatedResponseCommandSha256);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(protocolVersion, connectionGeneration, producerId, sequenceId, attestation,
                ledgerId, entryId, brokerEntryTimestamp);
        result = 31 * result + Arrays.hashCode(sendCommandSha256);
        return 31 * result + Arrays.hashCode(authenticatedResponseCommandSha256);
    }

    private static byte[] copyDigest(byte[] digest, String name) {
        Objects.requireNonNull(digest, name);
        if (digest.length != 32) {
            throw new IllegalArgumentException(name + " must contain exactly 32 bytes");
        }
        return digest.clone();
    }
}
