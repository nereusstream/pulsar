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
package org.apache.pulsar.client.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandSendError;
import org.apache.pulsar.common.api.proto.CommandSendReceipt;
import org.apache.pulsar.common.protocol.ByteBufPair;

/** Digest helpers for guarded protocol evidence. */
final class GuardedEvidenceUtils {
    private GuardedEvidenceUtils() {
    }

    static byte[] sha256(ByteBufPair pair) {
        MessageDigest digest = newDigest();
        update(digest, pair.getFirst());
        update(digest, pair.getSecond());
        return digest.digest();
    }

    static byte[] sha256(CommandSendReceipt receipt) {
        BaseCommand command = new BaseCommand().setType(BaseCommand.Type.SEND_RECEIPT);
        command.setSendReceipt().copyFrom(receipt);
        return sha256(command);
    }

    static byte[] sha256(CommandSendError sendError) {
        BaseCommand command = new BaseCommand().setType(BaseCommand.Type.SEND_ERROR);
        command.setSendError().copyFrom(sendError);
        return sha256(command);
    }

    private static byte[] sha256(BaseCommand command) {
        ByteBuf buffer = Unpooled.buffer(command.getSerializedSize());
        try {
            command.writeTo(buffer);
            return sha256(buffer);
        } finally {
            buffer.release();
        }
    }

    private static byte[] sha256(ByteBuf buffer) {
        MessageDigest digest = newDigest();
        update(digest, buffer);
        return digest.digest();
    }

    private static void update(MessageDigest digest, ByteBuf buffer) {
        byte[] chunk = new byte[Math.min(8192, Math.max(1, buffer.readableBytes()))];
        int index = buffer.readerIndex();
        int remaining = buffer.readableBytes();
        while (remaining > 0) {
            int length = Math.min(remaining, chunk.length);
            buffer.getBytes(index, chunk, 0, length);
            digest.update(chunk, 0, length);
            index += length;
            remaining -= length;
        }
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by guarded protocol evidence", e);
        }
    }
}
