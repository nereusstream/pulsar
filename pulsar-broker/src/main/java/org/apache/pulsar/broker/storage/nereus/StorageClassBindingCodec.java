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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32C;

/** Deterministic, checksummed NSB1 binding-record codec. */
public final class StorageClassBindingCodec {
    public static final int MAX_ENCODED_BYTES = 64 * 1024;
    private static final byte[] MAGIC = new byte[] {'N', 'S', 'B', '1'};
    private static final short ENVELOPE_VERSION = 1;
    private static final short FLAGS = 0;
    private static final int HEADER_BYTES = 4 + Short.BYTES + Short.BYTES + Integer.BYTES;
    private static final int CHECKSUM_BYTES = Integer.BYTES;
    private static final int MAX_STORAGE_CLASS_BYTES = 64;

    public byte[] encode(StorageClassBindingRecord record) {
        java.util.Objects.requireNonNull(record, "record");
        validateTextBounds(record);
        try {
            ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
            try (DataOutputStream payload = new DataOutputStream(payloadBytes)) {
                payload.writeInt(record.formatVersion());
                writeString(payload, record.persistenceName());
                writeString(payload, record.canonicalTopicName());
                writeString(payload, record.storageClass());
                payload.writeLong(record.bindingGeneration());
                writeString(payload, record.state().name());
                payload.writeLong(record.createdAtMillis());
                payload.writeLong(record.stateVersion());
                payload.writeLong(0);
            }
            byte[] body = payloadBytes.toByteArray();
            int totalLength = Math.addExact(Math.addExact(HEADER_BYTES, body.length), CHECKSUM_BYTES);
            if (totalLength > MAX_ENCODED_BYTES) {
                throw new IllegalArgumentException("encoded storage binding exceeds 64 KiB");
            }
            ByteBuffer result = ByteBuffer.allocate(totalLength).order(ByteOrder.BIG_ENDIAN);
            result.put(MAGIC).putShort(ENVELOPE_VERSION).putShort(FLAGS).putInt(body.length).put(body);
            CRC32C crc32c = new CRC32C();
            crc32c.update(result.array(), 0, result.position());
            result.putInt((int) crc32c.getValue());
            return result.array();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory binding encoding failed", impossible);
        }
    }

    public StorageClassBindingRecord decode(byte[] bytes, long backendVersion) {
        if (bytes == null || bytes.length < HEADER_BYTES + CHECKSUM_BYTES || bytes.length > MAX_ENCODED_BYTES) {
            throw corruption("invalid encoded storage binding size", null);
        }
        if (backendVersion < 0) {
            throw corruption("negative metadata backend version", null);
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(bytes).asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
            byte[] magic = new byte[MAGIC.length];
            buffer.get(magic);
            if (!Arrays.equals(magic, MAGIC)
                    || buffer.getShort() != ENVELOPE_VERSION
                    || buffer.getShort() != FLAGS) {
                throw corruption("unsupported storage binding envelope", null);
            }
            int payloadLength = buffer.getInt();
            if (payloadLength < 0 || payloadLength != buffer.remaining() - CHECKSUM_BYTES) {
                throw corruption("invalid storage binding payload length", null);
            }
            CRC32C crc32c = new CRC32C();
            crc32c.update(bytes, 0, bytes.length - CHECKSUM_BYTES);
            int expectedChecksum = ByteBuffer.wrap(bytes, bytes.length - CHECKSUM_BYTES, CHECKSUM_BYTES)
                    .order(ByteOrder.BIG_ENDIAN).getInt();
            if ((int) crc32c.getValue() != expectedChecksum) {
                throw corruption("storage binding checksum mismatch", null);
            }
            int payloadLimit = buffer.position() + payloadLength;
            int formatVersion = buffer.getInt();
            String persistenceName = readString(buffer, payloadLimit);
            String canonicalTopicName = readString(buffer, payloadLimit);
            String storageClass = readString(buffer, payloadLimit);
            long bindingGeneration = buffer.getLong();
            String stateName = readString(buffer, payloadLimit);
            long createdAtMillis = buffer.getLong();
            long stateVersion = buffer.getLong();
            long serializedMetadataVersion = buffer.getLong();
            if (buffer.position() != payloadLimit || serializedMetadataVersion != 0) {
                throw corruption("invalid storage binding payload tail", null);
            }
            StorageClassBindingState state = StorageClassBindingState.valueOf(stateName);
            StorageClassBindingRecord record = new StorageClassBindingRecord(
                    formatVersion,
                    persistenceName,
                    canonicalTopicName,
                    storageClass,
                    bindingGeneration,
                    state,
                    createdAtMillis,
                    stateVersion,
                    backendVersion);
            validateTextBounds(record);
            return record;
        } catch (IllegalStateException error) {
            throw error;
        } catch (RuntimeException error) {
            throw corruption("corrupt storage-class binding", error);
        }
    }

    private static void validateTextBounds(StorageClassBindingRecord record) {
        byte[] persistenceName = encodeStrict(record.persistenceName());
        byte[] canonicalTopicName = encodeStrict(record.canonicalTopicName());
        byte[] storageClass = encodeStrict(record.storageClass());
        encodeStrict(record.state().name());
        if (persistenceName.length > StorageClassBindingKeyspace.MAX_PERSISTENCE_NAME_BYTES
                || canonicalTopicName.length > StorageClassBindingKeyspace.MAX_PERSISTENCE_NAME_BYTES) {
            throw new IllegalArgumentException("storage binding topic identity exceeds 16 KiB");
        }
        if (storageClass.length > MAX_STORAGE_CLASS_BYTES) {
            throw new IllegalArgumentException("storage class exceeds 64 UTF-8 bytes");
        }
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] encoded = encodeStrict(value);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String readString(ByteBuffer buffer, int payloadLimit) {
        if (buffer.position() > payloadLimit - Integer.BYTES) {
            throw corruption("truncated storage binding string length", null);
        }
        int length = buffer.getInt();
        if (length < 0 || length > payloadLimit - buffer.position()) {
            throw corruption("invalid storage binding string length", null);
        }
        byte[] value = new byte[length];
        buffer.get(value);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(value))
                    .toString();
        } catch (CharacterCodingException error) {
            throw corruption("storage binding string is not strict UTF-8", error);
        }
    }

    private static byte[] encodeStrict(String value) {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(java.nio.CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException error) {
            throw new IllegalArgumentException("storage binding string is not strict UTF-8", error);
        }
    }

    private static IllegalStateException corruption(String message, Throwable cause) {
        return new IllegalStateException(message, cause);
    }
}
