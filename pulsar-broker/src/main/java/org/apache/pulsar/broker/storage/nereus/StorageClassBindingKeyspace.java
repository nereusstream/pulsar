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

import com.nereusstream.api.keys.DeterministicIds;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

/** Sole path builder for namespace-listable, collision-checked binding records. */
public final class StorageClassBindingKeyspace {
    public static final int MAX_PERSISTENCE_NAME_BYTES = 16 * 1024;
    private static final String ROOT = "/managed-ledger-storage-bindings/v1/";
    private static final String NAMESPACE_DOMAIN = "pulsar-storage-binding-namespace-v1\0";
    private static final String TOPIC_DOMAIN = "pulsar-storage-binding-topic-v1\0";

    public String namespaceRoot(NamespaceName namespace) {
        String canonicalNamespace = java.util.Objects.requireNonNull(namespace, "namespace").toString();
        requireStrictUtf8(canonicalNamespace, "namespace");
        return ROOT + DeterministicIds.stableHashComponent(NAMESPACE_DOMAIN + canonicalNamespace) + "/";
    }

    public String bindingKey(NamespaceName namespace, String exactPersistenceName) {
        byte[] persistenceBytes = requireStrictUtf8(exactPersistenceName, "exactPersistenceName");
        if (persistenceBytes.length > MAX_PERSISTENCE_NAME_BYTES) {
            throw new IllegalArgumentException("exactPersistenceName exceeds 16 KiB");
        }
        TopicName topicName = TopicName.get(TopicName.fromPersistenceNamingEncoding(exactPersistenceName));
        if (!topicName.getNamespaceObject().equals(namespace)) {
            throw new IllegalArgumentException("persistence name does not belong to namespace");
        }
        return namespaceRoot(namespace)
                + DeterministicIds.stableHashComponent(TOPIC_DOMAIN + exactPersistenceName);
    }

    private static byte[] requireStrictUtf8(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(java.nio.CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException error) {
            throw new IllegalArgumentException(field + " is not strict UTF-8", error);
        }
    }
}
