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

import java.io.IOException;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/** Deterministic JSON codec used by the MetadataStore-backed P1 ownership witness. */
public final class NereusOwnershipStateCodec {
    public byte[] encode(ServiceUnitStateData value) {
        try {
            return ObjectMapperFactory.getMapper().writer().writeValueAsBytes(value);
        } catch (IOException error) {
            throw new IllegalArgumentException("cannot encode ownership state", error);
        }
    }

    public ServiceUnitStateData decode(byte[] bytes) {
        try {
            return ObjectMapperFactory.getMapper().reader().readValue(bytes, ServiceUnitStateData.class);
        } catch (IOException error) {
            throw new IllegalArgumentException("cannot decode ownership state", error);
        }
    }
}
