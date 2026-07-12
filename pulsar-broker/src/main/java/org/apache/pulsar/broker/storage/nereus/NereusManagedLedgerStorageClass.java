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
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;

/** Stable Nereus storage-class view published after complete initialization. */
public final class NereusManagedLedgerStorageClass implements ManagedLedgerStorageClass {
    public static final String NAME = "nereus";
    private final ManagedLedgerFactory factory;

    public NereusManagedLedgerStorageClass(ManagedLedgerFactory factory) {
        this.factory = Objects.requireNonNull(factory, "factory");
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public ManagedLedgerFactory getManagedLedgerFactory() {
        return factory;
    }
}
