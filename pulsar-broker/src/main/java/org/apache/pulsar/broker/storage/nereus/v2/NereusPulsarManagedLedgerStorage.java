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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.common.naming.TopicName;

/** The explicit Broker storage route for namespaces selected by the Nereus provider. */
public interface NereusPulsarManagedLedgerStorage extends ManagedLedgerStorage {
    final class UnsettledOpenException extends RuntimeException {
        public UnsettledOpenException(Throwable cause) {
            super("Native ledger open still owns physical resources", cause);
        }
    }

    record OpenedLedger(
            ManagedLedger ledger,
            NereusPulsarTopicAuthority authority,
            Supplier<? extends CompletionStage<Void>> closeScope) {
    }

    boolean selects(TopicName topic);

    CompletionStage<OpenedLedger> openTopic(
            PulsarService broker, TopicName topic, ManagedLedgerFactory stockFactory, ManagedLedgerConfig config,
            Supplier<CompletableFuture<Boolean>> stockOwnership);
}
