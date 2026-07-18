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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class NereusAdminOperationTest {
    @Test
    public void enforcesTheClosedF4AdminMatrix() {
        NereusTopicFeatureValidator validator = new NereusTopicFeatureValidator();
        EnumSet<NereusAdminOperation> allowed = EnumSet.of(
                NereusAdminOperation.TERMINATE_TOPIC,
                NereusAdminOperation.DELETE_TOPIC,
                NereusAdminOperation.UNLOAD_TOPIC,
                NereusAdminOperation.DELETE_DURABLE_SUBSCRIPTION,
                NereusAdminOperation.ANALYZE_BACKLOG,
                NereusAdminOperation.CLEAR_BACKLOG,
                NereusAdminOperation.SKIP_MESSAGES,
                NereusAdminOperation.EXPIRE_MESSAGES,
                NereusAdminOperation.RESET_CURSOR);

        for (NereusAdminOperation operation : NereusAdminOperation.values()) {
            if (allowed.contains(operation)) {
                assertThatCode(() -> validator.validateAdminOperation(operation, false)).doesNotThrowAnyException();
            } else if (operation == NereusAdminOperation.TRIM_TOPIC) {
                assertThatThrownBy(() -> validator.validateAdminOperation(operation, false))
                        .hasMessage("NEREUS_UNSUPPORTED_ADMIN_OPERATION:TRIM_TOPIC:GENERATION_PROTOCOL_NOT_READY");
            } else {
                assertThatThrownBy(() -> validator.validateAdminOperation(operation, false))
                        .hasMessage("NEREUS_UNSUPPORTED_ADMIN_OPERATION:" + operation.name());
            }
        }
    }

    @Test
    public void trimRequiresGenerationReadinessWhileTruncateStaysClosed() {
        NereusTopicFeatureValidator validator = new NereusTopicFeatureValidator();
        assertThatThrownBy(() -> validator.validateAdminOperation(NereusAdminOperation.TRIM_TOPIC, false))
                .hasMessage("NEREUS_UNSUPPORTED_ADMIN_OPERATION:TRIM_TOPIC:GENERATION_PROTOCOL_NOT_READY");
        assertThatCode(() -> validator.validateAdminOperation(NereusAdminOperation.TRIM_TOPIC, true))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> validator.validateAdminOperation(NereusAdminOperation.TRUNCATE_TOPIC, true))
                .hasMessage("NEREUS_UNSUPPORTED_ADMIN_OPERATION:TRUNCATE_TOPIC");
    }

    @Test
    public void unloadedTrimWaitsForReadinessOnlyWhenGenerationProtocolIsEnabled() {
        AtomicInteger readinessCalls = new AtomicInteger();
        CompletableFuture<Object> readiness = new CompletableFuture<>();

        CompletableFuture<Void> disabled = NereusManagedLedgerStorage.validateBoundNereusAdminOperation(
                NereusAdminOperation.TRIM_TOPIC,
                false,
                () -> {
                    readinessCalls.incrementAndGet();
                    return readiness;
                });
        assertThatThrownBy(disabled::join)
                .hasRootCauseMessage(
                        "NEREUS_UNSUPPORTED_ADMIN_OPERATION:TRIM_TOPIC:GENERATION_PROTOCOL_NOT_READY");
        org.assertj.core.api.Assertions.assertThat(readinessCalls).hasValue(0);

        CompletableFuture<Void> enabled = NereusManagedLedgerStorage.validateBoundNereusAdminOperation(
                NereusAdminOperation.TRIM_TOPIC,
                true,
                () -> {
                    readinessCalls.incrementAndGet();
                    return readiness;
                });
        org.assertj.core.api.Assertions.assertThat(enabled).isNotDone();
        org.assertj.core.api.Assertions.assertThat(readinessCalls).hasValue(1);
        readiness.complete(new Object());
        enabled.join();
    }
}
