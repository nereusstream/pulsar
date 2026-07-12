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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

public class NereusStorageClassBindingStoreTest {
    private static final String PERSISTENCE_NAME = "tenant/ns/persistent/topic";

    @Test
    public void refusesToClaimExistingBookKeeperStorage() {
        MetadataStoreExtended metadata = mock(MetadataStoreExtended.class);
        ManagedLedgerFactory bookkeeper = mock(ManagedLedgerFactory.class);
        when(metadata.sync(anyString())).thenReturn(CompletableFuture.completedFuture(null));
        when(metadata.get(anyString())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(bookkeeper.asyncExists(PERSISTENCE_NAME)).thenReturn(CompletableFuture.completedFuture(true));
        NereusStorageClassBindingStore store =
                new NereusStorageClassBindingStore(metadata, bookkeeper, Duration.ofSeconds(1));

        assertThatThrownBy(() -> store.creationGuard().acquire(PERSISTENCE_NAME).join())
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("existing BookKeeper storage cannot be opened as Nereus");
        verify(metadata, never()).put(anyString(), any(), any());
    }

    @Test
    public void claimsMissingStorageAtGenerationOne() {
        MetadataStoreExtended metadata = mock(MetadataStoreExtended.class);
        ManagedLedgerFactory bookkeeper = mock(ManagedLedgerFactory.class);
        when(metadata.sync(anyString())).thenReturn(CompletableFuture.completedFuture(null));
        when(metadata.get(anyString())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(metadata.put(anyString(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(Stat.class)));
        when(bookkeeper.asyncExists(PERSISTENCE_NAME)).thenReturn(CompletableFuture.completedFuture(false));
        NereusStorageClassBindingStore store =
                new NereusStorageClassBindingStore(metadata, bookkeeper, Duration.ofSeconds(1));

        var permit = store.creationGuard().acquire(PERSISTENCE_NAME).join();

        assertThat(permit.persistenceName()).isEqualTo(PERSISTENCE_NAME);
        assertThat(permit.bindingGeneration()).isEqualTo(1);
    }
}
