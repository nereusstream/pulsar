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
import com.nereusstream.managedledger.NereusManagedLedgerFactory;
import com.nereusstream.managedledger.NereusStorageStateSnapshot;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.metadata.api.GetResult;
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

    @Test
    public void preparesAndActivatesFirstNereusGeneration() {
        MutableBindingMetadata bindingMetadata = new MutableBindingMetadata();
        ManagedLedgerFactory bookkeeper = mock(ManagedLedgerFactory.class);
        NereusManagedLedgerFactory nereus = mock(NereusManagedLedgerFactory.class);
        when(bookkeeper.asyncExists(PERSISTENCE_NAME)).thenReturn(CompletableFuture.completedFuture(false));
        when(nereus.inspectStorageState(PERSISTENCE_NAME))
                .thenReturn(CompletableFuture.completedFuture(NereusStorageStateSnapshot.missing()));
        NereusStorageClassBindingStore store = new NereusStorageClassBindingStore(
                bindingMetadata.store(), bookkeeper, Duration.ofSeconds(1));
        store.attachNereusFactory(nereus);

        StorageClassOpenPermit permit = store.prepareStorageClassOpen(
                PERSISTENCE_NAME, StorageClassBindingRecord.NEREUS, true).join();
        store.completeStorageClassOpen(permit).join();

        StorageClassBindingRecord active = bindingMetadata.record();
        assertThat(permit.activationRequired()).isTrue();
        assertThat(active.state()).isEqualTo(StorageClassBindingState.ACTIVE);
        assertThat(active.storageClass()).isEqualTo(StorageClassBindingRecord.NEREUS);
        assertThat(active.bindingGeneration()).isEqualTo(1);
    }

    @Test
    public void adoptsExistingBookKeeperAndRejectsClassSwitch() {
        MutableBindingMetadata bindingMetadata = new MutableBindingMetadata();
        ManagedLedgerFactory bookkeeper = mock(ManagedLedgerFactory.class);
        NereusManagedLedgerFactory nereus = mock(NereusManagedLedgerFactory.class);
        when(bookkeeper.asyncExists(PERSISTENCE_NAME)).thenReturn(CompletableFuture.completedFuture(true));
        when(nereus.inspectStorageState(PERSISTENCE_NAME))
                .thenReturn(CompletableFuture.completedFuture(NereusStorageStateSnapshot.missing()));
        NereusStorageClassBindingStore store = new NereusStorageClassBindingStore(
                bindingMetadata.store(), bookkeeper, Duration.ofSeconds(1));
        store.attachNereusFactory(nereus);

        assertThatThrownBy(() -> store.prepareStorageClassOpen(
                PERSISTENCE_NAME, StorageClassBindingRecord.NEREUS, true).join())
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("storage-class migration is required");
        StorageClassOpenPermit bookkeeperPermit = store.prepareStorageClassOpen(
                PERSISTENCE_NAME, StorageClassBindingRecord.BOOKKEEPER, true).join();

        assertThat(bookkeeperPermit.activationRequired()).isFalse();
        assertThat(bindingMetadata.record().state()).isEqualTo(StorageClassBindingState.ACTIVE);
        assertThat(bindingMetadata.record().storageClass()).isEqualTo(StorageClassBindingRecord.BOOKKEEPER);
    }

    @Test
    public void refusesDuplicateNereusFactoryAttachment() {
        NereusStorageClassBindingStore store = new NereusStorageClassBindingStore(
                mock(MetadataStoreExtended.class), mock(ManagedLedgerFactory.class), Duration.ofSeconds(1));
        NereusManagedLedgerFactory first = mock(NereusManagedLedgerFactory.class);

        store.attachNereusFactory(first);

        assertThatThrownBy(() -> store.attachNereusFactory(mock(NereusManagedLedgerFactory.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Nereus managed-ledger factory is already attached");
    }

    @Test
    public void deletesBoundBookKeeperClassAndStartsNextNereusGeneration() {
        MutableBindingMetadata bindingMetadata = new MutableBindingMetadata();
        ManagedLedgerFactory bookkeeper = mock(ManagedLedgerFactory.class);
        NereusManagedLedgerFactory nereus = mock(NereusManagedLedgerFactory.class);
        AtomicBoolean bookkeeperExists = new AtomicBoolean();
        when(bookkeeper.asyncExists(PERSISTENCE_NAME)).thenAnswer(
                ignored -> CompletableFuture.completedFuture(bookkeeperExists.get()));
        when(nereus.inspectStorageState(PERSISTENCE_NAME))
                .thenReturn(CompletableFuture.completedFuture(NereusStorageStateSnapshot.missing()));
        NereusStorageClassBindingStore store = new NereusStorageClassBindingStore(
                bindingMetadata.store(), bookkeeper, Duration.ofSeconds(1));
        store.attachNereusFactory(nereus);
        StorageClassOpenPermit openPermit = store.prepareStorageClassOpen(
                PERSISTENCE_NAME, StorageClassBindingRecord.BOOKKEEPER, true).join();
        store.completeStorageClassOpen(openPermit).join();
        bookkeeperExists.set(true);

        StorageClassDeletePermit deletePermit = store.prepareStorageClassDelete(PERSISTENCE_NAME)
                .join().orElseThrow();
        bookkeeperExists.set(false);
        store.completeStorageClassDelete(deletePermit).join();
        StorageClassOpenPermit next = store.prepareStorageClassOpen(
                PERSISTENCE_NAME, StorageClassBindingRecord.NEREUS, true).join();

        assertThat(deletePermit.storageClass()).isEqualTo(StorageClassBindingRecord.BOOKKEEPER);
        assertThat(next.storageClass()).isEqualTo(StorageClassBindingRecord.NEREUS);
        assertThat(next.bindingGeneration()).isEqualTo(2);
        assertThat(bindingMetadata.record().state()).isEqualTo(StorageClassBindingState.CLAIMED);
    }

    private static final class MutableBindingMetadata {
        private final MetadataStoreExtended store = mock(MetadataStoreExtended.class);
        private final AtomicReference<byte[]> value = new AtomicReference<>();
        private final AtomicLong version = new AtomicLong(-1);
        private final StorageClassBindingCodec codec = new StorageClassBindingCodec();

        private MutableBindingMetadata() {
            when(store.sync(anyString())).thenReturn(CompletableFuture.completedFuture(null));
            when(store.get(anyString())).thenAnswer(ignored -> {
                byte[] current = value.get();
                if (current == null) {
                    return CompletableFuture.completedFuture(Optional.empty());
                }
                return CompletableFuture.completedFuture(Optional.of(
                        new GetResult(current.clone(), stat(version.get()))));
            });
            when(store.put(anyString(), any(), any())).thenAnswer(invocation -> {
                byte[] updated = invocation.getArgument(1);
                long updatedVersion = version.incrementAndGet();
                value.set(updated.clone());
                return CompletableFuture.completedFuture(stat(updatedVersion));
            });
        }

        private MetadataStoreExtended store() {
            return store;
        }

        private StorageClassBindingRecord record() {
            return codec.decode(value.get(), version.get());
        }

        private static Stat stat(long version) {
            return new Stat("/binding", version, 0, 0, false, true);
        }
    }
}
