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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.nereusstream.managedledger.NereusDurableStorageState;
import com.nereusstream.managedledger.NereusManagedLedgerFactory;
import com.nereusstream.managedledger.NereusStorageStateSnapshot;
import com.nereusstream.managedledger.projection.VirtualLedgerProjection;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

public class NereusStorageClassBindingMultiBrokerTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Test(timeOut = 30_000)
    public void conflictingFirstCreatesPublishOnlyOneStorageClass() throws Exception {
        try (MetadataStoreExtended metadata = metadataStore()) {
            ManagedLedgerFactory bookkeeper = mock(ManagedLedgerFactory.class);
            NereusManagedLedgerFactory nereus = mock(NereusManagedLedgerFactory.class);
            when(bookkeeper.asyncExists(anyString())).thenReturn(CompletableFuture.completedFuture(false));
            when(nereus.inspectStorageState(anyString()))
                    .thenReturn(CompletableFuture.completedFuture(NereusStorageStateSnapshot.missing()));
            NereusStorageClassBindingStore firstBroker = bindingStore(metadata, bookkeeper, nereus);
            NereusStorageClassBindingStore secondBroker = bindingStore(metadata, bookkeeper, nereus);
            ExecutorService racers = Executors.newFixedThreadPool(2);
            try {
                for (int index = 0; index < 50; index++) {
                    String persistenceName = "tenant/ns/persistent/conflict-" + index;
                    CountDownLatch start = new CountDownLatch(1);
                    CompletableFuture<OpenOutcome> nereusCreate = raceOpen(
                            racers,
                            start,
                            firstBroker,
                            persistenceName,
                            StorageClassBindingRecord.NEREUS);
                    CompletableFuture<OpenOutcome> bookkeeperCreate = raceOpen(
                            racers,
                            start,
                            secondBroker,
                            persistenceName,
                            StorageClassBindingRecord.BOOKKEEPER);

                    start.countDown();
                    List<OpenOutcome> outcomes = List.of(nereusCreate.join(), bookkeeperCreate.join());
                    List<OpenOutcome> winners = outcomes.stream().filter(OpenOutcome::succeeded).toList();
                    List<OpenOutcome> losers = outcomes.stream().filter(outcome -> !outcome.succeeded()).toList();

                    assertThat(winners).hasSize(1);
                    assertThat(losers).hasSize(1);
                    assertThat(rootCause(losers.get(0).failure()))
                            .hasMessage("storage-class migration is required");
                    StorageClassOpenPermit winner = winners.get(0).permit();
                    StorageClassOpenPermit observed = firstBroker.prepareStorageClassOpen(
                            persistenceName, winner.storageClass(), false).join();
                    assertThat(observed.storageClass()).isEqualTo(winner.storageClass());
                    assertThat(observed.bindingGeneration()).isEqualTo(1);
                }
            } finally {
                racers.shutdownNow();
                assertThat(racers.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                firstBroker.close();
                secondBroker.close();
            }
        }
    }

    @Test(timeOut = 30_000)
    public void peerBrokerResumesOpenDeleteAndNextGenerationAfterRestarts() throws Exception {
        try (MetadataStoreExtended metadata = metadataStore()) {
            String persistenceName = "tenant/ns/persistent/restart-lifecycle";
            ManagedLedgerFactory bookkeeper = mock(ManagedLedgerFactory.class);
            NereusManagedLedgerFactory nereus = mock(NereusManagedLedgerFactory.class);
            AtomicBoolean bookkeeperExists = new AtomicBoolean();
            AtomicReference<NereusStorageStateSnapshot> nereusState =
                    new AtomicReference<>(NereusStorageStateSnapshot.missing());
            when(bookkeeper.asyncExists(persistenceName)).thenAnswer(
                    ignored -> CompletableFuture.completedFuture(bookkeeperExists.get()));
            when(nereus.inspectStorageState(persistenceName)).thenAnswer(
                    ignored -> CompletableFuture.completedFuture(nereusState.get()));

            NereusStorageClassBindingStore firstBroker = bindingStore(metadata, bookkeeper, nereus);
            StorageClassOpenPermit claimed = firstBroker.prepareStorageClassOpen(
                    persistenceName, StorageClassBindingRecord.NEREUS, true).join();
            nereusState.set(storageState(NereusDurableStorageState.ACTIVE, 1));
            firstBroker.completeStorageClassOpen(claimed).join();
            firstBroker.close();

            NereusStorageClassBindingStore secondBroker = bindingStore(metadata, bookkeeper, nereus);
            StorageClassOpenPermit active = secondBroker.prepareStorageClassOpen(
                    persistenceName, StorageClassBindingRecord.NEREUS, true).join();
            secondBroker.completeStorageClassOpen(active).join();
            StorageClassDeletePermit deleting = secondBroker.prepareStorageClassDelete(persistenceName)
                    .join().orElseThrow();
            secondBroker.close();

            NereusStorageClassBindingStore thirdBroker = bindingStore(metadata, bookkeeper, nereus);
            StorageClassDeletePermit resumedDelete = thirdBroker.prepareStorageClassDelete(persistenceName)
                    .join().orElseThrow();
            assertThat(resumedDelete.bindingGeneration()).isEqualTo(deleting.bindingGeneration());
            assertThat(resumedDelete.storageClass()).isEqualTo(StorageClassBindingRecord.NEREUS);
            nereusState.set(storageState(NereusDurableStorageState.DELETED, 1));
            thirdBroker.completeStorageClassDelete(resumedDelete).join();

            StorageClassOpenPermit next = thirdBroker.prepareStorageClassOpen(
                    persistenceName, StorageClassBindingRecord.BOOKKEEPER, true).join();
            assertThat(next.bindingGeneration()).isEqualTo(2);
            assertThat(next.storageClass()).isEqualTo(StorageClassBindingRecord.BOOKKEEPER);
            assertThat(next.activationRequired()).isTrue();
            thirdBroker.close();
        }
    }

    private static CompletableFuture<OpenOutcome> raceOpen(
            ExecutorService executor,
            CountDownLatch start,
            NereusStorageClassBindingStore store,
            String persistenceName,
            String storageClass) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                start.await();
                return store.prepareStorageClassOpen(persistenceName, storageClass, true).join();
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new CompletionException(error);
            }
        }, executor).handle((permit, failure) -> new OpenOutcome(permit, failure));
    }

    private static NereusStorageClassBindingStore bindingStore(
            MetadataStoreExtended metadata,
            ManagedLedgerFactory bookkeeper,
            NereusManagedLedgerFactory nereus) {
        NereusStorageClassBindingStore store =
                new NereusStorageClassBindingStore(metadata, bookkeeper, TIMEOUT);
        store.attachNereusFactory(nereus);
        return store;
    }

    private static MetadataStoreExtended metadataStore() throws Exception {
        return MetadataStoreExtended.create(
                "memory:" + UUID.randomUUID(), MetadataStoreConfig.builder().build());
    }

    private static NereusStorageStateSnapshot storageState(
            NereusDurableStorageState state, long generation) {
        NereusStorageStateSnapshot snapshot = mock(NereusStorageStateSnapshot.class);
        VirtualLedgerProjection projection = mock(VirtualLedgerProjection.class);
        when(snapshot.state()).thenReturn(state);
        when(snapshot.projection()).thenReturn(Optional.of(projection));
        when(projection.storageClassBindingGeneration()).thenReturn(generation);
        doAnswer(invocation -> {
            assertThat(invocation.<Long>getArgument(0)).isEqualTo(generation);
            return null;
        }).when(projection).requireStorageClassBindingGeneration(anyLong());
        return snapshot;
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current.getCause() != null)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private record OpenOutcome(StorageClassOpenPermit permit, Throwable failure) {
        private boolean succeeded() {
            return failure == null;
        }
    }
}
