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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.pulsar.broker.storage.BookkeeperManagedLedgerStorageClass;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;
import org.testng.annotations.Test;

public class NereusManagedLedgerStorageBookKeeperClientTest {
    @Test
    public void exposesTheExactStockBookKeeperClientAsABorrowedResource() {
        BookKeeper client = mock(BookKeeper.class);
        BookkeeperManagedLedgerStorageClass storageClass =
                mock(BookkeeperManagedLedgerStorageClass.class);
        when(storageClass.getBookKeeperClient()).thenReturn(client);

        assertThat(NereusManagedLedgerStorage.requireBorrowedBookKeeperClient(storageClass))
                .isSameAs(client);
    }

    @Test
    public void rejectsStorageClassesThatDoNotExposeABookKeeperClient() {
        ManagedLedgerStorageClass storageClass = mock(ManagedLedgerStorageClass.class);

        assertThatThrownBy(
                        () -> NereusManagedLedgerStorage.requireBorrowedBookKeeperClient(storageClass))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must expose its borrowed BookKeeper client");
    }

    @Test
    public void rejectsANullBookKeeperClient() {
        BookkeeperManagedLedgerStorageClass storageClass =
                mock(BookkeeperManagedLedgerStorageClass.class);

        assertThatThrownBy(
                        () -> NereusManagedLedgerStorage.requireBorrowedBookKeeperClient(storageClass))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("returned a null BookKeeper client");
    }
}
