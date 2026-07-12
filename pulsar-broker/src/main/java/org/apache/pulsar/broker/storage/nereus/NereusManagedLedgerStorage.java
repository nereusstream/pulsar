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

import com.nereusstream.managedledger.NereusManagedLedgerFactory;
import com.nereusstream.managedledger.NereusManagedLedgerRuntime;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import com.nereusstream.pulsar.NereusProcessIdentity;
import com.nereusstream.pulsar.NereusRuntimeConfiguration;
import com.nereusstream.pulsar.NereusRuntimeContext;
import com.nereusstream.pulsar.NereusRuntimeProvider;
import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ManagedLedgerClientFactory;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/** Hybrid BookKeeper-default plus opt-in Nereus managed-ledger storage provider. */
public final class NereusManagedLedgerStorage implements ManagedLedgerStorage {
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private ManagedLedgerClientFactory bookkeeperStorage;
    private NereusManagedLedgerFactory nereusFactory;
    private NereusStorageClassBindingStore bindingStore;
    private ManagedLedgerStorageClass bookkeeperClass;
    private ManagedLedgerStorageClass nereusClass;
    private Collection<ManagedLedgerStorageClass> storageClasses = List.of();

    public NereusManagedLedgerStorage() {
    }

    @Override
    public void initialize(
            ServiceConfiguration conf,
            MetadataStoreExtended metadataStore,
            BookKeeperClientFactory bookkeeperProvider,
            EventLoopGroup eventLoopGroup,
            OpenTelemetry openTelemetry) throws Exception {
        if (!initialized.compareAndSet(false, true)) {
            throw new IllegalStateException("Nereus managed-ledger storage is already initialized");
        }
        NereusManagedLedgerRuntime runtime = null;
        try {
            NereusBrokerStorageConfiguration checked = new NereusBrokerStorageConfiguration(conf);
            NereusProcessIdentity identity = NereusProcessIdentity.generate(new SecureRandom());
            NereusRuntimeConfiguration runtimeConfiguration = checked.runtimeConfiguration(identity);
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            ObjectStoreSecretResolver secretResolver = Reflections.createInstance(
                    checked.secretResolverClassName(), ObjectStoreSecretResolver.class, classLoader);
            NereusRuntimeProvider runtimeProvider = Reflections.createInstance(
                    checked.runtimeProviderClassName(), NereusRuntimeProvider.class, classLoader);

            ManagedLedgerClientFactory localBookkeeper = new ManagedLedgerClientFactory();
            localBookkeeper.initialize(
                    conf, metadataStore, bookkeeperProvider, eventLoopGroup, openTelemetry);
            bookkeeperStorage = localBookkeeper;
            bookkeeperClass = localBookkeeper.getDefaultStorageClass();
            if (!"bookkeeper".equals(bookkeeperClass.getName())) {
                throw new IllegalStateException("stock managed-ledger storage class must be bookkeeper");
            }

            bindingStore = new NereusStorageClassBindingStore(
                    metadataStore,
                    bookkeeperClass.getManagedLedgerFactory(),
                    Duration.ofSeconds(conf.getNereusMetadataTimeoutSeconds()));
            NereusRuntimeContext context = new NereusRuntimeContext(
                    eventLoopGroup,
                    openTelemetry,
                    bindingStore.creationGuard(),
                    secretResolver,
                    classLoader);
            runtime = runtimeProvider.create(runtimeConfiguration, context);
            ManagedLedgerFactoryConfig compatibilityFactoryConfig = new ManagedLedgerFactoryConfig();
            compatibilityFactoryConfig.setMaxCacheSize(0);
            nereusFactory = new NereusManagedLedgerFactory(
                    runtime,
                    bindingStore.creationGuard(),
                    new ManagedLedgerConfig(),
                    compatibilityFactoryConfig,
                    true);
            runtime = null;
            nereusClass = new NereusManagedLedgerStorageClass(nereusFactory);
            storageClasses = List.of(bookkeeperClass, nereusClass);
        } catch (Throwable failure) {
            if (runtime != null) {
                suppressClose(failure, runtime);
            }
            suppressClose(failure, bindingStore);
            suppressClose(failure, bookkeeperStorage);
            storageClasses = List.of();
            if (failure instanceof Exception exception) {
                throw exception;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("unexpected Nereus initialization failure", failure);
        }
    }

    @Override
    public Collection<ManagedLedgerStorageClass> getStorageClasses() {
        ensureReady();
        return storageClasses;
    }

    @Override
    public ManagedLedgerStorageClass getDefaultStorageClass() {
        ensureReady();
        return bookkeeperClass;
    }

    @Override
    public Optional<ManagedLedgerStorageClass> getManagedLedgerStorageClass(String name) {
        ensureReady();
        if (name == null || "bookkeeper".equals(name)) {
            return Optional.of(bookkeeperClass);
        }
        if (NereusManagedLedgerStorageClass.NAME.equals(name)) {
            return Optional.of(nereusClass);
        }
        return Optional.empty();
    }

    public NereusStorageClassBindingStore bindingStore() {
        ensureReady();
        return bindingStore;
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        IOException failure = null;
        failure = closeFactory(nereusFactory, failure);
        failure = closeResource(bookkeeperStorage, failure);
        failure = closeResource(bindingStore, failure);
        storageClasses = List.of();
        if (failure != null) {
            throw failure;
        }
    }

    private void ensureReady() {
        if (!initialized.get() || storageClasses.isEmpty() || closed.get()) {
            throw new IllegalStateException("Nereus managed-ledger storage is not available");
        }
    }

    private static IOException closeFactory(NereusManagedLedgerFactory factory, IOException failure) {
        if (factory == null) {
            return failure;
        }
        try {
            factory.shutdown();
            return failure;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return accumulate(failure, interrupted);
        } catch (ManagedLedgerException error) {
            return accumulate(failure, error);
        }
    }

    private static IOException closeResource(AutoCloseable resource, IOException failure) {
        if (resource == null) {
            return failure;
        }
        try {
            resource.close();
            return failure;
        } catch (Exception error) {
            return accumulate(failure, error);
        }
    }

    private static IOException accumulate(IOException existing, Throwable error) {
        IOException mapped = error instanceof IOException io ? io : new IOException(error);
        if (existing == null) {
            return mapped;
        }
        existing.addSuppressed(mapped);
        return existing;
    }

    private static void suppressClose(Throwable root, AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Throwable closeFailure) {
            root.addSuppressed(closeFailure);
        }
    }
}
