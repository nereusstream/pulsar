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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.JCloudBlobStoreProvider;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.domain.Credentials;
import org.testng.annotations.Test;

public class P6NativeLocalStackEvidenceTest {
    private static final int ENTRY_COUNT = 50_000;
    private static final int ENTRY_BYTES = 100;
    private static final int READ_BUFFER_BYTES = 1_024 * 1_024;

    @Test(timeOut = 600_000)
    public void writesPinnedNativeColdReadEvidence() throws Exception {
        String endpoint = required("nereus.p6.localstackEndpoint");
        String outputPath = required("nereus.p6.nativeEvidenceOutput");
        String sourceCommit = required("nereus.p6.pulsarSourceCommit");
        String bucket = "nereus-p6-native-" + UUID.randomUUID();
        OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder()
                .numThreads(5)
                .name("p6-native-evidence")
                .build();
        ScheduledExecutorService statsExecutor = Executors.newSingleThreadScheduledExecutor();
        LedgerOffloaderStats stats = LedgerOffloaderStats.create(false, false, statsExecutor, 60);
        OffsetsCache offsetsCache = new OffsetsCache();
        PulsarMockBookKeeper bookKeeper = new PulsarMockBookKeeper(scheduler);
        BlobStoreManagedLedgerOffloader offloader = null;
        ReadHandle source = null;
        ReadHandle offloaded = null;
        BlobStore bootstrap = null;
        try {
            TieredStorageConfiguration config = configuration(endpoint, bucket);
            config.validate();
            bootstrap = config.getBlobStore();
            assertTrue(bootstrap.createContainerInLocation(null, bucket));
            offloader = BlobStoreManagedLedgerOffloader.create(
                    config, new HashMap<>(), scheduler, scheduler, stats, offsetsCache);
            source = buildReadHandle(bookKeeper);
            UUID attempt = UUID.nameUUIDFromBytes("p6-native-baseline".getBytes(StandardCharsets.UTF_8));
            Map<String, String> metadata = new HashMap<>();
            metadata.put("ManagedLedgerName", "public/default/persistent/p6-native-evidence");
            long offloadStart = System.nanoTime();
            offloader.offload(source, attempt, metadata).get();
            long offloadMicros = microsSince(offloadStart);

            Map<String, String> persisted = new HashMap<>(offloader.getOffloadDriverMetadata());
            persisted.putAll(metadata);
            long openStart = System.nanoTime();
            offloaded = offloader.readOffloaded(source.getId(), attempt, persisted).get();
            long openMicros = microsSince(openStart);
            List<Long> randomMicros = new ArrayList<>();
            for (int sample = 0; sample < 20; sample++) {
                long entryId = Math.floorMod(sample * 7_919L, ENTRY_COUNT);
                long start = System.nanoTime();
                try (LedgerEntries entries = offloaded.read(entryId, entryId)) {
                    assertEquals(entries.iterator().next().getEntryId(), entryId);
                }
                randomMicros.add(microsSince(start));
            }
            long sequentialStart = System.nanoTime();
            try (LedgerEntries entries = offloaded.read(0, 999)) {
                int count = 0;
                for (var ignored : entries) {
                    count++;
                }
                assertEquals(count, 1_000);
            }
            long sequentialMicros = microsSince(sequentialStart);

            Path output = Path.of(outputPath);
            Files.createDirectories(output.toAbsolutePath().getParent());
            Files.writeString(
                    output,
                    json(sourceCommit, offloadMicros, openMicros, randomMicros, sequentialMicros),
                    StandardCharsets.UTF_8);
        } finally {
            if (offloaded != null) {
                offloaded.close();
            }
            if (source != null) {
                source.close();
            }
            if (offloader != null) {
                offloader.close();
            }
            bookKeeper.close();
            offsetsCache.close();
            stats.close();
            statsExecutor.shutdownNow();
            scheduler.shutdownNow();
            if (bootstrap != null) {
                bootstrap.getContext().close();
            }
        }
    }

    private static TieredStorageConfiguration configuration(String endpoint, String bucket) {
        Map<String, String> values = new HashMap<>();
        values.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.AWS_S3.getDriver());
        values.put("managedLedgerOffloadBucket", bucket);
        values.put("managedLedgerOffloadRegion", "us-east-1");
        values.put("managedLedgerOffloadServiceEndpoint", endpoint);
        values.put("managedLedgerOffloadMaxBlockSizeInBytes", Integer.toString(64 * 1_024 * 1_024));
        values.put("managedLedgerOffloadMinBlockSizeInBytes", Integer.toString(5 * 1_024 * 1_024));
        values.put("managedLedgerOffloadReadBufferSizeInBytes", Integer.toString(READ_BUFFER_BYTES));
        TieredStorageConfiguration config = TieredStorageConfiguration.create(values);
        config.setProviderCredentials(() -> new Credentials("test", "test"));
        return config;
    }

    private static ReadHandle buildReadHandle(PulsarMockBookKeeper bookKeeper) throws Exception {
        LedgerHandle ledger = bookKeeper.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32, "p6".getBytes());
        byte[] payload = new byte[ENTRY_BYTES];
        Random random = new Random(7);
        for (int entry = 0; entry < ENTRY_COUNT; entry++) {
            if ((entry & 1) == 0) {
                java.util.Arrays.fill(payload, (byte) 0);
            } else {
                random.nextBytes(payload);
            }
            ledger.addEntry(payload);
        }
        ledger.close();
        return bookKeeper.newOpenLedgerOp()
                .withLedgerId(ledger.getId())
                .withPassword("p6".getBytes())
                .withDigestType(DigestType.CRC32)
                .execute()
                .get();
    }

    private static String json(
            String sourceCommit,
            long offloadMicros,
            long openMicros,
            List<Long> randomMicros,
            long sequentialMicros) {
        return String.format(
                Locale.ROOT,
                "{\n"
                        + "  \"schema\": \"NEREUS_V2_M2_PULSAR_P6_NATIVE_BASELINE_V1\",\n"
                        + "  \"generatedAt\": \"%s\",\n"
                        + "  \"pulsarSourceCommit\": \"%s\",\n"
                        + "  \"provider\": \"S3_COMPATIBLE_LOCALSTACK\",\n"
                        + "  \"entryCount\": %d,\n"
                        + "  \"entryBytes\": %d,\n"
                        + "  \"sourceDeclaredReadBufferBytes\": %d,\n"
                        + "  \"offloadMicros\": %d,\n"
                        + "  \"openMicros\": %d,\n"
                        + "  \"randomP50Micros\": %d,\n"
                        + "  \"randomP99Micros\": %d,\n"
                        + "  \"sequential1000Micros\": %d,\n"
                        + "  \"randomSamples\": 20,\n"
                        + "  \"claimBoundary\": \"Pinned Pulsar jcloud path on LocalStack; request bytes are source-declared, not wire-observed\"\n"
                        + "}\n",
                Instant.now(),
                sourceCommit,
                ENTRY_COUNT,
                ENTRY_BYTES,
                READ_BUFFER_BYTES,
                offloadMicros,
                openMicros,
                percentile(randomMicros, 50),
                percentile(randomMicros, 99),
                sequentialMicros);
    }

    private static long percentile(List<Long> values, int percentile) {
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int index = Math.min(sorted.size() - 1, (int) Math.ceil(percentile * sorted.size() / 100.0) - 1);
        return sorted.get(index);
    }

    private static long microsSince(long startNanos) {
        return Math.max(1, (System.nanoTime() - startNanos) / 1_000);
    }

    private static String required(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) {
            value = System.getenv(name.toUpperCase(Locale.ROOT).replace('.', '_'));
        }
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing required P6 native evidence property " + name);
        }
        return value;
    }
}
