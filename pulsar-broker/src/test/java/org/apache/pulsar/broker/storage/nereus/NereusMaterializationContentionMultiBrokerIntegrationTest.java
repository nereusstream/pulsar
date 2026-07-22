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
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.managedledger.NereusManagedLedger;
import com.nereusstream.materialization.RegisteredMaterializationScanResult;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV1;
import com.nereusstream.pulsar.Phase4ObjectWalRuntime;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** Real two-broker, two-process materialization contention over the same durable stream registry. */
@Test(groups = "broker-isolated")
public class NereusMaterializationContentionMultiBrokerIntegrationTest {
    private static final String ACTIVATION_RUN =
            "phasefourm6materializationcontention";
    private static final int MAX_TOPIC_CANDIDATES = 16;
    private static final int WORKLOAD_ENTRIES = 16;
    private static final int WORKLOAD_PAYLOAD_BYTES = 128 * 1024;

    private final NereusMultiBrokerIntegrationTest cluster =
            new NereusMultiBrokerIntegrationTest(
                    true,
                    false,
                    StorageProfile.OBJECT_WAL_SYNC_OBJECT);

    @BeforeClass(alwaysRun = true)
    public void startCluster() throws Exception {
        cluster.startCluster();
    }

    @AfterClass(alwaysRun = true)
    public void closeCluster() throws Exception {
        cluster.closeCluster();
    }

    @Test(timeOut = 900_000)
    public void twoBrokerWorkerRuntimesContendOnTheSameStreamsAndConvergeExactReads()
            throws Exception {
        TopicFixture fixture = createTopicsOwnedByBothBrokers();
        cluster.awaitGenerationCapabilityConvergence();
        GenerationRegistrationBackfillReport activation =
                cluster.activateOrRolloverGeneration(0, ACTIVATION_RUN);
        assertThat(activation.failureCount()).isZero();
        assertThat(activation.nereusProjectionsRegistered())
                .isGreaterThanOrEqualTo(fixture.expectedByTopic().size());

        Map<Integer, RuntimeHandle> runtimes = runtimeByBroker(fixture);
        assertThat(runtimes).hasSize(2);
        assertThat(runtimes.get(0).runtime())
                .isNotSameAs(runtimes.get(1).runtime());
        assertThat(runtimes.get(0).processRunId())
                .isNotEqualTo(runtimes.get(1).processRunId());
        assertThat(runtimes.values())
                .allSatisfy(handle -> assertThat(
                                handle.runtime().materializationService().isRunning())
                        .isTrue());

        List<ScanOutcome> initial = contend(runtimes);
        assertThat(initial).hasSize(2).allSatisfy(outcome -> {
            if (outcome.result() != null) {
                assertCompleteRegistryPass(
                        outcome.result(), fixture.expectedByTopic().size());
            } else {
                assertThat(outcome.failure()).isInstanceOf(NereusException.class);
                assertThat(((NereusException) outcome.failure()).retriable()).isTrue();
            }
        });

        Map<Integer, RegisteredMaterializationScanResult> converged =
                new LinkedHashMap<>();
        runtimes.forEach((broker, handle) -> converged.put(
                broker,
                converge(handle.runtime(), fixture.expectedByTopic().size())));
        assertThat(converged).hasSize(2);

        String compactedPrefix = CompactedObjectFormatV1
                .prefix(cluster.clusterName(), ReadView.COMMITTED)
                .value();
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            Set<String> compacted = cluster.logicalObjectKeys().stream()
                    .filter(key -> key.startsWith(compactedPrefix))
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            assertThat(compacted)
                    .hasSizeGreaterThanOrEqualTo(
                            fixture.expectedByTopic().size());
        });

        fixture.expectedByTopic().forEach((topic, expected) ->
                assertDirectStorageRead(topic, expected.size()));
        try (PulsarClient client = cluster.multiBrokerClient()) {
            fixture.expectedByTopic().forEach((topic, expected) ->
                    assertExactRead(client, topic, expected));
            assertBookKeeperControl(client);
        }
    }

    private void assertDirectStorageRead(String topic, int expectedEntries) {
        NereusManagedLedger ledger = cluster.loadedNereusLedger(topic);
        for (int offset = 0; offset < expectedEntries; offset++) {
            Entry entry = null;
            try {
                entry = ledger.readAt(offset, ledger.currentMetadata())
                        .get(30, TimeUnit.SECONDS);
                assertThat(entry.getPosition().getLedgerId())
                        .isEqualTo(ledger.projection().virtualLedgerId());
                assertThat(entry.getPosition().getEntryId()).isEqualTo(offset);
            } catch (Throwable failure) {
                throw new AssertionError(
                        "direct materialized read failed for topic=" + topic
                                + ", offset=" + offset
                                + ", expectedEntries=" + expectedEntries,
                        unwrap(failure));
            } finally {
                if (entry != null) {
                    entry.release();
                }
            }
        }
    }

    private TopicFixture createTopicsOwnedByBothBrokers() throws Exception {
        PersistencePolicies nereusPolicy = new PersistencePolicies(
                1, 1, 1, 0, StorageClassBindingRecord.NEREUS);
        Map<Integer, String> anchorByBroker = new LinkedHashMap<>();
        Map<String, List<ExpectedMessage>> expectedByTopic =
                new LinkedHashMap<>();
        try (PulsarClient client = cluster.multiBrokerClient()) {
            for (int candidate = 0;
                    candidate < MAX_TOPIC_CANDIDATES
                            && anchorByBroker.size() < 2;
                    candidate++) {
                String topic = "persistent://" + cluster.namespace()
                        + "/phase4-m6-worker-contention-" + candidate;
                cluster.admin(0).topicPolicies().setPersistence(
                        topic, nereusPolicy);
                Awaitility.await().atMost(Duration.ofSeconds(30))
                        .untilAsserted(() -> assertThat(cluster.admin(0)
                                        .topicPolicies()
                                        .getPersistence(topic))
                                .isEqualTo(nereusPolicy));
                cluster.admin(0).topics().createNonPartitionedTopic(topic);
                List<ExpectedMessage> expected = new ArrayList<>();
                append(client, topic, expected, "owner-probe", 1, 64);
                expectedByTopic.put(topic, expected);
                int owner = cluster.awaitOwner(
                        cluster.admin(0), topic, null);
                anchorByBroker.putIfAbsent(owner, topic);
            }
            assertThat(anchorByBroker)
                    .withFailMessage(
                            "could not place one Nereus topic on each broker: %s",
                            anchorByBroker)
                    .containsOnlyKeys(0, 1);
            expectedByTopic.forEach((topic, expected) -> append(
                    client,
                    topic,
                    expected,
                    "contention-workload",
                    WORKLOAD_ENTRIES,
                    WORKLOAD_PAYLOAD_BYTES));
        }
        return new TopicFixture(anchorByBroker, expectedByTopic);
    }

    private Map<Integer, RuntimeHandle> runtimeByBroker(
            TopicFixture fixture) {
        Map<Integer, RuntimeHandle> result = new LinkedHashMap<>();
        fixture.anchorByBroker().forEach((broker, topic) -> {
            assertThat(cluster.awaitOwner(cluster.admin(0), topic, broker))
                    .isEqualTo(broker);
            NereusManagedLedger ledger = cluster.loadedNereusLedger(topic);
            Phase4ObjectWalRuntime runtime = cluster.materializationRuntime(topic);
            result.put(broker, new RuntimeHandle(
                    runtime, ledger.runtime().processRunId()));
        });
        return result;
    }

    private static List<ScanOutcome> contend(
            Map<Integer, RuntimeHandle> runtimes) throws Exception {
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService starters = Executors.newFixedThreadPool(2);
        try {
            List<CompletableFuture<ScanOutcome>> scans = runtimes.values()
                    .stream()
                    .map(handle -> CompletableFuture.supplyAsync(
                            () -> scanAfterBarrier(
                                    handle.runtime(), ready, start),
                            starters))
                    .toList();
            assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            return scans.stream()
                    .map(CompletableFuture::join)
                    .toList();
        } finally {
            start.countDown();
            starters.shutdownNow();
            assertThat(starters.awaitTermination(30, TimeUnit.SECONDS))
                    .isTrue();
        }
    }

    private static ScanOutcome scanAfterBarrier(
            Phase4ObjectWalRuntime runtime,
            CountDownLatch ready,
            CountDownLatch start) {
        ready.countDown();
        try {
            if (!start.await(30, TimeUnit.SECONDS)) {
                return new ScanOutcome(
                        null,
                        new AssertionError(
                                "materialization contention barrier timed out"));
            }
            return new ScanOutcome(
                    runtime.materializationService()
                            .scanNow()
                            .get(120, TimeUnit.SECONDS),
                    null);
        } catch (Throwable failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return new ScanOutcome(null, unwrap(failure));
        }
    }

    private static RegisteredMaterializationScanResult converge(
            Phase4ObjectWalRuntime runtime,
            int expectedRegistrations) {
        AtomicReference<RegisteredMaterializationScanResult> result =
                new AtomicReference<>();
        Awaitility.await()
                .atMost(Duration.ofSeconds(120))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    try {
                        RegisteredMaterializationScanResult current =
                                runtime.materializationService()
                                        .scanNow()
                                        .get(120, TimeUnit.SECONDS);
                        assertCompleteRegistryPass(
                                current, expectedRegistrations);
                        result.set(current);
                    } catch (ExecutionException transientFailure) {
                        Throwable exact = unwrap(transientFailure);
                        assertThat(exact).isInstanceOf(NereusException.class);
                        assertThat(((NereusException) exact).retriable())
                                .isTrue();
                        throw new AssertionError(
                                "worker contention has not converged", exact);
                    }
                });
        return result.get();
    }

    private static void assertCompleteRegistryPass(
            RegisteredMaterializationScanResult result,
            int expectedRegistrations) {
        assertThat(result.shardsScanned()).isEqualTo(64);
        assertThat(result.registrationsScanned())
                .isEqualTo(expectedRegistrations);
        assertThat(result.registrationsAdmitted())
                .isEqualTo(expectedRegistrations);
        assertThat(result.registrationsSkipped()).isZero();
    }

    private static void append(
            PulsarClient client,
            String topic,
            List<ExpectedMessage> expected,
            String prefix,
            int count,
            int payloadBytes) {
        try (Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .enableBatching(false)
                .sendTimeout(30, TimeUnit.SECONDS)
                .create()) {
            for (int index = 0; index < count; index++) {
                byte[] payload = payload(prefix, index, payloadBytes);
                Map<String, String> properties = Map.of(
                        "kind", prefix,
                        "sequence", Integer.toString(index));
                MessageId messageId = producer.newMessage()
                        .properties(properties)
                        .value(payload)
                        .send();
                expected.add(ExpectedMessage.from(
                        payload, properties, messageId));
            }
        } catch (Exception failure) {
            throw new AssertionError(
                    "failed to append contention workload to " + topic,
                    failure);
        }
    }

    private static byte[] payload(
            String prefix, int index, int payloadBytes) {
        byte[] payload = new byte[payloadBytes];
        Arrays.fill(payload, (byte) ('a' + index % 26));
        byte[] identity = (prefix + "-" + index + "|")
                .getBytes(StandardCharsets.UTF_8);
        System.arraycopy(
                identity,
                0,
                payload,
                0,
                Math.min(identity.length, payload.length));
        return payload;
    }

    private static void assertExactRead(
            PulsarClient client,
            String topic,
            List<ExpectedMessage> expected) {
        try (Reader<byte[]> reader = client.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .create()) {
            for (int index = 0; index < expected.size(); index++) {
                ExpectedMessage exact = expected.get(index);
                Message<byte[]> actual = reader.readNext(
                        30, TimeUnit.SECONDS);
                assertThat(actual)
                        .withFailMessage(
                                "topic=%s, index=%s, expectedEntries=%s",
                                topic,
                                index,
                                expected.size())
                        .isNotNull();
                assertThat(actual.getData()).containsExactly(exact.payload());
                assertThat(actual.getProperties())
                        .isEqualTo(exact.properties());
                exact.assertSamePosition(actual.getMessageId());
            }
            assertThat(reader.readNext(250, TimeUnit.MILLISECONDS))
                    .isNull();
        } catch (Exception failure) {
            throw new AssertionError(
                    "failed to verify exact materialized read for " + topic,
                    failure);
        }
    }

    private static void assertBookKeeperControl(
            PulsarClient client) throws Exception {
        String topic = "persistent://nereus-e2e/phase2/phase4-m6-bookkeeper-control";
        byte[] expected = "bookkeeper-control".getBytes(StandardCharsets.UTF_8);
        MessageId messageId;
        try (Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .enableBatching(false)
                .create()) {
            messageId = producer.send(expected);
        }
        try (Reader<byte[]> reader = client.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .create()) {
            Message<byte[]> actual = reader.readNext(
                    30, TimeUnit.SECONDS);
            assertThat(actual).isNotNull();
            assertThat(actual.getData()).containsExactly(expected);
            assertThat(actual.getMessageId()).isEqualTo(messageId);
        }
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while ((current instanceof ExecutionException
                        || current instanceof java.util.concurrent.CompletionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private record TopicFixture(
            Map<Integer, String> anchorByBroker,
            Map<String, List<ExpectedMessage>> expectedByTopic) {
        private TopicFixture {
            anchorByBroker = Map.copyOf(anchorByBroker);
            expectedByTopic = Map.copyOf(expectedByTopic);
        }
    }

    private record RuntimeHandle(
            Phase4ObjectWalRuntime runtime,
            String processRunId) {
    }

    private record ScanOutcome(
            RegisteredMaterializationScanResult result,
            Throwable failure) {
        private ScanOutcome {
            if ((result == null) == (failure == null)) {
                throw new IllegalArgumentException(
                        "scan outcome must contain exactly one result or failure");
            }
        }
    }

    private record ExpectedMessage(
            byte[] payload,
            Map<String, String> properties,
            MessageIdAdv messageId) {
        private ExpectedMessage {
            payload = payload.clone();
            properties = Map.copyOf(properties);
        }

        private static ExpectedMessage from(
                byte[] payload,
                Map<String, String> properties,
                MessageId messageId) {
            assertThat(messageId).isInstanceOf(MessageIdAdv.class);
            return new ExpectedMessage(
                    payload, properties, (MessageIdAdv) messageId);
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }

        private void assertSamePosition(MessageId actual) {
            assertThat(actual).isInstanceOf(MessageIdAdv.class);
            MessageIdAdv exact = (MessageIdAdv) actual;
            assertThat(exact.getLedgerId())
                    .isEqualTo(messageId.getLedgerId());
            assertThat(exact.getEntryId())
                    .isEqualTo(messageId.getEntryId());
            assertThat(exact.getPartitionIndex())
                    .isEqualTo(messageId.getPartitionIndex());
            assertThat(exact.getBatchIndex())
                    .isEqualTo(messageId.getBatchIndex());
        }
    }
}
