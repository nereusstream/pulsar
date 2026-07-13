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
import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.managedledger.NereusWriteFenceResolution;
import com.nereusstream.managedledger.NereusWriteFenceSnapshot;
import com.nereusstream.managedledger.NereusWriteFenceView;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

public class NereusWriteFenceBridgeTest {
    @Test
    public void returnsFalseWithoutCurrentFence() {
        NereusWriteFenceBridge bridge = new NereusWriteFenceBridge();
        MutableFenceView view = new MutableFenceView();

        assertThat(bridge.deferAutoUnfenceIfNeeded(view, Runnable::run, ignored -> { }))
                .isFalse();
        assertThat(view.awaitCount).hasValue(0);
    }

    @Test
    public void coalescesGenerationAndSchedulesImmutableResolution() {
        NereusWriteFenceBridge bridge = new NereusWriteFenceBridge();
        MutableFenceView view = new MutableFenceView();
        view.install(7);
        QueuedExecutor executor = new QueuedExecutor();
        List<NereusWriteFenceCompletion> completions = new ArrayList<>();

        assertThat(bridge.deferAutoUnfenceIfNeeded(view, executor, completions::add)).isTrue();
        assertThat(bridge.deferAutoUnfenceIfNeeded(view, executor, completions::add)).isTrue();
        assertThat(view.awaitCount).hasValue(1);

        view.resolve(NereusWriteFenceResolution.COMMITTED);
        assertThat(completions).isEmpty();
        executor.runAll();

        assertThat(completions).containsExactly(new NereusWriteFenceCompletion(
                7, NereusWriteFenceResolution.COMMITTED, null));
    }

    @Test
    public void handsOffExceptionalTerminalWithoutUnwrappingLoss() {
        NereusWriteFenceBridge bridge = new NereusWriteFenceBridge();
        MutableFenceView view = new MutableFenceView();
        view.install(3);
        IllegalStateException failure = new IllegalStateException("permanent recovery failure");
        AtomicReference<NereusWriteFenceCompletion> completion = new AtomicReference<>();

        bridge.deferAutoUnfenceIfNeeded(view, Runnable::run, completion::set);
        view.fail(failure);

        assertThat(completion.get().generation()).isEqualTo(3);
        assertThat(completion.get().resolution()).isNull();
        assertThat(completion.get().failure()).isSameAs(failure);
    }

    @Test
    public void closeDetachesContinuationWithoutCancellingCoreRecovery() {
        NereusWriteFenceBridge bridge = new NereusWriteFenceBridge();
        MutableFenceView view = new MutableFenceView();
        view.install(2);
        QueuedExecutor executor = new QueuedExecutor();
        List<NereusWriteFenceCompletion> completions = new ArrayList<>();

        bridge.deferAutoUnfenceIfNeeded(view, executor, completions::add);
        bridge.close();
        view.resolve(NereusWriteFenceResolution.PROVEN_NOT_COMMITTED);
        executor.runAll();

        assertThat(completions).isEmpty();
        assertThat(view.terminal.isCancelled()).isFalse();
    }

    @Test
    public void executorRejectionFailsClosedInsteadOfDeliveringSuccess() {
        NereusWriteFenceBridge bridge = new NereusWriteFenceBridge();
        MutableFenceView view = new MutableFenceView();
        view.install(4);
        AtomicReference<NereusWriteFenceCompletion> completion = new AtomicReference<>();

        bridge.deferAutoUnfenceIfNeeded(
                view,
                ignored -> {
                    throw new java.util.concurrent.RejectedExecutionException("executor closed");
                },
                completion::set);
        view.resolve(NereusWriteFenceResolution.COMMITTED);

        assertThat(completion.get().resolution()).isNull();
        assertThat(completion.get().failure())
                .isInstanceOf(java.util.concurrent.RejectedExecutionException.class);
    }

    @Test
    public void completionRequiresExactlyOneTerminalValue() {
        assertThatThrownBy(() -> new NereusWriteFenceCompletion(1, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NereusWriteFenceCompletion(
                1, NereusWriteFenceResolution.COMMITTED, new IllegalStateException()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static final class MutableFenceView implements NereusWriteFenceView {
        private final AtomicInteger awaitCount = new AtomicInteger();
        private Optional<NereusWriteFenceSnapshot> current = Optional.empty();
        private CompletableFuture<NereusWriteFenceResolution> terminal;

        private void install(long generation) {
            current = Optional.of(new NereusWriteFenceSnapshot(
                    generation, new AppendAttemptId("attempt-" + generation)));
            terminal = new CompletableFuture<>();
        }

        private void resolve(NereusWriteFenceResolution resolution) {
            current = Optional.empty();
            terminal.complete(resolution);
        }

        private void fail(Throwable error) {
            current = Optional.empty();
            terminal.completeExceptionally(error);
        }

        @Override
        public Optional<NereusWriteFenceSnapshot> currentWriteFence() {
            return current;
        }

        @Override
        public CompletableFuture<NereusWriteFenceResolution> awaitWriteFence(long generation) {
            awaitCount.incrementAndGet();
            return terminal;
        }
    }

    private static final class QueuedExecutor implements Executor {
        private final List<Runnable> tasks = new ArrayList<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        private void runAll() {
            List.copyOf(tasks).forEach(Runnable::run);
            tasks.clear();
        }
    }
}
