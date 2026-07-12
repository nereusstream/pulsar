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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/** Serializes authoritative live-policy refreshes and drops stale asynchronous completions. */
public final class NereusTopicPolicyUpdateCoordinator {
    private final AtomicLong requestedSequence = new AtomicLong();
    private final AtomicReference<CompletableFuture<Void>> latestRefresh = new AtomicReference<>();

    public CompletableFuture<Void> refresh(
            Supplier<CompletableFuture<NereusTopicPolicySnapshot>> loader,
            Executor applyExecutor,
            Function<NereusTopicPolicySnapshot, CompletableFuture<Void>> applier) {
        java.util.Objects.requireNonNull(loader, "loader");
        java.util.Objects.requireNonNull(applyExecutor, "applyExecutor");
        java.util.Objects.requireNonNull(applier, "applier");
        CompletableFuture<Void> result = new CompletableFuture<>();
        final long sequence;
        synchronized (this) {
            sequence = requestedSequence.incrementAndGet();
            latestRefresh.set(result);
        }
        CompletableFuture<NereusTopicPolicySnapshot> loaded;
        try {
            loaded = java.util.Objects.requireNonNull(loader.get(), "loader result");
        } catch (Throwable error) {
            completeOrFollowLatest(sequence, result, error);
            return result;
        }
        loaded.whenComplete((snapshot, loadError) -> {
            try {
                applyExecutor.execute(() -> {
                    final CompletableFuture<Void> applied;
                    synchronized (this) {
                        if (sequence != requestedSequence.get()) {
                            followLatest(result, latestRefresh.get());
                            return;
                        }
                        if (loadError != null) {
                            result.completeExceptionally(loadError);
                            return;
                        }
                        try {
                            applied = java.util.Objects.requireNonNull(
                                    applier.apply(snapshot), "applier result");
                        } catch (Throwable error) {
                            result.completeExceptionally(error);
                            return;
                        }
                    }
                    applied.whenComplete((__, applyError) ->
                            completeOrFollowLatest(sequence, result, applyError));
                });
            } catch (Throwable error) {
                completeOrFollowLatest(sequence, result, error);
            }
        });
        return result;
    }

    private void completeOrFollowLatest(
            long sequence, CompletableFuture<Void> result, Throwable error) {
        synchronized (this) {
            if (sequence != requestedSequence.get()) {
                followLatest(result, latestRefresh.get());
            } else if (error == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(error);
            }
        }
    }

    private static void followLatest(
            CompletableFuture<Void> result, CompletableFuture<Void> latest) {
        if (latest == result) {
            result.complete(null);
            return;
        }
        latest.whenComplete((__, latestError) -> {
            if (latestError == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(latestError);
            }
        });
    }
}
