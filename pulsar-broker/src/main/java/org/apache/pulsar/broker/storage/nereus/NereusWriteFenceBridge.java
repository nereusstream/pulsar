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

import com.nereusstream.managedledger.NereusWriteFenceResolution;
import com.nereusstream.managedledger.NereusWriteFenceSnapshot;
import com.nereusstream.managedledger.NereusWriteFenceView;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/** Coalesces managed-ledger write-fence terminals and hands immutable completions to a topic executor. */
public final class NereusWriteFenceBridge implements AutoCloseable {
    private final Map<Long, Object> attachments = new HashMap<>();
    private boolean closed;

    public boolean deferAutoUnfenceIfNeeded(
            NereusWriteFenceView view,
            Executor topicOrderedExecutor,
            Consumer<NereusWriteFenceCompletion> completionConsumer) {
        java.util.Objects.requireNonNull(view, "view");
        java.util.Objects.requireNonNull(topicOrderedExecutor, "topicOrderedExecutor");
        java.util.Objects.requireNonNull(completionConsumer, "completionConsumer");
        Optional<NereusWriteFenceSnapshot> current = java.util.Objects.requireNonNull(
                view.currentWriteFence(), "currentWriteFence result");
        if (current.isEmpty()) {
            return false;
        }
        long generation = current.orElseThrow().generation();
        Object attachment;
        synchronized (this) {
            if (closed) {
                return true;
            }
            if (attachments.containsKey(generation)) {
                return true;
            }
            attachment = new Object();
            attachments.put(generation, attachment);
        }

        final CompletableFuture<NereusWriteFenceResolution> terminal;
        try {
            terminal = java.util.Objects.requireNonNull(
                    view.awaitWriteFence(generation), "awaitWriteFence result");
        } catch (Throwable error) {
            dispatch(
                    generation,
                    attachment,
                    topicOrderedExecutor,
                    completionConsumer,
                    new NereusWriteFenceCompletion(generation, null, unwrap(error)));
            return true;
        }
        terminal.whenComplete((resolution, error) -> {
            NereusWriteFenceCompletion completion;
            if (error != null) {
                completion = new NereusWriteFenceCompletion(generation, null, unwrap(error));
            } else if (resolution == null) {
                completion = new NereusWriteFenceCompletion(
                        generation,
                        null,
                        new IllegalStateException("Nereus write-fence terminal returned a null resolution"));
            } else {
                completion = new NereusWriteFenceCompletion(generation, resolution, null);
            }
            dispatch(generation, attachment, topicOrderedExecutor, completionConsumer, completion);
        });
        return true;
    }

    private void dispatch(
            long generation,
            Object attachment,
            Executor executor,
            Consumer<NereusWriteFenceCompletion> consumer,
            NereusWriteFenceCompletion completion) {
        Runnable task = deliveryTask(generation, attachment, consumer, completion);
        try {
            executor.execute(task);
        } catch (Throwable rejected) {
            deliveryTask(
                    generation,
                    attachment,
                    consumer,
                    new NereusWriteFenceCompletion(generation, null, unwrap(rejected))).run();
        }
    }

    private Runnable deliveryTask(
            long generation,
            Object attachment,
            Consumer<NereusWriteFenceCompletion> consumer,
            NereusWriteFenceCompletion completion) {
        return () -> {
            synchronized (this) {
                if (closed || attachments.get(generation) != attachment) {
                    return;
                }
                attachments.remove(generation);
            }
            consumer.accept(completion);
        };
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        attachments.clear();
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
