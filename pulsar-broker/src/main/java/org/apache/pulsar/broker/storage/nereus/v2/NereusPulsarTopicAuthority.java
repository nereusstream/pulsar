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
package org.apache.pulsar.broker.storage.nereus.v2;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

/** Revalidates one original ledger's authority and never substitutes it for a captured operation word. */
public final class NereusPulsarTopicAuthority {
    private final Executor executor;
    private final Function<Runnable, CompletionStage<NereusPulsarAuthorityInstaller.Installation>> install;
    private volatile NereusPulsarAuthorityInstaller.Installation active;
    private CompletableFuture<Void> inFlight;
    private boolean stopping;

    public NereusPulsarTopicAuthority(Executor executor,
            Function<Runnable, CompletionStage<NereusPulsarAuthorityInstaller.Installation>> install) {
        this.executor = Objects.requireNonNull(executor);
        this.install = Objects.requireNonNull(install);
    }

    public NereusPulsarActiveFence.ValidWord captureOrNull() {
        var current = active;
        return current != null && current.fence().isCurrent(current.word()) ? current.word() : null;
    }

    public boolean isCurrent(NereusPulsarActiveFence.ValidWord captured) {
        return captured != null && captured == captureOrNull();
    }

    /** READY or an invalidation is a request to re-read. Only a successful A/read/B installs authority. */
    public synchronized CompletableFuture<Void> requestRefresh() {
        if (stopping) {
            return CompletableFuture.failedFuture(new IllegalStateException("Native topic authority is closing"));
        }
        if (captureOrNull() != null) {
            return CompletableFuture.completedFuture(null);
        }
        if (inFlight != null && !inFlight.isDone()) {
            return inFlight.copy();
        }
        var original = new CompletableFuture<Void>();
        inFlight = original;
        try {
            executor.execute(() -> refresh(original));
        } catch (RuntimeException failure) {
            original.completeExceptionally(failure);
        }
        return original.copy();
    }

    private void refresh(CompletableFuture<Void> original) {
        NereusPulsarAuthorityInstaller.Installation prior;
        synchronized (this) {
            prior = active;
            active = null;
        }
        try {
            if (prior != null) {
                prior.close();
            }
            install.apply(() -> onInvalidated(original)).whenComplete((next, error) -> {
                if (error != null) {
                    original.completeExceptionally(error);
                    return;
                }
                boolean accepted;
                synchronized (this) {
                    accepted = !stopping && next.fence().isCurrent(next.word());
                    if (accepted) {
                        active = next;
                    }
                }
                if (accepted) {
                    original.complete(null);
                } else {
                    try {
                        next.close();
                        original.completeExceptionally(new IllegalStateException(
                                "Native topic authority changed during revalidation"));
                    } catch (Exception failure) {
                        original.completeExceptionally(failure);
                    }
                }
            });
        } catch (Exception failure) {
            original.completeExceptionally(failure);
        }
    }

    private synchronized void onInvalidated(CompletableFuture<Void> installed) {
        if (!stopping && inFlight == installed && installed.isDone() && !installed.isCompletedExceptionally()) {
            requestRefresh();
        }
    }

    public synchronized void stop() {
        stopping = true;
        var original = active;
        if (original != null) {
            original.fence().invalidateIfCurrent(original.word());
        }
    }

    public CompletableFuture<Void> closeAsync() {
        stop();
        CompletableFuture<Void> settled;
        synchronized (this) {
            settled = inFlight == null ? CompletableFuture.completedFuture(null)
                    : inFlight.handle((unused, error) -> null);
        }
        return settled.thenRun(() -> {
            NereusPulsarAuthorityInstaller.Installation original;
            synchronized (this) {
                original = active;
                active = null;
            }
            if (original != null) {
                try {
                    original.close();
                } catch (Exception failure) {
                    throw new java.util.concurrent.CompletionException(failure);
                }
            }
        });
    }
}
