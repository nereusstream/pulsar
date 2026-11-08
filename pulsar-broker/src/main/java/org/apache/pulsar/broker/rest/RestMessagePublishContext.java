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
package org.apache.pulsar.broker.rest;

import io.netty.util.Recycler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.storage.nereus.v2.NereusPulsarActiveFence.ValidWord;

/**
 * PublishContext implementation for REST message publishing.
 */
@CustomLog
public class RestMessagePublishContext implements Topic.PublishContext {

    private Topic topic;
    private long startTimeNs;
    private CompletableFuture<Position> positionFuture;
    private ValidWord storageAuthorityWord;

    @Override
    public void setStorageAuthorityWord(ValidWord word) {
        this.storageAuthorityWord = word;
    }

    @Override
    public ValidWord getStorageAuthorityWord() {
        return storageAuthorityWord;
    }

    /**
     * Executed from managed ledger thread when the message is persisted.
     */
    @Override
    public void completed(Exception exception, long ledgerId, long entryId) {
        if (exception != null) {
            positionFuture.completeExceptionally(exception);
            log.info().attr("ledgerId", ledgerId).attr("entryId", entryId)
                    .log("Failed to write entry for rest produce request. Triggered send callback");
        } else {
            log.info().attr("topic", topic.getName()).attr("ledgerId", ledgerId).attr("entryId", entryId)
                    .log("Success write topic for rest produce request. Triggered send callback");
            topic.recordAddLatency(System.nanoTime() - startTimeNs, TimeUnit.NANOSECONDS);
            positionFuture.complete(PositionFactory.create(ledgerId, entryId));
        }
        recycle();
    }

    // recycler
    public static RestMessagePublishContext get(CompletableFuture<Position> positionFuture, Topic topic,
                                                     long startTimeNs) {
        RestMessagePublishContext callback = RECYCLER.get();
        callback.positionFuture = positionFuture;
        callback.topic = topic;
        callback.startTimeNs = startTimeNs;
        return callback;
    }

    private final Recycler.Handle<RestMessagePublishContext> recyclerHandle;

    private RestMessagePublishContext(Recycler.Handle<RestMessagePublishContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<RestMessagePublishContext> RECYCLER = new Recycler<RestMessagePublishContext>() {
        protected RestMessagePublishContext newObject(Handle<RestMessagePublishContext> handle) {
            return new RestMessagePublishContext(handle);
        }
    };

    public void recycle() {
        topic = null;
        storageAuthorityWord = null;
        startTimeNs = -1;
        recyclerHandle.recycle(this);
    }
}
