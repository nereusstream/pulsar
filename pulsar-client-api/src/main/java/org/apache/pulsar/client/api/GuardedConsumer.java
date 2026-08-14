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
package org.apache.pulsar.client.api;

import java.util.Optional;

/** A consumer whose SUBSCRIBE was admitted against one topic resource identity. */
public interface GuardedConsumer<T> extends Consumer<T> {

    /** The exact resource identity requested at subscription time. */
    TopicResourceGuard resourceGuard();

    /** The broker-attested physical topic identity returned for the current connection. */
    Optional<TopicResourceGuardAttestation> resourceGuardAttestation();

    /** Non-zero generation of the broker connection that admitted the guarded SUBSCRIBE. */
    long connectionGeneration();
}
