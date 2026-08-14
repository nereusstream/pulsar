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

import java.util.Objects;
import java.util.Optional;

/** Typed failure for a guarded producer or guarded SEND. */
public class TopicResourceGuardException extends PulsarClientException {
    private static final long serialVersionUID = 1L;

    private final TopicResourceGuard expectedGuard;
    private final Optional<GuardedSendErrorEvidence> responseEvidence;
    private final boolean definitelyNotPersisted;

    public TopicResourceGuardException(String message, TopicResourceGuard expectedGuard,
                                       Optional<GuardedSendErrorEvidence> responseEvidence,
                                       boolean definitelyNotPersisted) {
        super(message);
        this.expectedGuard = Objects.requireNonNull(expectedGuard, "expectedGuard");
        this.responseEvidence = Objects.requireNonNull(responseEvidence, "responseEvidence");
        this.definitelyNotPersisted = definitelyNotPersisted;
    }

    public TopicResourceGuard expectedGuard() {
        return expectedGuard;
    }

    public Optional<GuardedSendErrorEvidence> responseEvidence() {
        return responseEvidence;
    }

    public boolean definitelyNotPersisted() {
        return definitelyNotPersisted;
    }
}
