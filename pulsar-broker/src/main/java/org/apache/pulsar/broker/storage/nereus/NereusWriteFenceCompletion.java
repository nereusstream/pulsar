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

/** Immutable broker handoff for one terminal managed-ledger write-fence generation. */
public record NereusWriteFenceCompletion(
        long generation,
        NereusWriteFenceResolution resolution,
        Throwable failure) {
    public NereusWriteFenceCompletion {
        if (generation < 1) {
            throw new IllegalArgumentException("write-fence completion generation must be positive");
        }
        if ((resolution == null) == (failure == null)) {
            throw new IllegalArgumentException("exactly one write-fence resolution or failure is required");
        }
    }
}
