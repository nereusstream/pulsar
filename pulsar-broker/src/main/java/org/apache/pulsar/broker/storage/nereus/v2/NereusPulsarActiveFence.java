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
import java.util.concurrent.atomic.AtomicReference;

/** One-reference local ACTIVE authority word; data-path capture and recheck allocate nothing. */
public final class NereusPulsarActiveFence {
    public sealed interface Word permits InvalidWord, ValidWord {
        long sequence();

        long continuityEpoch();
    }

    public record InvalidWord(long sequence, long continuityEpoch) implements Word {
        public InvalidWord {
            requireNonNegative(sequence, continuityEpoch);
        }
    }

    public record ValidWord(
            long sequence,
            long continuityEpoch,
            NereusOwnershipWitness ownership,
            NereusPulsarBindingAuthority binding,
            NereusContinuityPermit continuityPermit) implements Word {
        public ValidWord {
            requireNonNegative(sequence, continuityEpoch);
            Objects.requireNonNull(ownership, "ownership");
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(continuityPermit, "continuityPermit");
            if (continuityEpoch != continuityPermit.invalidationEpoch()) {
                throw new IllegalArgumentException("VALID word must bind its exact continuity epoch");
            }
        }
    }

    private final AtomicReference<Word> word = new AtomicReference<>(new InvalidWord(0, 0));

    public Word current() {
        return word.get();
    }

    public InvalidWord invalidate(long continuityEpoch) {
        while (true) {
            Word current = word.get();
            if (current.sequence() == Long.MAX_VALUE) {
                throw new IllegalStateException("P1 local fence sequence exhausted");
            }
            InvalidWord invalid = new InvalidWord(
                    current.sequence() + 1, Math.max(continuityEpoch, current.continuityEpoch()));
            if (word.compareAndSet(current, invalid)) {
                return invalid;
            }
        }
    }

    /** Invalidates only the exact installation owned by the caller; stale close handles cannot fence a successor. */
    public boolean invalidateIfCurrent(ValidWord expected) {
        Objects.requireNonNull(expected, "expected");
        if (expected.sequence() == Long.MAX_VALUE) {
            throw new IllegalStateException("P1 local fence sequence exhausted");
        }
        return word.compareAndSet(
                expected,
                new InvalidWord(expected.sequence() + 1, expected.continuityEpoch()));
    }

    public ValidWord tryInstall(
            InvalidWord expected,
            NereusOwnershipWitness ownership,
            NereusPulsarBindingAuthority binding,
            NereusContinuityPermit continuityPermit) {
        Objects.requireNonNull(expected, "expected");
        if (continuityPermit.invalidationEpoch() < expected.continuityEpoch()) {
            return null;
        }
        ValidWord candidate = new ValidWord(
                expected.sequence(), continuityPermit.invalidationEpoch(), ownership, binding, continuityPermit);
        return word.compareAndSet(expected, candidate) ? candidate : null;
    }

    /** Allocation-free hot-path capture; null means fail closed. */
    public ValidWord captureValidOrNull() {
        Word current = word.get();
        return current instanceof ValidWord valid ? valid : null;
    }

    /** Reference equality is the complete success-completion fence check. */
    public boolean isCurrent(ValidWord expected) {
        return word.get() == expected;
    }

    private static void requireNonNegative(long sequence, long continuityEpoch) {
        if (sequence < 0 || continuityEpoch < 0) {
            throw new IllegalArgumentException("local fence values must be non-negative");
        }
    }
}
