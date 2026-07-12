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
import java.util.Map;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.annotations.Test;

public class NereusStorageBindingCapabilityTest {
    @Test
    public void publishesCapabilityOnlyForEnabledHybridProvider() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setNereusEnabled(true);
        configuration.setManagedLedgerStorageClassName(NereusManagedLedgerStorage.class.getName());

        Map<String, String> properties = NereusStorageBindingCapability.lookupProperties(
                configuration, Map.of("existing", "value"));

        assertThat(properties).containsEntry("existing", "value");
        assertThat(properties).containsEntry(
                NereusStorageBindingCapability.PROPERTY, NereusStorageBindingCapability.VERSION);
    }

    @Test
    public void doesNotPublishForStockProviderAndRejectsSpoofing() {
        ServiceConfiguration configuration = new ServiceConfiguration();

        assertThat(NereusStorageBindingCapability.lookupProperties(configuration, Map.of())).isEmpty();
        assertThatThrownBy(() -> NereusStorageBindingCapability.lookupProperties(
                configuration, Map.of(NereusStorageBindingCapability.PROPERTY, "1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nereus.storage-binding-protocol is reserved by the broker");
    }
}
