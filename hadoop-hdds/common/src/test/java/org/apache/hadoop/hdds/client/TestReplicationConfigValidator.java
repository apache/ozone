/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.client;

import org.apache.hadoop.hdds.conf.InMemoryConfiguration;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test ReplicationConfig validator.
 */
class TestReplicationConfigValidator {

  @Test
  void testValidation() {
    MutableConfigurationSource config = new InMemoryConfiguration();

    final ReplicationConfigValidator validator =
        config.getObject(ReplicationConfigValidator.class);
    String ecConfig1 = "rs-3-2-1024k";
    String ecConfig2 = "xor-6-3-2048k";
    //Supported data-parity are 3-2,6-3,10-4
    String invalidEcConfig1 = "xor-6-4-1024k";

    validator.validate(RatisReplicationConfig.getInstance(THREE));
    validator.validate(RatisReplicationConfig.getInstance(ONE));
    validator.validate(StandaloneReplicationConfig.getInstance(THREE));
    validator.validate(StandaloneReplicationConfig.getInstance(ONE));
    validator.validate(new ECReplicationConfig(ecConfig1));
    validator.validate(new ECReplicationConfig(ecConfig2));
    try {
      validator.validate(new ECReplicationConfig(invalidEcConfig1));
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains(
              "Invalid data-parity replication " +
          "config for type EC and replication xor-6-4-{CHUNK_SIZE}. " +
                      "Supported data-parity are 3-2,6-3,10-4", ex);
    }

  }

  @Test
  void testWithoutValidation() {
    MutableConfigurationSource config = new InMemoryConfiguration();
    config.set("ozone.replication.allowed-configs", "");

    final ReplicationConfigValidator validator =
        config.getObject(ReplicationConfigValidator.class);

    validator.validate(RatisReplicationConfig.getInstance(THREE));
    validator.validate(StandaloneReplicationConfig.getInstance(ONE));

  }

  @Test
  void testCustomValidation() {
    MutableConfigurationSource config = new InMemoryConfiguration();
    config.set("ozone.replication.allowed-configs", "RATIS/THREE");

    final ReplicationConfigValidator validator =
        config.getObject(ReplicationConfigValidator.class);

    validator.validate(RatisReplicationConfig.getInstance(THREE));

    assertThrows(IllegalArgumentException.class,
        () -> validator.validate(RatisReplicationConfig.getInstance(ONE)));

  }
}
