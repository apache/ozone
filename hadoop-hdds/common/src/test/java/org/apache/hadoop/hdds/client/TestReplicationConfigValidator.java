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
import org.junit.Test;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;

/**
 * Test ReplicationConfig validator.
 */
public class TestReplicationConfigValidator {

  @Test
  public void testValidation() {
    MutableConfigurationSource config = new InMemoryConfiguration();

    final ReplicationConfigValidator validator =
        config.getObject(ReplicationConfigValidator.class);

    validator.validate(RatisReplicationConfig.getInstance(THREE));
    validator.validate(RatisReplicationConfig.getInstance(ONE));
    validator.validate(new StandaloneReplicationConfig(THREE));
    validator.validate(new StandaloneReplicationConfig(ONE));

  }

  @Test
  public void testWithoutValidation() {
    MutableConfigurationSource config = new InMemoryConfiguration();
    config.set("ozone.replication.allowed-configs", "");

    final ReplicationConfigValidator validator =
        config.getObject(ReplicationConfigValidator.class);

    validator.validate(RatisReplicationConfig.getInstance(THREE));
    validator.validate(new StandaloneReplicationConfig(ONE));

  }

  @Test(expected = IllegalArgumentException.class)
  public void testCustomValidation() {
    MutableConfigurationSource config = new InMemoryConfiguration();
    config.set("ozone.replication.allowed-configs", "RATIS/THREE");

    final ReplicationConfigValidator validator =
        config.getObject(ReplicationConfigValidator.class);

    validator.validate(RatisReplicationConfig.getInstance(THREE));

    validator.validate(RatisReplicationConfig.getInstance(ONE));
    //exception is expected

  }
}