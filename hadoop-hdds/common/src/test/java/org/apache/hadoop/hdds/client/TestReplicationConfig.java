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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Test replicationConfig.
 */
@RunWith(Parameterized.class)
public class TestReplicationConfig {

  @SuppressWarnings("checkstyle:VisibilityModifier")
  @Parameterized.Parameter()
  public String type;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  @Parameterized.Parameter(1)
  public String factor;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  @Parameterized.Parameter(2)
  public Class<?> replicationConfigClass;

  @Parameterized.Parameters(name = "{0}/{1}")
  public static Object[][] parameters() {
    return new Object[][] {
        {"RATIS", "ONE", RatisReplicationConfig.class },
        {"RATIS", "THREE", RatisReplicationConfig.class},
        {"STAND_ALONE", "ONE", StandaloneReplicationConfig.class},
        {"STAND_ALONE", "THREE", StandaloneReplicationConfig.class}
    };
  }

  @Test
  public void testGetDefaultShouldCreateReplicationConfigFromDefaultConf() {
    OzoneConfiguration conf = new OzoneConfiguration();

    ReplicationConfig replicationConfig = ReplicationConfig.getDefault(conf);

    validate(replicationConfig,
        org.apache.hadoop.hdds.client.ReplicationType.RATIS,
        org.apache.hadoop.hdds.client.ReplicationFactor.THREE,
        RatisReplicationConfig.class);
  }

  @Test
  public void testGetDefaultShouldCreateReplicationConfFromCustomConfValues() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_REPLICATION_TYPE, type);
    conf.set(OZONE_REPLICATION, factor);

    ReplicationConfig replicationConfig = ReplicationConfig.getDefault(conf);

    validate(replicationConfig,
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        org.apache.hadoop.hdds.client.ReplicationFactor.valueOf(factor));
  }

  @Test
  public void deserialize() {
    final ReplicationConfig replicationConfig =
        ReplicationConfig.fromProtoTypeAndFactor(
            ReplicationType.valueOf(type),
            ReplicationFactor.valueOf(factor));

    validate(replicationConfig,
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        org.apache.hadoop.hdds.client.ReplicationFactor.valueOf(factor));
  }

  @Test
  public void fromJavaObjects() {
    final ReplicationConfig replicationConfig =
        ReplicationConfig.fromTypeAndFactor(
            org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
            org.apache.hadoop.hdds.client.ReplicationFactor.valueOf(factor));

    validate(replicationConfig,
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        org.apache.hadoop.hdds.client.ReplicationFactor.valueOf(factor));
  }

  @Test
  public void testParseFromTypeAndFactorAsString() {
    ConfigurationSource conf = new OzoneConfiguration();
    ReplicationConfig replicationConfig = ReplicationConfig.parse(
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        factor, conf);

    validate(replicationConfig,
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        org.apache.hadoop.hdds.client.ReplicationFactor.valueOf(factor));
  }

  @Test
  public void testParseFromTypeAndFactorAsStringifiedInteger() {
    ConfigurationSource conf = new OzoneConfiguration();
    String f =
        factor == "ONE" ? "1"
            : factor == "THREE" ? "3"
            : "Test adjustment needed!";

    ReplicationConfig replicationConfig = ReplicationConfig.parse(
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        f, conf);

    validate(replicationConfig,
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        org.apache.hadoop.hdds.client.ReplicationFactor.valueOf(this.factor));
  }

  @Test
  public void testAdjustReplication() {
    ConfigurationSource conf = new OzoneConfiguration();
    ReplicationConfig replicationConfig = ReplicationConfig.parse(
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        factor, conf);

    validate(
        ReplicationConfig.adjustReplication(replicationConfig, (short) 3, conf),
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        org.apache.hadoop.hdds.client.ReplicationFactor.THREE);

    validate(
        ReplicationConfig.adjustReplication(replicationConfig, (short) 1, conf),
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        org.apache.hadoop.hdds.client.ReplicationFactor.ONE);
  }

  /**
   * This is a bit of a tricky test in the parametrized environment.
   * The goal is to ensure that the following methods do validation while
   * creating the ReplicationConfig: getDefault, adjustReplication, parse.
   *
   * Two other creator methods fromProtoTypeAndFactor, and fromTypeAndFactor
   * should allow creation of disallowed ReplicationConfigs as well, as in the
   * system there might exist some keys that were created with a now disallowed
   * ReplicationConfig.
   */
  @Test
  public void testValidationBasedOnConfig() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_REPLICATION + ".allowed-configs",
        "^STANDALONE/ONE|RATIS/THREE$");
    conf.set(OZONE_REPLICATION, factor);
    conf.set(OZONE_REPLICATION_TYPE, type);

    if ((type.equals("RATIS") && factor.equals("THREE"))
        || (type.equals("STAND_ALONE") && factor.equals("ONE"))) {
      ReplicationConfig replicationConfig = ReplicationConfig.parse(
          org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
          factor, conf);
      if (type.equals("RATIS")) {
        assertThrows(IllegalArgumentException.class,
            () -> ReplicationConfig
                .adjustReplication(replicationConfig, (short) 1, conf));
      } else {
        assertThrows(IllegalArgumentException.class,
            () -> ReplicationConfig
                .adjustReplication(replicationConfig, (short) 3, conf));
      }
      ReplicationConfig.fromTypeAndFactor(
          org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
          org.apache.hadoop.hdds.client.ReplicationFactor.valueOf(factor));
      ReplicationConfig.fromProtoTypeAndFactor(
          ReplicationType.valueOf(type), ReplicationFactor.valueOf(factor));
      ReplicationConfig.getDefault(conf);
    } else {
      assertThrows(IllegalArgumentException.class,
          () -> ReplicationConfig.parse(
              org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
              factor, conf));
      assertThrows(IllegalArgumentException.class,
          () -> ReplicationConfig.getDefault(conf));
    }
    // CHAINED replication type is not supported by ReplicationConfig.
    assertThrows(RuntimeException.class,
        () -> ReplicationConfig.parse(
            org.apache.hadoop.hdds.client.ReplicationType.CHAINED, "", conf));
  }


  private void validate(ReplicationConfig replicationConfig,
      org.apache.hadoop.hdds.client.ReplicationType expectedType,
      org.apache.hadoop.hdds.client.ReplicationFactor expectedFactor) {

    validate(replicationConfig, expectedType, expectedFactor,
        replicationConfigClass);
  }


  private void validate(ReplicationConfig replicationConfig,
      org.apache.hadoop.hdds.client.ReplicationType expectedType,
      org.apache.hadoop.hdds.client.ReplicationFactor expectedFactor,
      Class<?> expectedReplicationConfigClass) {

    assertEquals(expectedReplicationConfigClass, replicationConfig.getClass());

    assertEquals(
        expectedType.name(), replicationConfig.getReplicationType().name());
    assertEquals(
        expectedFactor.getValue(), replicationConfig.getRequiredNodes());
    assertEquals(
        expectedFactor.name(),
        ((ReplicatedReplicationConfig) replicationConfig)
            .getReplicationFactor().name());
  }
}
