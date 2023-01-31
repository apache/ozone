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

import org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Test replicationConfig.
 */
public class TestReplicationConfig {

  private static final int MB = 1024 * 1024;
  private static final int KB = 1024;

  //NOTE: if a new chunkSize is used/added in the parameters other than KB or MB
  // please revisit the method createECDescriptor, to handle the new chunkSize.
  public static Stream<Arguments> replicaType() {
    return Stream.of(
        arguments("RATIS", "ONE", RatisReplicationConfig.class),
        arguments("RATIS", "THREE", RatisReplicationConfig.class),
        arguments("STAND_ALONE", "ONE", StandaloneReplicationConfig.class),
        arguments("STAND_ALONE", "THREE", StandaloneReplicationConfig.class)
    );
  }

  public static Stream<Arguments> ecType() {
    return Stream.of(
        arguments("RS", 3, 2, MB),
        arguments("RS", 3, 2, KB),
        arguments("RS", 6, 3, MB),
        arguments("RS", 6, 3, KB),
        arguments("RS", 10, 4, MB),
        arguments("RS", 10, 4, KB)
    );
  }

  @Test
  void testGetDefaultShouldReturnRatisThreeIfNotSetClientSide() {
    OzoneConfiguration conf = new OzoneConfiguration();

    ReplicationConfig replicationConfig = ReplicationConfig.getDefault(conf);
    validate(replicationConfig,
        org.apache.hadoop.hdds.client.ReplicationType.RATIS,
        org.apache.hadoop.hdds.client.ReplicationFactor.THREE,
        RatisReplicationConfig.class);
  }

  @ParameterizedTest
  @MethodSource("replicaType")
  void testGetDefaultShouldCreateReplicationConfFromConfValues(
      String type, String factor, Class<?> replicationConfigClass) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_REPLICATION_TYPE, type);
    conf.set(OZONE_REPLICATION, factor);

    ReplicationConfig replicationConfig = ReplicationConfig.getDefault(conf);

    validate(replicationConfig,
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        org.apache.hadoop.hdds.client.ReplicationFactor.valueOf(factor),
        replicationConfigClass);
  }

  @ParameterizedTest
  @MethodSource("ecType")
  void testGetDefaultShouldCreateECReplicationConfFromConfValues(
      String codec, int data, int parity, int chunkSize) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_REPLICATION_TYPE, "EC");
    conf.set(OZONE_REPLICATION, ecDescriptor(codec, data, parity, chunkSize));

    ReplicationConfig replicationConfig = ReplicationConfig.getDefault(conf);

    validate(replicationConfig,
        EcCodec.valueOf(codec), data, parity, chunkSize);
  }

  @ParameterizedTest
  @MethodSource("replicaType")
  void deserialize(String type, String factor,
      Class<?> replicationConfigClass) {
    final ReplicationConfig replicationConfig =
        ReplicationConfig.fromProtoTypeAndFactor(
            ReplicationType.valueOf(type),
            ReplicationFactor.valueOf(factor));

    validate(replicationConfig,
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        org.apache.hadoop.hdds.client.ReplicationFactor.valueOf(factor),
        replicationConfigClass);
  }

  @ParameterizedTest
  @MethodSource("ecType")
  void deserializeEC(String codec, int data, int parity, int chunkSize) {
    HddsProtos.ECReplicationConfig proto =
        HddsProtos.ECReplicationConfig.newBuilder()
            .setCodec(codec)
            .setData(data)
            .setParity(parity)
            .setEcChunkSize(chunkSize)
            .build();

    ReplicationConfig config = ReplicationConfig
        .fromProto(ReplicationType.EC, null, proto);

    validate(config, EcCodec.valueOf(codec), data, parity, chunkSize);
  }

  @ParameterizedTest
  @MethodSource("ecType")
  void testECReplicationConfigGetReplication(
      String codec, int data, int parity, int chunkSize) {
    HddsProtos.ECReplicationConfig proto =
        HddsProtos.ECReplicationConfig.newBuilder().setCodec(codec)
            .setData(data).setParity(parity).setEcChunkSize(chunkSize).build();

    ReplicationConfig config =
        ReplicationConfig.fromProto(ReplicationType.EC, null, proto);

    assertEquals(EcCodec.valueOf(
        codec) + ECReplicationConfig.EC_REPLICATION_PARAMS_DELIMITER
            + data + ECReplicationConfig.EC_REPLICATION_PARAMS_DELIMITER
            + parity + ECReplicationConfig.EC_REPLICATION_PARAMS_DELIMITER
            + chunkSize, config.getReplication());
  }

  @ParameterizedTest
  @MethodSource("replicaType")
  void testReplicationConfigGetReplication(String type, String factor,
      Class<?> replicationConfigClass) {
    final ReplicationConfig replicationConfig = ReplicationConfig
        .fromTypeAndFactor(
            org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
            org.apache.hadoop.hdds.client.ReplicationFactor.valueOf(factor));

    assertEquals(factor, replicationConfig.getReplication());
  }

  @ParameterizedTest
  @MethodSource("replicaType")
  void fromJavaObjects(String type, String factor,
      Class<?> replicationConfigClass) {
    final ReplicationConfig replicationConfig =
        ReplicationConfig.fromTypeAndFactor(
            org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
            org.apache.hadoop.hdds.client.ReplicationFactor.valueOf(factor));

    validate(replicationConfig,
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        org.apache.hadoop.hdds.client.ReplicationFactor.valueOf(factor),
        replicationConfigClass);
  }

  @ParameterizedTest
  @MethodSource("replicaType")
  void testParseFromTypeAndFactorAsString(String type, String factor,
      Class<?> replicationConfigClass) {
    ConfigurationSource conf = new OzoneConfiguration();
    ReplicationConfig replicationConfig = ReplicationConfig.parse(
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        factor, conf);

    validate(replicationConfig,
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        org.apache.hadoop.hdds.client.ReplicationFactor.valueOf(factor),
        replicationConfigClass);
  }

  @ParameterizedTest
  @MethodSource("replicaType")
  void testParseFromTypeAndFactorAsStringifiedInteger(
      String type, String factor, Class<?> replicationConfigClass) {
    ConfigurationSource conf = new OzoneConfiguration();
    String f =
        factor.equals("ONE") ? "1"
            : factor.equals("THREE") ? "3"
            : "Test adjustment needed!";

    ReplicationConfig replicationConfig = ReplicationConfig.parse(
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        f, conf);

    validate(replicationConfig,
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        org.apache.hadoop.hdds.client.ReplicationFactor.valueOf(factor),
        replicationConfigClass);
  }

  @ParameterizedTest
  @MethodSource("ecType")
  void testParseECReplicationConfigFromString(
      String codec, int data, int parity, int chunkSize) {

    ConfigurationSource conf = new OzoneConfiguration();
    ReplicationConfig repConfig = ReplicationConfig.parse(
        org.apache.hadoop.hdds.client.ReplicationType.EC,
        ecDescriptor(codec, data, parity, chunkSize), conf);

    validate(repConfig, EcCodec.valueOf(codec), data, parity, chunkSize);
  }

  /**
   * The adjustReplication is a method that is used by RootedOzoneFileSystem
   * to adjust the bucket's default replication config if needed.
   *
   * As we define, if the bucket's default replication configuration is RATIS
   * or STAND_ALONE, then replica count can be adjusted with the replication
   * factor.
   */
  @ParameterizedTest
  @MethodSource("replicaType")
  void testAdjustReplication(String type, String factor,
      Class<?> replicationConfigClass) {
    ConfigurationSource conf = new OzoneConfiguration();
    ReplicationConfig replicationConfig = ReplicationConfig.parse(
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        factor, conf);

    validate(
        ReplicationConfig.adjustReplication(replicationConfig, (short) 3, conf),
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        org.apache.hadoop.hdds.client.ReplicationFactor.THREE,
        replicationConfigClass);

    validate(
        ReplicationConfig.adjustReplication(replicationConfig, (short) 1, conf),
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        org.apache.hadoop.hdds.client.ReplicationFactor.ONE,
        replicationConfigClass);
  }

  /**
   * The adjustReplication is a method that is used by RootedOzoneFileSystem
   * to adjust the bucket's default replication config if needed.
   *
   * As we define, if the bucket's default replication configuration is EC,
   * then the client can not adjust the configuration via the replication
   * factor.
   */
  @ParameterizedTest
  @MethodSource("ecType")
  void testAdjustECReplication(String codec, int data, int parity,
      int chunkSize) {
    ConfigurationSource conf = new OzoneConfiguration();
    ReplicationConfig replicationConfig = ReplicationConfig.parse(
        org.apache.hadoop.hdds.client.ReplicationType.EC,
        ecDescriptor(codec, data, parity, chunkSize), conf);

    validate(
        ReplicationConfig.adjustReplication(replicationConfig, (short) 3, conf),
        EcCodec.valueOf(codec), data, parity, chunkSize);

    validate(
        ReplicationConfig.adjustReplication(replicationConfig, (short) 1, conf),
        EcCodec.valueOf(codec), data, parity, chunkSize);
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
  @ParameterizedTest
  @MethodSource("replicaType")
  void testValidationBasedOnConfig(String type, String factor,
      Class<?> replicationConfigClass) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_REPLICATION + ".allowed-configs",
        "^STANDALONE/ONE|RATIS/THREE$");
    conf.set(OZONE_REPLICATION, factor);
    conf.set(OZONE_REPLICATION_TYPE, type);
    org.apache.hadoop.hdds.client.ReplicationType replicationType =
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type);

    // in case of allowed configurations
    if ((type.equals("RATIS") && factor.equals("THREE"))
        || (type.equals("STAND_ALONE") && factor.equals("ONE"))) {
      ReplicationConfig replicationConfig = ReplicationConfig.parse(
              replicationType, factor, conf);
      // check if adjust throws exception when adjusting to a config that is
      // not allowed
      if (type.equals("RATIS")) {
        assertThrows(IllegalArgumentException.class,
            () -> ReplicationConfig
                .adjustReplication(replicationConfig, (short) 1, conf));
      } else {
        assertThrows(IllegalArgumentException.class,
            () -> ReplicationConfig
                .adjustReplication(replicationConfig, (short) 3, conf));
      }
      ReplicationConfig.getDefault(conf);
    } else {
      // parse should fail in case of a configuration that is not allowed.
      assertThrows(IllegalArgumentException.class,
          () -> ReplicationConfig.parse(
                  replicationType, factor, conf));
      // default can not be a configuration that is not allowed.
      assertThrows(IllegalArgumentException.class,
          () -> ReplicationConfig.getDefault(conf));
    }

    // From proto and java objects, we need to be able to create replication
    // configs even though they are not allowed, as there might have been
    // keys, that were created earlier when the config was allowed.
    ReplicationConfig.fromTypeAndFactor(
        org.apache.hadoop.hdds.client.ReplicationType.valueOf(type),
        org.apache.hadoop.hdds.client.ReplicationFactor.valueOf(factor));
    ReplicationConfig.fromProtoTypeAndFactor(
        ReplicationType.valueOf(type), ReplicationFactor.valueOf(factor));

    // CHAINED replication type is not supported by ReplicationConfig.
    assertThrows(RuntimeException.class,
        () -> ReplicationConfig.parse(
            org.apache.hadoop.hdds.client.ReplicationType.CHAINED, "", conf));
  }

  private void validate(ReplicationConfig replicationConfig,
      org.apache.hadoop.hdds.client.ReplicationType expectedType,
      org.apache.hadoop.hdds.client.ReplicationFactor expectedFactor,
      Class<?> expectedReplicationConfigClass) {

    assertSame(expectedReplicationConfigClass, replicationConfig.getClass());

    assertEquals(expectedType.name(),
        replicationConfig.getReplicationType().name());
    assertEquals(expectedFactor.getValue(),
        replicationConfig.getRequiredNodes());
    assertEquals(expectedFactor.name(),
        ((ReplicatedReplicationConfig) replicationConfig)
            .getReplicationFactor().name());
  }

  private void validate(ReplicationConfig replicationConfig,
      EcCodec expectedCodec,
      int expectedData, int expectedParity, int expectedChunkSize) {

    assertSame(ECReplicationConfig.class, replicationConfig.getClass());
    assertEquals(ReplicationType.EC, replicationConfig.getReplicationType());

    ECReplicationConfig ecReplicationConfig =
        (ECReplicationConfig) replicationConfig;

    assertEquals(expectedCodec, ecReplicationConfig.getCodec());
    assertEquals(expectedData, ecReplicationConfig.getData());
    assertEquals(expectedParity, ecReplicationConfig.getParity());
    assertEquals(expectedChunkSize, ecReplicationConfig.getEcChunkSize());

    assertEquals(expectedData + expectedParity,
        replicationConfig.getRequiredNodes());
  }

  private String ecDescriptor(String codec, int data, int parity,
      int chunkSize) {
    return codec.toUpperCase() + "-" + data + "-" + parity + "-" +
        (chunkSize == MB ? "1024K" : "1024");
  }

}
