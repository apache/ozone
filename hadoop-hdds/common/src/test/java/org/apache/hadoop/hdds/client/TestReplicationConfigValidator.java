/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.client;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ZERO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test ReplicationConfig validator.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestReplicationConfigValidator {

  private ReplicationConfigValidator defaultValidator;

  private ReplicationConfigValidator disabledValidator;

  @BeforeAll
  void setup() {
    defaultValidator = new InMemoryConfigurationForTesting()
        .getObject(ReplicationConfigValidator.class);

    MutableConfigurationSource disabled = new InMemoryConfigurationForTesting();
    disabled.set("ozone.replication.allowed-configs", "");
    disabledValidator = disabled
        .getObject(ReplicationConfigValidator.class);
  }

  static List<String> validConfigsForEC() {
    List<String> configs = new LinkedList<>();
    for (EcCodec codec : EcCodec.values()) {
      for (String dataParity : Arrays.asList("3-2", "6-3", "10-4")) {
        String[] parts = dataParity.split("-");
        int data = Integer.parseInt(parts[0]);
        int parity = Integer.parseInt(parts[1]);
        for (int chunkSize : Arrays.asList(512, 1024, 2048, 4096)) {
          ReplicationConfig config =
              new ECReplicationConfig(data, parity, codec, chunkSize * 1024);
          configs.add(config.getReplication());
        }
      }
    }
    return configs;
  }

  static String[] invalidConfigsForEC() {
    return new String[]{
        "rs-6-4-1024k", // invalid data-parity
        "xor-3-2-1024", // invalid chunk size
        "rs-6-3-1234k", // invalid chunk size
        // invalid codec is always rejected by ECReplicationConfig
    };
  }

  @Test
  void acceptsRatis() {
    defaultValidator.validate(RatisReplicationConfig.getInstance(THREE));
    defaultValidator.validate(RatisReplicationConfig.getInstance(ONE));
  }

  @Test
  void acceptsStandalone() {
    defaultValidator.validate(StandaloneReplicationConfig.getInstance(THREE));
    defaultValidator.validate(StandaloneReplicationConfig.getInstance(ONE));
  }

  @ParameterizedTest
  @MethodSource("validConfigsForEC")
  void acceptsValidEC(String config) {
    defaultValidator.validate(new ECReplicationConfig(config));
  }

  @ParameterizedTest
  @MethodSource("invalidConfigsForEC")
  void rejectsInvalidEC(String config) {
    assertThrows(IllegalArgumentException.class,
        () -> defaultValidator.validate(new ECReplicationConfig(config)));
  }

  @ParameterizedTest
  @MethodSource("invalidConfigsForEC")
  void disabledAcceptsInvalidEC(String config) {
    disabledValidator.validate(new ECReplicationConfig(config));
  }

  @Test
  void disabledAcceptsRatis() {
    disabledValidator.validate(RatisReplicationConfig.getInstance(ONE));
    disabledValidator.validate(RatisReplicationConfig.getInstance(THREE));
  }

  @Test
  void disabledAcceptsStandalone() {
    disabledValidator.validate(StandaloneReplicationConfig.getInstance(ONE));
    disabledValidator.validate(StandaloneReplicationConfig.getInstance(THREE));
    disabledValidator.validate(StandaloneReplicationConfig.getInstance(ZERO));
  }

  @Test
  void testCustomValidation() {
    MutableConfigurationSource config = new InMemoryConfigurationForTesting();
    config.set("ozone.replication.allowed-configs", "RATIS/THREE");

    final ReplicationConfigValidator validator =
        config.getObject(ReplicationConfigValidator.class);

    validator.validate(RatisReplicationConfig.getInstance(THREE));

    assertThrows(IllegalArgumentException.class,
        () -> validator.validate(RatisReplicationConfig.getInstance(ONE)));

  }

  @Test
  void getReusesValidatorForSamePattern() {
    MutableConfigurationSource config = new InMemoryConfigurationForTesting();
    config.set("ozone.replication.allowed-configs", "RATIS/THREE");
    MutableConfigurationSource sameAgain = new InMemoryConfigurationForTesting();
    sameAgain.set("ozone.replication.allowed-configs", "RATIS/THREE");

    ReplicationConfigValidator validator = ReplicationConfigValidator.get(config);
    // no new validator, so no reflection and no pattern compilation, for the same pattern
    assertSame(validator, ReplicationConfigValidator.get(config));
    assertSame(validator, ReplicationConfigValidator.get(sameAgain));
  }

  @Test
  void getFollowsConfigurationChanges() {
    MutableConfigurationSource config = new InMemoryConfigurationForTesting();
    config.set("ozone.replication.allowed-configs", "RATIS/THREE");
    assertThrows(IllegalArgumentException.class,
        () -> ReplicationConfig.parse(ReplicationType.RATIS, "ONE", config));

    config.set("ozone.replication.allowed-configs", "");
    assertEquals(RatisReplicationConfig.getInstance(ONE),
        ReplicationConfig.parse(ReplicationType.RATIS, "ONE", config));
  }

  @ParameterizedTest
  @MethodSource("invalidConfigsForEC")
  void getUsesDefaultPattern(String invalid) {
    ReplicationConfigValidator validator = ReplicationConfigValidator.get(new InMemoryConfigurationForTesting());

    validator.validate(RatisReplicationConfig.getInstance(THREE));
    validConfigsForEC().forEach(valid -> validator.validate(new ECReplicationConfig(valid)));
    assertThrows(IllegalArgumentException.class, () -> validator.validate(new ECReplicationConfig(invalid)));
  }

  /** Callers with different patterns keep replacing the one cached validator, and each must still get its own. */
  @Test
  void getIsSafeWithConcurrentPatterns() throws Exception {
    MutableConfigurationSource restricted = new InMemoryConfigurationForTesting();
    restricted.set("ozone.replication.allowed-configs", "RATIS/THREE");
    MutableConfigurationSource unrestricted = new InMemoryConfigurationForTesting();
    unrestricted.set("ozone.replication.allowed-configs", "");

    ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < 8; t++) {
        final boolean restrict = t % 2 == 0;
        futures.add(pool.submit(() -> {
          for (int i = 0; i < 1000; i++) {
            if (restrict) {
              assertThrows(IllegalArgumentException.class,
                  () -> ReplicationConfig.parse(ReplicationType.RATIS, "ONE", restricted));
            } else {
              ReplicationConfig.parse(ReplicationType.RATIS, "ONE", unrestricted);
            }
          }
          return null;
        }));
      }
      for (Future<?> f : futures) {
        f.get(60, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
  }
}
