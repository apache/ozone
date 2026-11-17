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

import static org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec.RS;
import static org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec.XOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit test for ECReplicationConfig.
 */
class TestECReplicationConfig {

  @BeforeEach
  void clearCache() {
    ECReplicationConfig.clearCache();
  }

  @Test
  void testSuccessfulStringParsing() {
    Map<String, ECReplicationConfig> valid = new HashMap();
    valid.put("rs-3-2-1024", new ECReplicationConfig(3, 2, RS, 1024));
    valid.put("RS-3-2-1024", new ECReplicationConfig(3, 2, RS, 1024));
    valid.put("rs-3-2-1024k", new ECReplicationConfig(3, 2, RS, 1024 * 1024));
    valid.put("rs-3-2-1024K", new ECReplicationConfig(3, 2, RS, 1024 * 1024));
    valid.put("xor-10-4-1", new ECReplicationConfig(10, 4, XOR, 1));
    valid.put("XOR-6-3-12345", new ECReplicationConfig(6, 3, XOR, 12345));

    for (Map.Entry<String, ECReplicationConfig> e : valid.entrySet()) {
      ECReplicationConfig ec = new ECReplicationConfig(e.getKey());
      assertEquals(e.getValue().getData(), ec.getData());
      assertEquals(e.getValue().getParity(), ec.getParity());
      assertEquals(e.getValue().getCodec(), ec.getCodec());
      assertEquals(e.getValue().getEcChunkSize(), ec.getEcChunkSize());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"3-2-1024", "rss-3-2-1024", "rs-3-0-1024",
      "rs-3-2-0k", "rs-3-2", "x3-2"})
  void testUnsuccessfulStringParsing(String invalidValue) {
    assertThrows(IllegalArgumentException.class,
            () -> new ECReplicationConfig(invalidValue));
    assertThrows(IllegalArgumentException.class, () -> ECReplicationConfig.getInstance(invalidValue));
  }

  @Test
  void testGetInstanceReusesInstance() {
    ECReplicationConfig config = ECReplicationConfig.getInstance("rs-6-3-1024k");
    assertEquals(new ECReplicationConfig(6, 3, RS, 1024 * 1024), config);

    assertSame(config, ECReplicationConfig.getInstance("RS-6-3-1024K"));
    assertSame(config, ECReplicationConfig.getInstance(config.toProto()));
    assertSame(config, ReplicationConfig.fromProto(HddsProtos.ReplicationType.EC, null, config.toProto()));
    assertSame(config, ReplicationConfig.parse(ReplicationType.EC, "rs-6-3-1024k", new OzoneConfiguration()));

    assertNotSame(config, ECReplicationConfig.getInstance("rs-6-3-2048k"));
  }

  /** Any config is shared, not only the ones allowed by default, whichever form it is decoded from. */
  @ParameterizedTest
  @ValueSource(strings = {"rs-3-2-1024k", "rs-6-3-1024k", "rs-10-4-4096k", "rs-4-2-1024k", "xor-3-1-512k"})
  void testGetInstanceSharesAnyConfig(String string) {
    final ECReplicationConfig config = ECReplicationConfig.getInstance(string);
    assertEquals(new ECReplicationConfig(string), config);

    assertSame(config, ECReplicationConfig.getInstance(string));
    assertSame(config, ECReplicationConfig.getInstance(string.toUpperCase()));
    assertSame(config, ECReplicationConfig.getInstance(config.getCodec() + "-" + config.getData() + "-"
        + config.getParity() + "-" + config.getEcChunkSize()));
    assertSame(config, ECReplicationConfig.getInstance(config.toProto()));
    assertSame(config, ECReplicationConfig.getInstance(config.toProto().toBuilder()
        .setCodec(config.getCodec().name()).build()));
  }

  /** The constructor still creates a new object, also for a shared config. */
  @ParameterizedTest
  @ValueSource(strings = {"rs-6-3-1024k", "RS-6-3-1024K", "rs-4-2-1024k"})
  void testConstructorCreatesNewInstance(String string) {
    final ECReplicationConfig config = new ECReplicationConfig(string);
    assertEquals(ECReplicationConfig.getInstance(string), config);
    assertNotSame(ECReplicationConfig.getInstance(string), config);
    assertNotSame(new ECReplicationConfig(string), config);
  }

  /** The cache is bounded, so a client sending arbitrary configs cannot grow it without limit. */
  @Test
  void testGetInstanceIsBounded() {
    final ECReplicationConfig cachedFromString = ECReplicationConfig.getInstance("rs-6-3-1024k");
    final ECReplicationConfig cachedFromProto = ECReplicationConfig.getInstance(new ECReplicationConfig(3, 2)
        .toProto());
    for (int data = 1; data <= 2 * ECReplicationConfig.MAX_CACHED; data++) {
      final String string = "rs-" + data + "-1-1024k";
      assertEquals(new ECReplicationConfig(string), ECReplicationConfig.getInstance(string));
    }

    // a new config is still decoded, but not shared any more
    final String string = "rs-6-1-8192k";
    final ECReplicationConfig expected = new ECReplicationConfig(string);
    final ECReplicationConfig fromString = ECReplicationConfig.getInstance(string);
    assertEquals(expected, fromString);
    assertNotSame(fromString, ECReplicationConfig.getInstance(string));
    final ECReplicationConfig fromProto = ECReplicationConfig.getInstance(expected.toProto());
    assertEquals(expected, fromProto);
    assertNotSame(fromProto, ECReplicationConfig.getInstance(expected.toProto()));

    // the configs cached before are still shared
    assertSame(cachedFromString, ECReplicationConfig.getInstance("rs-6-3-1024k"));
    assertSame(cachedFromString, ECReplicationConfig.getInstance(cachedFromString.toProto()));
    assertSame(cachedFromProto, ECReplicationConfig.getInstance(cachedFromProto.toProto()));
    assertSame(cachedFromProto, ECReplicationConfig.getInstance("rs-3-2-1024k"));
  }

  @Test
  void testGetInstanceRejectsUnknownCodec() {
    final HddsProtos.ECReplicationConfig proto = new ECReplicationConfig(6, 3).toProto().toBuilder()
        .setCodec("foo").build();
    assertThrows(IllegalArgumentException.class, () -> new ECReplicationConfig(proto));
    assertThrows(IllegalArgumentException.class, () -> ECReplicationConfig.getInstance(proto));
  }

  @Test
  void testSerializeToProtoAndBack() {
    ECReplicationConfig orig = new ECReplicationConfig(6, 3,
        ECReplicationConfig.EcCodec.XOR, 1024);

    HddsProtos.ECReplicationConfig proto = orig.toProto();

    ECReplicationConfig recovered = new ECReplicationConfig(proto);
    assertEquals(orig.getData(), recovered.getData());
    assertEquals(orig.getParity(), recovered.getParity());
    assertEquals(orig.getCodec(), recovered.getCodec());
    assertEquals(orig.getEcChunkSize(), recovered.getEcChunkSize());
    assertEquals(orig, recovered);
  }

}
