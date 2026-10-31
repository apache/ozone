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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit test for ECReplicationConfig.
 */
class TestECReplicationConfig {

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

  /** Callers racing to cache the same config must all get the one instance that won. */
  @Test
  void testGetInstanceConcurrently() throws Exception {
    final int threads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      // a new config each round, so that every round races to add it; stays well below MAX_CACHED
      for (int round = 1; round <= 32; round++) {
        final ECReplicationConfig expected = new ECReplicationConfig(100 + round, 4);
        CyclicBarrier start = new CyclicBarrier(threads);
        List<Future<ECReplicationConfig>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
          futures.add(pool.submit(() -> {
            start.await();
            return ECReplicationConfig.getInstance(expected.toProto());
          }));
        }
        ECReplicationConfig first = futures.get(0).get(10, TimeUnit.SECONDS);
        assertEquals(expected, first);
        for (Future<ECReplicationConfig> f : futures) {
          assertSame(first, f.get(10, TimeUnit.SECONDS));
        }
      }
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void testHashCodeMatchesObjectsHash() {
    for (ECReplicationConfig c : new ECReplicationConfig[] {new ECReplicationConfig(3, 2),
        new ECReplicationConfig(10, 4, XOR, 4096 * 1024), new ECReplicationConfig(6, 3, RS, 12345)}) {
      assertEquals(Objects.hash(c.getData(), c.getParity(), c.getCodec(), c.getEcChunkSize()), c.hashCode());
    }
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
