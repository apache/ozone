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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec.RS;
import static org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec.XOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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