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
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec.RS;
import static org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec.XOR;
import static org.junit.Assert.fail;

/**
 * Unit test for ECReplicationConfig.
 */
public class TestECReplicationConfig {

  @Test
  public void testSuccessfulStringParsing() {
    Map<String, ECReplicationConfig> valid = new HashMap();
    valid.put("rs-3-2-1024", new ECReplicationConfig(3, 2, RS, 1024));
    valid.put("RS-3-2-1024", new ECReplicationConfig(3, 2, RS, 1024));
    valid.put("rs-3-2-1024k", new ECReplicationConfig(3, 2, RS, 1024 * 1024));
    valid.put("rs-3-2-1024K", new ECReplicationConfig(3, 2, RS, 1024 * 1024));
    valid.put("xor-10-4-1", new ECReplicationConfig(10, 4, XOR, 1));
    valid.put("XOR-6-3-12345", new ECReplicationConfig(6, 3, XOR, 12345));

    for (Map.Entry<String, ECReplicationConfig> e : valid.entrySet()) {
      ECReplicationConfig ec = new ECReplicationConfig(e.getKey());
      Assert.assertEquals(e.getValue().getData(), ec.getData());
      Assert.assertEquals(e.getValue().getParity(), ec.getParity());
      Assert.assertEquals(e.getValue().getCodec(), ec.getCodec());
      Assert.assertEquals(e.getValue().getStripeSize(), ec.getStripeSize());
    }
  }

  @Test
  public void testUnsuccessfulStringParsing() {
    String[] invalid = {
        "3-2-1024",
        "rss-3-2-1024",
        "rs-3-0-1024",
        "rs-3-2-0k",
        "rs-3-2",
        "x3-2"
    };
    for (String s : invalid) {
      try {
        new ECReplicationConfig(s);
        fail(s + " should not parse correctly");
      } catch (IllegalArgumentException e) {
        // ignore, this expected
      }
    }
  }


  @Test
  public void testSerializeToProtoAndBack() {
    ECReplicationConfig orig = new ECReplicationConfig(6, 3,
        ECReplicationConfig.EcCodec.XOR, 1024);

    HddsProtos.ECReplicationConfig proto = orig.toProto();

    ECReplicationConfig recovered = new ECReplicationConfig(proto);
    Assert.assertEquals(orig.getData(), recovered.getData());
    Assert.assertEquals(orig.getParity(), recovered.getParity());
    Assert.assertEquals(orig.getCodec(), recovered.getCodec());
    Assert.assertEquals(orig.getStripeSize(), recovered.getStripeSize());
    Assert.assertTrue(orig.equals(recovered));
  }

}