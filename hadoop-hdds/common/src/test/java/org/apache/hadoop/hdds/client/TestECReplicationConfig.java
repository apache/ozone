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

/**
 * Unit test for ECReplicationConfig.
 */
public class TestECReplicationConfig {

  @Test
  public void testStringParsing() {
    final ECReplicationConfig ec = new ECReplicationConfig("3-2");
    Assert.assertEquals(ec.getData(), 3);
    Assert.assertEquals(ec.getParity(), 2);
  }


  @Test(expected = IllegalArgumentException.class)
  public void testStringParsingWithString() {
    new ECReplicationConfig("x3-2");
  }


  @Test(expected = IllegalArgumentException.class)
  public void testStringParsingWithZero() {
    new ECReplicationConfig("3-0");
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