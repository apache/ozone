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

package org.apache.hadoop.hdds.utils;

import static org.apache.hadoop.ozone.util.ProtobufUtils.fromProtobuf;
import static org.apache.hadoop.ozone.util.ProtobufUtils.toProtobuf;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.junit.jupiter.api.Test;

/**
 * Test-cases for {@link ProtobufUtils}.
 */
public class TestProtobufUtils {
  @Test
  public void testUuidToProtobuf() {
    UUID object = UUID.randomUUID();
    HddsProtos.UUID protobuf = toProtobuf(object);
    assertEquals(object.getLeastSignificantBits(), protobuf.getLeastSigBits());
    assertEquals(object.getMostSignificantBits(), protobuf.getMostSigBits());
  }

  @Test
  public void testUuidConversion() {
    UUID original = UUID.randomUUID();
    HddsProtos.UUID protobuf = toProtobuf(original);
    UUID deserialized = fromProtobuf(protobuf);
    assertEquals(original, deserialized);
  }
}
