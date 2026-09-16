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

package org.apache.hadoop.hdds.scm.ha.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ScmListCodec}.
 */
public class TestScmListCodec {

  @Test
  public void testListDecodeMissingTypeShouldFail() throws Exception {
    // ListArgument without type
    SCMRatisProtocol.ListArgument listArg =
        SCMRatisProtocol.ListArgument.newBuilder()
            // no type
            .addValue(ByteString.copyFromUtf8("x"))
            .build();

    ScmListCodec codec = new ScmListCodec(
        new ScmCodecFactory.ClassResolver(Collections.emptyList()));

    InvalidProtocolBufferException ex = assertThrows(
        InvalidProtocolBufferException.class,
        () -> codec.deserialize(listArg.toByteString()));

    assertTrue(ex.getMessage().contains("Missing ListArgument.type"));
  }

  /**
   * An empty list serialized with the Object.class sentinel must round-trip
   * cleanly without triggering "Failed to resolve java.lang.Object".
   */
  @Test
  public void testEmptyListRoundTrip() throws Exception {
    ScmListCodec codec = new ScmListCodec(
        new ScmCodecFactory.ClassResolver(Collections.emptyList()));

    List<?> result = (List<?>) codec.deserialize(codec.serialize(new ArrayList<>()));

    assertEquals(0, result.size());
  }

  /**
   * The EMPTY_LIST sentinel (type=java.lang.Object, no values) stored in an
   * existing Ratis log must deserialize successfully.
   */
  @Test
  public void testEmptyListSentinelDeserialization() throws Exception {
    SCMRatisProtocol.ListArgument sentinel =
        SCMRatisProtocol.ListArgument.newBuilder()
            .setType(Object.class.getName())
            // no values
            .build();

    ScmListCodec codec = new ScmListCodec(
        new ScmCodecFactory.ClassResolver(Collections.emptyList()));

    List<?> result = (List<?>) codec.deserialize(sentinel.toByteString());

    assertEquals(0, result.size());
  }

  /**
   * Deserialized empty lists must be concrete {@link ArrayList} instances.
   * Generated invokers (e.g. DeletedBlockLogStateManagerInvoker) cast the
   * decoded argument directly to {@code ArrayList}; returning an unmodifiable
   * or fixed-size list would cause a ClassCastException during Ratis log
   * replay even though the list is logically empty.
   */
  @Test
  public void testEmptyListDeserializedAsArrayList() throws Exception {
    ScmListCodec codec = new ScmListCodec(
        new ScmCodecFactory.ClassResolver(Collections.emptyList()));

    // Round-trip path: serialize an empty list then deserialize it.
    Object roundTrip = codec.deserialize(codec.serialize(new ArrayList<>()));
    assertInstanceOf(ArrayList.class, roundTrip,
        "round-trip empty list must be an ArrayList, not " + roundTrip.getClass());

    // Sentinel path: the exact bytes that older logs contain.
    SCMRatisProtocol.ListArgument sentinel =
        SCMRatisProtocol.ListArgument.newBuilder()
            .setType(Object.class.getName())
            .build();
    Object fromSentinel = codec.deserialize(sentinel.toByteString());
    assertInstanceOf(ArrayList.class, fromSentinel,
        "sentinel empty list must be an ArrayList, not " + fromSentinel.getClass());

    // Verify the cast that invokers actually perform does not throw.
    ArrayList<?> cast = (ArrayList<?>) roundTrip;
    assertEquals(0, cast.size());
  }
}
