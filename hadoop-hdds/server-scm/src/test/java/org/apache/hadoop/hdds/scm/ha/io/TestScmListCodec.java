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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        () -> codec.deserialize(List.class, listArg.toByteString()));

    assertTrue(ex.getMessage().contains("Missing ListArgument.type"));
  }
}
