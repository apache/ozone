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

import com.google.common.primitives.Ints;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;

/**
 * Encodes/decodes {@link HddsProtos.PipelineState} to/from a byte string.
 */
public class ScmHddsPipelineStateCodec implements ScmCodec<HddsProtos.PipelineState> {

  @Override
  public ByteString serialize(HddsProtos.PipelineState object) {
    // Store enum's numeric wire value.
    return UnsafeByteOperations.unsafeWrap(Ints.toByteArray(object.getNumber()));
  }

  @Override
  public HddsProtos.PipelineState deserialize(Class<?> type, ByteString value) {
    int n = Ints.fromByteArray(value.toByteArray());
    HddsProtos.PipelineState state = HddsProtos.PipelineState.forNumber(n);
    if (state == null) {
      throw new IllegalArgumentException("Unknown PipelineState enum value: " + n);
    }
    return state;
  }
}
