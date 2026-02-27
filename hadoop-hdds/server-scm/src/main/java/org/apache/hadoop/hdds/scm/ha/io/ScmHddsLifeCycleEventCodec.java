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
 * Encodes/decodes {@link HddsProtos.LifeCycleEvent}.
 */
public class ScmHddsLifeCycleEventCodec
    implements ScmCodec<HddsProtos.LifeCycleEvent> {

  @Override
  public ByteString serialize(HddsProtos.LifeCycleEvent object) {
    return UnsafeByteOperations.unsafeWrap(
        Ints.toByteArray(object.getNumber()));
  }

  @Override
  public HddsProtos.LifeCycleEvent deserialize(Class<?> type, ByteString value) {
    int n = Ints.fromByteArray(value.toByteArray());
    HddsProtos.LifeCycleEvent event = HddsProtos.LifeCycleEvent.forNumber(n);
    if (event == null) {
      throw new IllegalArgumentException(
          "Unknown LifeCycleEvent enum value: " + n);
    }
    return event;
  }
}
