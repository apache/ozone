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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.ListArgument;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * {@link ScmCodec} for {@link List} objects.
 */
class ScmListCodec implements ScmCodec<Object> {
  private static final ByteString EMPTY_LIST = ListArgument.newBuilder()
      .setType(Object.class.getName())
      .build()
      .toByteString();

  private final ScmCodecFactory.ClassResolver resolver;

  ScmListCodec(ScmCodecFactory.ClassResolver resolver) {
    this.resolver = resolver;
  }

  @Override
  public ByteString serialize(Object object) throws InvalidProtocolBufferException {
    if (!(object instanceof List)) {
      throw new InvalidProtocolBufferException(
          "Unexpected non-list object: " + object.getClass());
    }
    final List<?> elements = (List<?>) object;
    if (elements.isEmpty()) {
      return EMPTY_LIST;
    }

    final Class<?> resolved = resolver.get(elements.get(0).getClass());
    final ScmCodec<Object> elementCodec = ScmCodecFactory.getInstance().getCodec(resolved);

    final ListArgument.Builder builder = ListArgument.newBuilder()
        .setType(resolved.getName());
    for (Object e : elements) {
      builder.addValue(elementCodec.serialize(e));
    }
    return builder.build().toByteString();
  }

  @Override
  public Object deserialize(ByteString value) throws InvalidProtocolBufferException {
    final ListArgument argument = ListArgument.parseFrom(
        value.asReadOnlyByteBuffer());
    if (!argument.hasType()) {
      throw new InvalidProtocolBufferException(
          "Missing ListArgument.type: " + argument);
    }

    final Class<?> elementClass = resolver.get(argument.getType());

    final ScmCodec<?> elementCodec = ScmCodecFactory.getInstance().getCodec(elementClass);
    final List<Object> list = new ArrayList<>();
    for (ByteString element : argument.getValueList()) {
      list.add(elementCodec.deserialize(element));
    }
    return list;
  }
}
