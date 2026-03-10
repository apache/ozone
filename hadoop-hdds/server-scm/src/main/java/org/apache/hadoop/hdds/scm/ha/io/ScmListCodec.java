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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.ListArgument;
import org.apache.hadoop.hdds.scm.ha.ReflectionUtil;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * {@link ScmCodec} for {@link List} objects.
 */
public class ScmListCodec implements ScmCodec<Object> {

  private final Class<?> elementType;
  private final ScmCodec<?> elementCodec;

  public ScmListCodec(Class<?> elementType, ScmCodec<?> elementCodec) {
    this.elementType = elementType;
    this.elementCodec = elementCodec;
  }

  @Override
  public ByteString serialize(Object object)
      throws InvalidProtocolBufferException {
    final ListArgument.Builder listArgs = ListArgument.newBuilder();
    final List<?> values = (List<?>) object;

    listArgs.setType(elementType.getName());
    for (Object value : values) {
      listArgs.addValue(serializeElement(value));
    }
    return listArgs.build().toByteString();
  }

  @Override
  public Object deserialize(Class<?> type, ByteString value)
      throws InvalidProtocolBufferException {
    try {
      Class<?> concreteType = (type == List.class) ? ArrayList.class : type;

      @SuppressWarnings("unchecked")
      List<Object> result =
          (List<Object>) concreteType.getDeclaredConstructor().newInstance();

      final ListArgument listArgs = (ListArgument) ReflectionUtil
          .getMethod(ListArgument.class, "parseFrom", byte[].class)
          .invoke(null, (Object) value.toByteArray());

      if (!listArgs.hasType()) {
        throw new InvalidProtocolBufferException("Missing ListArgument.type");
      }

      for (ByteString element : listArgs.getValueList()) {
        result.add(deserializeElement(element));
      }
      return result;
    } catch (InstantiationException | NoSuchMethodException |
             IllegalAccessException | InvocationTargetException ex) {
      throw new InvalidProtocolBufferException(
          "Message cannot be decoded: " + ex.getMessage());
    }
  }

  private ByteString serializeElement(Object value)
      throws InvalidProtocolBufferException {
    @SuppressWarnings("unchecked")
    final ScmCodec<Object> codec = (ScmCodec<Object>) elementCodec;
    return codec.serialize(value);
  }

  private Object deserializeElement(ByteString value)
      throws InvalidProtocolBufferException {
    @SuppressWarnings("unchecked")
    final ScmCodec<Object> codec = (ScmCodec<Object>) elementCodec;
    return codec.deserialize(elementType, value);
  }
}
