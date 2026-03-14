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
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * {@link ScmCodec} for {@link List} objects.
 */
public class ScmListCodec<T> implements ScmCodec<List<T>> {

  private final Class<T> elementType;
  private final ScmCodec<T> elementCodec;

  public ScmListCodec(Class<T> elementType, ScmCodec<T> elementCodec) {
    this.elementType = elementType;
    this.elementCodec = elementCodec;
  }

  @Override
  public ByteString serialize(List<T> object)
      throws InvalidProtocolBufferException {
    final ListArgument.Builder listArgs = ListArgument.newBuilder();

    listArgs.setType(elementType.getName());
    for (T value : object) {
      listArgs.addValue(elementCodec.serialize(value));
    }
    return listArgs.build().toByteString();
  }

  @Override
  public List<T> deserialize(Class<?> type, ByteString value)
      throws InvalidProtocolBufferException {
    try {
      Class<?> concreteType = (type == List.class) ? ArrayList.class : type;

      @SuppressWarnings("unchecked")
      List<T> result =
          (List<T>) concreteType.getDeclaredConstructor().newInstance();

      final ListArgument listArgs = ListArgument.parseFrom(value.toByteArray());

      if (!listArgs.hasType()) {
        throw new InvalidProtocolBufferException("Missing ListArgument.type");
      }

      if (!listArgs.getType().equals(elementType.getName())) {
        throw new InvalidProtocolBufferException(
            "List element type mismatch, expected " + elementType.getName()
                + " but got " + listArgs.getType());
      }

      for (ByteString element : listArgs.getValueList()) {
        result.add(elementCodec.deserialize(elementType, element));
      }
      return result;
    } catch (InstantiationException | NoSuchMethodException |
             IllegalAccessException | InvocationTargetException ex) {
      throw new InvalidProtocolBufferException(
          "Message cannot be decoded: " + ex.getMessage());
    }
  }
}
