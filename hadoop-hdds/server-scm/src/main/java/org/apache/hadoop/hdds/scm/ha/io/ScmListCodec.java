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
public class ScmListCodec implements ScmCodec {

  @Override
  public ByteString serialize(Object object)
      throws InvalidProtocolBufferException {
    final ListArgument.Builder listArgs = ListArgument.newBuilder();
    final List<?> values = (List<?>) object;
    if (!values.isEmpty()) {
      Class<?> type = values.get(0).getClass();
      listArgs.setType(type.getName());
      for (Object value : values) {
        listArgs.addValue(ScmCodecFactory.getCodec(type).serialize(value));
      }
    } else {
      listArgs.setType(Object.class.getName());
    }
    return listArgs.build().toByteString();
  }

  @Override
  public Object deserialize(Class<?> type, ByteString value)
      throws InvalidProtocolBufferException {
    try {
      // If argument type is the generic interface, then determine a
      // concrete implementation.
      Class<?> concreteType = (type == List.class) ? ArrayList.class : type;

      List<Object> result = (List<Object>) concreteType.newInstance();
      final ListArgument listArgs = (ListArgument) ReflectionUtil
          .getMethod(ListArgument.class, "parseFrom", byte[].class)
          .invoke(null, (Object) value.toByteArray());

      // proto2 required-equivalent check
      if (!listArgs.hasType()) {
        throw new InvalidProtocolBufferException("Missing ListArgument.type");
      }

      final Class<?> dataType = ReflectionUtil.getClass(listArgs.getType());
      for (ByteString element : listArgs.getValueList()) {
        result.add(ScmCodecFactory.getCodec(dataType)
            .deserialize(dataType, element));
      }
      return result;
    } catch (InstantiationException | NoSuchMethodException |
        IllegalAccessException | InvocationTargetException |
        ClassNotFoundException ex) {
      throw new InvalidProtocolBufferException(
          "Message cannot be decoded: " + ex.getMessage());
    }
  }
}
