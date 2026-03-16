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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.ListArgument;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * {@link ScmCodec} for {@link List} objects.
 */
public class ScmListCodec implements ScmCodec<Object> {
  private static final ByteString EMPTY_LIST = ListArgument.newBuilder()
      .setType(Object.class.getName())
      .build()
      .toByteString();

  private final Map<String, Class<?>> classes;

  public ScmListCodec(Collection<Class<?>> classes) {
    final Map<String, Class<?>> map = new TreeMap<>();
    for (Class<?> c : classes) {
      map.put(c.getName(), c);
    }
    map.put(List.class.getName(), List.class);
    this.classes = Collections.unmodifiableMap(map);
  }

  private Class<?> getClass(String type) throws InvalidProtocolBufferException {
    final Class<?> clazz = classes.get(type);
    if (clazz == null) {
      throw new InvalidProtocolBufferException("Class not found for type: " + type);
    }
    return clazz;
  }

  @Override
  public ByteString serialize(Object object) throws InvalidProtocolBufferException {
    if (!(object instanceof List)) {
      throw new InvalidProtocolBufferException("Unexpected non-list object: " + object.getClass());
    }
    final List<?> elements = (List<?>) object;
    if (elements.isEmpty()) {
      return EMPTY_LIST;
    }

    Class<?> runtimeClass = elements.get(0).getClass();

    Class<?> registeredClass = null;
    for (Class<?> clazz : classes.values()) {
      if (clazz.isAssignableFrom(runtimeClass)) {
        registeredClass = clazz;
        break;
      }
    }

    if (registeredClass == null) {
      throw new InvalidProtocolBufferException(
          "Unsupported list element type: " + runtimeClass.getName());
    }

    final String elementType = registeredClass.getName();

    final ScmCodec<Object> elementCodec =
        ScmCodecFactory.getCodec(getClass(elementType));
    final ListArgument.Builder builder = ListArgument.newBuilder()
        .setType(elementType);
    for (Object e : elements) {
      builder.addValue(elementCodec.serialize(e));
    }
    return builder.build().toByteString();
  }

  @Override
  public Object deserialize(Class<?> type, ByteString value) throws InvalidProtocolBufferException {
    if (!List.class.isAssignableFrom(type)) {
      throw new InvalidProtocolBufferException("Unexpected non-list type: " + type);
    }
    final ListArgument argument = ListArgument.parseFrom(value.asReadOnlyByteBuffer());
    if (!argument.hasType()) {
      throw new InvalidProtocolBufferException("Missing ListArgument.type: " + argument);
    }
    final Class<?> elementClass = getClass(argument.getType());
    final ScmCodec<?> elementCodec = ScmCodecFactory.getCodec(elementClass);
    final List<Object> list = new ArrayList<>();
    for (ByteString element : argument.getValueList()) {
      list.add(elementCodec.deserialize(elementClass, element));
    }
    return list;
  }
}
