/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha.io;

import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ProtocolMessageEnum;
import org.apache.hadoop.hdds.scm.ha.ReflectionUtil;

import java.lang.reflect.InvocationTargetException;

/**
 * {@link Codec} for {@link ProtocolMessageEnum} objects.
 */
public class EnumCodec implements Codec {

  @Override
  public ByteString serialize(Object object)
      throws InvalidProtocolBufferException {
    return ByteString.copyFrom(Ints.toByteArray(
        ((ProtocolMessageEnum) object).getNumber()));
  }

  @Override
  public Object deserialize(Class<?> type, ByteString value)
      throws InvalidProtocolBufferException {
    try {
      return ReflectionUtil.getMethod(type, "valueOf", int.class)
          .invoke(null, Ints.fromByteArray(
              value.toByteArray()));
    } catch (NoSuchMethodException | IllegalAccessException
        | InvocationTargetException ex) {
      throw new InvalidProtocolBufferException(
          "Message cannot be decoded!" + ex.getMessage());
    }
  }
}
