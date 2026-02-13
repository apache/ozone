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

import com.google.protobuf.Message;
import java.lang.reflect.InvocationTargetException;
import org.apache.hadoop.hdds.scm.ha.ReflectionUtil;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * {@link Codec} implementation for non-shaded
 * {@link com.google.protobuf.Message} objects.
 */
public class GeneratedMessageCodec implements Codec {

  @Override
  public ByteString serialize(Object object)
      throws org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException {
    final byte[] bytes = ((Message) object).toByteString().toByteArray();
    return ByteString.copyFrom(bytes);
  }

  @Override
  public Object deserialize(Class<?> type, ByteString value)
      throws org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException {
    try {
      return ReflectionUtil.getMethod(type, "parseFrom", byte[].class)
          .invoke(null, (Object) value.toByteArray());
    } catch (NoSuchMethodException | IllegalAccessException
             | InvocationTargetException ex) {
      ex.printStackTrace();
      throw new InvalidProtocolBufferException("Message cannot be decoded: " + ex.getMessage());
    }
  }
}
