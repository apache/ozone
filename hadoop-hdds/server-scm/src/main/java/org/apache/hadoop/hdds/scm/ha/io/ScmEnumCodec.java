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
import com.google.protobuf.ProtocolMessageEnum;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.ratis.util.Preconditions;

/**
 * {@link ScmCodec} for {@link ProtocolMessageEnum} objects.
 */
class ScmEnumCodec<T extends ProtocolMessageEnum> implements ScmCodec<T> {
  private final Class<T> enumClass;
  private final T[] values;

  ScmEnumCodec(Class<T> enumClass) {
    Preconditions.assertTrue(enumClass.isEnum(), "values is not an enum");
    this.enumClass = enumClass;
    this.values = enumClass.getEnumConstants();
  }

  @Override
  public ByteString serialize(T object) {
    return UnsafeByteOperations.unsafeWrap(Ints.toByteArray(object.getNumber()));
  }

  @Override
  public T deserialize(Class<?> type, ByteString value) throws InvalidProtocolBufferException {
    final int n;
    try {
      n = Ints.fromByteArray(value.toByteArray());
    } catch (Exception e) {
      throw new InvalidProtocolBufferException(
          "Failed to deserialize enum " + enumClass + ": "
              + StringUtils.bytes2String(value.asReadOnlyByteBuffer()), e);
    }
    if (n < 0 || n > values.length) {
      throw new InvalidProtocolBufferException("Invalid enum index for " + enumClass + ": " + n);
    }
    return values[n];
  }
}
