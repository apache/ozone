/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.utils.db;

import java.util.function.IntFunction;
import javax.annotation.Nonnull;

import org.apache.hadoop.hdds.StringUtils;

/**
 * Codec to convert String to/from byte array.
 */
public final class StringCodec implements Codec<String> {
  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull String object,
      IntFunction<CodecBuffer> allocator) {
    final byte[] array = toPersistedFormat(object);
    return allocator.apply(array.length).put(array);
  }

  @Override
  public String fromCodecBuffer(@Nonnull CodecBuffer buffer) {
    return StringUtils.bytes2String(buffer.asReadOnlyByteBuffer());
  }

  @Override
  public byte[] toPersistedFormat(String object) {
    if (object != null) {
      return StringUtils.string2Bytes(object);
    } else {
      return null;
    }
  }

  @Override
  public String fromPersistedFormat(byte[] rawData) {
    if (rawData != null) {
      return StringUtils.bytes2String(rawData);
    } else {
      return null;
    }
  }

  @Override
  public String copyObject(String object) {
    return object;
  }
}
