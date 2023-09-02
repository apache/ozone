/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.utils.db;

import javax.annotation.Nonnull;
import java.util.function.IntFunction;

/**
 * Codec to serialize/deserialize {@link Boolean}.
 */
public final class BooleanCodec implements Codec<Boolean> {

  private static final byte TRUE = 1;
  private static final byte FALSE = 0;
  private static final BooleanCodec INSTANCE = new BooleanCodec();

  public static BooleanCodec get() {
    return INSTANCE;
  }

  private BooleanCodec() {
    // singleton
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(Boolean object,
      IntFunction<CodecBuffer> allocator) {
    return allocator.apply(1).put(TRUE);
  }

  @Override
  public Boolean fromCodecBuffer(@Nonnull CodecBuffer buffer) {
    return buffer.asReadOnlyByteBuffer().get() == 1;
  }

  @Override
  public byte[] toPersistedFormat(Boolean object) {
    return object ? new byte[]{TRUE} : new byte[]{FALSE};
  }

  @Override
  public Boolean fromPersistedFormat(byte[] rawData) {
    if (rawData.length != 1) {
      throw new IllegalStateException("Byte Buffer for boolean should be of " +
          "length 1 but provided byte array of length " + rawData.length);
    }
    return rawData[0] == 1;
  }

  @Override
  public Boolean copyObject(Boolean object) {
    return object;
  }
}
