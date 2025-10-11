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

package org.apache.hadoop.hdds.scm.metadata;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.db.Codec;

/**
 * Codec to serialize / deserialize PipelineID.
 */
public class OldPipelineIDCodecForTesting implements Codec<PipelineID> {
  @Override
  public Class<PipelineID> getTypeClass() {
    return PipelineID.class;
  }

  @Override
  public byte[] toPersistedFormat(PipelineID object) {
    byte[] bytes = new byte[16];
    System.arraycopy(
        asByteArray(object.getId().getMostSignificantBits()), 0, bytes, 0, 8);
    System.arraycopy(
        asByteArray(object.getId().getLeastSignificantBits()), 0, bytes, 8, 8);
    return bytes;
  }

  private byte[] asByteArray(long bits) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(bits);
    return buffer.array();
  }

  @Override
  public PipelineID fromPersistedFormatImpl(byte[] rawData) {
    long mostSiginificantBits = toLong(rawData, 0);
    long leastSignificantBits = toLong(rawData, 8);

    UUID id = new UUID(mostSiginificantBits, leastSignificantBits);
    return PipelineID.valueOf(id);
  }

  private long toLong(byte[] arr, int startIdx) {
    if (arr.length < startIdx + 8) {
      throw new ArrayIndexOutOfBoundsException(
          "Not enough bytes: length (=" + arr.length + ") - startIdx (=" + startIdx + ") < 8");
    }
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.put(arr, startIdx, 8);
    buffer.flip();
    return buffer.getLong();
  }

  @Override
  public PipelineID copyObject(PipelineID object) {
    throw new UnsupportedOperationException();
  }
}
