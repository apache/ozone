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
package org.apache.hadoop.hdds.scm.metadata;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.db.Codec;

/**
 * Codec to serialize / deserialize PipelineID.
 */
public class PipelineIDCodec implements Codec<PipelineID> {

  @Override
  public byte[] toPersistedFormat(PipelineID object) throws IOException {
    byte[] bytes = new byte[16];
    System.arraycopy(
        asByteArray(object.getId().getLeastSignificantBits()), 0, bytes, 8, 8);
    System.arraycopy(
        asByteArray(object.getId().getMostSignificantBits()), 0, bytes, 0, 8);
    return bytes;
  }

  private byte[] asByteArray(long bits) {
    byte[] bytes = new byte[8];
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) (bits >> (i * 8) & 0x00000000000000FF);
    }
    return bytes;
  }

  @Override
  public PipelineID fromPersistedFormat(byte[] rawData) throws IOException {
    if (rawData.length!=16) {
      throw new IllegalArgumentException("Invalid key in DB.");
    }
    long leastSignificantBits = toLong(rawData, 8);
    long mostSiginificantBits = toLong(rawData, 0);

    UUID id = new UUID(mostSiginificantBits, leastSignificantBits);
    return PipelineID.valueOf(id);
  }

  private long toLong(byte[] arr, int startIdx) {
    if (arr.length < startIdx + 8) {
      throw new ArrayIndexOutOfBoundsException();
    }
    long val = 0x0000000000000000L;
    for (int i=7; i>=0; i--) {
      val |= ((long) arr[i+startIdx]) << i * 8;
    }
    return val;
  }

  @Override
  public PipelineID copyObject(PipelineID object) {
    throw new UnsupportedOperationException();
  }
}
