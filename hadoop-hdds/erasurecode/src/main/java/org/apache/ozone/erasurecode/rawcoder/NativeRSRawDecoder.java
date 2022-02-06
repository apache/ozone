/**
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
package org.apache.ozone.erasurecode.rawcoder;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.ECReplicationConfig;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A Reed-Solomon raw decoder using Intel ISA-L library.
 */
@InterfaceAudience.Private
public class NativeRSRawDecoder extends AbstractNativeRawDecoder {

  static {
    ErasureCodeNative.checkNativeCodeLoaded();
  }

  public NativeRSRawDecoder(ECReplicationConfig ecReplicationConfig) {
    super(ecReplicationConfig);
    getDecoderLock().writeLock().lock();
    try {
      initImpl(ecReplicationConfig.getData(), ecReplicationConfig.getParity());
    } finally {
      getDecoderLock().writeLock().unlock();
    }
  }

  @Override
  protected void performDecodeImpl(
      ByteBuffer[] inputs, int[] inputOffsets, int dataLen, int[] erased,
      ByteBuffer[] outputs, int[] outputOffsets) throws IOException {
    decodeImpl(inputs, inputOffsets, dataLen, erased, outputs, outputOffsets);
  }

  @Override
  public void release() {
    getDecoderLock().writeLock().lock();
    try {
      destroyImpl();
    } finally {
      getDecoderLock().writeLock().unlock();
    }
  }

  @Override
  public boolean preferDirectBuffer() {
    return true;
  }

  private native void initImpl(int numDataUnits, int numParityUnits);

  private native void decodeImpl(
          ByteBuffer[] inputs, int[] inputOffsets, int dataLen, int[] erased,
          ByteBuffer[] outputs, int[] outputOffsets) throws IOException;

  private native void destroyImpl();

}