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
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.HadoopNativeECAccessorUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A XOR raw encoder using Intel ISA-L library.
 */
@InterfaceAudience.Private
public class NativeXORRawEncoder extends AbstractNativeRawEncoder {

  private org.apache.hadoop.io.erasurecode.rawcoder.NativeXORRawEncoder
      hadoopNativeXORRawEncoder;

  public NativeXORRawEncoder(ECReplicationConfig ecReplicationConfig) {
    super(ecReplicationConfig);
    hadoopNativeXORRawEncoder =
        new org.apache.hadoop.io.erasurecode.rawcoder.NativeXORRawEncoder(
            new ErasureCoderOptions(ecReplicationConfig.getData(),
                ecReplicationConfig.getParity()));
  }

  @Override
  protected void performEncodeImpl(ByteBuffer[] inputs, int[] inputOffsets,
      int dataLen, ByteBuffer[] outputs, int[] outputOffsets)
      throws IOException {
    HadoopNativeECAccessorUtil
        .performEncodeImpl(hadoopNativeXORRawEncoder, inputs,
            inputOffsets, dataLen, outputs, outputOffsets);
  }

  @Override
  public void release() {
    hadoopNativeXORRawEncoder.release();
  }
}