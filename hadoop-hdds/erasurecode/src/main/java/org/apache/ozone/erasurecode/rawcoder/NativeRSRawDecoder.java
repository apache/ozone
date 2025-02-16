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

package org.apache.ozone.erasurecode.rawcoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.HadoopNativeECAccessorUtil;

/**
 * A Reed-Solomon raw decoder using Intel ISA-L library.
 */
@InterfaceAudience.Private
public class NativeRSRawDecoder extends AbstractNativeRawDecoder {

  private org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawDecoder
      hadoopNativeRSRawDecoder;

  public NativeRSRawDecoder(ECReplicationConfig ecReplicationConfig) {
    super(ecReplicationConfig);
    hadoopNativeRSRawDecoder =
        new org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawDecoder(
            new ErasureCoderOptions(ecReplicationConfig.getData(),
                ecReplicationConfig.getParity()));
  }

  @Override
  protected void performDecodeImpl(ByteBuffer[] inputs, int[] inputOffsets,
      int dataLen, int[] erased, ByteBuffer[] outputs, int[] outputOffsets)
      throws IOException {
    HadoopNativeECAccessorUtil
        .performDecodeImpl(hadoopNativeRSRawDecoder, inputs,
            inputOffsets, dataLen, erased, outputs, outputOffsets);
  }

  @Override
  public void release() {
    hadoopNativeRSRawDecoder.release();
  }

  @Override
  public boolean preferDirectBuffer() {
    return hadoopNativeRSRawDecoder.preferDirectBuffer();
  }
}
