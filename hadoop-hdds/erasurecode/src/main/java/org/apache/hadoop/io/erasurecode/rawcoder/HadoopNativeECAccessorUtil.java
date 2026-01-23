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

package org.apache.hadoop.io.erasurecode.rawcoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;

/**
 * This class is used to access some of the protected API
 * from hadoop native EC java code.
 * This is needed until we adapt the native ec code from hadoop.
 */
@InterfaceAudience.Private
public final class HadoopNativeECAccessorUtil {
  public static void performEncodeImpl(NativeRSRawEncoder encoder,
      ByteBuffer[] inputs, int[] inputOffsets, int dataLen,
      ByteBuffer[] outputs, int[] outputOffsets) throws IOException {
    encoder.performEncodeImpl(inputs, inputOffsets, dataLen, outputs,
        outputOffsets);
  }

  public static void performDecodeImpl(NativeRSRawDecoder decoder,
      ByteBuffer[] inputs, int[] inputOffsets, int dataLen, int[] erased,
      ByteBuffer[] outputs, int[] outputOffsets) throws IOException {
    decoder.performDecodeImpl(inputs, inputOffsets, dataLen, erased, outputs,
        outputOffsets);
  }

  public static void performEncodeImpl(NativeXORRawEncoder encoder,
      ByteBuffer[] inputs, int[] inputOffsets, int dataLen,
      ByteBuffer[] outputs, int[] outputOffsets) throws IOException {
    encoder.performEncodeImpl(inputs, inputOffsets, dataLen, outputs,
        outputOffsets);
  }

  public static void performDecodeImpl(NativeXORRawDecoder decoder,
      ByteBuffer[] inputs, int[] inputOffsets, int dataLen, int[] erased,
      ByteBuffer[] outputs, int[] outputOffsets) throws IOException {
    decoder.performDecodeImpl(inputs, inputOffsets, dataLen, erased, outputs,
        outputOffsets);
  }

  private HadoopNativeECAccessorUtil() {
  }
}
