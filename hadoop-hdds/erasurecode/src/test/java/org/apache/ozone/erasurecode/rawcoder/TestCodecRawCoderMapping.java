/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ozone.erasurecode.rawcoder;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.ozone.erasurecode.rawcoder.util.CodecUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the codec to raw coder mapping.
 */
public class TestCodecRawCoderMapping {

  private static final String RS_CODEC_NAME = "rs";
  private static final String XOR_CODEC_NAME = "xor";

  @Test
  public void testRSDefaultRawCoder() {
    ECReplicationConfig coderOptions =
        new ECReplicationConfig(RS_CODEC_NAME + "-6-3-1024K");
    // should return default raw coder of rs codec
    RawErasureEncoder encoder =
        CodecUtil.createRawEncoderWithFallback(coderOptions);
    RawErasureDecoder decoder =
        CodecUtil.createRawDecoderWithFallback(coderOptions);
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      Assert.assertTrue(encoder instanceof NativeRSRawEncoder);
      Assert.assertTrue(decoder instanceof NativeRSRawDecoder);
    } else {
      Assert.assertTrue(encoder instanceof RSRawEncoder);
      Assert.assertTrue(decoder instanceof RSRawDecoder);
    }
  }

  @Test
  public void testXORRawCoder() {
    ECReplicationConfig coderOptions =
        new ECReplicationConfig(XOR_CODEC_NAME + "-6-3-1024K");
    // should return default raw coder of rs codec
    RawErasureEncoder encoder =
        CodecUtil.createRawEncoderWithFallback(coderOptions);
    RawErasureDecoder decoder =
        CodecUtil.createRawDecoderWithFallback(coderOptions);
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      Assert.assertTrue(encoder instanceof NativeXORRawEncoder);
      Assert.assertTrue(decoder instanceof NativeXORRawDecoder);
    } else {
      Assert.assertTrue(encoder instanceof XORRawEncoder);
      Assert.assertTrue(decoder instanceof XORRawDecoder);
    }
  }
}