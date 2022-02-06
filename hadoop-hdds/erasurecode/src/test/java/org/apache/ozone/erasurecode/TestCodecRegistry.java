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
package org.apache.ozone.erasurecode;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.ozone.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.ozone.erasurecode.rawcoder.NativeXORRawErasureCoderFactory;
import org.apache.ozone.erasurecode.rawcoder.RSRawErasureCoderFactory;
import org.apache.ozone.erasurecode.rawcoder.RawErasureCoderFactory;
import org.apache.ozone.erasurecode.rawcoder.XORRawErasureCoderFactory;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test CodecRegistry.
 */
public class TestCodecRegistry {
  public static final String DUMMY_CODEC_NAME = "dummy";
  public static final String RS_CODEC_NAME = "rs";
  public static final String RS_LEGACY_CODEC_NAME = "rs-legacy";
  public static final String XOR_CODEC_NAME = "xor";
  public static final String HHXOR_CODEC_NAME = "hhxor";

  @Test
  public void testGetCodecs() {
    Set<String> codecs = CodecRegistry.getInstance().getCodecNames();
    assertEquals(2, codecs.size());
    assertTrue(
        codecs.contains(ECReplicationConfig.EcCodec.RS.name().toLowerCase()));
    assertTrue(
        codecs.contains(ECReplicationConfig.EcCodec.XOR.name().toLowerCase()));
  }

  @Test
  public void testGetCoders() {
    List<RawErasureCoderFactory> coders = CodecRegistry.getInstance().
        getCoders(ECReplicationConfig.EcCodec.RS.name().toLowerCase());
    assertEquals(2, coders.size());
    assertTrue(coders.get(0) instanceof NativeRSRawErasureCoderFactory);
    assertTrue(coders.get(1) instanceof RSRawErasureCoderFactory);

    coders = CodecRegistry.getInstance().
        getCoders(ECReplicationConfig.EcCodec.XOR.name().toLowerCase());
    assertEquals(2, coders.size());
    assertTrue(coders.get(0) instanceof NativeXORRawErasureCoderFactory);
    assertTrue(coders.get(1) instanceof XORRawErasureCoderFactory);
  }

  @Test
  public void testGetCodersWrong() {
    List<RawErasureCoderFactory> coders =
        CodecRegistry.getInstance().getCoders("WRONG_CODEC");
    assertNull(coders);
  }

  @Test
  public void testGetCoderByNameWrong() {
    RawErasureCoderFactory coder = CodecRegistry.getInstance().
        getCoderByName(ECReplicationConfig.EcCodec.RS.name().toLowerCase(),
            "WRONG_RS");
    assertNull(coder);
  }

  @Test
  public void testGetCoderNames() {
    String[] coderNames = CodecRegistry.getInstance().
        getCoderNames(RS_CODEC_NAME);
    assertEquals(2, coderNames.length);
    assertEquals(NativeRSRawErasureCoderFactory.CODER_NAME, coderNames[0]);
    assertEquals(RSRawErasureCoderFactory.CODER_NAME, coderNames[1]);

    coderNames = CodecRegistry.getInstance().
        getCoderNames(XOR_CODEC_NAME);
    assertEquals(2, coderNames.length);
    assertEquals(NativeXORRawErasureCoderFactory.CODER_NAME, coderNames[0]);
    assertEquals(XORRawErasureCoderFactory.CODER_NAME, coderNames[1]);
  }

  @Test
  public void testGetCoderByName() {
    RawErasureCoderFactory coder = CodecRegistry.getInstance().
        getCoderByName(RS_CODEC_NAME, RSRawErasureCoderFactory.CODER_NAME);
    assertTrue(coder instanceof RSRawErasureCoderFactory);

    coder = CodecRegistry.getInstance().getCoderByName(RS_CODEC_NAME,
        NativeRSRawErasureCoderFactory.CODER_NAME);
    assertTrue(coder instanceof NativeRSRawErasureCoderFactory);

    coder = CodecRegistry.getInstance()
        .getCoderByName(XOR_CODEC_NAME, XORRawErasureCoderFactory.CODER_NAME);
    assertTrue(coder instanceof XORRawErasureCoderFactory);

    coder = CodecRegistry.getInstance().getCoderByName(XOR_CODEC_NAME,
        NativeXORRawErasureCoderFactory.CODER_NAME);
    assertTrue(coder instanceof NativeXORRawErasureCoderFactory);
  }
}
