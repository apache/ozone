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
import org.apache.ozone.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.ozone.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.ozone.erasurecode.rawcoder.XORRawErasureCoderFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test CodecRegistry.
 */
public class TestCodecRegistry {

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
  public void testUpdateCoders() {
    class RSUserDefinedIncorrectFactory implements RawErasureCoderFactory {
      public RawErasureEncoder createEncoder(ECReplicationConfig coderOptions) {
        return null;
      }

      public RawErasureDecoder createDecoder(ECReplicationConfig coderOptions) {
        return null;
      }

      public String getCoderName() {
        return "rs_java";
      }

      public String getCodecName() {
        return ECReplicationConfig.EcCodec.RS.name().toLowerCase();
      }
    }

    List<RawErasureCoderFactory> userDefinedFactories = new ArrayList<>();
    userDefinedFactories.add(new RSUserDefinedIncorrectFactory());
    CodecRegistry.getInstance().updateCoders(userDefinedFactories);

    // check RS coders
    List<RawErasureCoderFactory> rsCoders = CodecRegistry.getInstance().
        getCoders(ECReplicationConfig.EcCodec.RS.name().toLowerCase());
    assertEquals(2, rsCoders.size());
    assertTrue(rsCoders.get(0) instanceof NativeRSRawErasureCoderFactory);
    assertTrue(rsCoders.get(1) instanceof RSRawErasureCoderFactory);

    // check RS coder names
    String[] rsCoderNames = CodecRegistry.getInstance().
        getCoderNames(ECReplicationConfig.EcCodec.RS.name().toLowerCase());
    assertEquals(2, rsCoderNames.length);
    assertEquals(NativeRSRawErasureCoderFactory.CODER_NAME, rsCoderNames[0]);
    assertEquals(RSRawErasureCoderFactory.CODER_NAME, rsCoderNames[1]);
  }

  @Test
  public void testGetCoderNames() {
    String[] coderNames = CodecRegistry.getInstance().
        getCoderNames(ECReplicationConfig.EcCodec.RS.name().toLowerCase());
    assertEquals(2, coderNames.length);
    assertEquals(NativeRSRawErasureCoderFactory.CODER_NAME, coderNames[0]);
    assertEquals(RSRawErasureCoderFactory.CODER_NAME, coderNames[1]);

    coderNames = CodecRegistry.getInstance().
        getCoderNames(ECReplicationConfig.EcCodec.XOR.name().toLowerCase());
    assertEquals(2, coderNames.length);
    assertEquals(NativeXORRawErasureCoderFactory.CODER_NAME, coderNames[0]);
    assertEquals(XORRawErasureCoderFactory.CODER_NAME, coderNames[1]);
  }

  @Test
  public void testGetCoderByName() {
    RawErasureCoderFactory coder = CodecRegistry.getInstance().
        getCoderByName(ECReplicationConfig.EcCodec.RS.name().toLowerCase(),
            RSRawErasureCoderFactory.CODER_NAME);
    assertTrue(coder instanceof RSRawErasureCoderFactory);

    coder = CodecRegistry.getInstance()
        .getCoderByName(ECReplicationConfig.EcCodec.RS.name().toLowerCase(),
            NativeRSRawErasureCoderFactory.CODER_NAME);
    assertTrue(coder instanceof NativeRSRawErasureCoderFactory);

    coder = CodecRegistry.getInstance()
        .getCoderByName(ECReplicationConfig.EcCodec.XOR.name().toLowerCase(),
            XORRawErasureCoderFactory.CODER_NAME);
    assertTrue(coder instanceof XORRawErasureCoderFactory);

    coder = CodecRegistry.getInstance()
        .getCoderByName(ECReplicationConfig.EcCodec.XOR.name().toLowerCase(),
            NativeXORRawErasureCoderFactory.CODER_NAME);
    assertTrue(coder instanceof NativeXORRawErasureCoderFactory);
  }
}
