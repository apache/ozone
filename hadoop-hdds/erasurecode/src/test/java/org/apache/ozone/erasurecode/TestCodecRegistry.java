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

package org.apache.ozone.erasurecode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.ozone.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.ozone.erasurecode.rawcoder.NativeXORRawErasureCoderFactory;
import org.apache.ozone.erasurecode.rawcoder.RSRawErasureCoderFactory;
import org.apache.ozone.erasurecode.rawcoder.RawErasureCoderFactory;
import org.apache.ozone.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.ozone.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.ozone.erasurecode.rawcoder.XORRawErasureCoderFactory;
import org.junit.jupiter.api.Test;

/**
 * Test CodecRegistry.
 */
public class TestCodecRegistry {

  @Test
  public void testGetCodecs() {
    Set<String> codecs = CodecRegistry.getInstance().getCodecNames();
    assertEquals(2, codecs.size());
    assertThat(codecs).contains(ECReplicationConfig.EcCodec.RS.name().toLowerCase());
    assertThat(codecs).contains(ECReplicationConfig.EcCodec.XOR.name().toLowerCase());
  }

  @Test
  public void testGetCoders() {
    List<RawErasureCoderFactory> coders = CodecRegistry.getInstance().
        getCoders(ECReplicationConfig.EcCodec.RS.name().toLowerCase());
    assertEquals(2, coders.size());
    assertInstanceOf(NativeRSRawErasureCoderFactory.class, coders.get(0));
    assertInstanceOf(RSRawErasureCoderFactory.class, coders.get(1));

    coders = CodecRegistry.getInstance().
        getCoders(ECReplicationConfig.EcCodec.XOR.name().toLowerCase());
    assertEquals(2, coders.size());
    assertInstanceOf(NativeXORRawErasureCoderFactory.class, coders.get(0));
    assertInstanceOf(XORRawErasureCoderFactory.class, coders.get(1));
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
      @Override
      public RawErasureEncoder createEncoder(ECReplicationConfig coderOptions) {
        return null;
      }

      @Override
      public RawErasureDecoder createDecoder(ECReplicationConfig coderOptions) {
        return null;
      }

      @Override
      public String getCoderName() {
        return "rs_java";
      }

      @Override
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
    assertInstanceOf(NativeRSRawErasureCoderFactory.class, rsCoders.get(0));
    assertInstanceOf(RSRawErasureCoderFactory.class, rsCoders.get(1));

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
    assertInstanceOf(RSRawErasureCoderFactory.class, coder);

    coder = CodecRegistry.getInstance()
        .getCoderByName(ECReplicationConfig.EcCodec.RS.name().toLowerCase(),
            NativeRSRawErasureCoderFactory.CODER_NAME);
    assertInstanceOf(NativeRSRawErasureCoderFactory.class, coder);

    coder = CodecRegistry.getInstance()
        .getCoderByName(ECReplicationConfig.EcCodec.XOR.name().toLowerCase(),
            XORRawErasureCoderFactory.CODER_NAME);
    assertInstanceOf(XORRawErasureCoderFactory.class, coder);

    coder = CodecRegistry.getInstance()
        .getCoderByName(ECReplicationConfig.EcCodec.XOR.name().toLowerCase(),
            NativeXORRawErasureCoderFactory.CODER_NAME);
    assertInstanceOf(NativeXORRawErasureCoderFactory.class, coder);
  }
}
