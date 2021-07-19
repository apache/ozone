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
import org.apache.ozone.erasurecode.rawcoder.RSRawErasureCoderFactory;
import org.apache.ozone.erasurecode.rawcoder.RawErasureCoderFactory;
import org.apache.ozone.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.ozone.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.ozone.erasurecode.rawcoder.XORRawErasureCoderFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test CodecRegistry.
 */
public class TestCodecRegistry {
  @Test
  public void testGetCodecs() {
    Set<String> codecs = CodecRegistry.getInstance().getCodecNames();
    assertEquals(3, codecs.size());
    assertTrue(codecs.contains(ECReplicationConfig.RS_CODEC));
    assertTrue(codecs.contains(ECReplicationConfig.XOR_CODEC));
  }

  @Test
  public void testGetCoders() {
    List<RawErasureCoderFactory> coders = CodecRegistry.getInstance().
        getCoders(ECReplicationConfig.RS_CODEC);
    assertEquals(1, coders.size());
    assertTrue(coders.get(0) instanceof RSRawErasureCoderFactory);

    coders = CodecRegistry.getInstance().
        getCoders(ECReplicationConfig.XOR_CODEC);
    assertEquals(1, coders.size());
    assertTrue(coders.get(0) instanceof XORRawErasureCoderFactory);
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
        getCoderByName(ECReplicationConfig.RS_CODEC, "WRONG_RS");
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
        return ECReplicationConfig.RS_CODEC;
      }
    }

    List<RawErasureCoderFactory> userDefinedFactories = new ArrayList<>();
    userDefinedFactories.add(new RSUserDefinedIncorrectFactory());
    CodecRegistry.getInstance().updateCoders(userDefinedFactories);

    // check RS coders
    List<RawErasureCoderFactory> rsCoders = CodecRegistry.getInstance().
        getCoders(ECReplicationConfig.RS_CODEC);
    assertEquals(1, rsCoders.size());
    assertTrue(rsCoders.get(1) instanceof RSRawErasureCoderFactory);

    // check RS coder names
    String[] rsCoderNames = CodecRegistry.getInstance().
        getCoderNames(ECReplicationConfig.RS_CODEC);
    assertEquals(1, rsCoderNames.length);
    assertEquals(RSRawErasureCoderFactory.CODER_NAME, rsCoderNames[1]);
  }
}
