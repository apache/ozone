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

package org.apache.hadoop.hdds.utils.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class for CodecBufferCodec.
 */
public class TestCodecBufferCodec {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testFromCodecBufferMethod(boolean direct) throws CodecException {
    Codec<CodecBuffer> codecBufferCodec = CodecBufferCodec.get(direct);
    try (CodecBuffer codecBuffer = CodecBuffer.allocateDirect(10)) {
      CodecBuffer value = codecBufferCodec.fromCodecBuffer(codecBuffer);
      assertSame(codecBuffer, value);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testFromPersistedFormat(boolean direct) throws CodecException {
    Codec<CodecBuffer> codecBufferCodec = CodecBufferCodec.get(direct);
    String testString = "Test123";
    byte[] bytes = StringCodec.get().toPersistedFormat(testString);

    try (CodecBuffer codecBuffer = codecBufferCodec.fromPersistedFormat(bytes)) {
      String value = StringCodec.get().fromCodecBuffer(codecBuffer);
      assertEquals(testString, value);
    }
  }
}
