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

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ozone.erasurecode.ECChunk;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test dummy raw coder.
 */
public class TestDummyRawCoder extends TestRawCoderBase {

  public TestDummyRawCoder() {
    super(DummyRawErasureCoderFactory.class, DummyRawErasureCoderFactory.class);
  }

  @BeforeEach
  public void setup() {
    setAllowDump(false);
    setChunkSize(baseChunkSize);
  }

  @Test
  public void testCoding6x3ErasingD0Dd2() {
    prepare(null, 6, 3, new int[]{0, 2}, new int[0], false);
    testCodingDoMixed();
  }

  @Test
  public void testCoding6x3ErasingD0P0() {
    prepare(null, 6, 3, new int[]{0}, new int[]{0}, false);
    testCodingDoMixed();
  }

  @Override
  protected void testCoding(boolean usingDirectBuffer) {
    this.usingDirectBuffer = usingDirectBuffer;
    prepareCoders(true);

    prepareBufferAllocator(true);

    // Generate data and encode
    ECChunk[] dataChunks = prepareDataChunksForEncoding();
    markChunks(dataChunks);
    ECChunk[] parityChunks = prepareParityChunksForEncoding();
    try {
      encode(dataChunks, parityChunks);
    } catch (IOException e) {
      fail("Unexpected IOException: " + e.getMessage());
    }
    compareAndVerify(parityChunks, getEmptyChunks(parityChunks.length));

    // Decode
    restoreChunksFromMark(dataChunks);
    backupAndEraseChunks(dataChunks, parityChunks);
    ECChunk[] inputChunks = prepareInputChunksForDecoding(
        dataChunks, parityChunks);
    ensureOnlyLeastRequiredChunks(inputChunks);
    ECChunk[] recoveredChunks = prepareOutputChunksForDecoding();
    try {
      decode(inputChunks, getErasedIndexesForDecoding(),
          recoveredChunks);
    } catch (IOException e) {
      fail("Unexpected IOException: " + e.getMessage());
    }
    compareAndVerify(recoveredChunks, getEmptyChunks(recoveredChunks.length));
  }

  private ECChunk[] getEmptyChunks(int num) {
    ECChunk[] chunks = new ECChunk[num];
    for (int i = 0; i < chunks.length; i++) {
      chunks[i] = new ECChunk(ByteBuffer.wrap(getZeroChunkBytes()));
    }
    return chunks;
  }
}
