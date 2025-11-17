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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.ozone.erasurecode.ECChunk;
import org.apache.ozone.erasurecode.TestCoderBase;
import org.junit.jupiter.api.Test;

/**
 * Raw coder test base with utilities.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class TestRawCoderBase extends TestCoderBase {
  private final Class<? extends RawErasureCoderFactory> encoderFactoryClass;
  private final Class<? extends RawErasureCoderFactory> decoderFactoryClass;
  private RawErasureEncoder encoder;
  private RawErasureDecoder decoder;

  public TestRawCoderBase(
      Class<? extends RawErasureCoderFactory> encoderFactoryClass,
      Class<? extends RawErasureCoderFactory> decoderFactoryClass) {
    this.encoderFactoryClass = encoderFactoryClass;
    this.decoderFactoryClass = decoderFactoryClass;
  }

  /**
   * Doing twice to test if the coders can be repeatedly reused. This matters
   * as the underlying coding buffers are shared, which may have bugs.
   */
  protected void testCodingDoMixAndTwice() {
    testCodingDoMixed();
    testCodingDoMixed();
  }

  /**
   * Doing in mixed buffer usage model to test if the coders can be repeatedly
   * reused with different buffer usage model. This matters as the underlying
   * coding buffers are shared, which may have bugs.
   */
  protected void testCodingDoMixed() {
    testCoding(true);
    testCoding(false);
  }

  /**
   * Generating source data, encoding, recovering and then verifying.
   * RawErasureCoder mainly uses ECChunk to pass input and output data buffers,
   * it supports two kinds of ByteBuffers, one is array backed, the other is
   * direct ByteBuffer. Use usingDirectBuffer indicate which case to test.
   *
   * @param usingDirectBuffer
   */
  protected void testCoding(boolean usingDirectBuffer) {
    this.usingDirectBuffer = usingDirectBuffer;
    prepareCoders(true);

    /**
     * The following runs will use 3 different chunkSize for inputs and outputs,
     * to verify the same encoder/decoder can process variable width of data.
     */
    performTestCoding(baseChunkSize, true, false, false);
    performTestCoding(baseChunkSize - 17, false, false, false);
    performTestCoding(baseChunkSize + 16, true, false, false);
  }

  /**
   * Similar to above, but perform negative cases using bad input for encoding.
   * @param usingDirectBuffer
   */
  protected void testCodingWithBadInput(boolean usingDirectBuffer) {
    this.usingDirectBuffer = usingDirectBuffer;
    prepareCoders(true);
    assertThrows(Exception.class,
        () -> performTestCoding(baseChunkSize, false, true, false),
        "Encoding test with bad input should fail");
  }

  /**
   * Similar to above, but perform negative cases using bad output for decoding.
   * @param usingDirectBuffer
   */
  protected void testCodingWithBadOutput(boolean usingDirectBuffer) {
    this.usingDirectBuffer = usingDirectBuffer;
    prepareCoders(true);
    assertThrows(Exception.class,
        () -> performTestCoding(baseChunkSize, false, false, true),
        "Decoding test with bad output should fail");
  }

  /**
   * Test encode / decode after release(). It should raise IOException.
   *
   * @throws Exception
   */
  void testAfterRelease() throws Exception {
    prepareCoders(true);
    prepareBufferAllocator(true);

    encoder.release();
    final ECChunk[] data = prepareDataChunksForEncoding();
    final ECChunk[] parity = prepareParityChunksForEncoding();
    IOException ioException = assertThrows(IOException.class,
        () -> encoder.encode(data, parity));
    assertThat(ioException.getMessage()).contains("closed");
    decoder.release();
    final ECChunk[] in = prepareInputChunksForDecoding(data, parity);
    final ECChunk[] out = prepareOutputChunksForDecoding();
    ioException = assertThrows(IOException.class,
        () -> decoder.decode(in, getErasedIndexesForDecoding(), out));
    assertThat(ioException.getMessage()).contains("closed");
  }

  @Test
  public void testCodingWithErasingTooMany() {
    assertThrows(Exception.class, () -> testCoding(true), "Decoding test erasing too many should fail");
    assertThrows(Exception.class, () -> testCoding(false), "Decoding test erasing too many should fail");
  }

  @Test
  public void testIdempotentReleases() {
    prepareCoders(true);

    for (int i = 0; i < 3; i++) {
      encoder.release();
      decoder.release();
    }
  }

  private void performTestCoding(int chunkSize, boolean usingSlicedBuffer,
      boolean useBadInput, boolean useBadOutput) {
    setChunkSize(chunkSize);
    prepareBufferAllocator(usingSlicedBuffer);

    dumpSetting();

    // Generate data and encode
    ECChunk[] dataChunks = prepareDataChunksForEncoding();
    if (useBadInput) {
      corruptSomeChunk(dataChunks);
    }
    dumpChunks("Testing data chunks", dataChunks);

    ECChunk[] parityChunks = prepareParityChunksForEncoding();

    // Backup all the source chunks for later recovering because some coders
    // may affect the source data.
    ECChunk[] clonedDataChunks = cloneChunksWithData(dataChunks);
    markChunks(dataChunks);

    try {
      encoder.encode(dataChunks, parityChunks);
    } catch (IOException e) {
      fail("Should not get IOException: " + e.getMessage());
    }
    dumpChunks("Encoded parity chunks", parityChunks);

    //TODOif (!allowChangeInputs) {
    restoreChunksFromMark(dataChunks);
    compareAndVerify(clonedDataChunks, dataChunks);
    //}

    // Backup and erase some chunks
    ECChunk[] backupChunks =
        backupAndEraseChunks(clonedDataChunks, parityChunks);

    // Decode
    ECChunk[] inputChunks = prepareInputChunksForDecoding(
        clonedDataChunks, parityChunks);

    // Remove unnecessary chunks,
    //     allowing only least required chunks to be read.
    ensureOnlyLeastRequiredChunks(inputChunks);

    ECChunk[] recoveredChunks = prepareOutputChunksForDecoding();
    if (useBadOutput) {
      corruptSomeChunk(recoveredChunks);
    }

    ECChunk[] clonedInputChunks = null;
    //TODOif (!allowChangeInputs) {
    markChunks(inputChunks);
    clonedInputChunks = cloneChunksWithData(inputChunks);
    //}

    dumpChunks("Decoding input chunks", inputChunks);
    try {
      decoder.decode(inputChunks, getErasedIndexesForDecoding(),
          recoveredChunks);
    } catch (IOException e) {
      fail("Should not get IOException: " + e.getMessage());
    }
    dumpChunks("Decoded/recovered chunks", recoveredChunks);

    //TODOif (!allowChangeInputs) {
    restoreChunksFromMark(inputChunks);
    compareAndVerify(clonedInputChunks, inputChunks);
    //}

    // Compare
    compareAndVerify(backupChunks, recoveredChunks);
  }

  /**
   * Set true during setup if want to dump test settings and coding data,
   * useful in debugging.
   * @param allowDump
   */
  protected void setAllowDump(boolean allowDump) {
    this.allowDump = allowDump;
  }

  protected void prepareCoders(boolean recreate) {
    if (encoder == null || recreate) {
      encoder = createEncoder();
    }

    if (decoder == null || recreate) {
      decoder = createDecoder();
    }
  }

  protected void ensureOnlyLeastRequiredChunks(ECChunk[] inputChunks) {
    int leastRequiredNum = numDataUnits;
    int erasedNum = erasedDataIndexes.length + erasedParityIndexes.length;
    int goodNum = inputChunks.length - erasedNum;
    int redundantNum = goodNum - leastRequiredNum;

    for (int i = 0; i < inputChunks.length && redundantNum > 0; i++) {
      if (inputChunks[i] != null) {
        inputChunks[i] = null; // Setting it null, not needing it actually
        redundantNum--;
      }
    }
  }

  /**
   * Create the raw erasure encoder to test.
   */
  protected RawErasureEncoder createEncoder() {
    ECReplicationConfig replicationConfig =
        new ECReplicationConfig(numDataUnits, numParityUnits);
    try {
      RawErasureCoderFactory factory = encoderFactoryClass.newInstance();
      return factory.createEncoder(replicationConfig);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create encoder", e);
    }
  }

  /**
   * Create the raw erasure decoder to test.
   */
  protected RawErasureDecoder createDecoder() {
    ECReplicationConfig replicationConfig =
        new ECReplicationConfig(numDataUnits, numParityUnits);
    try {
      RawErasureCoderFactory factory = encoderFactoryClass.newInstance();
      return factory.createDecoder(replicationConfig);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create decoder", e);
    }
  }

  /**
   * Tests that the input buffer's position is moved to the end after
   * encode/decode.
   */
  protected void testInputPosition(boolean usingDirectBuffer) {
    this.usingDirectBuffer = usingDirectBuffer;
    prepareCoders(true);
    prepareBufferAllocator(false);

    // verify encode
    ECChunk[] dataChunks = prepareDataChunksForEncoding();
    ECChunk[] parityChunks = prepareParityChunksForEncoding();
    ECChunk[] clonedDataChunks = cloneChunksWithData(dataChunks);
    try {
      encoder.encode(dataChunks, parityChunks);
    } catch (IOException e) {
      fail("Should not get IOException: " + e.getMessage());
    }
    verifyBufferPositionAtEnd(dataChunks);

    // verify decode
    backupAndEraseChunks(clonedDataChunks, parityChunks);
    ECChunk[] inputChunks = prepareInputChunksForDecoding(
        clonedDataChunks, parityChunks);
    ensureOnlyLeastRequiredChunks(inputChunks);
    ECChunk[] recoveredChunks = prepareOutputChunksForDecoding();
    try {
      decoder.decode(inputChunks, getErasedIndexesForDecoding(),
          recoveredChunks);
    } catch (IOException e) {
      fail("Should not get IOException: " + e.getMessage());
    }
    verifyBufferPositionAtEnd(inputChunks);
  }

  private void verifyBufferPositionAtEnd(ECChunk[] inputChunks) {
    for (ECChunk chunk : inputChunks) {
      if (chunk != null) {
        assertEquals(0, chunk.getBuffer().remaining());
      }
    }
  }

  public void encode(ECChunk[] inputs, ECChunk[] outputs) throws IOException {
    this.encoder.encode(inputs, outputs);
  }

  public void decode(ECChunk[] inputs, int[] erasedIndexes,
      ECChunk[] outputs) throws IOException {
    this.decoder.decode(inputs, erasedIndexes, outputs);
  }
}
