/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Utility class to write random keys from a limited buffer.
 */
public class ContentGenerator {

  /**
   * Size of the destination object (key or file).
   */
  private final long keySize;

  /**
   * Buffer for the pre-allocated content (will be reused if less than the
   * keySize).
   */
  private final int bufferSize;

  /**
   * Number of bytes to write in one call.
   * <p>
   * Should be no larger than the bufferSize.
   */
  private final int copyBufferSize;

  private final byte[] buffer;

  ContentGenerator(long keySize, int bufferSize) {
    this(keySize, bufferSize, bufferSize);
  }

  ContentGenerator(long keySize, int bufferSize, int copyBufferSize) {
    this.keySize = keySize;
    this.bufferSize = bufferSize;
    this.copyBufferSize = copyBufferSize;
    buffer = RandomStringUtils.randomAscii(bufferSize)
        .getBytes(StandardCharsets.UTF_8);
  }

  ContentGenerator(ContentGeneratorBuilder contentgeneratorbuilder) {
    this.keySize = contentgeneratorbuilder.keySize;
    this.bufferSize = contentgeneratorbuilder.bufferSize;
    this.copyBufferSize = contentgeneratorbuilder.copyBufferSize;
    buffer = RandomStringUtils.randomAscii(bufferSize)
        .getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Write the required bytes to the output stream.
   */
  public void write(OutputStream outputStream) throws IOException {
    for (long nrRemaining = keySize;
         nrRemaining > 0; nrRemaining -= bufferSize) {
      int curSize = (int) Math.min(bufferSize, nrRemaining);
      if (copyBufferSize == 1) {
        for (int i = 0; i < curSize; i++) {
          outputStream.write(buffer[i]);
        }
      } else {
        for (int i = 0; i < curSize; i += copyBufferSize) {
          outputStream.write(buffer, i,
              Math.min(copyBufferSize, curSize - i));
        }
      }
    }
  }


  public static class ContentGeneratorBuilder{

    private final long keySize; // Required
    private final int bufferSize; // Required
    private int copyBufferSize; // Optional

    ContentGeneratorBuilder(long keysize, int buffersize)
    {
      this.keySize = keysize;
      this.bufferSize = buffersize;
    }

    // Return an object of ContentGeneratorBuilder which will have optional
    // parameter copyBufferSize Initialized
    public ContentGeneratorBuilder copyBufferSize(int copybuffersize){
      this.copyBufferSize = copybuffersize;
      return this;
    }

    //Return the final constructed builder object
    public ContentGenerator build() {
      ContentGenerator contentgenerator =  new ContentGenerator(this);
    // validateBuilderObject(contentgenerator); For validating the builder object
      return contentgenerator;
    }
  }

  @VisibleForTesting
  byte[] getBuffer() {
    return buffer;
  }
}
