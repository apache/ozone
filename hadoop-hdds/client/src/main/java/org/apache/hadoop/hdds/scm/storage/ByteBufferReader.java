/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.ByteBufferReadable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * An {@link ByteReaderStrategy} implementation which supports ByteBuffer as the
 * input read data buffer.
 */
public class ByteBufferReader implements ByteReaderStrategy {
  private final ByteBuffer readBuf;
  private int targetLen;

  public ByteBufferReader(ByteBuffer buf) {
    if (buf == null) {
      throw new NullPointerException();
    }
    this.readBuf = buf;
    this.targetLen = buf.remaining();
  }

  @Override
  public int readFromBlock(InputStream is, int numBytesToRead) throws
      IOException {
    Preconditions.checkArgument(is != null);
    Preconditions.checkArgument(is instanceof ByteBufferReadable);
    // change buffer limit
    int bufferLimit = readBuf.limit();
    if (numBytesToRead < targetLen) {
      readBuf.limit(readBuf.position() + numBytesToRead);
    }
    int numBytesRead;
    try {
      numBytesRead = ((ByteBufferReadable)is).read(readBuf);
    } finally {
      // restore buffer limit
      if (numBytesToRead < targetLen) {
        readBuf.limit(bufferLimit);
      }
    }
    targetLen -= numBytesRead;
    return numBytesRead;
  }

  @Override
  public int getTargetLength() {
    return this.targetLen;
  }
}