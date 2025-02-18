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

package org.apache.hadoop.hdds.scm.storage;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;

/**
 * An {@link ByteReaderStrategy} implementation which supports byte[] as the
 * input
 * read data buffer.
 */
public class ByteArrayReader implements ByteReaderStrategy {
  private final byte[] readBuf;
  private int offset;
  private int targetLen;

  public ByteArrayReader(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }

    this.readBuf = b;
    this.offset = off;
    this.targetLen = len;
  }

  @Override
  public int readFromBlock(InputStream is, int numBytesToRead) throws
      IOException {
    Preconditions.checkArgument(is != null);
    int numBytesRead = is.read(readBuf, offset, numBytesToRead);
    offset += numBytesRead;
    targetLen -= numBytesRead;
    return numBytesRead;
  }

  @Override
  public int getTargetLength() {
    return this.targetLen;
  }
}
