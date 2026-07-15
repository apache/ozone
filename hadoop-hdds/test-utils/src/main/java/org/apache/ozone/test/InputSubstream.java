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

package org.apache.ozone.test;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * A filter input stream implementation that exposes a range of the underlying input stream.
 */
public class InputSubstream extends FilterInputStream {
  private static final int MAX_SKIPS = 100;
  private long currentPosition;
  private final long requestedSkipOffset;
  private final long requestedLength;
  private long markedPosition = 0;

  public InputSubstream(InputStream in, long skip, long length) {
    super(Objects.requireNonNull(in, "in == null"));
    this.currentPosition = 0;
    this.requestedSkipOffset = skip;
    this.requestedLength = length;
  }

  @Override
  public int read() throws IOException {
    byte[] b = new byte[1];
    int bytesRead = read(b, 0, 1);

    if (bytesRead == -1) {
      return bytesRead;
    }
    return b[0];
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int count = 0;
    while (currentPosition < requestedSkipOffset) {
      long skippedBytes = super.skip(requestedSkipOffset - currentPosition);
      if (skippedBytes == 0) {
        count++;
        if (count > MAX_SKIPS) {
          throw new IOException(
              "Unable to position the currentPosition from "
                  + currentPosition + " to "
                  + requestedSkipOffset);
        }
      }
      currentPosition += skippedBytes;
    }

    long bytesRemaining =
        (requestedLength + requestedSkipOffset) - currentPosition;
    if (bytesRemaining <= 0) {
      return -1;
    }

    len = (int) Math.min(len, bytesRemaining);
    int bytesRead = super.read(b, off, len);
    currentPosition += bytesRead;

    return bytesRead;
  }

  @Override
  public synchronized void mark(int readlimit) {
    markedPosition = currentPosition;
    super.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    currentPosition = markedPosition;
    super.reset();
  }

  @Override
  public void close() throws IOException {
    // No-op operation since we don't want to close the underlying stream
    // when the susbtream has been read
  }

  @Override
  public int available() throws IOException {
    long bytesRemaining;
    if (currentPosition < requestedSkipOffset) {
      bytesRemaining = requestedLength;
    } else {
      bytesRemaining =
          (requestedLength + requestedSkipOffset) - currentPosition;
    }

    return (int) Math.min(bytesRemaining, super.available());
  }
}
