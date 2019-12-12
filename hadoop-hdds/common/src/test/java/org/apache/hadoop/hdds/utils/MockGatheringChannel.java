/*
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
package org.apache.hadoop.hdds.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;

import static com.google.common.base.Preconditions.checkElementIndex;

/**
 * {@link GatheringByteChannel} implementation for testing.  Delegates
 * to a {@link WritableByteChannel}.
 *
 * @see java.nio.channels.Channels#newChannel(java.io.OutputStream)
 */
public class MockGatheringChannel implements GatheringByteChannel {

  private final WritableByteChannel delegate;

  public MockGatheringChannel(WritableByteChannel delegate) {
    this.delegate = delegate;
  }

  @Override
  public long write(ByteBuffer[] srcs, int offset, int length)
      throws IOException {

    checkElementIndex(offset, srcs.length, "offset");
    checkElementIndex(offset+length-1, srcs.length, "offset+length");

    long bytes = 0;
    for (ByteBuffer b : srcs) {
      bytes += write(b);
    }
    return bytes;
  }

  @Override
  public long write(ByteBuffer[] srcs) throws IOException {
    return write(srcs, 0, srcs.length);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return delegate.write(src);
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
