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

import java.nio.ByteBuffer;

/**
 * Used for streaming write.
 */
public class StreamBuffer {
  private final ByteBuffer buffer;

  public StreamBuffer(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  public StreamBuffer(ByteBuffer buffer, int offset, int length) {
    this((ByteBuffer) buffer.asReadOnlyBuffer().position(offset)
        .limit(offset + length));
  }

  public ByteBuffer duplicate() {
    return buffer.duplicate();
  }

  public int remaining() {
    return buffer.remaining();
  }

  public int position() {
    return buffer.position();
  }

  public void put(StreamBuffer sb) {
    buffer.put(sb.buffer);
  }

  public static StreamBuffer allocate(int size) {
    return new StreamBuffer(ByteBuffer.allocate(size));
  }

}
