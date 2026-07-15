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

import org.apache.ratis.util.Preconditions;

class Buffer {
  private final CodecBuffer.Capacity initialCapacity;
  private final PutToByteBuffer<RuntimeException> source;
  private CodecBuffer buffer;

  Buffer(CodecBuffer.Capacity initialCapacity,
      PutToByteBuffer<RuntimeException> source) {
    this.initialCapacity = initialCapacity;
    this.source = source;
  }

  void release() {
    if (buffer != null) {
      buffer.release();
    }
  }

  private void prepare() {
    if (buffer == null) {
      allocate();
    } else {
      buffer.clear();
    }
  }

  private void allocate() {
    if (buffer != null) {
      buffer.release();
    }
    buffer = CodecBuffer.allocateDirect(-initialCapacity.get());
  }

  CodecBuffer getFromDb() {
    if (source == null) {
      return null;
    }

    for (prepare(); ; allocate()) {
      final Integer required = buffer.putFromSource(source);
      if (required == null) {
        return null; // the source is unavailable
      } else if (required == buffer.readableBytes()) {
        return buffer; // buffer size is big enough
      }
      // buffer size too small, try increasing the capacity.
      if (buffer.setCapacity(required)) {
        buffer.clear();
        // retry with the new capacity
        final int retried = buffer.putFromSource(source);
        Preconditions.assertSame(required.intValue(), retried, "required");
        return buffer;
      }

      // failed to increase the capacity
      // increase initial capacity and reallocate it
      initialCapacity.increase(required);
    }
  }
}
