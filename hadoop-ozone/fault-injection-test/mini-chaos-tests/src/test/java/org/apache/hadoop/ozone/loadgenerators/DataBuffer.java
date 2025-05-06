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

package org.apache.hadoop.ozone.loadgenerators;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.StorageUnit;

/**
 * List of buffers used by the load generators.
 */
public class DataBuffer {
  private List<ByteBuffer> buffers;
  // number of buffer to be allocated, each is allocated with length which
  // is multiple of 2, each buffer is populated with random data.
  private int numBuffers;

  public DataBuffer(int numBuffers) {
    // allocate buffers and populate random data.
    this.numBuffers = numBuffers;
    this.buffers = new ArrayList<>();
    for (int i = 0; i < numBuffers; i++) {
      int size = (int) StorageUnit.KB.toBytes(1 << i);
      ByteBuffer buffer = ByteBuffer.allocate(size);
      buffer.put(RandomUtils.secure().randomBytes(size));
      this.buffers.add(buffer);
    }
    // TODO: add buffers of sizes of prime numbers.
  }

  public ByteBuffer getBuffer(int keyIndex) {
    return buffers.get(keyIndex % numBuffers);
  }

}
