/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.common;

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

/**
 * {@link ChecksumByteBuffer} implementation based on {@link Checksum}.
 */
public class ChecksumByteBufferImpl implements ChecksumByteBuffer {

  private Checksum checksum;

  public ChecksumByteBufferImpl(Checksum impl) {
    this.checksum = impl;
  }

  @Override
  // TODO - when we eventually move to a minimum Java version >= 9 this method
  //        should be refactored to simply call checksum.update(buffer), as the
  //        Checksum interface has been enhanced to allow this since Java 9.
  public void update(ByteBuffer buffer) {
    if (buffer.hasArray()) {
      checksum.update(buffer.array(), buffer.position() + buffer.arrayOffset(),
          buffer.remaining());
    } else {
      byte[] b = new byte[buffer.remaining()];
      buffer.get(b);
      checksum.update(b, 0, b.length);
    }
  }

  @Override
  public void update(byte[] b, int off, int len) {
    checksum.update(b, off, len);
  }

  @Override
  public void update(int i) {
    checksum.update(i);
  }

  @Override
  public long getValue() {
    return checksum.getValue();
  }

  @Override
  public void reset() {
    checksum.reset();
  }

}
