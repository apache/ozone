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

package org.apache.hadoop.ozone.common;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;
import org.apache.hadoop.hdds.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ChecksumByteBuffer} implementation based on {@link Checksum}.
 */
public class ChecksumByteBufferImpl implements ChecksumByteBuffer {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChecksumByteBufferImpl.class);

  private final Checksum checksum;

  private static final Field IS_READY_ONLY_FIELD;
  // To access Checksum.update(ByteBuffer) API from Java 9+.
  private static final MethodHandle BYTE_BUFFER_UPDATE;

  static {
    Field f = null;
    if (JavaUtils.isJavaVersionAtMost(8)) {
      try {
        f = ByteBuffer.class
            .getDeclaredField("isReadOnly");
        f.setAccessible(true);
      } catch (NoSuchFieldException e) {
        LOG.error("No isReadOnly field in ByteBuffer", e);
      }
    }
    IS_READY_ONLY_FIELD = f;

    MethodHandle byteBufferUpdate = null;
    if (JavaUtils.isJavaVersionAtLeast(9)) {
      try {
        byteBufferUpdate = MethodHandles.publicLookup().findVirtual(Checksum.class, "update",
            MethodType.methodType(void.class, ByteBuffer.class));
      } catch (Throwable t) {
        throw new IllegalStateException("Failed to lookup Checksum.update(ByteBuffer).");
      }
    }
    BYTE_BUFFER_UPDATE = byteBufferUpdate;

  }

  public ChecksumByteBufferImpl(Checksum impl) {
    this.checksum = impl;
  }

  @Override
  // TODO - when we eventually move to a minimum Java version >= 9 this method
  //        should be refactored to simply call checksum.update(buffer), as the
  //        Checksum interface has been enhanced to allow this since Java 9.
  public void update(ByteBuffer buffer) {
    // Prefer JDK9+ implementation that allows ByteBuffer. This allows DirectByteBuffer to be checksum directly in
    // native memory.
    if (BYTE_BUFFER_UPDATE != null) {
      try {
        BYTE_BUFFER_UPDATE.invokeExact(checksum, buffer);
        return;
      } catch (Throwable e) {
        throw new IllegalStateException("Error invoking " + BYTE_BUFFER_UPDATE,  e);
      }
    }

    // this is a hack to not do memory copy.
    if (IS_READY_ONLY_FIELD != null) {
      try {
        IS_READY_ONLY_FIELD.setBoolean(buffer, false);
      } catch (IllegalAccessException e) {
        LOG.error("Cannot access isReadOnly in ByteBuffer", e);
      }
    }

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
