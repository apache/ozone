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

import org.apache.hadoop.util.PureJavaCrc32C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class ChecksumByteBufferImpl implements ChecksumByteBuffer {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChecksumByteBufferImpl.class);

  private static volatile boolean useJava9Crc32C = isJavaVersionAtLeast(9);

  // "1.8"->8, "9"->9, "10"->10
  private static final int JAVA_SPEC_VER = Math.max(8, Integer.parseInt(
      System.getProperty("java.specification.version").split("\\.")[0]));

  /**
   * Query to see if major version of Java specification of the system
   * is equal or greater than the parameter.
   *
   * @param version 8, 9, 10 etc.
   * @return comparison with system property, always true for 8
   */
  public static boolean isJavaVersionAtLeast(int version) {
    return JAVA_SPEC_VER >= version;
  }

  public static class Java9Crc32CFactory {
    private static final MethodHandle NEW_CRC32C_MH;

    static {
      MethodHandle newCRC32C = null;
      try {
        newCRC32C = MethodHandles.publicLookup()
            .findConstructor(
                Class.forName("java.util.zip.CRC32C"),
                MethodType.methodType(void.class)
            );
      } catch (ReflectiveOperationException e) {
        // Should not reach here.
        throw new RuntimeException(e);
      }
      NEW_CRC32C_MH = newCRC32C;
    }

    public static java.util.zip.Checksum createChecksum() {
      try {
        // Should throw nothing
        return (Checksum) NEW_CRC32C_MH.invoke();
      } catch (Throwable t) {
        throw (t instanceof RuntimeException) ? (RuntimeException) t
            : new RuntimeException(t);
      }
    }
  };

  public static ChecksumByteBuffer crc32Impl() {
    return new ChecksumByteBufferImpl(new CRC32());
  }

  public static ChecksumByteBuffer crc32Cimpl() {
    if (useJava9Crc32C) {
      try {
        return new ChecksumByteBufferImpl(Java9Crc32CFactory.createChecksum());
      } catch (Throwable e) {
        // should not happen
        LOG.error("CRC32C creation failed, switching to PureJavaCrc32C", e);
        useJava9Crc32C = false;
      }
    }
    return new ChecksumByteBufferImpl(new PureJavaCrc32C());
  }

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