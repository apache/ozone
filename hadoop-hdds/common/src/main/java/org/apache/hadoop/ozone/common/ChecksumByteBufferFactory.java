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
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.apache.hadoop.hdds.JavaUtils;
import org.apache.hadoop.util.PureJavaCrc32C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class containing factories for creating various checksum impls.
 */
public final class ChecksumByteBufferFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChecksumByteBufferImpl.class);

  private static volatile boolean useJava9Crc32C
      = JavaUtils.isJavaVersionAtLeast(9);

  /**
   * Factory for CRC32C implementation introduced in Java 9.
   */
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
  }

  public static ChecksumByteBuffer crc32Impl() {
    return new ChecksumByteBufferImpl(new CRC32());
  }

  public static ChecksumByteBuffer crc32CImpl() {
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

  /**
   * Private Constructor.
   */
  private ChecksumByteBufferFactory() {
  }
}
