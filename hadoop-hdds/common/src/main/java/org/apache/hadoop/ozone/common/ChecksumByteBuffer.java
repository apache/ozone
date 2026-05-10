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

/*
 * Some portions of this file Copyright (c) 2004-2006 Intel Corporation and licensed under the BSD license.
 */

package org.apache.hadoop.ozone.common;

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

/**
 * A sub-interface of {@link Checksum}
 * with a method to update checksum from a {@link ByteBuffer}.
 */
public interface ChecksumByteBuffer extends Checksum {
  /**
   * Updates the current checksum with the specified bytes in the buffer.
   * Upon return, the buffer's position will be equal to its limit.
   *
   * @param buffer the bytes to update the checksum with
   *
   * @apiNote {@link Override} annotation is missing since {@link Checksum#update(ByteBuffer)} introduced only in Java9.
   * TODO: Remove when Java 1.8 support is dropped.
   * TODO: <a href="https://issues.apache.org/jira/browse/HDDS-12366">HDDS-12366</a>
   */
  @SuppressWarnings("PMD.MissingOverride")
  void update(ByteBuffer buffer);

  @Override
  default void update(byte[] b, int off, int len) {
    update(ByteBuffer.wrap(b, off, len).asReadOnlyBuffer());
  }
}
