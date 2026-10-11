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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.Seekable;

/**
 * A stream that can be a part of a {@link MultipartInputStream}.
 */
public interface PartInputStream
    extends CanUnbuffer, Seekable {
  long getLength();

  default long getRemaining() throws IOException {
    return getLength() - getPos();
  }

  void close() throws IOException;

  /**
   * Positioned read within this part starting at {@code partOffset}.
   *
   * @return bytes copied into {@code buffer}, {@code -1} if {@code buffer} has no remaining space
   *         or at EOF
   * @throws UnsupportedOperationException if this part stream does not support positioned read
   */
  default int readPositioned(long partOffset, ByteBuffer buffer) throws IOException {
    throw new UnsupportedOperationException(
        "Positioned read is not supported by " + getClass().getName());
  }
}
