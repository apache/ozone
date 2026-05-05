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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.Syncable;

/**
 * This interface is similar to {@link java.io.OutputStream}
 * except that this class support {@link ByteBuffer} instead of byte[].
 */
public interface ByteBufferStreamOutput extends Closeable, Syncable {
  /**
   * Similar to {@link java.io.OutputStream#write(byte[])},
   * except that the parameter of this method is a {@link ByteBuffer}.
   *
   * @param buffer the buffer containing data to be written.
   * @exception IOException if an I/O error occurs.
   */
  default void write(ByteBuffer buffer) throws IOException {
    final ByteBuffer b = buffer.asReadOnlyBuffer();
    write(b, b.position(), b.remaining());
  }

  /**
   * Similar to {@link java.io.OutputStream#write(byte[], int, int)},
   * except that the parameter of this method is a {@link ByteBuffer}.
   *
   * @param buffer the buffer containing data to be written.
   * @param off the start offset.
   * @param len the number of bytes to write.
   * @exception  IOException  if an I/O error occurs.
   */
  void write(ByteBuffer buffer, int off, int len) throws IOException;

  /**
   * Flush this output and force any buffered output bytes to be written out.
   *
   * @exception  IOException  if an I/O error occurs.
   */
  void flush() throws IOException;
}
