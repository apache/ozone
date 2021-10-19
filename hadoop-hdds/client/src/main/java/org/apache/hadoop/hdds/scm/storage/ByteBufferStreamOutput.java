/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
* This interface is for writing an output stream of ByteBuffers.
* An ByteBufferStreamOutput accepts nio ByteBuffer and sends them to some sink.
*/
public interface ByteBufferStreamOutput extends Closeable {
  /**
   * Try to write all the bytes in ByteBuf b to DataStream.
   *
   * @param b the data.
   * @exception IOException if an I/O error occurs.
   */
  default void write(ByteBuffer b) throws IOException {
    write(b, b.position(), b.remaining());
  }

  /**
   * Try to write the [off:off + len) slice in ByteBuf b to DataStream.
   *
   * @param b the data.
   * @param off the start offset in the data.
   * @param len the number of bytes to write.
   * @exception  IOException  if an I/O error occurs.
   */
  void write(ByteBuffer b, int off, int len) throws IOException;

  /**
   * Flushes this DataStream output and forces any buffered output bytes
   * to be written out.
   *
   * @exception  IOException  if an I/O error occurs.
   */
  void flush() throws IOException;
}
