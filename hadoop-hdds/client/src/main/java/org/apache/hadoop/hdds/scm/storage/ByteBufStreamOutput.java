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

import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;

/**
* This interface is for writing an output stream of ByteBuffers.
* An ByteBufStreamOutput accepts Netty ByteBuf and sends them to some sink.
*/
public interface ByteBufStreamOutput extends Closeable {
  /**
   * Try to write all the bytes in ByteBuf b to DataStream.
   *
   * @param b the data.
   * @exception IOException if an I/O error occurs.
   */
  void write(ByteBuf b) throws IOException;

  /**
   * Try to write the [off:off + len) slice in ByteBuf b to DataStream.
   *
   * @param b the data.
   * @param off the start offset in the data.
   * @param len the number of bytes to write.
   * @exception  IOException  if an I/O error occurs.
   */
  default void write(ByteBuf b, int off, int len) throws IOException {
    write(b.slice(off, len));
  }

  /**
   * Flushes this DataStream output and forces any buffered output bytes
   * to be written out.
   *
   * @exception  IOException  if an I/O error occurs.
   */
  void flush() throws IOException;
}
