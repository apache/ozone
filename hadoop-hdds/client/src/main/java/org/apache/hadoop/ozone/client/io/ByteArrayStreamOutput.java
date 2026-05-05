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

package org.apache.hadoop.ozone.client.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput;

/**
 * An {@link OutputStream} supporting {@link ByteBufferStreamOutput}.
 * The subclass classes of this class must implement and optimize
 * the {@link OutputStream#write(byte[], int, int)} method
 * and optionally override
 * the {@link ByteBufferStreamOutput#write(ByteBuffer, int, int)} method.
 */
public abstract class ByteArrayStreamOutput extends OutputStream
    implements ByteBufferStreamOutput {
  private static final int ARRAY_SIZE_LIMIT = 64 << 10; //64KB

  @Override
  public void write(ByteBuffer buffer, int off, int len) throws IOException {
    if (len == 0) {
      return;
    }

    if (buffer.hasArray()) {
      write(buffer.array(), off, len);
    } else {
      final byte[] array = new byte[Math.min(ARRAY_SIZE_LIMIT, len)];
      for (; len > 0;) {
        final ByteBuffer readonly = buffer.asReadOnlyBuffer();
        final int writeSize = Math.min(array.length, len);
        readonly.position(off);
        off += writeSize;
        readonly.limit(off);
        readonly.get(array, 0, writeSize);
        write(array, 0, writeSize);
        len -= writeSize;
      }
    }
  }

  @Override
  public void write(int b) throws IOException {
    write(new byte[]{(byte) b});
  }
}
