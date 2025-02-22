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

package org.apache.hadoop.hdds.utils.io;

import jakarta.annotation.Nonnull;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * An {@link FilterOutputStream} recording the length of bytes written.
 */
public class LengthOutputStream extends FilterOutputStream {
  private int length;

  /**
   * Create an {@link FilterOutputStream}
   * which writes to the given underlying {@link OutputStream}.
   */
  public LengthOutputStream(OutputStream out) {
    super(out);
  }

  /** @return the length. */
  public int getLength() {
    return length;
  }

  @Override
  public void write(int b) throws IOException {
    out.write(b);
    length++;
  }

  @Override
  public void write(@Nonnull byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
    length += len;
  }
}
