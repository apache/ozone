/**
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

package org.apache.hadoop.ozone.client.io;

import java.io.FilterInputStream;
import java.io.InputStream;

/**
 * An input stream with length.
 */
public class LengthInputStream extends FilterInputStream {

  private final long length;

  /**
   * Create an stream.
   * @param in the underlying input stream.
   * @param length the length of the stream.
   */
  public LengthInputStream(InputStream in, long length) {
    super(in);
    this.length = length;
  }

  /** @return the length. */
  public long getLength() {
    return length;
  }

  public InputStream getWrappedStream() {
    return in;
  }
}
