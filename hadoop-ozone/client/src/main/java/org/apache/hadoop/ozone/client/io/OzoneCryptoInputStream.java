/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.io;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.crypto.CryptoStreamUtils;
import org.apache.hadoop.fs.Seekable;

/**
 * A CryptoInputStream for Ozone with length. This stream is used to read
 * Keys in Encrypted Buckets.
 */
public class OzoneCryptoInputStream extends CryptoInputStream
    implements Seekable {

  private final long length;
  private final int bufferSize;

  public OzoneCryptoInputStream(LengthInputStream in,
      CryptoCodec codec, byte[] key, byte[] iv) throws IOException {
    super(in.getWrappedStream(), codec, key, iv);
    this.length = in.getLength();
    // This is the buffer size used while creating the CryptoInputStream
    // internally
    this.bufferSize = CryptoStreamUtils.getBufferSize(codec.getConf());
  }

  public long getLength() {
    return length;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public long getRemaining() throws IOException {
    return length - getPos();
  }
}
