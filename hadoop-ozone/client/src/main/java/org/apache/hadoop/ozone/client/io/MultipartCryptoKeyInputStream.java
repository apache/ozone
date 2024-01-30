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

import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.apache.hadoop.hdds.scm.storage.MultipartInputStream;
import org.apache.hadoop.hdds.scm.storage.PartInputStream;

import java.util.List;

/**
 * {@link MultipartInputStream} for accessing MPU keys in encrypted buckets.
 * see also {@link OzoneCryptoInputStream}
 */
public class MultipartCryptoKeyInputStream extends MultipartInputStream {

  public MultipartCryptoKeyInputStream(
      String keyName,
      List<? extends PartInputStream> inputStreams) {
    super(" Key: " + keyName, inputStreams);
  }

  @Override
  protected int getNumBytesToRead(ByteReaderStrategy strategy,
                                  PartInputStream current) {
    //OzoneCryptoInputStream read position is aligned with
    // the crypto buffer boundary. We don't limit the requested length here
    // to let OzoneCryptoInputStream decide itself how many bytes to read.
    return strategy.getTargetLength();
  }
}
