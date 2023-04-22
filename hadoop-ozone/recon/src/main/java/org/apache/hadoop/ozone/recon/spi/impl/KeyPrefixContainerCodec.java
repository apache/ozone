/*
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

package org.apache.hadoop.ozone.recon.spi.impl;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.recon.api.types.KeyPrefixContainer;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.commons.compress.utils.CharsetNames.UTF_8;

/**
 * Codec to encode KeyPrefixContainer as byte array.
 */
public class KeyPrefixContainerCodec implements Codec<KeyPrefixContainer> {

  private static final String KEY_DELIMITER = "_";

  @Override
  public byte[] toPersistedFormat(KeyPrefixContainer keyPrefixContainer)
      throws IOException {
    Preconditions.checkNotNull(keyPrefixContainer,
            "Null object can't be converted to byte array.");
    byte[] keyPrefixBytes = keyPrefixContainer.getKeyPrefix().getBytes(UTF_8);

    //Prefix seek can be done only with keyPrefix. In that case, we can
    // expect the version and the containerId to be undefined.
    if (keyPrefixContainer.getKeyVersion() != -1) {
      keyPrefixBytes = ArrayUtils.addAll(keyPrefixBytes, KEY_DELIMITER
          .getBytes(UTF_8));
      keyPrefixBytes = ArrayUtils.addAll(keyPrefixBytes, Longs.toByteArray(
          keyPrefixContainer.getKeyVersion()));
    }

    if (keyPrefixContainer.getContainerId() != -1) {
      keyPrefixBytes = ArrayUtils.addAll(keyPrefixBytes, KEY_DELIMITER
          .getBytes(UTF_8));
      keyPrefixBytes = ArrayUtils.addAll(keyPrefixBytes, Longs.toByteArray(
          keyPrefixContainer.getContainerId()));
    }

    return keyPrefixBytes;
  }

  @Override
  public KeyPrefixContainer fromPersistedFormat(byte[] rawData)
      throws IOException {

    // When reading from byte[], we can always expect to have the key, version
    // and version parts in the byte array.
    byte[] keyBytes = ArrayUtils.subarray(rawData,
        0, rawData.length - Long.BYTES * 2 - 2);
    String keyPrefix = new String(keyBytes, UTF_8);

    // Second 8 bytes is the key version.
    byte[] versionBytes = ArrayUtils.subarray(rawData,
        rawData.length - Long.BYTES * 2 - 1,
        rawData.length - Long.BYTES - 1);
    long version = ByteBuffer.wrap(versionBytes).getLong();

    // Last 8 bytes is the containerId.
    long containerIdFromDB = ByteBuffer.wrap(ArrayUtils.subarray(rawData,
        rawData.length - Long.BYTES,
        rawData.length)).getLong();
    return new KeyPrefixContainer(keyPrefix, version, containerIdFromDB);
  }

  @Override
  public KeyPrefixContainer copyObject(KeyPrefixContainer object) {
    return object;
  }
}
