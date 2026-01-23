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

package org.apache.hadoop.ozone.recon.spi.impl;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.primitives.Longs;
import jakarta.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.ozone.recon.api.types.KeyPrefixContainer;

/**
 * Codec to serialize/deserialize {@link KeyPrefixContainer}.
 */
public final class KeyPrefixContainerCodec
    implements Codec<KeyPrefixContainer> {

  private static final Codec<KeyPrefixContainer> INSTANCE =
      new KeyPrefixContainerCodec();

  private static final String KEY_DELIMITER = "_";
  private static final byte[] KEY_DELIMITER_BYTES = KEY_DELIMITER.getBytes(UTF_8);
  private static final ByteBuffer KEY_DELIMITER_BUFFER = ByteBuffer.wrap(KEY_DELIMITER_BYTES).asReadOnlyBuffer();
  public static final int LONG_SERIALIZED_SIZE = KEY_DELIMITER_BYTES.length + Long.BYTES;

  public static Codec<KeyPrefixContainer> get() {
    return INSTANCE;
  }

  private KeyPrefixContainerCodec() {
    // singleton
  }

  @Override
  public Class<KeyPrefixContainer> getTypeClass() {
    return KeyPrefixContainer.class;
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull KeyPrefixContainer object, CodecBuffer.Allocator allocator) {
    Objects.requireNonNull(object, "Null object can't be converted to CodecBuffer.");

    final byte[] keyPrefixBytes = object.getKeyPrefix().getBytes(UTF_8);
    int totalSize = keyPrefixBytes.length;

    if (object.getKeyVersion() != -1) {
      totalSize += LONG_SERIALIZED_SIZE;

      if (object.getContainerId() != -1) {
        totalSize += LONG_SERIALIZED_SIZE;
      }
    }

    final CodecBuffer buffer = allocator.apply(totalSize);
    buffer.put(ByteBuffer.wrap(keyPrefixBytes));

    if (object.getKeyVersion() != -1) {
      buffer.put(KEY_DELIMITER_BUFFER.duplicate());
      buffer.putLong(object.getKeyVersion());

      if (object.getContainerId() != -1) {
        buffer.put(KEY_DELIMITER_BUFFER.duplicate());
        buffer.putLong(object.getContainerId());
      }
    }

    return buffer;
  }

  @Override
  public KeyPrefixContainer fromCodecBuffer(@Nonnull CodecBuffer buffer) throws CodecException {
    final ByteBuffer byteBuffer = buffer.asReadOnlyByteBuffer();
    final int totalLength = byteBuffer.remaining();

    if (totalLength == 0) {
      throw new CodecException("Empty buffer");
    }

    final byte[] data = new byte[totalLength];
    byteBuffer.get(data);

    int lastDelimiter = findLastDelimiter(data);
    if (lastDelimiter == -1) {
      return KeyPrefixContainer.get(new String(data, UTF_8));
    }

    int secondLastDelimiter = findLastDelimiter(data, lastDelimiter - 1);
    if (secondLastDelimiter == -1) {
      String keyPrefix = new String(data, 0, lastDelimiter, UTF_8);
      long version = Longs.fromByteArray(ArrayUtils.subarray(data,
          lastDelimiter + 1, lastDelimiter + 1 + Long.BYTES));
      return KeyPrefixContainer.get(keyPrefix, version);
    }

    String keyPrefix = new String(data, 0, secondLastDelimiter, UTF_8);
    long version = Longs.fromByteArray(ArrayUtils.subarray(data,
        secondLastDelimiter + 1, secondLastDelimiter + 1 + Long.BYTES));
    long containerId = Longs.fromByteArray(ArrayUtils.subarray(data,
        lastDelimiter + 1, lastDelimiter + 1 + Long.BYTES));

    return KeyPrefixContainer.get(keyPrefix, version, containerId);
  }

  private int findLastDelimiter(byte[] data) {
    return findLastDelimiter(data, data.length - 1);
  }

  private int findLastDelimiter(byte[] data, int endPos) {
    for (int i = endPos - Long.BYTES; i >= 0; i--) {
      if (data[i] == '_') {
        return i;
      }
    }
    return -1;
  }

  @Override
  public byte[] toPersistedFormat(KeyPrefixContainer keyPrefixContainer) {
    Objects.requireNonNull(keyPrefixContainer,
            "Null object can't be converted to byte array.");
    byte[] keyPrefixBytes = keyPrefixContainer.getKeyPrefix().getBytes(UTF_8);

    // Prefix seek can be done only with keyPrefix. In that case, we can
    // expect the version and the containerId to be undefined.
    if (keyPrefixContainer.getKeyVersion() != -1) {
      keyPrefixBytes = ArrayUtils.addAll(keyPrefixBytes, KEY_DELIMITER_BYTES);
      keyPrefixBytes = ArrayUtils.addAll(keyPrefixBytes, Longs.toByteArray(
          keyPrefixContainer.getKeyVersion()));
      if (keyPrefixContainer.getContainerId() != -1) {
        keyPrefixBytes = ArrayUtils.addAll(keyPrefixBytes, KEY_DELIMITER_BYTES);
        keyPrefixBytes = ArrayUtils.addAll(keyPrefixBytes, Longs.toByteArray(
            keyPrefixContainer.getContainerId()));
      }
    }

    return keyPrefixBytes;
  }

  @Override
  public KeyPrefixContainer fromPersistedFormat(byte[] rawData) {
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
    return KeyPrefixContainer.get(keyPrefix, version, containerIdFromDB);
  }

  @Override
  public KeyPrefixContainer copyObject(KeyPrefixContainer object) {
    return object;
  }
}
