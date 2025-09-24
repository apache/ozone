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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import jakarta.annotation.Nonnull;
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
  public CodecBuffer toCodecBuffer(@Nonnull KeyPrefixContainer object, CodecBuffer.Allocator allocator) throws CodecException {
    Preconditions.checkNotNull(object, "Null object can't be converted to CodecBuffer.");

    final byte[] keyPrefixBytes = object.getKeyPrefix().getBytes(UTF_8);
    int totalSize = keyPrefixBytes.length;

    if (object.getKeyVersion() != -1) {
      totalSize += KEY_DELIMITER.getBytes(UTF_8).length + Long.BYTES;
    }
    if (object.getContainerId() != -1) {
      totalSize += KEY_DELIMITER.getBytes(UTF_8).length + Long.BYTES;
    }

    final CodecBuffer buffer = allocator.apply(totalSize);
    buffer.put(ByteBuffer.wrap(keyPrefixBytes));

    if (object.getKeyVersion() != -1) {
      buffer.put(ByteBuffer.wrap(KEY_DELIMITER.getBytes(UTF_8)));
      buffer.putLong(object.getKeyVersion());
    }

    if (object.getContainerId() != -1) {
      buffer.put(ByteBuffer.wrap(KEY_DELIMITER.getBytes(UTF_8)));
      buffer.putLong(object.getContainerId());
    }

    return buffer;
  }

  @Override
  public KeyPrefixContainer fromCodecBuffer(@Nonnull CodecBuffer buffer) throws CodecException {

    Object wrapped = buffer.getWrapped();
    if (wrapped instanceof byte[]) {
      return fromPersistedFormat((byte[]) wrapped);
    }

    final ByteBuffer byteBuffer = buffer.asReadOnlyByteBuffer();
    final int totalLength = byteBuffer.remaining();
    final int startPosition = byteBuffer.position();

    final byte[] delimiterBytes = KEY_DELIMITER.getBytes(UTF_8);

    int firstDelimiterIndex = findDelimiterInBuffer(byteBuffer, delimiterBytes);

    byteBuffer.position(startPosition);

    // If no delimiter found, entire buffer is key prefix
    if (firstDelimiterIndex == -1) {
      String keyPrefix = decodeStringFromBuffer(byteBuffer, totalLength);
      return KeyPrefixContainer.get(keyPrefix, -1, -1);
    }

    // Decode key prefix without copying
    String keyPrefix = decodeStringFromBuffer(byteBuffer, firstDelimiterIndex);

    // Skip delimiter
    byteBuffer.position(byteBuffer.position() + delimiterBytes.length);

    long version = -1;
    long containerId = -1;

    if (byteBuffer.remaining() >= Long.BYTES) {
      version = byteBuffer.getLong();

      if (byteBuffer.remaining() >= delimiterBytes.length + Long.BYTES) {
        // Skip delimiter
        byteBuffer.position(byteBuffer.position() + delimiterBytes.length);
        containerId = byteBuffer.getLong();
      }
    }

    return KeyPrefixContainer.get(keyPrefix, version, containerId);
  }

  /**
   * Find delimiter in ByteBuffer without copying data.
   * Returns relative position of delimiter, or -1 if not found.
   */
  private int findDelimiterInBuffer(ByteBuffer buffer, byte[] delimiter) {
    final int startPos = buffer.position();
    final int limit = buffer.limit();

    for (int i = startPos; i <= limit - delimiter.length; i++) {
      boolean found = true;
      for (int j = 0; j < delimiter.length; j++) {
        if (buffer.get(i + j) != delimiter[j]) {
          found = false;
          break;
        }
      }
      if (found) {
        return i - startPos;
      }
    }
    return -1;
  }

  /**
   * Decode string from ByteBuffer without copying to intermediate byte array.
   * Uses CharsetDecoder for efficient decoding.
   */
  private String decodeStringFromBuffer(ByteBuffer buffer, int length) throws CodecException {
    if (length == 0) {
      return "";
    }

    try {
      ByteBuffer slice = buffer.duplicate();
      slice.limit(buffer.position() + length);

      buffer.position(buffer.position() + length);

      CharsetDecoder decoder = UTF_8.newDecoder();
      CharBuffer charBuffer = decoder.decode(slice);
      return charBuffer.toString();

    } catch (CharacterCodingException e) {
      throw new CodecException("Failed to decode UTF-8 string from buffer", e);
    }
  }

  @Override
  public byte[] toPersistedFormat(KeyPrefixContainer keyPrefixContainer) {
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
