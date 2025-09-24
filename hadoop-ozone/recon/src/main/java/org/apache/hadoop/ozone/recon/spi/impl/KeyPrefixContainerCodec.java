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
    final int delimiterLength = delimiterBytes.length;

    // Check if we have at least one delimiter + long value
    if (totalLength >= delimiterLength + Long.BYTES) {
      // Check for delimiter before the last 8 bytes - could be containerId or version
      boolean hasLastDelimiter = true;
      int lastDelimiterStart = startPosition + totalLength - Long.BYTES - delimiterLength;
      byteBuffer.position(lastDelimiterStart);
      for (byte delimiterByte : delimiterBytes) {
        if (byteBuffer.get() != delimiterByte) {
          hasLastDelimiter = false;
          break;
        }
      }

      if (hasLastDelimiter) {
        // Extract the last value
        long lastValue = byteBuffer.getLong();
        int remainingLength = lastDelimiterStart - startPosition;

        // Check if there's another delimiter+long before this one
        if (remainingLength >= delimiterLength + Long.BYTES) {
          boolean hasSecondLastDelimiter = true;
          int secondLastDelimiterStart = startPosition + remainingLength - Long.BYTES - delimiterLength;
          byteBuffer.position(secondLastDelimiterStart);
          for (byte delimiterByte : delimiterBytes) {
            if (byteBuffer.get() != delimiterByte) {
              hasSecondLastDelimiter = false;
              break;
            }
          }

          if (hasSecondLastDelimiter) {
            long version = byteBuffer.getLong();

            byteBuffer.position(startPosition);
            String keyPrefix = decodeStringFromBuffer(byteBuffer, secondLastDelimiterStart - startPosition);
            return KeyPrefixContainer.get(keyPrefix, version, lastValue);
          }
        }

        // Only one delimiter+value pair - it's a version, not containerId
        byteBuffer.position(startPosition);
        String keyPrefix = decodeStringFromBuffer(byteBuffer, remainingLength);
        return KeyPrefixContainer.get(keyPrefix, lastValue, -1);
      }
    }

    byteBuffer.position(startPosition);
    String keyPrefix = decodeStringFromBuffer(byteBuffer, totalLength);
    return KeyPrefixContainer.get(keyPrefix, -1, -1);
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
