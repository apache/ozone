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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.api.types;

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.ratis.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.IntFunction;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * An implementation of both {@link ContainerKeyPrefix}
 * and {@link KeyPrefixContainer}.
 * <p>
 * This class is immutable.
 */
final class ContainerKeyPrefixImpl
    implements ContainerKeyPrefix, KeyPrefixContainer {
  static ContainerKeyPrefixImpl get(long containerId, String keyPrefix,
      long keyVersion) {
    return new ContainerKeyPrefixImpl(containerId, keyPrefix, keyVersion);
  }

  private final long containerId;
  private final String keyPrefix;
  private final long keyVersion;

  private ContainerKeyPrefixImpl(long containerId, String keyPrefix,
      long keyVersion) {
    this.containerId = containerId;
    this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
    this.keyVersion = keyVersion;
  }

  @Override
  public long getContainerId() {
    return containerId;
  }

  @Override
  public @Nonnull String getKeyPrefix() {
    return keyPrefix;
  }

  @Override
  public long getKeyVersion() {
    return keyVersion;
  }

  @Override
  public ContainerKeyPrefix toContainerKeyPrefix() {
    return this;
  }

  @Override
  public KeyPrefixContainer toKeyPrefixContainer() {
    return keyPrefix.isEmpty() ? null : this;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(containerId).hashCode()
        + 13 * keyPrefix.hashCode()
        + 17 * Long.valueOf(keyVersion).hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof ContainerKeyPrefixImpl)) {
      return false;
    }
    final ContainerKeyPrefixImpl that = (ContainerKeyPrefixImpl) o;
    return this.containerId == that.containerId
        && Objects.equals(this.keyPrefix, that.keyPrefix)
        && this.keyVersion == that.keyVersion;
  }

  @Override
  public String toString() {
    return "Impl{" +
        "containerId=" + containerId +
        ", keyPrefix='" + keyPrefix + '\'' +
        ", keyVersion=" + keyVersion +
        '}';
  }

  /**
   * For implementing {@link ContainerKeyPrefix#getCodec()}
   * and {@link KeyPrefixContainer#getCodec()}.
   * These two codecs are essentially the same
   * except for the serialization ordering.
   */
  abstract static class CodecBase implements Codec<ContainerKeyPrefix> {
    /** A delimiter to separate fields in the serializations. */
    public static class Delimiter {
      private static final String VALUE = "_";
      private static final byte[] BYTES = VALUE.getBytes(UTF_8);
      private static final byte[] COPY = Arrays.copyOf(BYTES, BYTES.length);

      static byte[] getBytes() {
        Preconditions.assertTrue(Arrays.equals(COPY, BYTES));
        return BYTES;
      }

      static int length() {
        return BYTES.length;
      }

      static CodecBuffer skip(CodecBuffer buffer) {
        final String d = buffer.getUtf8(BYTES.length);
        Preconditions.assertTrue(Objects.equals(d, VALUE),
            () -> "Unexpected delimiter: \"" + d
                + "\", KEY_DELIMITER = \"" + VALUE + "\"");
        return buffer;
      }
    }

    CodecBase() { }

    @Override
    public final int getSerializedSizeUpperBound(ContainerKeyPrefix object) {
      final String keyPrefix = object.getKeyPrefix();
      return Long.BYTES // containerId
          + Delimiter.length()
          + StringCodec.get().getSerializedSizeUpperBound(keyPrefix)
          + Delimiter.length()
          + Long.BYTES; // keyVersion
    }

    @Override
    public final boolean supportCodecBuffer() {
      return true;
    }

    @Override
    public abstract CodecBuffer toCodecBuffer(
        @Nonnull ContainerKeyPrefix object, IntFunction<CodecBuffer> allocator);

    @Override
    public abstract ContainerKeyPrefix fromCodecBuffer(
        @Nonnull CodecBuffer buffer);

    @Override
    public final byte[] toPersistedFormat(ContainerKeyPrefix object) {
      try (CodecBuffer b = toCodecBuffer(object, CodecBuffer::allocateHeap)) {
        return b.getArray();
      }
    }

    @Override
    public final ContainerKeyPrefix fromPersistedFormat(byte[] rawData) {
      return fromCodecBuffer(CodecBuffer.wrap(rawData));
    }

    @Override
    public final ContainerKeyPrefix copyObject(ContainerKeyPrefix object) {
      return object;
    }
  }
}
