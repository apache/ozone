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

package org.apache.hadoop.ozone.recon.api.types;

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.IntFunction;

/**
 * Class to encapsulate the Key information needed for the Recon container DB.
 * Currently, it is the whole key + key version and the containerId.
 * <p>
 * The implementations of this interface MUST be immutable.
 */
public interface KeyPrefixContainer {

  static KeyPrefixContainer get(String keyPrefix, long keyVersion,
      long containerId) {
    Objects.requireNonNull(keyPrefix, "keyPrefix == null");
    return ContainerKeyPrefixImpl.get(containerId, keyPrefix, keyVersion);
  }

  static KeyPrefixContainer get(String keyPrefix, long keyVersion) {
    return get(keyPrefix, keyVersion, -1);
  }

  static KeyPrefixContainer get(String keyPrefix) {
    return get(keyPrefix, -1, -1);
  }

  long getContainerId();

  @Nonnull String getKeyPrefix();

  long getKeyVersion();

  ContainerKeyPrefix toContainerKeyPrefix();

  static Codec<KeyPrefixContainer> getCodec() {
    return CodecImpl.CODEC;
  }

  /** Implementing {@link Codec} for {@link KeyPrefixContainer}. */
  final class CodecImpl extends ContainerKeyPrefixImpl.CodecBase {
    /**
     * Serialization:
     * (s1) Use {@link KeyPrefixContainer#toContainerKeyPrefix()}.
     * (s2) Use {@link CodecImpl} to serialize.
     * <p>
     * Deserialization:
     * (d1) Use {@link CodecImpl} to deserialize.
     * (d2) Use {@link ContainerKeyPrefix#toKeyPrefixContainer()}.
     *      Note that,
     *      since the object was serialized from a {@link KeyPrefixContainer},
     *      {@link ContainerKeyPrefix#getKeyPrefix()} is always non-empty.
     */
    private static final Codec<KeyPrefixContainer> CODEC = new DelegatedCodec<>(
        new CodecImpl(),
        KeyPrefixContainer.class::cast,
        KeyPrefixContainer::toContainerKeyPrefix,
        DelegatedCodec.CopyType.SHALLOW);

    private CodecImpl() {
    }

    @Override
    public ContainerKeyPrefix fromCodecBuffer(@Nonnull CodecBuffer buffer) {
      final int keyPrefixLength = buffer.readableBytes()
          - 2 * (Long.BYTES + Delimiter.length());
      final String keyPrefix = buffer.getUtf8(keyPrefixLength);
      final long keyVersion = Delimiter.skip(buffer).getLong();
      final long containerId = Delimiter.skip(buffer).getLong();
      return ContainerKeyPrefix.get(containerId, keyPrefix, keyVersion);
    }

    @Override
    public CodecBuffer toCodecBuffer(@Nonnull ContainerKeyPrefix object,
        IntFunction<CodecBuffer> allocator) {
      return allocator.apply(getSerializedSizeUpperBound(object))
          .putUtf8(object.getKeyPrefix())
          .put(Delimiter.getBytes())
          .putLong(object.getKeyVersion())
          .put(Delimiter.getBytes())
          .putLong(object.getContainerId());
    }
  }
}
