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

import javax.annotation.Nonnull;
import java.util.function.IntFunction;

/**
 * Class to encapsulate the Key information needed for the Recon container DB.
 * Currently, it is the containerId and the whole key + key version.
 * <p>
 * The implementations of this interface MUST be immutable.
 */
public interface ContainerKeyPrefix {
  static ContainerKeyPrefix get(long containerId, String keyPrefix,
      long keyVersion) {
    return ContainerKeyPrefixImpl.get(containerId, keyPrefix, keyVersion);
  }

  static ContainerKeyPrefix get(long containerId, String keyPrefix) {
    return get(containerId, keyPrefix, -1);
  }

  static ContainerKeyPrefix get(long containerId) {
    return get(containerId, null, -1);
  }

  long getContainerId();

  @Nonnull String getKeyPrefix();

  long getKeyVersion();

  KeyPrefixContainer toKeyPrefixContainer();

  static Codec<ContainerKeyPrefix> getCodec() {
    return CodecImpl.INSTANCE;
  }

  /** Implementing {@link Codec} for {@link ContainerKeyPrefix}. */
  final class CodecImpl extends ContainerKeyPrefixImpl.CodecBase {
    private static final Codec<ContainerKeyPrefix> INSTANCE = new CodecImpl();

    private CodecImpl() {
      // singleton
    }

    @Override
    public CodecBuffer toCodecBuffer(@Nonnull ContainerKeyPrefix object,
        IntFunction<CodecBuffer> allocator) {
      final int upperBound = getSerializedSizeUpperBound(object);
      final CodecBuffer buffer = allocator.apply(upperBound)
          .putLong(object.getContainerId());
      final String keyPrefix = object.getKeyPrefix();
      if (!keyPrefix.isEmpty()) {
        buffer.put(Delimiter.getBytes()).putUtf8(keyPrefix);
      }
      return buffer.put(Delimiter.getBytes())
          .putLong(object.getKeyVersion());
    }

    @Override
    public ContainerKeyPrefix fromCodecBuffer(@Nonnull CodecBuffer buffer) {
      final long containerId = buffer.getLong();
      final int keyPrefixLength = buffer.readableBytes()
          - Long.BYTES - 2 * Delimiter.length();
      final String keyPrefix = keyPrefixLength < 0 ? ""
          : Delimiter.skip(buffer).getUtf8(keyPrefixLength);
      final long keyVersion = Delimiter.skip(buffer).getLong();
      return ContainerKeyPrefix.get(containerId, keyPrefix, keyVersion);
    }
  }
}
