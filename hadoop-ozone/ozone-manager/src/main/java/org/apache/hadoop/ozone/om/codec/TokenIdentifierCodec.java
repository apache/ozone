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

package org.apache.hadoop.ozone.om.codec;

import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;

/**
 * Codec to serialize/deserialize {@link OzoneTokenIdentifier}.
 */
public final class TokenIdentifierCodec implements Codec<OzoneTokenIdentifier> {

  private static final Codec<OzoneTokenIdentifier> INSTANCE =
      new TokenIdentifierCodec();

  public static Codec<OzoneTokenIdentifier> get() {
    return INSTANCE;
  }

  private TokenIdentifierCodec() {
    // singleton
  }

  @Override
  public Class<OzoneTokenIdentifier> getTypeClass() {
    return OzoneTokenIdentifier.class;
  }

  @Override
  public byte[] toPersistedFormat(OzoneTokenIdentifier object) {
    Objects.requireNonNull(object, "object == null");
    return object.toProtoBuf().toByteArray();
  }

  @Override
  public OzoneTokenIdentifier fromPersistedFormatImpl(byte[] rawData) throws IOException {
    try {
      return OzoneTokenIdentifier.readProtoBuf(rawData);
    } catch (IOException first) {
      try {
        return OzoneTokenIdentifier.newInstance().fromUniqueSerializedKey(rawData);
      } catch (IOException e) {
        e.addSuppressed(first);
        throw e;
      }
    }
  }

  @Override
  public OzoneTokenIdentifier copyObject(OzoneTokenIdentifier object) {
    return object;
  }
}
