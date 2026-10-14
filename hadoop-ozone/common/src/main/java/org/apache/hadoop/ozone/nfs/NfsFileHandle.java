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

package org.apache.hadoop.ozone.nfs;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/**
 * Persistent identity payload for an Ozone NFS handle. Decoding does not resolve
 * an object or authorize access; the caller must validate the export and consult OM.
 */
public final class NfsFileHandle {
  public static final int ENCODED_LENGTH = 48;
  private static final int MAGIC = 0x4f5a4e46;
  private static final byte VERSION = 1;

  private final UUID exportId;
  private final long volumeId;
  private final long bucketId;
  private final long objectId;
  private final Kind kind;

  /** Object kinds with explicit values independent of Java enum ordering. */
  public enum Kind {
    BUCKET(1), DIRECTORY(2), FILE(3);

    private final byte code;

    Kind(int code) {
      this.code = (byte) code;
    }

    private static Kind fromCode(byte code) {
      for (Kind kind : values()) {
        if (kind.code == code) {
          return kind;
        }
      }
      throw new IllegalArgumentException("Unknown NFS handle object kind");
    }
  }

  public NfsFileHandle(UUID exportId, long volumeId, long bucketId, long objectId, Kind kind) {
    this.exportId = Objects.requireNonNull(exportId, "exportId");
    this.volumeId = volumeId;
    this.bucketId = bucketId;
    this.objectId = objectId;
    this.kind = Objects.requireNonNull(kind, "kind");
  }

  /** Encode in network byte order without paths or mutable metadata. */
  public byte[] toBytes() {
    return ByteBuffer.allocate(ENCODED_LENGTH)
        .putInt(MAGIC).put(VERSION).put(kind.code).putShort((short) 0)
        .putLong(exportId.getMostSignificantBits()).putLong(exportId.getLeastSignificantBits())
        .putLong(volumeId).putLong(bucketId).putLong(objectId).array();
  }

  /**
   * Decode the backend payload, excluding any Ganesha wrapper.
   * @throws IllegalArgumentException if the payload is malformed or unsupported
   */
  public static NfsFileHandle fromBytes(byte[] payload) {
    if (payload == null || payload.length != ENCODED_LENGTH) {
      throw new IllegalArgumentException("Invalid NFS handle length");
    }
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.getInt() != MAGIC) {
      throw new IllegalArgumentException("Invalid NFS handle magic");
    }
    if (buffer.get() != VERSION) {
      throw new IllegalArgumentException("Unsupported NFS handle version");
    }
    Kind kind = Kind.fromCode(buffer.get());
    if (buffer.getShort() != 0) {
      throw new IllegalArgumentException("Nonzero NFS handle reserved field");
    }
    UUID exportId = new UUID(buffer.getLong(), buffer.getLong());
    return new NfsFileHandle(exportId, buffer.getLong(), buffer.getLong(), buffer.getLong(), kind);
  }

  public UUID getExportId() {
    return exportId;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public long getBucketId() {
    return bucketId;
  }

  public long getObjectId() {
    return objectId;
  }

  public Kind getKind() {
    return kind;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof NfsFileHandle)) {
      return false;
    }
    NfsFileHandle that = (NfsFileHandle) other;
    return volumeId == that.volumeId && bucketId == that.bucketId && objectId == that.objectId
        && exportId.equals(that.exportId) && kind == that.kind;
  }

  @Override
  public int hashCode() {
    return Objects.hash(exportId, volumeId, bucketId, objectId, kind);
  }
}
