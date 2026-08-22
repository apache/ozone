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

package org.apache.hadoop.ozone.om.snapshot.diff;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Compact intermediate value stored in the {@code newList}/{@code oldList}
 * column families of the optimized snapshot diff. It keeps only the fields
 * required to classify a diff entry in the later merge-join stage, avoiding the
 * cost of holding a full {@code OmKeyInfo}/{@code OmDirectoryInfo}.
 *
 * <p>The fields are:
 * <ul>
 *   <li>{@code parentId} - parent object id (parent directory for FSO).</li>
 *   <li>{@code name} - leaf name for FSO buckets, full key path for OBS.</li>
 *   <li>{@code isDir} - whether the entry is a directory.</li>
 *   <li>{@code signature} - SHA-256 compare signature computed by
 *       {@code SnapshotDiffValueParser} over the meaningful fields.</li>
 * </ul>
 *
 * <p>The wire layout is fixed so both the full diff and the DAG diff Stage 1
 * readers produce identical bytes:
 * <pre>
 *   | parentId (8, big-endian) | isDir (1) | sigLen (4, big-endian) | signature | name (UTF-8, remaining) |
 * </pre>
 */
public final class EntryValue {

  private static final int PARENT_ID_BYTES = Long.BYTES;
  private static final int IS_DIR_BYTES = 1;
  private static final int SIG_LEN_BYTES = Integer.BYTES;
  private static final int HEADER_BYTES = PARENT_ID_BYTES + IS_DIR_BYTES + SIG_LEN_BYTES;

  private final long parentId;
  private final String name;
  private final boolean isDir;
  private final byte[] signature;

  public EntryValue(long parentId, String name, boolean isDir, byte[] signature) {
    this.parentId = parentId;
    this.name = name == null ? "" : name;
    this.isDir = isDir;
    this.signature = signature == null ? new byte[0] : signature;
  }

  public long getParentId() {
    return parentId;
  }

  public String getName() {
    return name;
  }

  public boolean isDir() {
    return isDir;
  }

  public byte[] getSignature() {
    return Arrays.copyOf(signature, signature.length);
  }

  /**
   * Serializes this value to its fixed byte layout.
   */
  public byte[] toBytes() {
    byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    ByteBuffer buffer = ByteBuffer.allocate(HEADER_BYTES + signature.length + nameBytes.length);
    buffer.putLong(parentId);
    buffer.put((byte) (isDir ? 1 : 0));
    buffer.putInt(signature.length);
    buffer.put(signature);
    buffer.put(nameBytes);
    return buffer.array();
  }

  /**
   * Deserializes a value previously produced by {@link #toBytes()}.
   */
  public static EntryValue fromBytes(byte[] bytes) {
    Objects.requireNonNull(bytes, "bytes must not be null");
    if (bytes.length < HEADER_BYTES) {
      throw new IllegalArgumentException("EntryValue byte array too short: " + bytes.length);
    }
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    long parentId = buffer.getLong();
    boolean isDir = buffer.get() != 0;
    int sigLen = buffer.getInt();
    if (sigLen < 0 || sigLen > buffer.remaining()) {
      throw new IllegalArgumentException("EntryValue has invalid signature length: " + sigLen);
    }
    byte[] signature = new byte[sigLen];
    buffer.get(signature);
    byte[] nameBytes = new byte[buffer.remaining()];
    buffer.get(nameBytes);
    String name = new String(nameBytes, StandardCharsets.UTF_8);
    return new EntryValue(parentId, name, isDir, signature);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EntryValue that = (EntryValue) o;
    return parentId == that.parentId
        && isDir == that.isDir
        && name.equals(that.name)
        && Arrays.equals(signature, that.signature);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(parentId, name, isDir);
    result = 31 * result + Arrays.hashCode(signature);
    return result;
  }

  @Override
  public String toString() {
    return "EntryValue{parentId=" + parentId
        + ", name='" + name + '\''
        + ", isDir=" + isDir
        + ", signatureLen=" + signature.length + '}';
  }
}
