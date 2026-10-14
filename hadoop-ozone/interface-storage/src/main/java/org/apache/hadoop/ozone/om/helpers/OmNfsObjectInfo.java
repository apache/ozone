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

package org.apache.hadoop.ozone.om.helpers;

import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedNfsObjectInfo;

/**
 * Immutable pointer from an FSO object identity to its canonical parent/name record.
 * ACLs, attributes, and block locations must be read from the canonical tables.
 */
@Immutable
public final class OmNfsObjectInfo {
  private static final Codec<OmNfsObjectInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(PersistedNfsObjectInfo.getDefaultInstance()),
      OmNfsObjectInfo::getFromProtobuf,
      OmNfsObjectInfo::getProtobuf,
      OmNfsObjectInfo.class,
      DelegatedCodec.CopyType.SHALLOW);

  private final PersistedNfsObjectInfo.Kind kind;
  private final long parentObjectID;
  private final String name;

  public OmNfsObjectInfo(PersistedNfsObjectInfo.Kind kind, long parentObjectID, String name) {
    this.kind = Objects.requireNonNull(kind, "kind");
    this.name = Objects.requireNonNull(name, "name");
    if (name.isEmpty() || name.equals(".") || name.equals("..") || name.indexOf('/') >= 0) {
      throw new IllegalArgumentException("NFS index name must be a final path component");
    }
    this.parentObjectID = parentObjectID;
  }

  public static Codec<OmNfsObjectInfo> getCodec() {
    return CODEC;
  }

  public PersistedNfsObjectInfo.Kind getKind() {
    return kind;
  }

  public long getParentObjectID() {
    return parentObjectID;
  }

  public String getName() {
    return name;
  }

  public PersistedNfsObjectInfo getProtobuf() {
    return PersistedNfsObjectInfo.newBuilder()
        .setKind(kind)
        .setParentObjectID(parentObjectID)
        .setName(name)
        .build();
  }

  public static OmNfsObjectInfo getFromProtobuf(PersistedNfsObjectInfo info) {
    if (!info.isInitialized()) {
      throw new IllegalArgumentException("NFS index record is missing required fields");
    }
    return new OmNfsObjectInfo(info.getKind(), info.getParentObjectID(), info.getName());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof OmNfsObjectInfo)) {
      return false;
    }
    OmNfsObjectInfo that = (OmNfsObjectInfo) other;
    return kind == that.kind && parentObjectID == that.parentObjectID && name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kind, parentObjectID, name);
  }
}
