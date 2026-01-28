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

package org.apache.hadoop.ozone.om.snapshot.diff.helper;

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.SnapDiffObjectInfo;

/**
 * Represents information about an object in a snapshot difference.
 * This class provides methods for converting between its internal
 * representation and a Protobuf-based representation using a custom
 * serialization codec.
 */
public class SnapshotDiffObjectInfo {

  private static final Codec<SnapshotDiffObjectInfo> CODEC =
      new DelegatedCodec<>(Proto2Codec.get(SnapDiffObjectInfo.getDefaultInstance()),
          SnapshotDiffObjectInfo::getFromProtobuf,
          SnapshotDiffObjectInfo::toProtobuf,
          SnapshotDiffObjectInfo.class);

  private long objectId;
  private String key;

  public SnapshotDiffObjectInfo(long objectId, String key) {
    this.objectId = objectId;
    this.key = key;
  }

  public static Codec<SnapshotDiffObjectInfo> getCodec() {
    return CODEC;
  }

  @SuppressWarnings("PMD.UnusedPrivateMethod")
  private static SnapshotDiffObjectInfo getFromProtobuf(SnapDiffObjectInfo objectInfo) {
    return new SnapshotDiffObjectInfo(objectInfo.getObjectID(), objectInfo.getKeyName());
  }

  @SuppressWarnings("PMD.UnusedPrivateMethod")
  private SnapDiffObjectInfo toProtobuf() {
    return SnapDiffObjectInfo.newBuilder()
        .setObjectID(objectId)
        .setKeyName(key)
        .build();
  }
}
