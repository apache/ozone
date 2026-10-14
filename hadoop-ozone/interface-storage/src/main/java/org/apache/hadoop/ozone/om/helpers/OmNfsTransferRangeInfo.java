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

import com.google.protobuf.ByteString;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedNfsTransferRangeInfo;

/** Immutable pointer to a durable range staged for an NFS writer lease. */
@Immutable
public final class OmNfsTransferRangeInfo {
  private static final Codec<OmNfsTransferRangeInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(PersistedNfsTransferRangeInfo.getDefaultInstance()),
      OmNfsTransferRangeInfo::getFromProtobuf,
      OmNfsTransferRangeInfo::getProtobuf,
      OmNfsTransferRangeInfo.class,
      DelegatedCodec.CopyType.SHALLOW);

  private final String exportUUID;
  private final long volumeID;
  private final long bucketID;
  private final long objectID;
  private final long leaseGeneration;
  private final long offset;
  private final long length;
  private final String transferID;
  private final String stagingKey;
  private final ByteString sha256;
  private final long transactionIndex;

  @SuppressWarnings("parameternumber")
  public OmNfsTransferRangeInfo(String exportUUID, long volumeID, long bucketID, long objectID,
      long leaseGeneration, long offset, long length, String transferID, String stagingKey,
      ByteString sha256, long transactionIndex) {
    this.exportUUID = requireText(exportUUID, "exportUUID");
    this.transferID = requireText(transferID, "transferID");
    this.stagingKey = requireText(stagingKey, "stagingKey");
    this.sha256 = Objects.requireNonNull(sha256, "sha256");
    if (volumeID == 0 || bucketID == 0 || objectID == 0 || leaseGeneration == 0 || offset < 0 || length <= 0
        || sha256.size() != 32 || transactionIndex < 0) {
      throw new IllegalArgumentException("NFS transfer range identity or length is invalid");
    }
    Math.addExact(offset, length);
    this.volumeID = volumeID;
    this.bucketID = bucketID;
    this.objectID = objectID;
    this.leaseGeneration = leaseGeneration;
    this.offset = offset;
    this.length = length;
    this.transactionIndex = transactionIndex;
  }

  @SuppressWarnings("parameternumber")
  public OmNfsTransferRangeInfo(String exportUUID, long volumeID, long bucketID, long objectID,
      long leaseGeneration, long offset, long length, String transferID, String stagingKey, ByteString sha256) {
    this(exportUUID, volumeID, bucketID, objectID, leaseGeneration, offset, length, transferID, stagingKey, sha256, 0);
  }

  public static Codec<OmNfsTransferRangeInfo> getCodec() {
    return CODEC;
  }

  public String getExportUUID() {
    return exportUUID;
  }

  public long getVolumeID() {
    return volumeID;
  }

  public long getBucketID() {
    return bucketID;
  }

  public long getObjectID() {
    return objectID;
  }

  public long getLeaseGeneration() {
    return leaseGeneration;
  }

  public long getOffset() {
    return offset;
  }

  public long getLength() {
    return length;
  }

  public String getTransferID() {
    return transferID;
  }

  public String getStagingKey() {
    return stagingKey;
  }

  public ByteString getSha256() {
    return sha256;
  }

  public long getTransactionIndex() {
    return transactionIndex;
  }

  public PersistedNfsTransferRangeInfo getProtobuf() {
    return PersistedNfsTransferRangeInfo.newBuilder()
        .setExportUUID(exportUUID)
        .setVolumeID(volumeID)
        .setBucketID(bucketID)
        .setObjectID(objectID)
        .setLeaseGeneration(leaseGeneration)
        .setOffset(offset)
        .setLength(length)
        .setTransferID(transferID)
        .setStagingKey(stagingKey)
        .setSha256(sha256)
        .setTransactionIndex(transactionIndex)
        .build();
  }

  public static OmNfsTransferRangeInfo getFromProtobuf(PersistedNfsTransferRangeInfo info) {
    if (!info.isInitialized()) {
      throw new IllegalArgumentException("NFS transfer range record is missing required fields");
    }
    return new OmNfsTransferRangeInfo(info.getExportUUID(), info.getVolumeID(), info.getBucketID(),
        info.getObjectID(), info.getLeaseGeneration(), info.getOffset(), info.getLength(),
        info.getTransferID(), info.getStagingKey(), info.getSha256(), info.getTransactionIndex());
  }

  private static String requireText(String value, String field) {
    Objects.requireNonNull(value, field);
    if (value.isEmpty()) {
      throw new IllegalArgumentException("NFS transfer range " + field + " is empty");
    }
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof OmNfsTransferRangeInfo)) {
      return false;
    }
    OmNfsTransferRangeInfo that = (OmNfsTransferRangeInfo) other;
    return volumeID == that.volumeID && bucketID == that.bucketID && objectID == that.objectID
        && leaseGeneration == that.leaseGeneration && offset == that.offset && length == that.length
        && transactionIndex == that.transactionIndex
        && exportUUID.equals(that.exportUUID) && transferID.equals(that.transferID)
        && stagingKey.equals(that.stagingKey) && sha256.equals(that.sha256);
  }

  @Override
  public int hashCode() {
    return Objects.hash(exportUUID, volumeID, bucketID, objectID, leaseGeneration, offset, length, transactionIndex,
        transferID, stagingKey, sha256);
  }
}
