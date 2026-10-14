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
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedNfsLeaseInfo;

/** Immutable OM-owned NFS export or file writer lease. */
@Immutable
public final class OmNfsLeaseInfo {
  private static final Codec<OmNfsLeaseInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(PersistedNfsLeaseInfo.getDefaultInstance()),
      OmNfsLeaseInfo::getFromProtobuf,
      OmNfsLeaseInfo::getProtobuf,
      OmNfsLeaseInfo.class,
      DelegatedCodec.CopyType.SHALLOW);

  private final PersistedNfsLeaseInfo info;

  private OmNfsLeaseInfo(PersistedNfsLeaseInfo info) {
    this.info = Objects.requireNonNull(info, "info");
    validate(info);
  }

  public static Codec<OmNfsLeaseInfo> getCodec() {
    return CODEC;
  }

  public static OmNfsLeaseInfo createExportLease(String exportUuid, long volumeId, long bucketId, long generation,
      long expiresAtMillis, String gatewayId, long gatewayGeneration) {
    return getFromProtobuf(PersistedNfsLeaseInfo.newBuilder()
        .setKind(PersistedNfsLeaseInfo.Kind.EXPORT)
        .setExportUUID(exportUuid)
        .setVolumeID(volumeId)
        .setBucketID(bucketId)
        .setGeneration(generation)
        .setExpiresAtMillis(expiresAtMillis)
        .setGatewayID(gatewayId)
        .setGatewayGeneration(gatewayGeneration)
        .build());
  }

  /** Acquire an export lease, advancing both fencing generations after expiry. */
  public static OmNfsLeaseInfo acquireExportLease(OmNfsLeaseInfo existing, String exportUuid, long volumeId,
      long bucketId, String gatewayId, long nowMillis, long leaseDurationMillis) {
    validateLeaseRequest(exportUuid, gatewayId, nowMillis, leaseDurationMillis);
    long generation = 1;
    long gatewayGeneration = 1;
    if (existing != null) {
      PersistedNfsLeaseInfo previous = existing.info;
      if (previous.getKind() != PersistedNfsLeaseInfo.Kind.EXPORT
          || !previous.getExportUUID().equals(exportUuid)
          || previous.getVolumeID() != volumeId || previous.getBucketID() != bucketId) {
        throw new IllegalArgumentException("NFS export identity cannot be changed");
      }
      if (previous.getExpiresAtMillis() > nowMillis) {
        throw new IllegalStateException("NFS export lease is active");
      }
      generation = Math.addExact(previous.getGeneration(), 1);
      gatewayGeneration = Math.addExact(previous.getGatewayGeneration(), 1);
    }
    return createExportLease(exportUuid, volumeId, bucketId, generation,
        Math.addExact(nowMillis, leaseDurationMillis), gatewayId, gatewayGeneration);
  }

  /** Renew an active lease only when both fencing generations still match. */
  public OmNfsLeaseInfo renewExportLease(String gatewayId, long gatewayGeneration, long generation,
      long nowMillis, long leaseDurationMillis) {
    validateLeaseRequest(info.getExportUUID(), gatewayId, nowMillis, leaseDurationMillis);
    requireCurrentExportOwner(gatewayId, gatewayGeneration, generation, nowMillis);
    return createExportLease(info.getExportUUID(), info.getVolumeID(), info.getBucketID(), generation,
        Math.addExact(nowMillis, leaseDurationMillis), gatewayId, gatewayGeneration);
  }

  /** Release an active lease and advance its fence so the former owner is stale immediately. */
  public OmNfsLeaseInfo releaseExportLease(String gatewayId, long gatewayGeneration, long generation,
      long nowMillis) {
    if (nowMillis <= 0) {
      throw new IllegalArgumentException("NFS lease timestamp must be positive");
    }
    requireCurrentExportOwner(gatewayId, gatewayGeneration, generation, nowMillis);
    return createExportLease(info.getExportUUID(), info.getVolumeID(), info.getBucketID(),
        Math.addExact(generation, 1), nowMillis, gatewayId, gatewayGeneration);
  }

  /** Acquire an object writer lease under the current export owner. */
  @SuppressWarnings("parameternumber")
  public static OmNfsLeaseInfo acquireFileLease(OmNfsLeaseInfo existing, OmNfsLeaseInfo exportLease,
      long objectId, String transferId, String ownerPrincipal, ByteString ownerToken, long nowMillis,
      long leaseDurationMillis) {
    if (exportLease == null) {
      throw new IllegalStateException("NFS export lease is missing");
    }
    requireCurrentExportLease(exportLease, exportLease.info.getGatewayID(),
        exportLease.info.getGatewayGeneration(), nowMillis, leaseDurationMillis);
    PersistedNfsLeaseInfo export = exportLease.info;
    if (transferId == null || transferId.isEmpty() || ownerPrincipal == null || ownerPrincipal.isEmpty()
        || ownerToken == null || ownerToken.isEmpty()) {
      throw new IllegalArgumentException("NFS file lease request is invalid");
    }
    long generation = 1;
    if (existing != null) {
      PersistedNfsLeaseInfo previous = existing.info;
      if (!sameFile(previous, export, objectId)) {
        throw new IllegalArgumentException("NFS file lease identity cannot be changed");
      }
      if (previous.getExpiresAtMillis() > nowMillis) {
        throw new IllegalStateException("NFS file lease is active");
      }
      generation = Math.addExact(previous.getGeneration(), 1);
    }
    return createFileLease(export, objectId, transferId, ownerPrincipal, ownerToken, generation, nowMillis,
        leaseDurationMillis);
  }

  /** Renew a file lease only while both the export and file fencing identities remain current. */
  public OmNfsLeaseInfo renewFileLease(OmNfsLeaseInfo exportLease, long generation, String transferId,
      ByteString ownerToken, long nowMillis, long leaseDurationMillis) {
    requireCurrentExportLease(exportLease, info.getGatewayID(), info.getGatewayGeneration(), nowMillis,
        leaseDurationMillis);
    requireCurrentFileOwner(generation, transferId, ownerToken, nowMillis);
    return createFileLease(exportLease.info, info.getObjectID(), transferId, info.getOwnerPrincipal(), ownerToken,
        generation, nowMillis, leaseDurationMillis).withContentPublished(info.getContentPublished());
  }

  /** Take over a file lease only after the previous gateway has been fenced or the file lease has expired. */
  @SuppressWarnings("parameternumber")
  public OmNfsLeaseInfo recoverFileLease(OmNfsLeaseInfo exportLease, String requesterPrincipal,
      ByteString newOwnerToken, long nowMillis, long leaseDurationMillis) {
    requireCurrentExportLease(exportLease, exportLease == null ? "" : exportLease.info.getGatewayID(),
        exportLease == null ? 0 : exportLease.info.getGatewayGeneration(), nowMillis, leaseDurationMillis);
    if (info.getKind() != PersistedNfsLeaseInfo.Kind.FILE
        || !info.getExportUUID().equals(exportLease.info.getExportUUID())
        || info.getVolumeID() != exportLease.info.getVolumeID() || info.getBucketID() != exportLease.info.getBucketID()
        || requesterPrincipal == null || requesterPrincipal.isEmpty()
        || !requesterPrincipal.equals(getOwnerPrincipal()) || newOwnerToken == null || newOwnerToken.isEmpty()
        || newOwnerToken.equals(info.getOwnerToken())) {
      throw new IllegalStateException("NFS file lease recovery identity is invalid");
    }
    boolean sameCurrentGateway = info.getGatewayID().equals(exportLease.info.getGatewayID())
        && info.getGatewayGeneration() == exportLease.info.getGatewayGeneration();
    if (info.getGatewayGeneration() >= exportLease.info.getGatewayGeneration()
        && info.getExpiresAtMillis() > nowMillis && !sameCurrentGateway) {
      throw new IllegalStateException("NFS file lease is still owned by the active gateway");
    }
    return createFileLease(exportLease.info, info.getObjectID(), info.getTransferID(), getOwnerPrincipal(),
        newOwnerToken, Math.addExact(info.getGeneration(), 1), nowMillis, leaseDurationMillis)
        .withContentPublished(info.getContentPublished());
  }

  /** Release a file lease and advance its fence so the former owner is stale immediately. */
  public OmNfsLeaseInfo releaseFileLease(OmNfsLeaseInfo exportLease, long generation, String transferId,
      ByteString ownerToken, long nowMillis) {
    requireCurrentExportLease(exportLease, info.getGatewayID(), info.getGatewayGeneration(), nowMillis, 0);
    requireCurrentFileOwner(generation, transferId, ownerToken, nowMillis);
    return getFromProtobuf(info.toBuilder().setGeneration(Math.addExact(generation, 1))
        .setExpiresAtMillis(nowMillis).build());
  }

  public OmNfsLeaseInfo withContentPublished(boolean contentPublished) {
    if (info.getKind() != PersistedNfsLeaseInfo.Kind.FILE) {
      throw new IllegalStateException("NFS export lease cannot track published file content");
    }
    return getFromProtobuf(info.toBuilder().setContentPublished(contentPublished).build());
  }

  /** Verify the NFS file and export fencing identities before a file commit. */
  @SuppressWarnings("parameternumber")
  public void validateFileCommit(OmNfsLeaseInfo exportLease, String exportUuid, String gatewayId,
      long gatewayGeneration, String transferId, long generation, ByteString ownerToken, long nowMillis) {
    if (info.getKind() != PersistedNfsLeaseInfo.Kind.FILE || !info.getExportUUID().equals(exportUuid)
        || !info.getGatewayID().equals(gatewayId) || info.getGatewayGeneration() != gatewayGeneration) {
      throw new IllegalStateException("NFS file lease identity is stale");
    }
    requireCurrentExportLease(exportLease, gatewayId, gatewayGeneration, nowMillis, 0);
    requireCurrentFileOwner(generation, transferId, ownerToken, nowMillis);
  }

  @SuppressWarnings("parameternumber")
  private static OmNfsLeaseInfo createFileLease(PersistedNfsLeaseInfo export, long objectId, String transferId,
      String ownerPrincipal, ByteString ownerToken, long generation, long nowMillis, long leaseDurationMillis) {
    validateLeaseRequest(export.getExportUUID(), export.getGatewayID(), nowMillis, leaseDurationMillis);
    return getFromProtobuf(PersistedNfsLeaseInfo.newBuilder().setKind(PersistedNfsLeaseInfo.Kind.FILE)
        .setExportUUID(export.getExportUUID()).setVolumeID(export.getVolumeID()).setBucketID(export.getBucketID())
        .setObjectID(objectId).setGatewayID(export.getGatewayID()).setGatewayGeneration(export.getGatewayGeneration())
        .setTransferID(transferId).setOwnerToken(ownerToken).setOwnerPrincipal(ownerPrincipal).setGeneration(generation)
        .setExpiresAtMillis(Math.addExact(nowMillis, leaseDurationMillis)).build());
  }

  private static void requireCurrentExportLease(OmNfsLeaseInfo exportLease, String gatewayId,
      long gatewayGeneration, long nowMillis, long leaseDurationMillis) {
    if (exportLease == null || exportLease.info.getKind() != PersistedNfsLeaseInfo.Kind.EXPORT) {
      throw new IllegalStateException("NFS export lease is missing");
    }
    exportLease.requireCurrentExportOwner(gatewayId, gatewayGeneration, exportLease.info.getGeneration(), nowMillis);
    if (leaseDurationMillis > 0
        && Math.addExact(nowMillis, leaseDurationMillis) > exportLease.info.getExpiresAtMillis()) {
      throw new IllegalArgumentException("NFS file lease exceeds its export lease");
    }
  }

  private void requireCurrentFileOwner(long generation, String transferId, ByteString ownerToken, long nowMillis) {
    if (info.getKind() != PersistedNfsLeaseInfo.Kind.FILE || info.getExpiresAtMillis() <= nowMillis
        || info.getGeneration() != generation || !info.getTransferID().equals(transferId)
        || ownerToken == null || !info.getOwnerToken().equals(ownerToken)) {
      throw new IllegalStateException("NFS file lease owner is stale");
    }
  }

  private static boolean sameFile(PersistedNfsLeaseInfo file, PersistedNfsLeaseInfo export, long objectId) {
    return file.getKind() == PersistedNfsLeaseInfo.Kind.FILE
        && file.getExportUUID().equals(export.getExportUUID()) && file.getVolumeID() == export.getVolumeID()
        && file.getBucketID() == export.getBucketID() && file.getObjectID() == objectId;
  }

  private void requireCurrentExportOwner(String gatewayId, long gatewayGeneration, long generation,
      long nowMillis) {
    if (info.getKind() != PersistedNfsLeaseInfo.Kind.EXPORT || info.getExpiresAtMillis() <= nowMillis
        || !info.getGatewayID().equals(gatewayId) || info.getGatewayGeneration() != gatewayGeneration
        || info.getGeneration() != generation) {
      throw new IllegalStateException("NFS export lease owner is stale");
    }
  }

  private static void validateLeaseRequest(String exportUuid, String gatewayId, long nowMillis,
      long leaseDurationMillis) {
    if (exportUuid == null || exportUuid.isEmpty() || gatewayId == null || gatewayId.isEmpty()
        || nowMillis <= 0 || leaseDurationMillis <= 0) {
      throw new IllegalArgumentException("NFS export lease request is invalid");
    }
    Math.addExact(nowMillis, leaseDurationMillis);
  }

  public static OmNfsLeaseInfo getFromProtobuf(PersistedNfsLeaseInfo info) {
    if (!info.isInitialized()) {
      throw new IllegalArgumentException("NFS lease record is missing required fields");
    }
    return new OmNfsLeaseInfo(info);
  }

  public PersistedNfsLeaseInfo getProtobuf() {
    return info;
  }

  public String getOwnerPrincipal() {
    return info.hasOwnerPrincipal() ? info.getOwnerPrincipal() : "";
  }

  private static void validate(PersistedNfsLeaseInfo info) {
    if (info.getExportUUID().isEmpty() || info.getGeneration() == 0 || info.getExpiresAtMillis() == 0) {
      throw new IllegalArgumentException("NFS lease record contains an empty identity or generation");
    }
    if (info.getKind() == PersistedNfsLeaseInfo.Kind.EXPORT) {
      if (!info.hasGatewayID() || info.getGatewayID().isEmpty() || !info.hasGatewayGeneration()
          || info.getGatewayGeneration() == 0 || !info.hasVolumeID() || !info.hasBucketID() || info.hasObjectID()) {
        throw new IllegalArgumentException("NFS export lease record has invalid fields");
      }
    } else if (info.getKind() == PersistedNfsLeaseInfo.Kind.FILE) {
      if (!info.hasGatewayID() || info.getGatewayID().isEmpty() || !info.hasGatewayGeneration()
          || info.getGatewayGeneration() == 0 || !info.hasVolumeID() || !info.hasBucketID() || !info.hasObjectID()
          || !info.hasTransferID() || info.getTransferID().isEmpty() || !info.hasOwnerToken()
          || info.getOwnerToken().isEmpty()) {
        throw new IllegalArgumentException("NFS file lease record has invalid fields");
      }
    } else {
      throw new IllegalArgumentException("NFS lease record has an unsupported kind");
    }
  }

  @Override
  public boolean equals(Object other) {
    return this == other || other instanceof OmNfsLeaseInfo && info.equals(((OmNfsLeaseInfo) other).info);
  }

  @Override
  public int hashCode() {
    return info.hashCode();
  }
}
