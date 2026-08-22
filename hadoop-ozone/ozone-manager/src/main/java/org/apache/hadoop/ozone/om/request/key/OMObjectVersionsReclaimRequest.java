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

package org.apache.hadoop.ozone.om.request.key;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.OMSystemAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMObjectVersionsReclaimResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ObjectVersionsBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReclaimObjectVersionsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReclaimObjectVersionsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the reclamation the lifecycle scan selects on a versioned bucket:
 * noncurrent versions a NoncurrentVersionExpiration rule expires, which leave
 * the versionedKeyTable with their blocks queued in the deletedTable, and
 * expired delete markers, which leave the keyTable so the key disappears with
 * them. Nothing here reclaims blocks directly.
 *
 * <p>The scan selects from the DB as of its own pass, so a record may already
 * be gone, or no longer eligible, by the time this request applies - a version
 * permanently deleted or promoted into the keyTable, a marker superseded by a
 * write. Every condition the scan checked is checked again here under the
 * bucket lock, and anything that no longer holds is skipped; a later pass
 * reselects if it still applies.
 */
public class OMObjectVersionsReclaimRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMObjectVersionsReclaimRequest.class);

  private static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.OMSYSTEMLOGGER);
  private static final String AUDIT_PARAM_NUM_VERSIONS =
      "numObjectVersionsReclaimed";

  public OMObjectVersionsReclaimRequest(OMRequest omRequest) {
    // S3 object versioning is supported on OBJECT_STORE buckets only, so every
    // version this request touches lives in the OBJECT_STORE keyTable.
    super(omRequest, BucketLayout.OBJECT_STORE);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    ReclaimObjectVersionsRequest reclaimRequest =
        getOmRequest().getReclaimObjectVersionsRequest();
    List<ObjectVersionsBucket> versionsPerBucket =
        reclaimRequest.getVersionsPerBucketList();

    long numSubmitted = 0;
    for (ObjectVersionsBucket bucket : versionsPerBucket) {
      numSubmitted +=
          bucket.getVersionKeysCount() + bucket.getMarkerKeysCount();
    }

    OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    omResponse.setReclaimObjectVersionsResponse(
        ReclaimObjectVersionsResponse.newBuilder());

    OMClientResponse omClientResponse = null;
    List<String> reclaimedVersionKeys = new ArrayList<>();
    List<String> reclaimedMarkerKeys = new ArrayList<>();
    Map<String, RepeatedOmKeyInfo> keysToDelete = new HashMap<>();
    List<OmBucketInfo> updatedBuckets = new ArrayList<>();
    Map<String, String> auditParams = new LinkedHashMap<>();
    try {
      for (ObjectVersionsBucket bucket : versionsPerBucket) {
        reclaimBucketVersions(ozoneManager, trxnLogIndex, bucket,
            reclaimedVersionKeys, reclaimedMarkerKeys, keysToDelete,
            updatedBuckets);
      }

      omClientResponse = new OMObjectVersionsReclaimResponse(
          omResponse.build(), reclaimedVersionKeys, reclaimedMarkerKeys,
          keysToDelete, updatedBuckets);

      int reclaimed = reclaimedVersionKeys.size() + reclaimedMarkerKeys.size();
      ozoneManager.getDeletionMetrics()
          .incrNumObjectVersionsReclaimed(reclaimed);
      auditParams.put(AUDIT_PARAM_NUM_VERSIONS, String.valueOf(reclaimed));
      AUDIT.logWriteSuccess(ozoneManager.buildAuditMessageForSuccess(
          OMSystemAction.OBJECT_VERSION_CLEANUP, auditParams));
      LOG.debug("Reclaimed {} object versions and {} expired delete markers "
          + "out of {} submitted.", reclaimedVersionKeys.size(),
          reclaimedMarkerKeys.size(), numSubmitted);
    } catch (IOException ex) {
      AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(
          OMSystemAction.OBJECT_VERSION_CLEANUP, auditParams, ex));
      LOG.error("Failed to reclaim {} submitted records.", numSubmitted, ex);
      omClientResponse = new OMObjectVersionsReclaimResponse(
          createErrorOMResponse(omResponse, ex));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    return omClientResponse;
  }

  /** Whether the key has any version left in the versionedKeyTable. */
  private boolean hasNoncurrentVersion(OMMetadataManager omMetadataManager,
      OmKeyInfo marker) throws IOException {
    try (Table.KeyValueIterator<String, OmKeyInfo> versions =
             omMetadataManager.getVersionedKeyTable().iterator(
                 omMetadataManager.getVersionedOzoneKeyPrefix(
                     marker.getVolumeName(), marker.getBucketName(),
                     marker.getKeyName()))) {
      return versions.hasNext();
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private void reclaimBucketVersions(OzoneManager ozoneManager,
      long trxnLogIndex, ObjectVersionsBucket versionsBucket,
      List<String> reclaimedVersionKeys, List<String> reclaimedMarkerKeys,
      Map<String, RepeatedOmKeyInfo> keysToDelete,
      List<OmBucketInfo> updatedBuckets) throws IOException {

    String volumeName = versionsBucket.getVolumeName();
    String bucketName = versionsBucket.getBucketName();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    boolean acquiredLock = false;
    try {
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      OmBucketInfo omBucketInfo =
          getBucketInfo(omMetadataManager, volumeName, bucketName);
      if (omBucketInfo == null) {
        LOG.debug("Bucket {}/{} no longer exists, skipping the {} object "
            + "versions and {} delete markers submitted for it.", volumeName,
            bucketName, versionsBucket.getVersionKeysCount(),
            versionsBucket.getMarkerKeysCount());
        return;
      }

      boolean reclaimedAny = false;
      for (String versionKey : versionsBucket.getVersionKeysList()) {
        OmKeyInfo version =
            omMetadataManager.getVersionedKeyTable().get(versionKey);
        if (version == null) {
          // Already reclaimed, permanently deleted, or promoted into the
          // keyTable since the service selected it.
          continue;
        }
        if (trxnLogIndex < version.getUpdateID()) {
          LOG.warn("Transaction log index {} is smaller than the current "
              + "updateID {} of version {}, skipping reclamation.",
              trxnLogIndex, version.getUpdateID(), versionKey);
          continue;
        }

        version = version.toBuilder().setUpdateID(trxnLogIndex).build();
        omMetadataManager.getVersionedKeyTable().addCacheEntry(
            new CacheKey<>(versionKey), CacheValue.get(trxnLogIndex));
        reclaimedVersionKeys.add(versionKey);

        // A delete marker holds no blocks, so it releases namespace but no
        // space, and there is nothing to reclaim for it: an empty record is
        // not queued in the deletedTable at all, as every other delete path
        // does through AbstractOMKeyDeleteResponse.
        boolean isVersionNonEmpty = !OmKeyInfo.isKeyEmpty(version);
        if (isVersionNonEmpty) {
          // Versions of one key share a deletedTable entry: it holds a
          // RepeatedOmKeyInfo list that KeyDeletingService evaluates one
          // record at a time.
          String ozoneKey = omMetadataManager.getOzoneKey(volumeName,
              bucketName, version.getKeyName());
          addKeyInfoToDeleteMap(ozoneManager, trxnLogIndex, ozoneKey,
              omBucketInfo.getObjectID(),
              version.withCommittedKeyDeletedFlag(true), keysToDelete);
        }
        omBucketInfo.decrUsedBytes(sumBlockLengths(version), isVersionNonEmpty);
        omBucketInfo.decrUsedNamespace(1L, isVersionNonEmpty);
        reclaimedAny = true;
      }

      for (String markerKey : versionsBucket.getMarkerKeysList()) {
        OmKeyInfo marker =
            omMetadataManager.getKeyTable(getBucketLayout()).get(markerKey);
        if (marker == null) {
          continue;
        }
        // A write since the scan supersedes the marker: the key's current
        // version is then a real object, and there is nothing expired here.
        if (!marker.isDeleteMarker()) {
          continue;
        }
        if (trxnLogIndex < marker.getUpdateID()) {
          LOG.warn("Transaction log index {} is smaller than the current "
              + "updateID {} of marker {}, skipping reclamation.",
              trxnLogIndex, marker.getUpdateID(), markerKey);
          continue;
        }
        // Re-checked here and not only in the scan: removing the marker while
        // a noncurrent version survives would make that version current again,
        // resurrecting an object the user deleted.
        if (hasNoncurrentVersion(omMetadataManager, marker)) {
          continue;
        }

        omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
            new CacheKey<>(markerKey), CacheValue.get(trxnLogIndex));
        reclaimedMarkerKeys.add(markerKey);

        // The marker holds no blocks, so it never reaches the deletedTable; it
        // does hold a namespace slot of its own.
        omBucketInfo.decrUsedNamespace(1L, false);
        reclaimedAny = true;
      }

      if (reclaimedAny) {
        updatedBuckets.add(omBucketInfo.copyObject());
      }
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
    }
  }
}
