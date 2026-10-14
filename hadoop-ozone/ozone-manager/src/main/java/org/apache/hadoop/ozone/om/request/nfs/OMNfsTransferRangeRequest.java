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

package org.apache.hadoop.ozone.om.request.nfs;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NFS_LEASE_CONFLICT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsTransferRangeInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.nfs.OMNfsTransferRangeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;

/** Applies a fenced, idempotent NFS write-range manifest mutation through Ratis. */
public class OMNfsTransferRangeRequest extends OMClientRequest {
  public OMNfsTransferRangeRequest(OMRequest request) {
    super(request);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest request = super.preExecute(ozoneManager);
    NfsTransferRangeRequest rangeRequest = request.getNfsTransferRangeRequest();
    validateRequest(rangeRequest);
    if (ozoneManager.getAclsEnabled()) {
      checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET, OzoneObj.StoreType.OZONE,
          IAccessAuthorizer.ACLType.WRITE, rangeRequest.getVolumeName(), rangeRequest.getBucketName(), null);
    }
    return request.toBuilder().setNfsTransferRangeRequest(
        rangeRequest.toBuilder().setLeaderTimeMillis(Time.now())).build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    NfsTransferRangeRequest request = getOmRequest().getNfsTransferRangeRequest();
    OMResponse.Builder response = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put("exportUUID", request.getExportUUID());
    auditMap.put("objectID", Long.toUnsignedString(request.getObjectID()));
    auditMap.put("offset", Long.toUnsignedString(request.getOffset()));
    auditMap.put("transferID", request.getTransferID());
    auditMap.put("operation", request.getOperation().name());
    boolean locked = false;
    Exception exception = null;
    OMClientResponse clientResponse;
    try {
      validateRequest(request);
      if (!request.hasLeaderTimeMillis() || request.getLeaderTimeMillis() <= 0) {
        throw new OMException("NFS transfer range is missing the OM leader timestamp", INVALID_REQUEST);
      }
      if (!metadataManager.isNfsDurableWriteRangesEnabled() || !metadataManager.isNfsObjectIndexReady()) {
        throw new OMException("NFS durable write ranges are not enabled", NOT_SUPPORTED_OPERATION);
      }
      locked = metadataManager.getLock().acquireWriteLock(
          BUCKET_LOCK, request.getVolumeName(), request.getBucketName()).isLockAcquired();
      if (!locked) {
        throw new OMException("Cannot acquire NFS transfer bucket lock", INVALID_REQUEST);
      }
      OmBucketInfo bucket = metadataManager.getBucketTable().get(
          metadataManager.getBucketKey(request.getVolumeName(), request.getBucketName()));
      if (bucket == null) {
        throw new OMException("NFS transfer bucket does not exist", BUCKET_NOT_FOUND);
      }
      if (bucket.getBucketLayout() != BucketLayout.FILE_SYSTEM_OPTIMIZED) {
        throw new OMException("NFS durable writes require a native FSO bucket", NOT_SUPPORTED_OPERATION);
      }

      long volumeId = metadataManager.getVolumeId(request.getVolumeName());
      long bucketId = bucket.getObjectID();
      OmNfsLeaseInfo exportLease = metadataManager.getNfsExportLeaseTable().get(
          metadataManager.getNfsExportLeaseKey(request.getExportUUID()));
      String leaseKey = metadataManager.getNfsFileLeaseKey(volumeId, bucketId, request.getObjectID());
      OmNfsLeaseInfo fileLease = metadataManager.getNfsFileLeaseTable().get(leaseKey);
      if (exportLease == null || fileLease == null) {
        throw new OMException("NFS transfer lease is missing", NFS_LEASE_CONFLICT);
      }
      try {
        fileLease.validateFileCommit(exportLease, request.getExportUUID(), request.getGatewayID(),
            request.getGatewayGeneration(), request.getTransferID(), request.getLeaseGeneration(),
            request.getOwnerToken(), request.getLeaderTimeMillis());
      } catch (IllegalStateException conflict) {
        throw new OMException(conflict.getMessage(), NFS_LEASE_CONFLICT);
      }

      OmNfsTransferRangeInfo range = new OmNfsTransferRangeInfo(request.getExportUUID(), volumeId, bucketId,
          request.getObjectID(), request.getLeaseGeneration(), request.getOffset(), request.getLength(),
          request.getTransferID(), request.getStagingKey(), request.getSha256(), context.getIndex());
      String rangeKey = metadataManager.getNfsTransferRangeKey(request.getExportUUID(), volumeId, bucketId,
          request.getObjectID(), request.getLeaseGeneration(), request.getOffset(), request.getStagingKey());
      Table<String, OmNfsTransferRangeInfo> rangeTable = metadataManager.getNfsTransferRangeTable();
      OmNfsTransferRangeInfo existing = rangeTable.get(rangeKey);
      boolean delete = request.getOperation() == NfsTransferRangeRequest.Operation.DELETE;
      boolean newRange = existing == null || !sameManifest(existing, range);
      OmNfsLeaseInfo updatedFileLease = null;
      if (delete) {
        if (existing != null && !sameManifest(existing, range)) {
          throw new OMException("NFS transfer range delete does not match the manifest", NFS_LEASE_CONFLICT);
        }
      } else if (existing != null && existing.getStagingKey().equals(range.getStagingKey())
          && !sameManifest(existing, range)) {
        throw new OMException("NFS staging key was reused for different range data", NFS_LEASE_CONFLICT);
      }

      long transactionIndex = existing != null && sameManifest(existing, range)
          ? existing.getTransactionIndex() : context.getIndex();
      if (transactionIndex != range.getTransactionIndex()) {
        range = new OmNfsTransferRangeInfo(request.getExportUUID(), volumeId, bucketId, request.getObjectID(),
            request.getLeaseGeneration(), request.getOffset(), request.getLength(), request.getTransferID(),
            request.getStagingKey(), request.getSha256(), transactionIndex);
      }
      if (delete) {
        rangeTable.addCacheEntry(rangeKey, context.getIndex());
      } else {
        rangeTable.addCacheEntry(rangeKey, range, context.getIndex());
        if (newRange && fileLease.getProtobuf().getContentPublished()) {
          updatedFileLease = fileLease.withContentPublished(false);
          metadataManager.getNfsFileLeaseTable().addCacheEntry(leaseKey, updatedFileLease, context.getIndex());
        }
      }
      response.setNfsTransferRangeResponse(NfsTransferRangeResponse.newBuilder().setRecorded(true)
          .setTransactionIndex(transactionIndex).build());
      clientResponse = new OMNfsTransferRangeResponse(response.build(), rangeKey, range, delete,
          updatedFileLease == null ? null : leaseKey, updatedFileLease);
    } catch (IOException | IllegalArgumentException | IllegalStateException | ArithmeticException ex) {
      exception = ex;
      if (ex instanceof IllegalStateException) {
        exception = new OMException(ex.getMessage(), NFS_LEASE_CONFLICT);
      } else if (ex instanceof IllegalArgumentException || ex instanceof ArithmeticException) {
        exception = new OMException(ex.getMessage(), INVALID_REQUEST);
      }
      clientResponse = new OMNfsTransferRangeResponse(createErrorOMResponse(response, exception));
    } finally {
      if (locked) {
        metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, request.getVolumeName(), request.getBucketName());
      }
      markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
          OMAction.NFS_TRANSFER_RANGE, auditMap, exception, getOmRequest().getUserInfo()));
    }
    return clientResponse;
  }

  private static boolean sameManifest(OmNfsTransferRangeInfo existing, OmNfsTransferRangeInfo incoming) {
    return existing.getExportUUID().equals(incoming.getExportUUID())
        && existing.getVolumeID() == incoming.getVolumeID() && existing.getBucketID() == incoming.getBucketID()
        && existing.getObjectID() == incoming.getObjectID()
        && existing.getLeaseGeneration() == incoming.getLeaseGeneration()
        && existing.getOffset() == incoming.getOffset() && existing.getLength() == incoming.getLength()
        && existing.getTransferID().equals(incoming.getTransferID())
        && existing.getStagingKey().equals(incoming.getStagingKey())
        && existing.getSha256().equals(incoming.getSha256());
  }

  private static void validateRequest(NfsTransferRangeRequest request) throws OMException {
    if (!request.isInitialized()) {
      throw new OMException("NFS transfer range request is incomplete", INVALID_REQUEST);
    }
    try {
      UUID.fromString(request.getExportUUID());
    } catch (IllegalArgumentException ex) {
      throw new OMException("Invalid NFS export UUID", INVALID_REQUEST);
    }
    if (request.getVolumeName().isEmpty() || request.getBucketName().isEmpty() || request.getGatewayID().isEmpty()
        || request.getTransferID().isEmpty()) {
      throw new OMException("NFS transfer range identity is incomplete", INVALID_REQUEST);
    }
    if (request.getOwnerToken().isEmpty() || request.getGatewayGeneration() == 0 || request.getLeaseGeneration() == 0) {
      throw new OMException("NFS transfer range lease identity is incomplete", INVALID_REQUEST);
    }
    if (request.getOffset() < 0 || request.getLength() <= 0 || request.getStagingKey().isEmpty()
        || request.getSha256().size() != 32 || request.getStagingKey().startsWith("/")) {
      throw new OMException("NFS transfer range data is invalid", INVALID_REQUEST);
    }
    for (String component : request.getStagingKey().split("/")) {
      if (component.equals("..")) {
        throw new OMException("NFS staging key contains a parent traversal", INVALID_REQUEST);
      }
    }
    try {
      Math.addExact(request.getOffset(), request.getLength());
    } catch (ArithmeticException ex) {
      throw new OMException("NFS transfer range end overflows", INVALID_REQUEST);
    }
  }
}
