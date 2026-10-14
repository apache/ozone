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
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NFS_LEASE_CONFLICT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.nfs.OMNfsFileLeaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileLeaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;

/** Applies an OM-timestamped NFS file lease renewal or release through Ratis. */
public class OMNfsFileLeaseRequest extends OMClientRequest {
  private static final long MAX_LEASE_DURATION_MILLIS = 60_000;

  public OMNfsFileLeaseRequest(OMRequest request) {
    super(request);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest request = super.preExecute(ozoneManager);
    NfsFileLeaseRequest leaseRequest = request.getNfsFileLeaseRequest();
    validateRequest(leaseRequest);
    if (ozoneManager.getAclsEnabled()) {
      checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET, OzoneObj.StoreType.OZONE,
          IAccessAuthorizer.ACLType.WRITE, leaseRequest.getVolumeName(), leaseRequest.getBucketName(), null);
    }
    NfsFileLeaseRequest.Builder leaseRequestBuilder = leaseRequest.toBuilder().setLeaderTimeMillis(Time.now());
    if (leaseRequest.getOperation() == NfsFileLeaseRequest.Operation.RECOVER) {
      UUID ownerToken = UUID.randomUUID();
      leaseRequestBuilder.setOwnerToken(ByteString.copyFrom(ByteBuffer.allocate(16)
          .putLong(ownerToken.getMostSignificantBits()).putLong(ownerToken.getLeastSignificantBits()).array()));
    }
    return request.toBuilder().setNfsFileLeaseRequest(leaseRequestBuilder).build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    NfsFileLeaseRequest request = getOmRequest().getNfsFileLeaseRequest();
    OMResponse.Builder response = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put("exportUUID", request.getExportUUID());
    auditMap.put("objectID", Long.toUnsignedString(request.getObjectID()));
    auditMap.put("transferID", request.getTransferID());
    auditMap.put("gatewayID", request.getGatewayID());
    auditMap.put("operation", request.getOperation().name());
    boolean locked = false;
    Exception exception = null;
    OMClientResponse clientResponse;
    try {
      validateRequest(request);
      if (request.getOperation() == NfsFileLeaseRequest.Operation.RECOVER
          && request.getOwnerToken().size() != 16) {
        throw new OMException("NFS file lease recovery token is invalid", INVALID_REQUEST);
      }
      if (!metadataManager.isNfsWriterLeaseEnabled() || !metadataManager.isNfsObjectIndexReady()) {
        throw new OMException("NFS writer lease feature is not ready", NOT_SUPPORTED_OPERATION);
      }
      locked = metadataManager.getLock().acquireWriteLock(
          BUCKET_LOCK, request.getVolumeName(), request.getBucketName()).isLockAcquired();
      if (!locked) {
        throw new OMException("Cannot acquire NFS file bucket lock", INVALID_REQUEST);
      }
      OmBucketInfo bucket = metadataManager.getBucketTable().get(
          metadataManager.getBucketKey(request.getVolumeName(), request.getBucketName()));
      if (bucket == null) {
        throw new OMException("NFS file bucket does not exist", BUCKET_NOT_FOUND);
      }
      if (bucket.getBucketLayout() != BucketLayout.FILE_SYSTEM_OPTIMIZED) {
        throw new OMException("NFS file leases require a native FSO bucket", NOT_SUPPORTED_OPERATION);
      }
      long volumeId = metadataManager.getVolumeId(request.getVolumeName());
      long bucketId = bucket.getObjectID();
      String exportKey = metadataManager.getNfsExportLeaseKey(request.getExportUUID());
      OmNfsLeaseInfo exportLease = metadataManager.getNfsExportLeaseTable().get(exportKey);
      if (exportLease == null || !isCurrentExport(exportLease, request, volumeId, bucketId)) {
        throw new OMException("NFS export owner is stale", NFS_LEASE_CONFLICT);
      }
      String leaseKey = metadataManager.getNfsFileLeaseKey(volumeId, bucketId, request.getObjectID());
      OmNfsLeaseInfo current = metadataManager.getNfsFileLeaseTable().get(leaseKey);
      if (current == null && request.getOperation() == NfsFileLeaseRequest.Operation.RECOVER) {
        throw new OMException("NFS file has no recoverable writer lease", FILE_NOT_FOUND);
      }
      if (current == null || !isFileIdentity(current, request, volumeId, bucketId)
          || (request.getOperation() != NfsFileLeaseRequest.Operation.RECOVER
          && !isCurrentFileOwner(current, request))) {
        throw new OMException("NFS file lease owner is stale", NFS_LEASE_CONFLICT);
      }
      OmNfsLeaseInfo updated = transition(current, exportLease, request);
      metadataManager.getNfsFileLeaseTable().addCacheEntry(leaseKey, updated, context.getIndex());
      NfsFileLeaseResponse.Builder leaseResponse = NfsFileLeaseResponse.newBuilder()
          .setGeneration(updated.getProtobuf().getGeneration())
          .setExpiresAtMillis(updated.getProtobuf().getExpiresAtMillis())
          .setContentPublished(updated.getProtobuf().getContentPublished());
      if (request.getOperation() == NfsFileLeaseRequest.Operation.RECOVER) {
        leaseResponse.setTransferID(updated.getProtobuf().getTransferID())
            .setOwnerToken(updated.getProtobuf().getOwnerToken()).setOwnerPrincipal(updated.getOwnerPrincipal());
      }
      response.setNfsFileLeaseResponse(leaseResponse);
      clientResponse = new OMNfsFileLeaseResponse(response.build(), leaseKey, updated);
    } catch (IOException | IllegalArgumentException | IllegalStateException | ArithmeticException ex) {
      exception = ex;
      if (ex instanceof IllegalStateException) {
        exception = new OMException(ex.getMessage(), NFS_LEASE_CONFLICT);
      } else if (ex instanceof IllegalArgumentException || ex instanceof ArithmeticException) {
        exception = new OMException(ex.getMessage(), INVALID_REQUEST);
      }
      clientResponse = new OMNfsFileLeaseResponse(createErrorOMResponse(response, exception));
    } finally {
      if (locked) {
        metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, request.getVolumeName(), request.getBucketName());
      }
      markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
          OMAction.NFS_FILE_LEASE, auditMap, exception, getOmRequest().getUserInfo()));
    }
    return clientResponse;
  }

  private static OmNfsLeaseInfo transition(OmNfsLeaseInfo current, OmNfsLeaseInfo exportLease,
      NfsFileLeaseRequest request) throws IOException {
    if (!request.hasLeaderTimeMillis() || request.getLeaderTimeMillis() <= 0) {
      throw new OMException("NFS file lease request is missing the OM leader timestamp", INVALID_REQUEST);
    }
    long now = request.getLeaderTimeMillis();
    ByteString ownerToken = request.getOwnerToken();
    try {
      if (request.getOperation() == NfsFileLeaseRequest.Operation.RENEW) {
        return current.renewFileLease(exportLease, request.getGeneration(), request.getTransferID(), ownerToken,
            now, request.getLeaseDurationMillis());
      }
      if (request.getOperation() == NfsFileLeaseRequest.Operation.RECOVER) {
        return current.recoverFileLease(exportLease, request.getRequesterPrincipal(), ownerToken,
            now, request.getLeaseDurationMillis());
      }
      return current.releaseFileLease(exportLease, request.getGeneration(), request.getTransferID(), ownerToken, now);
    } catch (IllegalStateException conflict) {
      throw new OMException(conflict.getMessage(), NFS_LEASE_CONFLICT);
    }
  }

  private static boolean isCurrentExport(OmNfsLeaseInfo lease, NfsFileLeaseRequest request,
      long volumeId, long bucketId) {
    return lease.getProtobuf().getExportUUID().equals(request.getExportUUID())
        && lease.getProtobuf().getVolumeID() == volumeId && lease.getProtobuf().getBucketID() == bucketId
        && lease.getProtobuf().getGatewayID().equals(request.getGatewayID())
        && lease.getProtobuf().getGatewayGeneration() == request.getGatewayGeneration();
  }

  private static boolean isFileIdentity(OmNfsLeaseInfo lease, NfsFileLeaseRequest request,
      long volumeId, long bucketId) {
    return lease.getProtobuf().getExportUUID().equals(request.getExportUUID())
        && lease.getProtobuf().getVolumeID() == volumeId && lease.getProtobuf().getBucketID() == bucketId
        && lease.getProtobuf().getObjectID() == request.getObjectID();
  }

  private static boolean isCurrentFileOwner(OmNfsLeaseInfo lease, NfsFileLeaseRequest request) {
    return lease.getProtobuf().getGatewayID().equals(request.getGatewayID())
        && lease.getProtobuf().getGatewayGeneration() == request.getGatewayGeneration();
  }

  private static void validateRequest(NfsFileLeaseRequest request) throws OMException {
    if (!request.isInitialized()) {
      throw new OMException("NFS file lease request is incomplete", INVALID_REQUEST);
    }
    try {
      UUID.fromString(request.getExportUUID());
    } catch (IllegalArgumentException ex) {
      throw new OMException("Invalid NFS export UUID", INVALID_REQUEST);
    }
    if (request.getGatewayID().isEmpty() || request.getGatewayGeneration() == 0) {
      throw new OMException("NFS file lease identity is incomplete", INVALID_REQUEST);
    }
    if (request.getOperation() == NfsFileLeaseRequest.Operation.RECOVER) {
      if (request.getRequesterPrincipal().isEmpty() || !request.hasLeaseDurationMillis()
          || request.getLeaseDurationMillis() == 0
          || request.getLeaseDurationMillis() > MAX_LEASE_DURATION_MILLIS) {
        throw new OMException("NFS file lease recovery identity or duration is invalid", INVALID_REQUEST);
      }
    } else if (request.getTransferID().isEmpty() || request.getOwnerToken().isEmpty() || request.getGeneration() == 0) {
      throw new OMException("NFS file lease identity is incomplete", INVALID_REQUEST);
    } else if (request.getOperation() == NfsFileLeaseRequest.Operation.RENEW) {
      if (!request.hasLeaseDurationMillis() || request.getLeaseDurationMillis() == 0
          || request.getLeaseDurationMillis() > MAX_LEASE_DURATION_MILLIS) {
        throw new OMException("NFS file lease duration must be between 1 and 60000 ms", INVALID_REQUEST);
      }
    } else if (request.hasLeaseDurationMillis()) {
      throw new OMException("NFS file lease release must not specify a duration", INVALID_REQUEST);
    }
  }
}
