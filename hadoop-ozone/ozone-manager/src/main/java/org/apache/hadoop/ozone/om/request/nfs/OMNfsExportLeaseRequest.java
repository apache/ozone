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
import org.apache.hadoop.ozone.om.response.nfs.OMNfsExportLeaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;

/** Applies a leader-timestamped export lease transition through the OM Ratis log. */
public class OMNfsExportLeaseRequest extends OMClientRequest {
  private static final long MAX_LEASE_DURATION_MILLIS = 60_000;

  public OMNfsExportLeaseRequest(OMRequest request) {
    super(request);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest request = super.preExecute(ozoneManager);
    NfsExportLeaseRequest leaseRequest = request.getNfsExportLeaseRequest();
    validateRequest(leaseRequest);
    if (ozoneManager.getAclsEnabled()) {
      checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET, OzoneObj.StoreType.OZONE,
          IAccessAuthorizer.ACLType.WRITE, leaseRequest.getVolumeName(), leaseRequest.getBucketName(), null);
    }
    return request.toBuilder().setNfsExportLeaseRequest(
        leaseRequest.toBuilder().setLeaderTimeMillis(Time.now())).build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    NfsExportLeaseRequest request = getOmRequest().getNfsExportLeaseRequest();
    OMResponse.Builder response = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put("exportUUID", request.getExportUUID());
    auditMap.put("volume", request.getVolumeName());
    auditMap.put("bucket", request.getBucketName());
    auditMap.put("gatewayID", request.getGatewayID());
    auditMap.put("operation", request.getOperation().name());

    boolean locked = false;
    Exception exception = null;
    OMClientResponse clientResponse;
    try {
      validateRequest(request);
      if (!metadataManager.isNfsWriterLeaseEnabled() || !metadataManager.isNfsObjectIndexReady()) {
        throw new OMException("NFS writer lease feature is not ready", NOT_SUPPORTED_OPERATION);
      }
      locked = metadataManager.getLock().acquireWriteLock(
          BUCKET_LOCK, request.getVolumeName(), request.getBucketName()).isLockAcquired();
      if (!locked) {
        throw new OMException("Cannot acquire NFS export bucket lock", INVALID_REQUEST);
      }
      OmBucketInfo bucket = metadataManager.getBucketTable().get(
          metadataManager.getBucketKey(request.getVolumeName(), request.getBucketName()));
      if (bucket == null) {
        throw new OMException("NFS export bucket does not exist", BUCKET_NOT_FOUND);
      }
      if (bucket.getBucketLayout() != BucketLayout.FILE_SYSTEM_OPTIMIZED) {
        throw new OMException("NFS exports require a native FSO bucket", NOT_SUPPORTED_OPERATION);
      }

      String leaseKey = metadataManager.getNfsExportLeaseKey(request.getExportUUID());
      OmNfsLeaseInfo current = metadataManager.getNfsExportLeaseTable().get(leaseKey);
      OmNfsLeaseInfo updated = transition(current, request,
          metadataManager.getVolumeId(request.getVolumeName()), bucket.getObjectID());
      metadataManager.getNfsExportLeaseTable().addCacheEntry(
          leaseKey, updated, context.getIndex());
      NfsExportLeaseResponse leaseResponse = toResponse(updated);
      response.setNfsExportLeaseResponse(leaseResponse);
      clientResponse = new OMNfsExportLeaseResponse(response.build(), leaseKey, updated);
    } catch (IOException | IllegalArgumentException | IllegalStateException | ArithmeticException ex) {
      exception = ex;
      if (ex instanceof IllegalStateException) {
        exception = new OMException(ex.getMessage(), NFS_LEASE_CONFLICT);
      } else if (ex instanceof IllegalArgumentException || ex instanceof ArithmeticException) {
        exception = new OMException(ex.getMessage(), INVALID_REQUEST);
      }
      clientResponse = new OMNfsExportLeaseResponse(createErrorOMResponse(response, exception));
    } finally {
      if (locked) {
        metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, request.getVolumeName(), request.getBucketName());
      }
      markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
          OMAction.NFS_EXPORT_LEASE, auditMap, exception, getOmRequest().getUserInfo()));
    }
    return clientResponse;
  }

  private static OmNfsLeaseInfo transition(OmNfsLeaseInfo current, NfsExportLeaseRequest request,
      long volumeId, long bucketId) throws IOException {
    if (!request.hasLeaderTimeMillis() || request.getLeaderTimeMillis() <= 0) {
      throw new OMException("NFS lease request is missing the OM leader timestamp", INVALID_REQUEST);
    }
    long now = request.getLeaderTimeMillis();
    switch (request.getOperation()) {
    case ACQUIRE:
      try {
        return OmNfsLeaseInfo.acquireExportLease(current, request.getExportUUID(), volumeId, bucketId,
            request.getGatewayID(), now, request.getLeaseDurationMillis());
      } catch (IllegalStateException conflict) {
        throw new OMException(conflict.getMessage(), NFS_LEASE_CONFLICT);
      }
    case RENEW:
      if (current == null) {
        throw new OMException("NFS export lease does not exist", NFS_LEASE_CONFLICT);
      }
      try {
        return current.renewExportLease(request.getGatewayID(), request.getGatewayGeneration(),
            request.getGeneration(), now, request.getLeaseDurationMillis());
      } catch (IllegalStateException conflict) {
        throw new OMException(conflict.getMessage(), NFS_LEASE_CONFLICT);
      }
    case RELEASE:
      if (current == null) {
        throw new OMException("NFS export lease does not exist", NFS_LEASE_CONFLICT);
      }
      try {
        return current.releaseExportLease(request.getGatewayID(), request.getGatewayGeneration(),
            request.getGeneration(), now);
      } catch (IllegalStateException conflict) {
        throw new OMException(conflict.getMessage(), NFS_LEASE_CONFLICT);
      }
    default:
      throw new OMException("Unsupported NFS export lease operation", INVALID_REQUEST);
    }
  }

  private static NfsExportLeaseResponse toResponse(OmNfsLeaseInfo lease) {
    return NfsExportLeaseResponse.newBuilder().setExportUUID(lease.getProtobuf().getExportUUID())
        .setVolumeID(lease.getProtobuf().getVolumeID()).setBucketID(lease.getProtobuf().getBucketID())
        .setGatewayID(lease.getProtobuf().getGatewayID())
        .setGatewayGeneration(lease.getProtobuf().getGatewayGeneration())
        .setGeneration(lease.getProtobuf().getGeneration())
        .setExpiresAtMillis(lease.getProtobuf().getExpiresAtMillis()).build();
  }

  private static void validateRequest(NfsExportLeaseRequest request) throws OMException {
    try {
      UUID.fromString(request.getExportUUID());
    } catch (IllegalArgumentException ex) {
      throw new OMException("Invalid NFS export UUID", INVALID_REQUEST);
    }
    if (request.getGatewayID().isEmpty()) {
      throw new OMException("NFS gateway identity is empty", INVALID_REQUEST);
    }
    switch (request.getOperation()) {
    case ACQUIRE:
      if (request.hasGeneration() || request.hasGatewayGeneration()) {
        throw new OMException("Acquire must not specify lease generations", INVALID_REQUEST);
      }
      validateDuration(request);
      break;
    case RENEW:
      validateOwner(request);
      validateDuration(request);
      break;
    case RELEASE:
      validateOwner(request);
      break;
    default:
      throw new OMException("Unsupported NFS export lease operation", INVALID_REQUEST);
    }
  }

  private static void validateOwner(NfsExportLeaseRequest request) throws OMException {
    if (!request.hasGatewayGeneration() || request.getGatewayGeneration() == 0
        || !request.hasGeneration() || request.getGeneration() == 0) {
      throw new OMException("NFS export lease generations are required", INVALID_REQUEST);
    }
  }

  private static void validateDuration(NfsExportLeaseRequest request) throws OMException {
    if (!request.hasLeaseDurationMillis() || request.getLeaseDurationMillis() == 0
        || request.getLeaseDurationMillis() > MAX_LEASE_DURATION_MILLIS) {
      throw new OMException("NFS export lease duration must be between 1 and 60000 ms", INVALID_REQUEST);
    }
  }
}
