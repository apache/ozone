/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.request.s3.tenant;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.RESOURCE_BUSY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TENANT_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.USER_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.multitenant.AccessPolicy;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMRangerServiceVersionSyncResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RangerServiceVersionSyncRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RangerServiceVersionSyncResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserVolumeInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/*
 * This request is issued by the RangerSync Background thread to update the
 * OzoneServiceVersion as read from the Ranger during the  most up-to-date
 * ranger-to-OMDB sync operation.
 */

/**
 * Handles OMRangerServiceVersionSync request.
 */
public class OMRangerServiceVersionSyncRequest extends OMClientRequest {
  public static final Logger LOG =
      LoggerFactory.getLogger(OMRangerServiceVersionSyncRequest.class);

  public OMRangerServiceVersionSyncRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    final RangerServiceVersionSyncRequest request =
        getOmRequest().getRangerServiceVersionSyncRequest();
    final long proposedVersion = request.getRangerServiceVersion();

    if (!ozoneManager.getMultiTenantManager()
        .tryAcquireInProgressMtOp(WAIT_MILISECONDS)) {
      throw new OMException("Only One MultiTenant operation allowed at a " +
          "time", RESOURCE_BUSY);
    }

    final OMRequest.Builder omRequestBuilder = getOmRequest().toBuilder()
        .setRangerServiceVersionSyncRequest(
            RangerServiceVersionSyncRequest.newBuilder()
                .setRangerServiceVersion(proposedVersion))
        // TODO: Can the three lines below be ignored?
        .setUserInfo(getUserInfo())
        .setCmdType(getOmRequest().getCmdType())
        .setClientId(getOmRequest().getClientId());

    if (getOmRequest().hasTraceID()) {
      omRequestBuilder.setTraceID(getOmRequest().getTraceID());
    }

    return omRequestBuilder.build();

  }

  @Override
  public void handleRequestFailure(OzoneManager ozoneManager) {
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OMClientResponse omClientResponse = null;
    final OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    final RangerServiceVersionSyncRequest request
        = getOmRequest().getRangerServiceVersionSyncRequest();
    final long proposedVersion = request.getRangerServiceVersion();
    Exception exception = null;

    try {
      omMetadataManager.getOmRangerStateTable().addCacheEntry(
          new CacheKey<>(OmMetadataManagerImpl.RangerOzoneServiceVersionKey),
          new CacheValue<>(Optional.of(proposedVersion), transactionLogIndex));
      omResponse.setRangerServiceVersionSyncResponse(
          RangerServiceVersionSyncResponse.newBuilder().build()
      );

      omClientResponse = new OMRangerServiceVersionSyncResponse(
          omResponse.build(), proposedVersion,
          OmMetadataManagerImpl.RangerOzoneServiceVersionKey);

    } catch (Exception ex) {
      // Prepare omClientResponse
      omResponse.setRangerServiceVersionSyncResponse(
          RangerServiceVersionSyncResponse.newBuilder().build());
      omClientResponse = new OMTenantCreateResponse(
          createErrorOMResponse(omResponse, new IOException(ex.getMessage())));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(ozoneManagerDoubleBufferHelper
            .add(omClientResponse, transactionLogIndex));
      }
      ozoneManager.getMultiTenantManager().resetInProgressMtOpState();
    }

    return omClientResponse;
  }
}
