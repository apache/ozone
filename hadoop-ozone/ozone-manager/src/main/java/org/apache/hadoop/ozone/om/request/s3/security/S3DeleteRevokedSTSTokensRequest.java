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

package org.apache.hadoop.ozone.om.request.s3.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3DeleteRevokedSTSTokensResponse;
import org.apache.hadoop.ozone.om.service.RevokedSTSTokenCleanupService;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteRevokedSTSTokensRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles DeleteRevokedSTSTokens requests submitted by {@link RevokedSTSTokenCleanupService}.
 * Each request lists originalAccessKeyIds to remove from the revocation table. When {@code purgeBeforeMillis} is set,
 * an entry is removed only if its current cutoff is older than that threshold. Requests without
 * {@code purgeBeforeMillis} delete all listed ids.
 */
public class S3DeleteRevokedSTSTokensRequest extends OMClientRequest {

  private static final Logger LOG = LoggerFactory.getLogger(S3DeleteRevokedSTSTokensRequest.class);

  public S3DeleteRevokedSTSTokensRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final UserGroupInformation ugi;
    try {
      ugi = createUGI();
    } catch (AuthenticationException e) {
      throw new OMException(e, OMException.ResultCodes.PERMISSION_DENIED);
    }
    if (!ozoneManager.isAdmin(ugi) && !ozoneManager.isS3Admin(ugi)) {
      throw new OMException("Only admins can delete revoked STS tokens", OMException.ResultCodes.PERMISSION_DENIED);
    }

    return super.preExecute(ozoneManager);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final DeleteRevokedSTSTokensRequest request = getOmRequest().getDeleteRevokedSTSTokensRequest();
    final OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());

    final List<String> originalAccessKeyIds = request.getOriginalAccessKeyIdList();
    final Table<String, Long> table = ozoneManager.getMetadataManager().getS3RevokedStsTokenTable();
    final List<String> deletedIds;
    try {
      if (!request.hasPurgeBeforeMillis()) {
        // Delete all listed ids if the request does not have purgeBeforeMillis
        deletedIds = originalAccessKeyIds;
      } else {
        final long purgeBeforeMillis = request.getPurgeBeforeMillis();
        deletedIds = new ArrayList<>();
        for (String originalAccessKeyId : originalAccessKeyIds) {
          final Long revocationTimeMillis = table.getIfExist(originalAccessKeyId);
          // A cutoff written after the scan, such as a newer revocation of the same key, is kept unless it is also
          // older than purgeBeforeMillis.
          if (revocationTimeMillis != null && revocationTimeMillis < purgeBeforeMillis) {
            deletedIds.add(originalAccessKeyId);
          }
        }
      }
    } catch (IOException e) {
      return new S3DeleteRevokedSTSTokensResponse(null, createErrorOMResponse(omResponse, e));
    }

    if (request.hasPurgeBeforeMillis() && deletedIds.size() < originalAccessKeyIds.size()) {
      LOG.info(
          "Skipped {} of {} revoked STS token purge entries because the cutoff is not older than the purge " +
          "threshold or the entry was already removed.", originalAccessKeyIds.size() - deletedIds.size(),
          originalAccessKeyIds.size());
    }

    // The full table cache only evicts delete markers, so without one a deleted key's old cutoff stays cached
    for (String originalAccessKeyId : deletedIds) {
      table.addCacheEntry(new CacheKey<>(originalAccessKeyId), CacheValue.get(context.getIndex()));
    }
    return new S3DeleteRevokedSTSTokensResponse(deletedIds, omResponse.build());
  }
}


