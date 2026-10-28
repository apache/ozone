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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
 * Each request contains originalAccessKeyIds to remove from the revocation table, together with the cutoff the
 * cleanup scan read for each of them. An entry is removed only if its current cutoff still equals that value.
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
    final List<Long> revocationTimeMillisList = request.getRevocationTimeMillisList();
    final Table<String, Long> table = ozoneManager.getMetadataManager().getS3RevokedStsTokenTable();
    final List<String> deletedIds;
    try {
      if (revocationTimeMillisList.isEmpty()) {
        // Releases before cutoffs were added replicate requests with ids only.  Apply as they did, and add cache
        // delete markers those releases omitted.
        deletedIds = originalAccessKeyIds;
      } else if (revocationTimeMillisList.size() != originalAccessKeyIds.size()) {
        // Validated at apply time, since this request is submitted to Ratis without preExecute.
        // Uses INVALID_REQUEST rather than INTERNAL_ERROR, since the OM state machine terminates on INTERNAL_ERROR.
        throw new OMException(
            "Got " + revocationTimeMillisList.size() + " revocationTimeMillis for " + originalAccessKeyIds.size() +
            " originalAccessKeyIds", INVALID_REQUEST);
      } else {
        deletedIds = new ArrayList<>();
        for (int i = 0; i < originalAccessKeyIds.size(); i++) {
          // A cutoff written after the scan, such as a newer revocation of the same key, is kept
          if (Objects.equals(revocationTimeMillisList.get(i), table.getIfExist(originalAccessKeyIds.get(i)))) {
            deletedIds.add(originalAccessKeyIds.get(i));
          }
        }
      }
    } catch (IOException e) {
      return new S3DeleteRevokedSTSTokensResponse(null, createErrorOMResponse(omResponse, e));
    }

    if (!revocationTimeMillisList.isEmpty() && deletedIds.size() < originalAccessKeyIds.size()) {
      LOG.info(
          "Skipped {} of {} revoked STS token purge entries because the cutoff changed or the entry was " +
          "already removed.", originalAccessKeyIds.size() - deletedIds.size(), originalAccessKeyIds.size());
    }

    // The full table cache only evicts delete markers, so without one a deleted key's old cutoff stays cached
    for (String originalAccessKeyId : deletedIds) {
      table.addCacheEntry(new CacheKey<>(originalAccessKeyId), CacheValue.get(context.getIndex()));
    }
    return new S3DeleteRevokedSTSTokensResponse(deletedIds, omResponse.build());
  }
}


