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

import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMRangerServiceVersionSyncResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RangerServiceVersionSyncRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RangerServiceVersionSyncResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

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
  public OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OMClientResponse omClientResponse;
    final OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    final RangerServiceVersionSyncRequest request =
        getOmRequest().getRangerServiceVersionSyncRequest();
    final long proposedVersion = request.getRangerServiceVersion();
    final String proposedVersionStr = String.valueOf(proposedVersion);

    omMetadataManager.getMetaTable().addCacheEntry(
        new CacheKey<>(OzoneConsts.RANGER_OZONE_SERVICE_VERSION_KEY),
        new CacheValue<>(Optional.of(proposedVersionStr), transactionLogIndex));
    omResponse.setRangerServiceVersionSyncResponse(
        RangerServiceVersionSyncResponse.newBuilder().build());

    omClientResponse = new OMRangerServiceVersionSyncResponse(
        omResponse.build(),
        OzoneConsts.RANGER_OZONE_SERVICE_VERSION_KEY,
        proposedVersionStr);
    addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
        ozoneManagerDoubleBufferHelper);

    return omClientResponse;
  }
}
