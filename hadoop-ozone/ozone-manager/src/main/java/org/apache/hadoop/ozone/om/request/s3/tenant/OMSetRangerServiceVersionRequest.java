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

package org.apache.hadoop.ozone.om.request.s3.tenant;

import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMSetRangerServiceVersionResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetRangerServiceVersionRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetRangerServiceVersionResponse;

/**
 * Handles OMSetRangerServiceVersionRequest.
 *
 * This is an Ozone Manager internal request issued only by the Ranger
 * Background Sync service (OMRangerBGSyncService). This request writes
 * OzoneServiceVersion (retrieved from Ranger) to OM DB during the sync.
 */
public class OMSetRangerServiceVersionRequest extends OMClientRequest {

  public OMSetRangerServiceVersionRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {

    OMClientResponse omClientResponse;
    final OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    final SetRangerServiceVersionRequest request =
        getOmRequest().getSetRangerServiceVersionRequest();
    final long proposedVersion = request.getRangerServiceVersion();
    final String proposedVersionStr = String.valueOf(proposedVersion);

    omMetadataManager.getMetaTable().addCacheEntry(
        new CacheKey<>(OzoneConsts.RANGER_OZONE_SERVICE_VERSION_KEY),
        CacheValue.get(context.getIndex(), proposedVersionStr));
    omResponse.setSetRangerServiceVersionResponse(
        SetRangerServiceVersionResponse.newBuilder().build());

    omClientResponse = new OMSetRangerServiceVersionResponse(
        omResponse.build(),
        OzoneConsts.RANGER_OZONE_SERVICE_VERSION_KEY,
        proposedVersionStr);

    return omClientResponse;
  }
}
