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

package org.apache.hadoop.ozone.om.request.eventlistener;

import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.eventlistener.OMSetEventNotificationCheckpointResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetEventNotificationCheckpointRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetEventNotificationCheckpointResponse;

/**
 * Handles OMSetEventNotificationCheckpointRequest.
 *
 * This is an Ozone Manager internal request used to persist event notification checkpoints
 * inside the metaTable.
 */
public class OMSetEventNotificationCheckpointRequest extends OMClientRequest {

  public OMSetEventNotificationCheckpointRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {

    OMClientResponse omClientResponse;
    final OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    final SetEventNotificationCheckpointRequest request =
        getOmRequest().getSetEventNotificationCheckpointRequest();

    final String checkpointKey = request.getCheckpointKey();
    final String checkpointValue = request.getCheckpointValue();

    // Store in metaTable using CacheKey with our specific checkpoint prefix
    final String dbKey = OzoneConsts.EVENT_NOTIFICATION_CHECKPOINT_PREFIX + checkpointKey;

    omMetadataManager.getMetaTable().addCacheEntry(
        new CacheKey<>(dbKey),
        CacheValue.get(context.getIndex(), checkpointValue));

    omResponse.setSetEventNotificationCheckpointResponse(
        SetEventNotificationCheckpointResponse.newBuilder().build());

    omClientResponse = new OMSetEventNotificationCheckpointResponse(
        omResponse.build(),
        dbKey,
        checkpointValue);

    return omClientResponse;
  }
}
