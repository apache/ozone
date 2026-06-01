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

package org.apache.hadoop.ozone.om.request.lifecycle;

import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleScanState;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.lifecycle.OMLifecycleSaveScanStateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SaveLifecycleScanStateRequest;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Handles SaveLifecycleScanState request.
 */
public class OMLifecycleSaveScanStateRequest extends OMClientRequest {

  public OMLifecycleSaveScanStateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws OMException {
    if (ozoneManager.getAclsEnabled()) {
      UserGroupInformation ugi = createUGIForApi();
      if (!ozoneManager.isAdmin(ugi)) {
        throw new OMException("Access denied for user " + ugi + ". "
            + "Superuser privilege is required to save Lifecycle Service task state.",
            OMException.ResultCodes.ACCESS_DENIED);
      }
    }
    return getOmRequest();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    SaveLifecycleScanStateRequest request = getOmRequest().getSaveLifecycleScanStateRequest();
    OmLifecycleScanState state = OmLifecycleScanState.getFromProtobuf(request.getState());

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    // Update cache
    ozoneManager.getMetadataManager().getLifecycleScanStateTable()
        .addCacheEntry(new CacheKey<>(state.getBucketKey()),
            CacheValue.get(context.getIndex(), state));

    return new OMLifecycleSaveScanStateResponse(omResponse.build(), state);
  }
}
