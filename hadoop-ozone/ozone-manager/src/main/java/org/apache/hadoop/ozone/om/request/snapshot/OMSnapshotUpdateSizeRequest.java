/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.request.snapshot;

import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotUpdateSizeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotSize;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Updates the exclusive size of the snapshot.
 */
public class OMSnapshotUpdateSizeRequest extends OMClientRequest {

  public OMSnapshotUpdateSizeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    OMClientResponse omClientResponse = null;
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OzoneManagerProtocolProtos.SnapshotUpdateSizeRequest
        snapshotUpdateSizeRequest = getOmRequest()
        .getSnapshotUpdateSizeRequest();

    List<SnapshotSize> snapshotSizeList = snapshotUpdateSizeRequest
        .getSnapshotSizeList();
    Map<String, SnapshotInfo> updatedSnapInfos = new HashMap<>();

    try {
      for (SnapshotSize snapshotSize: snapshotSizeList) {
        String snapshotKey = snapshotSize.getSnapshotKey();
        long exclusiveSize = snapshotSize.getExclusiveSize();
        long exclusiveReplicatedSize = snapshotSize
            .getExclusiveReplicatedSize();
        SnapshotInfo snapshotInfo = metadataManager
            .getSnapshotInfoTable().get(snapshotKey);

        if (snapshotInfo == null) {
          continue;
        }

        // Set Exclusive size.
        snapshotInfo.setExclusiveSize(exclusiveSize);
        snapshotInfo.setExclusiveReplicatedSize(exclusiveReplicatedSize);
        // Update Table Cache
        metadataManager.getSnapshotInfoTable().addCacheEntry(
            new CacheKey<>(snapshotKey),
            CacheValue.get(trxnLogIndex, snapshotInfo));
        updatedSnapInfos.put(snapshotKey, snapshotInfo);
      }
      omClientResponse = new OMSnapshotUpdateSizeResponse(
          omResponse.build(), updatedSnapInfos);
    } catch (IOException ex) {
      omClientResponse = new OMSnapshotUpdateSizeResponse(
          createErrorOMResponse(omResponse, ex));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
    }
    return omClientResponse;
  }
}
