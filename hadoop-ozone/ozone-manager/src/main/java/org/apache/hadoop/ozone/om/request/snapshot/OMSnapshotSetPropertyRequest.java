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
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotSetPropertyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Updates the exclusive size of the snapshot.
 */
public class OMSnapshotSetPropertyRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMSnapshotSetPropertyRequest.class);

  public OMSnapshotSetPropertyRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    OMClientResponse omClientResponse = null;
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OzoneManagerProtocolProtos.SetSnapshotPropertyRequest
        setSnapshotPropertyRequest = getOmRequest()
        .getSetSnapshotPropertyRequest();

    List<SnapshotProperty> snapshotPropertyList = setSnapshotPropertyRequest
        .getSnapshotPropertyList();
    Map<String, SnapshotInfo> updatedSnapInfos = new HashMap<>();

    try {
      for (SnapshotProperty snapshotProperty: snapshotPropertyList) {
        String snapshotKey = snapshotProperty.getSnapshotKey();
        long exclusiveSize = snapshotProperty.getExclusiveSize();
        long exclusiveReplicatedSize = snapshotProperty
            .getExclusiveReplicatedSize();
        SnapshotInfo snapshotInfo = metadataManager
            .getSnapshotInfoTable().get(snapshotKey);

        if (snapshotInfo == null) {
          LOG.error("SnapshotInfo for Snapshot: {} is not found", snapshotKey);
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
      omClientResponse = new OMSnapshotSetPropertyResponse(
          omResponse.build(), updatedSnapInfos);
    } catch (IOException ex) {
      omClientResponse = new OMSnapshotSetPropertyResponse(
          createErrorOMResponse(omResponse, ex));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
    }
    return omClientResponse;
  }
}
