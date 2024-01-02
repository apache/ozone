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

import org.apache.ratis.server.protocol.TermIndex;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_SNAPSHOT_ERROR;

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
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, TermIndex termIndex) {

    OMClientResponse omClientResponse = null;
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OzoneManagerProtocolProtos.SetSnapshotPropertyRequest
        setSnapshotPropertyRequest = getOmRequest()
        .getSetSnapshotPropertyRequest();

    SnapshotProperty snapshotProperty = setSnapshotPropertyRequest
        .getSnapshotProperty();
    SnapshotInfo updatedSnapInfo = null;

    try {
      String snapshotKey = snapshotProperty.getSnapshotKey();
      long exclusiveSize = snapshotProperty.getExclusiveSize();
      long exclusiveReplicatedSize = snapshotProperty
          .getExclusiveReplicatedSize();
      updatedSnapInfo = metadataManager.getSnapshotInfoTable()
          .get(snapshotKey);

      if (updatedSnapInfo == null) {
        LOG.error("SnapshotInfo for Snapshot: {} is not found", snapshotKey);
        throw new OMException("SnapshotInfo for Snapshot: " + snapshotKey +
            " is not found", INVALID_SNAPSHOT_ERROR);
      }

      // Set Exclusive size.
      updatedSnapInfo.setExclusiveSize(exclusiveSize);
      updatedSnapInfo.setExclusiveReplicatedSize(exclusiveReplicatedSize);
      // Update Table Cache
      metadataManager.getSnapshotInfoTable().addCacheEntry(
          new CacheKey<>(snapshotKey),
          CacheValue.get(termIndex.getIndex(), updatedSnapInfo));
      omClientResponse = new OMSnapshotSetPropertyResponse(
          omResponse.build(), updatedSnapInfo);
    } catch (IOException ex) {
      omClientResponse = new OMSnapshotSetPropertyResponse(
          createErrorOMResponse(omResponse, ex));
    }

    return omClientResponse;
  }
}
