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

package org.apache.hadoop.ozone.om.response.volume;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.volume.OMQuotaRepairRequest;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for {@link OMQuotaRepairRequest} request.
 */
@CleanupTableInfo(cleanupTables = {VOLUME_TABLE, BUCKET_TABLE})
public class OMQuotaRepairResponse extends OMClientResponse {
  private Map<String, OmVolumeArgs> volumeArgsMap;
  private Map<Pair<String, String>, OmBucketInfo> volBucketInfoMap;

  /**
   * for quota failure response update.
   */
  public OMQuotaRepairResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
  }

  public OMQuotaRepairResponse(
      @Nonnull OMResponse omResponse, Map<String, OmVolumeArgs> volumeArgsMap,
      Map<Pair<String, String>, OmBucketInfo> volBucketInfoMap) {
    super(omResponse);
    this.volBucketInfoMap = volBucketInfoMap;
    this.volumeArgsMap = volumeArgsMap;
  }

  @Override
  public void addToDBBatch(OMMetadataManager metadataManager,
      BatchOperation batchOp) throws IOException {
    for (OmBucketInfo omBucketInfo : volBucketInfoMap.values()) {
      metadataManager.getBucketTable().putWithBatch(batchOp,
          metadataManager.getBucketKey(omBucketInfo.getVolumeName(),
              omBucketInfo.getBucketName()), omBucketInfo);
    }
    for (OmVolumeArgs volArgs : volumeArgsMap.values()) {
      metadataManager.getVolumeTable().putWithBatch(batchOp, volArgs.getVolume(), volArgs);
    }
  }
}
