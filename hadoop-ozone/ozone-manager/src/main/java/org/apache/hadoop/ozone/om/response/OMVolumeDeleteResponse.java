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

package org.apache.hadoop.ozone.om.response;

import java.io.IOException;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.utils.db.BatchOperation;

/**
 * Response for CreateVolume request.
 */
public class OMVolumeDeleteResponse implements OMClientResponse {
  private String volume;
  private String owner;
  private OzoneManagerProtocolProtos.VolumeList updatedVolumeList;

  public OMVolumeDeleteResponse(String volume, String owner,
      OzoneManagerProtocolProtos.VolumeList updatedVolumeList) {
    this.volume = volume;
    this.owner = owner;
    this.updatedVolumeList = updatedVolumeList;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    String dbUserKey = omMetadataManager.getUserKey(owner);
    OzoneManagerProtocolProtos.VolumeList volumeList =
        updatedVolumeList;
    if (updatedVolumeList.getVolumeNamesList().size() == 0) {
      omMetadataManager.getUserTable().deleteWithBatch(batchOperation,
          dbUserKey);
    } else {
      omMetadataManager.getUserTable().putWithBatch(batchOperation, dbUserKey,
          volumeList);
    }
    omMetadataManager.getVolumeTable().deleteWithBatch(batchOperation,
        omMetadataManager.getVolumeKey(volume));
  }
}

