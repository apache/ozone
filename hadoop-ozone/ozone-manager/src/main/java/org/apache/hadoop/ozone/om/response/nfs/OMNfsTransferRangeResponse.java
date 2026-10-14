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

package org.apache.hadoop.ozone.om.response.nfs;

import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsTransferRangeInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/** Persists one NFS write-range manifest transition in the OM Ratis batch. */
public class OMNfsTransferRangeResponse extends OMClientResponse {
  private final String rangeKey;
  private final OmNfsTransferRangeInfo range;
  private final boolean delete;
  private final String leaseKey;
  private final OmNfsLeaseInfo fileLease;

  public OMNfsTransferRangeResponse(OMResponse response, String rangeKey,
      OmNfsTransferRangeInfo range, boolean delete, String leaseKey, OmNfsLeaseInfo fileLease) {
    super(response);
    this.rangeKey = rangeKey;
    this.range = range;
    this.delete = delete;
    this.leaseKey = leaseKey;
    this.fileLease = fileLease;
  }

  public OMNfsTransferRangeResponse(OMResponse response) {
    super(response);
    checkStatusNotOK();
    rangeKey = null;
    range = null;
    delete = false;
    leaseKey = null;
    fileLease = null;
  }

  @Override
  protected void addToDBBatch(OMMetadataManager manager, BatchOperation batch) throws IOException {
    if (delete) {
      manager.getNfsTransferRangeTable().deleteWithBatch(batch, rangeKey);
    } else {
      manager.getNfsTransferRangeTable().putWithBatch(batch, rangeKey, range);
    }
    if (fileLease != null) {
      manager.getNfsFileLeaseTable().putWithBatch(batch, leaseKey, fileLease);
    }
  }
}
