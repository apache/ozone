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

package org.apache.hadoop.ozone.om.response;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;

/**
 * Interface for OM Responses, each OM response should implement this interface.
 */
public abstract class OMClientResponse {

  private final OMResponse omResponse;
  private OMLockDetails omLockDetails;

  public OMClientResponse(OMResponse omResponse) {
    Objects.requireNonNull(omResponse, "omResponse == null");
    this.omResponse = omResponse;
  }

  public BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  /**
   * For error case, check that the status of omResponse is not OK.
   */
  public void checkStatusNotOK() {
    Preconditions.checkArgument(!omResponse.getStatus().equals(Status.OK));
  }

  /**
   * Check if omResponse status is OK. If yes, add to DB.
   * For OmResponse with failure, this should do nothing. This method is not
   * called in failure scenario in OM code.
   */
  public void checkAndUpdateDB(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (omResponse.getStatus() == Status.OK) {
      addToDBBatch(omMetadataManager, batchOperation);
    }
  }

  /**
   * Implement logic to add the response to batch. This function should be
   * called from checkAndUpdateDB only.
   */
  protected abstract void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException;

  /**
   * Return OMResponse.
   * @return OMResponse
   */
  public OMResponse getOMResponse() {
    return omResponse;
  }

  public OMLockDetails getOmLockDetails() {
    return omLockDetails;
  }

  public void setOmLockDetails(
      OMLockDetails omLockDetails) {
    this.omLockDetails = omLockDetails;
  }
}

