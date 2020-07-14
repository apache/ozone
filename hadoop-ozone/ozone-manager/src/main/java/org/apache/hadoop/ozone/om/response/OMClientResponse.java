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
import java.util.concurrent.CompletableFuture;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

/**
 * Interface for OM Responses, each OM response should implement this interface.
 */
public abstract class OMClientResponse {

  private OMResponse omResponse;
  private CompletableFuture<Void> flushFuture = null;

  public OMClientResponse(OMResponse omResponse) {
    Preconditions.checkNotNull(omResponse);
    this.omResponse = omResponse;
  }

  /**
   * For error case, check that the status of omResponse is not OK.
   */
  public void checkStatusNotOK() {
    Preconditions.checkArgument(!omResponse.getStatus().equals(
        OzoneManagerProtocolProtos.Status.OK));
  }

  /**
   * Check if omResponse status is OK. If yes, add to DB.
   * For OmResponse with failure, this should do nothing. This method is not
   * called in failure scenario in OM code.
   * @param omMetadataManager
   * @param batchOperation
   * @throws IOException
   */
  public void checkAndUpdateDB(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (omResponse.getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      addToDBBatch(omMetadataManager, batchOperation);
    }
  }

  /**
   * Implement logic to add the response to batch. This function should be
   * called from checkAndUpdateDB only.
   * @param omMetadataManager
   * @param batchOperation
   * @throws IOException
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

  public void setFlushFuture(CompletableFuture<Void> flushFuture) {
    this.flushFuture = flushFuture;
  }

  public CompletableFuture<Void> getFlushFuture() {
    return flushFuture;
  }

}

