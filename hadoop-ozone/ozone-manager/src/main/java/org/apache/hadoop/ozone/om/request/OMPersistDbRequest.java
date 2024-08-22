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

package org.apache.hadoop.ozone.om.request;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.DummyOMClientResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle OMQuotaRepairRequest Request.
 */
public class OMPersistDbRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMPersistDbRequest.class);

  public OMPersistDbRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    UserGroupInformation ugi = createUGIForApi();
    if (ozoneManager.getAclsEnabled() && !ozoneManager.isAdmin(ugi)) {
      throw new OMException("Access denied for user " + ugi + ". Admin privilege is required.",
          OMException.ResultCodes.ACCESS_DENIED);
    }
    return super.preExecute(ozoneManager);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, TermIndex termIndex) {
    OzoneManagerProtocolProtos.OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OzoneManagerProtocolProtos.PersistDbRequest dbUpdateRequest = getOmRequest().getPersistDbRequest();
    LOG.warn("sumit..dbupdate...{}--{}", dbUpdateRequest.getCacheIndex(), termIndex.getIndex());

    try (BatchOperation batchOperation = ozoneManager.getMetadataManager().getStore()
        .initBatchOperation()) {
      List<OzoneManagerProtocolProtos.DBTableUpdate> tableUpdatesList = dbUpdateRequest.getTableUpdatesList();
      for (OzoneManagerProtocolProtos.DBTableUpdate tblUpdates : tableUpdatesList) {
        Table table = ozoneManager.getMetadataManager().getTable(tblUpdates.getTableName());
        List<OzoneManagerProtocolProtos.DBTableRecord> recordsList = tblUpdates.getRecordsList();
        for (OzoneManagerProtocolProtos.DBTableRecord record : recordsList) {
          if (record.hasValue()) {
            table.getRawTable().putWithBatch(batchOperation, record.getKey().toByteArray(), record.getValue().toByteArray());
            // put
          } else {
            // delete
            table.getRawTable().deleteWithBatch(batchOperation, record.getKey().toByteArray());
          }
        }
      }
      ozoneManager.getMetadataManager().getStore().commitBatchOperation(batchOperation);
      omResponse.setPersistDbResponse(OzoneManagerProtocolProtos.PersistDbResponse.newBuilder().build());
    } catch (Exception ex) {
      LOG.warn("sumit..dbupdate..failed...{}--{}", dbUpdateRequest.getCacheIndex(), termIndex.getIndex());
      return new DummyOMClientResponse(createErrorOMResponse(omResponse, ex));
    }
    LOG.warn("sumit..dbupdate..success...{}--{}", dbUpdateRequest.getCacheIndex(), termIndex.getIndex());
    OMClientResponse omClientResponse = new DummyOMClientResponse(omResponse.build());
    return omClientResponse;
  }
}
