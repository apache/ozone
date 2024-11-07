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
package org.apache.hadoop.ozone.om.ratis.execution.request;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.audit.OMSystemAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.DummyOMClientResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.ratis.execution.request.OmKeyUtils.createErrorOMResponse;

/**
 * Handle OMQuotaRepairRequest Request.
 */
public class OMPersistDbRequest extends OMRequestBase {
  private static final Logger LOG = LoggerFactory.getLogger(OMPersistDbRequest.class);

  public OMPersistDbRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preProcess(OzoneManager ozoneManager) throws IOException {
    throw new OMException("External request not allowed", OMException.ResultCodes.PERMISSION_DENIED);
  }

  @SuppressWarnings("methodlength")
  @Override
  public OMClientResponse process(OzoneManager ozoneManager, ExecutionContext exeCtx) throws IOException {
    OzoneManagerProtocolProtos.OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    BatchOperation batchOperation = exeCtx.getBatchOperation();
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    Table<String, OmBucketInfo> bucketTable = metadataManager.getBucketTable();
    Table<String, OmVolumeArgs> volumeTable = metadataManager.getVolumeTable();
    OzoneManagerProtocolProtos.PersistDbRequest dbUpdateRequest = getOmRequest().getPersistDbRequest();
    List<OzoneManagerProtocolProtos.DBTableUpdate> tableUpdatesList = dbUpdateRequest.getTableUpdatesList();
    try {
      for (OzoneManagerProtocolProtos.DBTableUpdate tblUpdates : tableUpdatesList) {
        // update volume changes
        if (volumeTable.getName().equals(tblUpdates.getTableName())) {
          updateVolumeRecord(volumeTable, batchOperation, tblUpdates.getRecordsList());
          continue;
        }
        // update bucket changes
        if (bucketTable.getName().equals(tblUpdates.getTableName())) {
          updateBucketRecord(bucketTable, batchOperation, tblUpdates.getRecordsList());
          continue;
        }
        // update other table changes
        Table table = metadataManager.getTable(tblUpdates.getTableName());
        for (OzoneManagerProtocolProtos.DBTableRecord record : tblUpdates.getRecordsList()) {
          if (record.hasValue()) {
            table.getRawTable().putWithBatch(batchOperation, record.getKey().toByteArray(),
                record.getValue().toByteArray());
          } else {
            // delete
            table.getRawTable().deleteWithBatch(batchOperation, record.getKey().toByteArray());
          }
        }
      }
      for (OzoneManagerProtocolProtos.BucketQuotaCount quota : dbUpdateRequest.getBucketQuotaCountList()) {
        String bucketKey = metadataManager.getBucketKey(quota.getVolName(), quota.getBucketName());
        // TODO remove bucket lock
        metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, quota.getVolName(), quota.getBucketName());
        try {
          OmBucketInfo bucketInfo = bucketTable.get(bucketKey);
          if (null == bucketInfo || bucketInfo.getObjectID() != quota.getBucketObjectId()) {
            continue;
          }
          bucketInfo.incrUsedBytes(quota.getDiffUsedBytes());
          bucketInfo.incrUsedNamespace(quota.getDiffUsedNamespace());
          bucketTable.putWithBatch(batchOperation, bucketKey, bucketInfo);
          bucketTable.addCacheEntry(bucketKey, bucketInfo, -1);
          LOG.debug("Updated bucket quota {}-{} for key {}", quota.getDiffUsedBytes(), quota.getDiffUsedNamespace(),
              quota.getBucketName());
        } finally {
          metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, quota.getVolName(), quota.getBucketName());
        }
      }
      omResponse.setPersistDbResponse(OzoneManagerProtocolProtos.PersistDbResponse.newBuilder().build());
    } catch (IOException ex) {
      audit(ozoneManager, getOmRequest(), exeCtx, ex);
      LOG.error("Db persist exception", ex);
      return new DummyOMClientResponse(createErrorOMResponse(omResponse, ex));
    } finally {
      bucketTable.cleanupCache(Collections.singletonList(Long.MAX_VALUE));
      volumeTable.cleanupCache(Collections.singletonList(Long.MAX_VALUE));
    }
    audit(ozoneManager, getOmRequest(), exeCtx, null);
    return new DummyOMClientResponse(omResponse.build());
  }

  private static void updateBucketRecord(
      Table<String, OmBucketInfo> bucketTable, BatchOperation batchOperation,
      List<OzoneManagerProtocolProtos.DBTableRecord> recordsList) throws IOException {
    for (OzoneManagerProtocolProtos.DBTableRecord record : recordsList) {
      String key = record.getKey().toStringUtf8();
      if (record.hasValue()) {
        OmBucketInfo updateInfo = OmBucketInfo.getCodec().fromPersistedFormat(record.getValue().toByteArray());
        OmBucketInfo bucketInfo = bucketTable.getSkipCache(key);
        if (null != bucketInfo) {
          updateInfo.incrUsedBytes(-updateInfo.getUsedBytes() + bucketInfo.getUsedBytes());
          updateInfo.incrUsedNamespace(-updateInfo.getUsedNamespace() + bucketInfo.getUsedNamespace());
          bucketTable.putWithBatch(batchOperation, key, updateInfo);
        } else {
          bucketTable.putWithBatch(batchOperation, record.getKey().toStringUtf8(), updateInfo);
          bucketTable.addCacheEntry(key, updateInfo, -1);
        }
      } else {
        bucketTable.deleteWithBatch(batchOperation, record.getKey().toStringUtf8());
        bucketTable.addCacheEntry(key, -1);
      }
    }
  }
  private static void updateVolumeRecord(
      Table<String, OmVolumeArgs> volumeTable, BatchOperation batchOperation,
      List<OzoneManagerProtocolProtos.DBTableRecord> recordsList) throws IOException {
    for (OzoneManagerProtocolProtos.DBTableRecord record : recordsList) {
      String key = record.getKey().toStringUtf8();
      if (record.hasValue()) {
        OmVolumeArgs updateInfo = OmVolumeArgs.getCodec().fromPersistedFormat(record.getValue().toByteArray());
        volumeTable.putWithBatch(batchOperation, record.getKey().toStringUtf8(), updateInfo);
        volumeTable.addCacheEntry(key, updateInfo, -1);
      } else {
        volumeTable.deleteWithBatch(batchOperation, record.getKey().toStringUtf8());
        volumeTable.addCacheEntry(key, -1);
      }
    }
  }

  public void audit(OzoneManager ozoneManager, OMRequest request, ExecutionContext executionContext, Throwable th) {
    List<Long> indexList = request.getExecutionControlRequest().getRequestInfoList().stream()
        .map(e -> e.getIndex()).collect(Collectors.toList());
    Map<String, String> auditMap = new HashMap<>();
    auditMap.put("requestIndexes", indexList.stream().map(String::valueOf).collect(Collectors.joining(",")));
    auditMap.put("transactionIndex", executionContext.getTermIndex().getIndex() + "");
    if (null != th) {
      ozoneManager.getSystemAuditLogger().logWriteFailure(ozoneManager.buildAuditMessageForFailure(
          OMSystemAction.DBPERSIST, auditMap, th));
    } else {
      ozoneManager.getSystemAuditLogger().logWriteSuccess(ozoneManager.buildAuditMessageForSuccess(
          OMSystemAction.DBPERSIST, auditMap));
    }
  }
}
