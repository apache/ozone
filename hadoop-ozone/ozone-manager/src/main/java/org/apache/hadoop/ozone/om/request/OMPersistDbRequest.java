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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.audit.OMSystemAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.execution.OmBucketInfoQuotaTracker;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.DummyOMClientResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handle OMQuotaRepairRequest Request.
 */
public class OMPersistDbRequest extends OMClientRequest {
  private static final Logger LOG = LoggerFactory.getLogger(OMPersistDbRequest.class);

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
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    Table<String, OmBucketInfo> bucketTable = metadataManager.getBucketTable();
    Table<String, OmVolumeArgs> volumeTable = metadataManager.getVolumeTable();
    OzoneManagerProtocolProtos.PersistDbRequest dbUpdateRequest = getOmRequest().getPersistDbRequest();
    List<OzoneManagerProtocolProtos.DBTableUpdate> tableUpdatesList = dbUpdateRequest.getTableUpdatesList();

    try (BatchOperation batchOperation = metadataManager.getStore()
        .initBatchOperation()) {
      for (OzoneManagerProtocolProtos.DBTableUpdate tblUpdates : tableUpdatesList) {
        Table table = metadataManager.getTable(tblUpdates.getTableName());
        List<OzoneManagerProtocolProtos.DBTableRecord> recordsList = tblUpdates.getRecordsList();
        if (bucketTable.getName().equals(tblUpdates.getTableName())) {
          updateBucketRecord(bucketTable, batchOperation, recordsList);
          continue;
        }
        if (volumeTable.getName().equals(tblUpdates.getTableName())) {
          updateVolumeRecord(volumeTable, batchOperation, recordsList);
          continue;
        }
        for (OzoneManagerProtocolProtos.DBTableRecord record : recordsList) {
          if (record.hasValue()) {
            // put
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
          if (bucketInfo instanceof OmBucketInfoQuotaTracker) {
            bucketInfo = bucketTable.getSkipCache(bucketKey);
          }
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
      long txIndex = 0;
      TransactionInfo transactionInfo = TransactionInfo.readTransactionInfo(metadataManager);
      if (transactionInfo != null && transactionInfo.getIndex() != null) {
        txIndex = transactionInfo.getIndex();
      }
      txIndex = Math.max(Collections.max(getOmRequest().getPersistDbRequest().getIndexList()).longValue(), txIndex);
      metadataManager.getTransactionInfoTable().putWithBatch(
          batchOperation, TRANSACTION_INFO_KEY, TransactionInfo.valueOf(termIndex, txIndex));
      metadataManager.getStore().commitBatchOperation(batchOperation);
      omResponse.setPersistDbResponse(OzoneManagerProtocolProtos.PersistDbResponse.newBuilder().build());
    } catch (IOException ex) {
      audit(ozoneManager, dbUpdateRequest, termIndex, ex);
      LOG.error("Db persist exception", ex);
      return new DummyOMClientResponse(createErrorOMResponse(omResponse, ex));
    } finally {
      bucketTable.cleanupCache(Collections.singletonList(Long.MAX_VALUE));
      volumeTable.cleanupCache(Collections.singletonList(Long.MAX_VALUE));
    }
    audit(ozoneManager, dbUpdateRequest, termIndex, null);
    OMClientResponse omClientResponse = new DummyOMClientResponse(omResponse.build());
    return omClientResponse;
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
          bucketTable.put(key, updateInfo);
        } else {
          bucketTable.getRawTable().putWithBatch(batchOperation, record.getKey().toByteArray(),
              record.getValue().toByteArray());
          bucketTable.addCacheEntry(key, updateInfo, -1);
        }
      } else {
        bucketTable.getRawTable().deleteWithBatch(batchOperation, record.getKey().toByteArray());
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
        volumeTable.getRawTable().putWithBatch(batchOperation, record.getKey().toByteArray(),
            record.getValue().toByteArray());
        volumeTable.addCacheEntry(key, updateInfo, -1);
      } else {
        volumeTable.getRawTable().deleteWithBatch(batchOperation, record.getKey().toByteArray());
        volumeTable.addCacheEntry(key, -1);
      }
    }
  }

  public void audit(OzoneManager ozoneManager, OzoneManagerProtocolProtos.PersistDbRequest request,
                          TermIndex termIndex, Throwable th) {
    List<Long> indexList = request.getIndexList();
    Map<String, String> auditMap = new HashMap<>();
    auditMap.put("requestIndexes", indexList.stream().map(String::valueOf).collect(Collectors.joining(",")));
    auditMap.put("transactionIndex", termIndex.getIndex() + "");
    if (null != th) {
      ozoneManager.getSystemAuditLogger().logWriteFailure(ozoneManager.buildAuditMessageForFailure(
          OMSystemAction.DBPERSIST, auditMap, th));
    } else {
      ozoneManager.getSystemAuditLogger().logWriteSuccess(ozoneManager.buildAuditMessageForSuccess(
          OMSystemAction.DBPERSIST, auditMap));
    }
  }
}
