/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.ozone.om.service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Quota repair task.
 */
public class QuotaRepairTask {
  private static final Logger LOG = LoggerFactory.getLogger(
      QuotaRepairTask.class);
  private final OMMetadataManager metadataManager;
  private final Map<String, OmBucketInfo> nameBucketInfoMap = new HashMap<>();
  private final Map<String, OmBucketInfo> idBucketInfoMap = new HashMap<>();
  private ExecutorService executor;

  public QuotaRepairTask(OMMetadataManager metadataManager) {
    this.metadataManager = metadataManager;
  }
  
  public void repair() throws Exception {
    LOG.info("Starting quota repair task");
    executor = Executors.newFixedThreadPool(3);
    prepareAllVolumeBucketInfo();
    repairCount();
    LOG.info("Completed quota repair task");
    executor.shutdown();
  }
  
  private void prepareAllVolumeBucketInfo() throws IOException {
    try (TableIterator<String, ? extends Table.KeyValue<String, OmVolumeArgs>>
        iterator = metadataManager.getVolumeTable().iterator()) {

      OmVolumeArgs omVolumeArgs;
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmVolumeArgs> entry =
            iterator.next();
        omVolumeArgs = entry.getValue();
        getAllBuckets(omVolumeArgs.getVolume(), omVolumeArgs.getObjectID());
      }
    }
  }
  
  private void getAllBuckets(String volumeName, long volumeId)
      throws IOException {
    List<OmBucketInfo> bucketList = metadataManager.listBuckets(
        volumeName, null, null, Integer.MAX_VALUE, false);
    for (OmBucketInfo bucketInfo : bucketList) {
      bucketInfo.incrUsedNamespace(-bucketInfo.getUsedNamespace());
      bucketInfo.incrUsedBytes(-bucketInfo.getUsedBytes());
      nameBucketInfoMap.put(buildNamePath(volumeName,
          bucketInfo.getBucketName()), bucketInfo);
      idBucketInfoMap.put(buildIdPath(volumeId, bucketInfo.getObjectID()),
          bucketInfo);
    }
  }
  
  private String buildNamePath(String volumeName, String bucketName) {
    final StringBuilder builder = new StringBuilder();
    builder.append(OM_KEY_PREFIX)
        .append(volumeName)
        .append(OM_KEY_PREFIX)
        .append(bucketName)
        .append(OM_KEY_PREFIX);
    return builder.toString();
  }

  private String buildIdPath(long volumeId, long bucketId) {
    final StringBuilder builder = new StringBuilder();
    builder.append(OM_KEY_PREFIX)
        .append(volumeId)
        .append(OM_KEY_PREFIX)
        .append(bucketId)
        .append(OM_KEY_PREFIX);
    return builder.toString();
  }
  
  private void repairCount() throws Exception {
    LOG.info("Starting quota repair for all keys, files and directories");
    try {
      List<Future<?>> tasks = new ArrayList<>();
      tasks.add(executor.submit(() -> recalculateKeyUsages()));
      tasks.add(executor.submit(() -> recalculateFileUsages()));
      tasks.add(executor.submit(() -> recalculateDirectoryUsages()));
      for (Future<?> f : tasks) {
        f.get();
      }
    } catch (UncheckedIOException ex) {
      LOG.error("quota repair failure", ex.getCause());
      throw ex.getCause();
    }
    
    // persist bucket info
    try (BatchOperation batchOperation = metadataManager.getStore()
        .initBatchOperation()) {
      for (Map.Entry<String, OmBucketInfo> entry
          : nameBucketInfoMap.entrySet()) {
        String bucketKey = metadataManager.getBucketKey(
            entry.getValue().getVolumeName(),
            entry.getValue().getBucketName());
        metadataManager.getBucketTable().putWithBatch(batchOperation,
            bucketKey, entry.getValue());
      }
      metadataManager.getStore().commitBatchOperation(batchOperation);
    }
    LOG.info("Completed quota repair for all keys, files and directories");
  }
  
  private void recalculateKeyUsages() throws UncheckedIOException {
    LOG.info("Starting recalculate key usages");
    Map<String, CountPair> prefixUsageMap = new HashMap<>();
    Table<String, OmKeyInfo> keyTable = metadataManager.getKeyTable(
        BucketLayout.OBJECT_STORE);
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             keyIter = keyTable.iterator()) {
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
        String prefix = getVolumeBucketPrefix(kv.getKey());
        CountPair usage = prefixUsageMap.getOrDefault(prefix, new CountPair());
        usage.setSpace(usage.getSpace() + kv.getValue().getReplicatedSize());
        usage.setNamespace(usage.getNamespace() + 1L);
        prefixUsageMap.putIfAbsent(prefix, usage);
      }
      LOG.info("Recalculate key usages completed");
      updateCount(nameBucketInfoMap, prefixUsageMap);
      LOG.info("Update of recalculate key usages completed");
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }
  
  private void recalculateFileUsages() throws UncheckedIOException {
    LOG.info("Starting recalculate files usages");
    Map<String, CountPair> prefixUsageMap = new HashMap<>();
    Table<String, OmKeyInfo> fileTable = metadataManager.getKeyTable(
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             keyIter = fileTable.iterator()) {
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
        String prefix = getVolumeBucketPrefix(kv.getKey());
        CountPair usage = prefixUsageMap.getOrDefault(prefix, new CountPair());
        usage.setSpace(usage.getSpace() + kv.getValue().getReplicatedSize());
        usage.setNamespace(usage.getNamespace() + 1L);
        prefixUsageMap.putIfAbsent(prefix, usage);
      }
      LOG.info("Recalculate file usages completed");
      updateCount(idBucketInfoMap, prefixUsageMap);
      LOG.info("Update of recalculate file usages completed");
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }
  
  private void recalculateDirectoryUsages() throws UncheckedIOException {
    LOG.info("Starting recalculate directory usages");
    Map<String, CountPair> prefixUsageMap = new HashMap<>();
    Table<String, OmDirectoryInfo> dirTable
        = metadataManager.getDirectoryTable();
    try (TableIterator<String, ? extends Table.KeyValue<String,
        OmDirectoryInfo>> keyIter = dirTable.iterator()) {
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> kv = keyIter.next();
        String prefix = getVolumeBucketPrefix(kv.getKey());
        CountPair usage = prefixUsageMap.getOrDefault(prefix, new CountPair());
        usage.setNamespace(usage.getNamespace() + 1L);
        prefixUsageMap.putIfAbsent(prefix, usage);
      }
      LOG.info("Recalculate directory usages completed");
      updateCount(idBucketInfoMap, prefixUsageMap);
      LOG.info("Update of recalculate directory usages completed");
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }
  
  private synchronized void updateCount(
      Map<String, OmBucketInfo> bucketInfoMap,
      Map<String, CountPair> prefixUsageMap) {
    for (Map.Entry<String, CountPair> entry : prefixUsageMap.entrySet()) {
      // update bucket info if available
      OmBucketInfo omBucketInfo = bucketInfoMap.get(entry.getKey());
      if (omBucketInfo != null) {
        omBucketInfo.incrUsedBytes(entry.getValue().getSpace());
        omBucketInfo.incrUsedNamespace(entry.getValue().getNamespace());
      }
    }
  }
  
  private String getVolumeBucketPrefix(String key) {
    // get bucket prefix with /<volume>/<bucket>/ 
    // -- as represents name in OBS and id in FSO
    String prefix = key;
    int idx = key.indexOf(OM_KEY_PREFIX, 1);
    if (idx != -1) {
      idx = key.indexOf(OM_KEY_PREFIX, idx + 1);
      if (idx != -1) {
        prefix = key.substring(0, idx + 1);
      }
    }
    return prefix;
  }
  
  private static class CountPair {
    private long space;
    private long namespace;

    public long getSpace() {
      return space;
    }

    public void setSpace(long space) {
      this.space = space;
    }

    public long getNamespace() {
      return namespace;
    }

    public void setNamespace(long namespace) {
      this.namespace = namespace;
    }
  }
}
