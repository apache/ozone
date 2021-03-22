/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaTwoImpl;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Rule;
import org.junit.rules.Timeout;

/**
 * A helper class used by {@link TestStorageContainerManager} to generate
 * some keys and helps to verify containers and blocks locations.
 */
public class TestStorageContainerManagerHelper {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private final MiniOzoneCluster cluster;
  private final OzoneConfiguration conf;

  public TestStorageContainerManagerHelper(MiniOzoneCluster cluster,
      OzoneConfiguration conf) throws IOException {
    this.cluster = cluster;
    this.conf = conf;
  }

  public Map<String, OmKeyInfo> createKeys(int numOfKeys, int keySize)
      throws Exception {
    Map<String, OmKeyInfo> keyLocationMap = Maps.newHashMap();

    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster);
    // Write 20 keys in bucketName.
    Set<String> keyNames = Sets.newHashSet();
    for (int i = 0; i < numOfKeys; i++) {
      String keyName = RandomStringUtils.randomAlphabetic(5) + i;
      keyNames.add(keyName);

      TestDataUtil
          .createKey(bucket, keyName, RandomStringUtils.randomAlphabetic(5));
    }

    for (String key : keyNames) {
      OmKeyArgs arg = new OmKeyArgs.Builder()
          .setVolumeName(bucket.getVolumeName())
          .setBucketName(bucket.getName())
          .setKeyName(key)
          .setRefreshPipeline(true)
          .build();
      OmKeyInfo location = cluster.getOzoneManager()
          .lookupKey(arg);
      keyLocationMap.put(key, location);
    }
    return keyLocationMap;
  }

  public List<String> getPendingDeletionBlocks(Long containerID)
      throws IOException {
    List<String> pendingDeletionBlocks = Lists.newArrayList();
    ReferenceCountedDB meta = getContainerMetadata(containerID);
    KeyPrefixFilter filter =
        new KeyPrefixFilter().addFilter(OzoneConsts.DELETING_KEY_PREFIX);

    List<? extends Table.KeyValue<String, BlockData>> kvs =
        meta.getStore().getBlockDataTable()
        .getRangeKVs(null, Integer.MAX_VALUE, filter);

    for (Table.KeyValue<String, BlockData> entry : kvs) {
      pendingDeletionBlocks
              .add(entry.getKey().replace(OzoneConsts.DELETING_KEY_PREFIX, ""));
    }
    meta.close();
    return pendingDeletionBlocks;
  }

  public List<Long> getAllBlocks(Set<Long> containerIDs)
      throws IOException {
    List<Long> allBlocks = Lists.newArrayList();
    for (Long containerID : containerIDs) {
      allBlocks.addAll(getAllBlocks(containerID));
    }
    return allBlocks;
  }

  public List<Long> getAllBlocks(Long containeID) throws IOException {
    List<Long> allBlocks = Lists.newArrayList();
    ReferenceCountedDB meta = getContainerMetadata(containeID);

    List<? extends Table.KeyValue<String, BlockData>> kvs =
          meta.getStore().getBlockDataTable()
          .getRangeKVs(null, Integer.MAX_VALUE,
          MetadataKeyFilters.getUnprefixedKeyFilter());

    for (Table.KeyValue<String, BlockData> entry : kvs) {
      allBlocks.add(Long.valueOf(entry.getKey()));
    }
    meta.close();
    return allBlocks;
  }

  public boolean verifyBlocksWithTxnTable(Map<Long, List<Long>> containerBlocks)
      throws IOException {
    for (Map.Entry<Long, List<Long>> entry : containerBlocks.entrySet()) {
      ReferenceCountedDB meta = getContainerMetadata(entry.getKey());
      DatanodeStore ds = meta.getStore();
      DatanodeStoreSchemaTwoImpl dnStoreTwoImpl =
          (DatanodeStoreSchemaTwoImpl) ds;
      List<? extends Table.KeyValue<Long, DeletedBlocksTransaction>>
          txnsInTxnTable = dnStoreTwoImpl.getDeleteTransactionTable()
          .getRangeKVs(null, Integer.MAX_VALUE, null);
      List<Long> conID = new ArrayList<>();
      for (Table.KeyValue<Long, DeletedBlocksTransaction> txn :
          txnsInTxnTable) {
        conID.addAll(txn.getValue().getLocalIDList());
      }
      if (!conID.equals(containerBlocks.get(entry.getKey()))) {
        return false;
      }
      meta.close();
    }
    return true;
  }

  private ReferenceCountedDB getContainerMetadata(Long containerID)
      throws IOException {
    ContainerWithPipeline containerWithPipeline = cluster
        .getStorageContainerManager().getClientProtocolServer()
        .getContainerWithPipeline(containerID);

    DatanodeDetails dn =
        containerWithPipeline.getPipeline().getFirstNode();
    OzoneContainer containerServer =
        getContainerServerByDatanodeUuid(dn.getUuidString());
    KeyValueContainerData containerData =
        (KeyValueContainerData) containerServer.getContainerSet()
        .getContainer(containerID).getContainerData();
    return BlockUtils.getDB(containerData, conf);
  }

  private OzoneContainer getContainerServerByDatanodeUuid(String dnUUID)
      throws IOException {
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      if (dn.getDatanodeDetails().getUuidString().equals(dnUUID)) {
        return dn.getDatanodeStateMachine().getContainer();
      }
    }
    throw new IOException("Unable to get the ozone container "
        + "for given datanode ID " + dnUUID);
  }
}
