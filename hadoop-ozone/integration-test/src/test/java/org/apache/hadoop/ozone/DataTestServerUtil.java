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

package org.apache.hadoop.ozone;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;

/**
 * Utility class with helper methods for creating and managing test data in integration tests.
 */
public final class DataTestServerUtil {

  private DataTestServerUtil() {
  }

  public static Map<String, OmKeyInfo> createKeys(MiniOzoneCluster cluster, int numOfKeys)
      throws Exception {
    Map<String, OmKeyInfo> keyLocationMap = Maps.newHashMap();

    try (OzoneClient client = cluster.newClient()) {
      OzoneBucket bucket = DataTestUtil.createVolumeAndBucket(client);
      for (int i = 0; i < numOfKeys; i++) {
        String keyName = RandomStringUtils.secure().nextAlphabetic(5) + i;
        DataTestUtil.createKey(bucket, keyName, ReplicationConfig
            .fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.ONE),
            RandomStringUtils.secure().nextAlphabetic(5).getBytes(UTF_8));
        keyLocationMap.put(keyName, lookupOmKeyInfo(cluster, bucket, keyName));
      }
    }
    return keyLocationMap;
  }

  public static void cleanupDeletedTable(OzoneManager ozoneManager) throws IOException {
    Table<String, RepeatedOmKeyInfo> deletedTable = ozoneManager.getMetadataManager().getDeletedTable();
    List<String> nameList = new ArrayList<>();
    try (Table.KeyValueIterator<String, RepeatedOmKeyInfo> keyIter = deletedTable.iterator()) {
      while (keyIter.hasNext()) {
        Table.KeyValue<String, RepeatedOmKeyInfo> kv = keyIter.next();
        nameList.add(kv.getKey());
      }
    }
    nameList.forEach(k -> {
      try {
        deletedTable.delete(k);
      } catch (IOException e) {
        // do nothing
      }
    });
  }

  public static void cleanupOpenKeyTable(OzoneManager ozoneManager, BucketLayout bucketLayout) throws IOException {
    Table<String, OmKeyInfo> openKeyTable = ozoneManager.getMetadataManager().getOpenKeyTable(bucketLayout);
    List<String> nameList = new ArrayList<>();
    try (Table.KeyValueIterator<String, OmKeyInfo> keyIter = openKeyTable.iterator()) {
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
        nameList.add(kv.getKey());
      }
    }
    nameList.forEach(k -> {
      try {
        openKeyTable.delete(k);
      } catch (IOException e) {
        // do nothing
      }
    });
  }

  private static OmKeyInfo lookupOmKeyInfo(MiniOzoneCluster cluster,
      OzoneBucket bucket, String key) throws IOException {
    OmKeyArgs arg = new OmKeyArgs.Builder()
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(key)
        .build();
    return cluster.getOzoneManager().lookupKey(arg);
  }
}
