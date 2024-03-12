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
package org.apache.hadoop.hdds.scm;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;


import com.google.common.collect.Maps;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * A helper class used by {@link TestStorageContainerManager} to generate
 * some keys and helps to verify containers and blocks locations.
 */
public class StorageContainerManagerTestHelper {

  private final MiniOzoneCluster cluster;
  private final OzoneConfiguration conf;

  public StorageContainerManagerTestHelper(MiniOzoneCluster cluster,
      OzoneConfiguration conf) throws IOException {
    this.cluster = cluster;
    this.conf = conf;
  }

  public static Map<String, OmKeyInfo> createKeys(MiniOzoneCluster cluster, int numOfKeys)
      throws Exception {
    Map<String, OmKeyInfo> keyLocationMap = Maps.newHashMap();

    try (OzoneClient client = cluster.newClient()) {
      OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);
      for (int i = 0; i < numOfKeys; i++) {
        String keyName = RandomStringUtils.randomAlphabetic(5) + i;
        TestDataUtil.createKey(bucket, keyName, RandomStringUtils.randomAlphabetic(5));
        keyLocationMap.put(keyName, lookupOmKeyInfo(cluster, bucket, keyName));
      }
    }
    return keyLocationMap;
  }

  private static OmKeyInfo lookupOmKeyInfo(MiniOzoneCluster cluster, OzoneBucket bucket, String key) throws IOException {
    OmKeyArgs arg = new OmKeyArgs.Builder()
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(key)
        .build();
    return cluster.getOzoneManager().lookupKey(arg);
  }
}
