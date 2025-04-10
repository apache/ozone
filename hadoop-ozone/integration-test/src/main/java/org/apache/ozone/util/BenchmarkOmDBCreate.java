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

package org.apache.ozone.util;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RocksDBConfiguration;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;

/**
 * Utility for creating a db.
 */
public final class BenchmarkOmDBCreate {

  private BenchmarkOmDBCreate() {
  }

  private static String randomString;
  private static OmKeyInfo getOmKeyInfo(long idx) throws IOException {
    OmKeyInfo.Builder omKeyInfoBuilder = new OmKeyInfo.Builder();
    omKeyInfoBuilder.setObjectID(idx);
    omKeyInfoBuilder.setKeyName(randomString + "key" + idx);
    omKeyInfoBuilder.setParentObjectID(idx - 1);
    omKeyInfoBuilder.setBucketName(randomString + "bucket" + idx);
    omKeyInfoBuilder.setVolumeName(randomString + "vol" + idx);
    omKeyInfoBuilder.setReplicationConfig(new ECReplicationConfig(3, 2));
    omKeyInfoBuilder.addAcl(OzoneAcl.of(IAccessAuthorizer.ACLIdentityType.GROUP, "key1" + idx,
        OzoneAcl.AclScope.ACCESS, IAccessAuthorizer.ACLType.ALL));
    omKeyInfoBuilder.addAcl(OzoneAcl.of(IAccessAuthorizer.ACLIdentityType.GROUP, "key2" + idx,
        OzoneAcl.AclScope.ACCESS, IAccessAuthorizer.ACLType.ALL));
    omKeyInfoBuilder.setOmKeyLocationInfos(Arrays.asList(new OmKeyLocationInfoGroup(1,
        Arrays.asList(new OmKeyLocationInfo.Builder().setBlockID(new BlockID(1L, 2L))
            .setLength(1000).setCreateVersion(1).setPartNumber(2).build()))));
    return omKeyInfoBuilder.build();
  }
  public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
    Long numberOfKeys = Long.parseLong(args[2]);
    Integer keyLength = Integer.parseInt(args[3]);
    randomString = RandomStringUtils.random(keyLength);
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setInt(OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES, -1);
    RocksDBConfiguration rocksDBConfiguration = ozoneConfiguration.getObject(RocksDBConfiguration.class);
    int maxThreads = Integer.parseInt(args[1]);
    rocksDBConfiguration.setParallelIteratorMaxPoolSize(maxThreads);
    ozoneConfiguration.setFromObject(rocksDBConfiguration);
    Path input = Paths.get(args[0]);
    Path rocksdbPath = input.resolve("om.db");
    OmMetadataManagerImpl omMetadataManager = OmMetadataManagerImpl.createCheckpointMetadataManager(ozoneConfiguration,
        new RocksDBCheckpoint(rocksdbPath));
    AtomicLong counter = new AtomicLong();
    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(maxThreads, maxThreads, 5,
        TimeUnit.MINUTES, new LinkedBlockingDeque<>());
    CompletableFuture<Boolean> future = CompletableFuture.completedFuture(true);
    for (int i = 0; i < maxThreads; i++) {
      CompletableFuture<Boolean> stat = CompletableFuture.completedFuture(true);
      threadPoolExecutor.submit(() -> {
        long id;
        while ((id = counter.incrementAndGet()) < numberOfKeys) {
          try {
            OmKeyInfo keyInfo = getOmKeyInfo(id);
            omMetadataManager.getFileTable().put("key/" + id + "/" + id, keyInfo);
          } catch (IOException e) {
            stat.completeExceptionally(e);
          }
        }
        return stat.complete(true);
      });
      future = future.thenCombine(stat, (v1, v2) -> null);
    }
    future.get();
  }
}
