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

package org.apache.hadoop.ozone.om.response.key;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

/**
 * Base test class for key response.
 */
@SuppressWarnings("visibilitymodifier")
public class TestOMKeyResponse {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  protected OMMetadataManager omMetadataManager;
  protected BatchOperation batchOperation;

  protected String volumeName;
  protected String bucketName;
  protected String keyName;
  protected HddsProtos.ReplicationFactor replicationFactor;
  protected HddsProtos.ReplicationType replicationType;
  protected OmBucketInfo omBucketInfo;
  protected long clientID;
  protected Random random;
  protected long txnLogId = 100000L;
  protected RepeatedOmKeyInfo keysToDelete;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration ozoneConfiguration = getOzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    batchOperation = omMetadataManager.getStore().initBatchOperation();

    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    keyName = UUID.randomUUID().toString();
    replicationFactor = HddsProtos.ReplicationFactor.ONE;
    replicationType = HddsProtos.ReplicationType.RATIS;
    clientID = 1000L;
    random = new Random();
    keysToDelete = null;

    final OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
            .setVolume(volumeName)
            .setAdminName("admin")
            .setOwnerName("owner")
            .setObjectID(System.currentTimeMillis())
            .build();

    omMetadataManager.getVolumeTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getVolumeKey(volumeName)),
            new CacheValue<>(Optional.of(volumeArgs), 1));

    omBucketInfo = OmBucketInfo.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setObjectID(System.currentTimeMillis())
            .setCreationTime(System.currentTimeMillis())
            .setStorageType(StorageType.DISK)
            .setIsVersionEnabled(false)
            .build();

    omMetadataManager.getBucketTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getBucketKey(
                    volumeName, bucketName)),
            new CacheValue<>(Optional.of(omBucketInfo), 1));
  }

  @NotNull
  protected String getOpenKeyName()  throws IOException {
    return omMetadataManager.getOpenKey(volumeName, bucketName, keyName,
            clientID);
  }

  @NotNull
  protected OmKeyInfo getOmKeyInfo() {
    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
            replicationType, replicationFactor);
  }

  @NotNull
  protected OzoneConfiguration getOzoneConfiguration() {
    return new OzoneConfiguration();
  }

  @After
  public void stop() {
    Mockito.framework().clearInlineMocks();
    if (batchOperation != null) {
      batchOperation.close();
    }
  }

  public BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }

}
