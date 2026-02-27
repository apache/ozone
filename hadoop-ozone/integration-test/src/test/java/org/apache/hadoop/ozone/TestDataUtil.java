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
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;

/**
 * Utility to help to generate test data.
 */
public final class TestDataUtil {

  private TestDataUtil() {
  }

  public static OzoneBucket createVolumeAndBucket(OzoneClient client,
      String volumeName, String bucketName) throws IOException {
    return createVolumeAndBucket(client, volumeName, bucketName, getDefaultBucketLayout(client));
  }

  public static OzoneBucket createVolumeAndBucket(OzoneClient client,
      String volumeName, String bucketName, BucketLayout bucketLayout) throws IOException {
    BucketArgs omBucketArgs;
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    if (bucketLayout != null) {
      builder.setBucketLayout(bucketLayout);
    }
    omBucketArgs = builder.build();

    return createVolumeAndBucket(client, volumeName, bucketName,
        omBucketArgs);
  }

  public static OzoneBucket createVolumeAndBucket(OzoneClient client,
      String volumeName, String bucketName, BucketLayout bucketLayout, DefaultReplicationConfig replicationConfig)
      throws IOException {
    BucketArgs omBucketArgs;
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    if (bucketLayout != null) {
      builder.setBucketLayout(bucketLayout);
    }

    if (replicationConfig != null) {
      builder.setDefaultReplicationConfig(replicationConfig);
    }
    omBucketArgs = builder.build();

    return createVolumeAndBucket(client, volumeName, bucketName,
        omBucketArgs);
  }

  public static OzoneBucket createVolumeAndBucket(OzoneClient client,
                                                  String volumeName,
                                                  String bucketName,
                                                  BucketArgs omBucketArgs)
      throws IOException {
    OzoneVolume volume = createVolume(client, volumeName);
    volume.createBucket(bucketName, omBucketArgs);
    return volume.getBucket(bucketName);

  }

  public static OzoneVolume createVolume(OzoneClient client,
                                         String volumeName) throws IOException {
    String userName = "user" + RandomStringUtils.secure().nextNumeric(5);
    String adminName = "admin" + RandomStringUtils.secure().nextNumeric(5);
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setAdmin(adminName)
        .setOwner(userName)
        .build();

    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(volumeName, volumeArgs);
    return objectStore.getVolume(volumeName);

  }

  public static byte[] createStringKey(OzoneBucket bucket, String keyName, int length)
      throws IOException {
    byte[] content = RandomStringUtils.secure().nextAlphanumeric(length).getBytes(UTF_8);
    createKey(bucket, keyName, content);
    return content;
  }

  public static void createKey(OzoneBucket bucket, String keyName,
                               byte[] content) throws IOException {
    createKey(bucket, keyName, null, content);

  }

  public static OutputStream createOutputStream(OzoneBucket bucket, String keyName,
                                                ReplicationConfig repConfig, byte[] content)
      throws IOException {
    return repConfig == null
        ? bucket.createKey(keyName, content.length)
        : bucket.createKey(keyName, content.length, repConfig, new HashMap<>());
  }

  public static void createKey(OzoneBucket bucket, String keyName,
                               ReplicationConfig repConfig, byte[] content)
      throws IOException {
    try (OutputStream stream = createOutputStream(bucket, keyName,
        repConfig, content)) {
      stream.write(content);
    }
  }

  public static void readFully(OzoneBucket bucket, String keyName) throws IOException {
    int len = Math.toIntExact(bucket.getKey(keyName).getDataSize());
    try (InputStream inputStream = bucket.readKey(keyName)) {
      assertDoesNotThrow(() -> IOUtils.readFully(inputStream, len));
    }
  }

  public static String getKey(OzoneBucket bucket, String keyName)
      throws IOException {
    try (InputStream stream = bucket.readKey(keyName)) {
      return new Scanner(stream, UTF_8.name()).useDelimiter("\\A").next();
    }
  }

  public static OzoneBucket createVolumeAndBucket(OzoneClient client)
      throws IOException {
    return createVolumeAndBucket(client, getDefaultBucketLayout(client));
  }

  private static BucketLayout getDefaultBucketLayout(OzoneClient client) {
    return BucketLayout.fromString(client
        .getConfiguration()
        .get(OZONE_DEFAULT_BUCKET_LAYOUT, OZONE_DEFAULT_BUCKET_LAYOUT_DEFAULT));
  }

  public static OzoneBucket createBucket(OzoneClient client,
      String vol, BucketArgs bucketArgs, String bukName)
      throws IOException {
    return createBucket(client, vol, bucketArgs, bukName, false);
  }

  public static OzoneBucket createBucket(OzoneClient client,
                                         String vol, BucketArgs bucketArgs, String bukName,
                                         boolean createLinkedBucket)
      throws IOException {
    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume volume = objectStore.getVolume(vol);
    String sourceBucket = bukName;
    if (createLinkedBucket) {
      sourceBucket = bukName + RandomStringUtils.secure().nextNumeric(5);
    }
    volume.createBucket(sourceBucket, bucketArgs);
    OzoneBucket ozoneBucket = volume.getBucket(sourceBucket);
    if (createLinkedBucket) {
      ozoneBucket = createLinkedBucket(client, vol, sourceBucket, bukName);
    }
    return ozoneBucket;
  }

  public static OzoneBucket createLinkedBucket(OzoneClient client, String vol, String sourceBucketName,
                                               String linkedBucketName) throws IOException {
    BucketArgs.Builder bb = new BucketArgs.Builder()
        .setStorageType(StorageType.DEFAULT)
        .setVersioning(false)
        .setSourceVolume(vol)
        .setSourceBucket(sourceBucketName);
    return createBucket(client, vol, bb.build(), linkedBucketName);
  }

  public static OzoneBucket createVolumeAndBucket(OzoneClient client,
                                                  BucketLayout bucketLayout)
      throws IOException {
    return createVolumeAndBucket(client, bucketLayout, null, false);
  }

  public static OzoneBucket createVolumeAndBucket(OzoneClient client, BucketLayout bucketLayout,
                                                  DefaultReplicationConfig replicationConfig)
      throws IOException {
    return createVolumeAndBucket(client, bucketLayout, replicationConfig, false);
  }

  public static OzoneBucket createVolumeAndBucket(OzoneClient client, BucketLayout bucketLayout,
                                                  DefaultReplicationConfig replicationConfig,
                                                  boolean createLinkedBucket)
      throws IOException {
    final int attempts = 5;
    for (int i = 0; i < attempts; i++) {
      try {
        String volumeName = "volume" + RandomStringUtils.secure().nextNumeric(5);
        String bucketName = "bucket" + RandomStringUtils.secure().nextNumeric(5);
        OzoneBucket ozoneBucket = createVolumeAndBucket(client, volumeName, bucketName,
            bucketLayout, replicationConfig);
        if (createLinkedBucket) {
          String targetBucketName = ozoneBucket.getName() + RandomStringUtils.secure().nextNumeric(5);
          ozoneBucket = createLinkedBucket(client, volumeName, bucketName, targetBucketName);
        }
        return ozoneBucket;
      } catch (OMException e) {
        if (e.getResult() != OMException.ResultCodes.VOLUME_ALREADY_EXISTS
            && e.getResult() != OMException.ResultCodes.BUCKET_ALREADY_EXISTS) {
          throw e;
        }
      }
    }
    throw new IllegalStateException(
        "Could not create unique volume/bucket " + "in " + attempts
            + " attempts");
  }

  public static Map<String, OmKeyInfo> createKeys(MiniOzoneCluster cluster, int numOfKeys)
      throws Exception {
    Map<String, OmKeyInfo> keyLocationMap = Maps.newHashMap();

    try (OzoneClient client = cluster.newClient()) {
      OzoneBucket bucket = createVolumeAndBucket(client);
      for (int i = 0; i < numOfKeys; i++) {
        String keyName = RandomStringUtils.secure().nextAlphabetic(5) + i;
        createKey(bucket, keyName, ReplicationConfig
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
