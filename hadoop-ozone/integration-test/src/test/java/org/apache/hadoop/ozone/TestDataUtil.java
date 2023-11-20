/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Utility to help to generate test data.
 */
public final class TestDataUtil {

  private TestDataUtil() {
  }

  public static OzoneBucket createVolumeAndBucket(OzoneClient client,
      String volumeName, String bucketName) throws IOException {
    return createVolumeAndBucket(client, volumeName, bucketName,
        BucketLayout.LEGACY);
  }

  public static OzoneBucket createVolumeAndBucket(OzoneClient client,
      String volumeName, String bucketName, BucketLayout bucketLayout)
      throws IOException {
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
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setAdmin(adminName)
        .setOwner(userName)
        .build();

    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(volumeName, volumeArgs);
    return objectStore.getVolume(volumeName);

  }

  public static void createKey(OzoneBucket bucket, String keyName,
                               String content) throws IOException {
    createKey(bucket, keyName, ReplicationFactor.ONE,
        ReplicationType.RATIS, content);
  }

  public static void createKey(OzoneBucket bucket, String keyName,
      ReplicationFactor repFactor, ReplicationType repType, String content)
      throws IOException {
    ReplicationConfig repConfig = ReplicationConfig
        .fromTypeAndFactor(repType, repFactor);
    createKey(bucket, keyName, repConfig, content);
  }

  public static void createKey(OzoneBucket bucket, String keyName,
      ReplicationConfig repConfig, String content)
      throws IOException {
    try (OutputStream stream = bucket
        .createKey(keyName, content.length(), repConfig,
            new HashMap<>())) {
      stream.write(content.getBytes(UTF_8));
    }
  }

  public static void createKey(OzoneBucket bucket, String keyName,
      ReplicationFactor repFactor, ReplicationType repType,
      ByteBuffer data) throws IOException {
    ReplicationConfig repConfig = ReplicationConfig
        .fromTypeAndFactor(repType, repFactor);
    try (OutputStream stream = bucket
        .createKey(keyName, data.capacity(), repConfig,
            new HashMap<>())) {
      stream.write(data.array());
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
    return createVolumeAndBucket(client, BucketLayout.LEGACY);
  }

  public static OzoneBucket createBucket(OzoneClient client,
      String vol, BucketArgs bucketArgs, String bukName)
      throws IOException {
    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume volume = objectStore.getVolume(vol);
    volume.createBucket(bukName, bucketArgs);
    return volume.getBucket(bukName);
  }

  public static OzoneBucket createVolumeAndBucket(OzoneClient client,
      BucketLayout bucketLayout) throws IOException {
    final int attempts = 5;
    for (int i = 0; i < attempts; i++) {
      try {
        String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
        String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
        return createVolumeAndBucket(client, volumeName, bucketName,
            bucketLayout);
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
}
