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

package org.apache.hadoop.ozone.debug.replicas;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.BiConsumer;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.OzoneAddress;

/**
 * Utils class for the replicas package.
 */

public final class ReplicasUtils {
  private ReplicasUtils() {
  }

  static class KeyParts {
    private String volumeName;
    private String bucketName;
    private String keyName;

    KeyParts(String volumeName, String bucketName, String keyName) {
      this.volumeName = volumeName;
      this.bucketName = bucketName;
      this.keyName = keyName;
    }

    public String getBucketName() {
      return bucketName;
    }

    public String getVolumeName() {
      return volumeName;
    }

    public void setVolumeName(String volumeName) {
      this.volumeName = volumeName;
    }

    public void setBucketName(String bucketName) {
      this.bucketName = bucketName;
    }

    public String getKeyName() {
      return keyName;
    }

    public void setKeyName(String keyName) {
      this.keyName = keyName;
    }
  }

  /**
   * This method walks the key address and applies the keyProcessor function to all discovered keys.
   * @param ozoneClient
   * @param address
   * @param keyProcessor
   * @throws IOException
   */
  static void findCandidateKeys(OzoneClient ozoneClient, OzoneAddress address,
      BiConsumer<OzoneClient, KeyParts> keyProcessor) throws IOException {
    ObjectStore objectStore = ozoneClient.getObjectStore();
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();
    if (!keyName.isEmpty()) {
      keyProcessor.accept(ozoneClient, new KeyParts(volumeName, bucketName, keyName));
    } else if (!bucketName.isEmpty()) {
      OzoneVolume volume = objectStore.getVolume(volumeName);
      OzoneBucket bucket = volume.getBucket(bucketName);
      checkBucket(bucket, ozoneClient, keyProcessor);
    } else if (!volumeName.isEmpty()) {
      OzoneVolume volume = objectStore.getVolume(volumeName);
      checkVolume(volume, ozoneClient, keyProcessor);
    } else {
      for (Iterator<? extends OzoneVolume> it = objectStore.listVolumes(null); it.hasNext();) {
        checkVolume(it.next(), ozoneClient, keyProcessor);
      }
    }
  }

  static void checkVolume(OzoneVolume volume, OzoneClient ozoneClient,
      BiConsumer<OzoneClient, KeyParts> keyProcessor) throws IOException {
    for (Iterator<? extends OzoneBucket> it = volume.listBuckets(null); it.hasNext();) {
      OzoneBucket bucket = it.next();
      checkBucket(bucket, ozoneClient, keyProcessor);
    }
  }

  static void checkBucket(OzoneBucket bucket, OzoneClient ozoneClient,
      BiConsumer<OzoneClient, KeyParts> keyProcessor) throws IOException {
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    for (Iterator<? extends OzoneKey> it = bucket.listKeys(null); it.hasNext();) {
      OzoneKey key = it.next();
//    TODO: Remove this check once HDDS-12094 is fixed
      if (key.getName().endsWith("/")) {
        continue;
      }
      keyProcessor.accept(ozoneClient, new KeyParts(volumeName, bucketName, key.getName()));
    }
  }
}
