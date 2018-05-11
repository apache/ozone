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
package org.apache.hadoop.ozone.client;

import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.rest.response.BucketInfo;
import org.apache.hadoop.ozone.client.rest.response.KeyInfo;
import org.apache.hadoop.ozone.client.rest.response.VolumeInfo;
import org.apache.hadoop.ozone.client.rest.response.VolumeOwner;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;

import static org.apache.hadoop.ozone.web.utils.OzoneUtils.formatTime;

/** A utility class for OzoneClient. */
public final class OzoneClientUtils {

  private OzoneClientUtils() {}

  /**
   * Returns a BucketInfo object constructed using fields of the input
   * OzoneBucket object.
   *
   * @param bucket OzoneBucket instance from which BucketInfo object needs to
   *               be created.
   * @return BucketInfo instance
   */
  public static BucketInfo asBucketInfo(OzoneBucket bucket) {
    BucketInfo bucketInfo =
        new BucketInfo(bucket.getVolumeName(), bucket.getName());
    bucketInfo.setCreatedOn(OzoneUtils.formatTime(bucket.getCreationTime()));
    bucketInfo.setStorageType(bucket.getStorageType());
    bucketInfo.setVersioning(
        OzoneConsts.Versioning.getVersioning(bucket.getVersioning()));
    bucketInfo.setAcls(bucket.getAcls());
    return bucketInfo;
  }

  /**
   * Returns a VolumeInfo object constructed using fields of the input
   * OzoneVolume object.
   *
   * @param volume OzoneVolume instance from which VolumeInfo object needs to
   *               be created.
   * @return VolumeInfo instance
   */
  public static VolumeInfo asVolumeInfo(OzoneVolume volume) {
    VolumeInfo volumeInfo =
        new VolumeInfo(volume.getName(), formatTime(volume.getCreationTime()),
            volume.getOwner());
    volumeInfo.setQuota(OzoneQuota.getOzoneQuota(volume.getQuota()));
    volumeInfo.setOwner(new VolumeOwner(volume.getOwner()));
    return volumeInfo;
  }

  /**
   * Returns a KeyInfo object constructed using fields of the input
   * OzoneKey object.
   *
   * @param key OzoneKey instance from which KeyInfo object needs to
   *            be created.
   * @return KeyInfo instance
   */
  public static KeyInfo asKeyInfo(OzoneKey key) {
    KeyInfo keyInfo = new KeyInfo();
    keyInfo.setKeyName(key.getName());
    keyInfo.setCreatedOn(formatTime(key.getCreationTime()));
    keyInfo.setModifiedOn(formatTime(key.getModificationTime()));
    keyInfo.setSize(key.getDataSize());
    return keyInfo;
  }
}
