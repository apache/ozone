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

package org.apache.hadoop.ozone.s3.util;

import static org.apache.hadoop.hdds.client.OzoneStoragePolicy.COLD;
import static org.apache.hadoop.hdds.client.OzoneStoragePolicy.HOT;
import static org.apache.hadoop.hdds.client.OzoneStoragePolicy.WARM;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_GLACIER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_STANDARD;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_STANDARD_IA;

import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.StoragePolicy;

/**
 * Maps S3 storage class values to Ozone StoragePolicy.
 */
public enum S3StorageClass {

  STANDARD(S3_STORAGE_CLASS_STANDARD, HOT),
  STANDARD_IA(S3_STORAGE_CLASS_STANDARD_IA, WARM),
  GLACIER(S3_STORAGE_CLASS_GLACIER, COLD);

  private final String s3StorageClass;
  private final StoragePolicy storagePolicy;

  S3StorageClass(String s3StorageClass, StoragePolicy storagePolicy) {
    this.s3StorageClass = s3StorageClass;
    this.storagePolicy = storagePolicy;
  }

  public StoragePolicy getStoragePolicy() {
    return storagePolicy;
  }

  public String getS3StorageClass() {
    return s3StorageClass;
  }

  /**
   * Look up the S3StorageClass matching an S3 storage-class header value.
   *
   * @param s3StorageClass the S3 storage class name (case-insensitive).
   * @return the matching S3StorageClass.
   * @throws IllegalArgumentException if the value is null or does not map to
   *         a supported storage class.
   */
  public static S3StorageClass fromS3StorageClass(String s3StorageClass) {
    if (s3StorageClass == null) {
      throw new IllegalArgumentException("Not supported s3StorageClass: null");
    }
    try {
      return S3StorageClass.valueOf(s3StorageClass.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Not supported s3StorageClass: " + s3StorageClass);
    }
  }

  /**
   * Look up the S3StorageClass matching an Ozone StoragePolicy.
   *
   * @param storagePolicy the Ozone StoragePolicy.
   * @return the matching S3StorageClass.
   * @throws IllegalArgumentException if the policy is null or does not map to
   *         a supported storage class.
   */
  public static S3StorageClass fromStoragePolicy(StoragePolicy storagePolicy) {
    if (!(storagePolicy instanceof OzoneStoragePolicy)) {
      throw new IllegalArgumentException(
          "Not supported storagePolicy: " + storagePolicy);
    }
    switch ((OzoneStoragePolicy) storagePolicy) {
    case HOT:
      return STANDARD;
    case WARM:
      return STANDARD_IA;
    case COLD:
      return GLACIER;
    default:
      throw new IllegalArgumentException(
          "Not supported storagePolicy: " + storagePolicy);
    }
  }
}
