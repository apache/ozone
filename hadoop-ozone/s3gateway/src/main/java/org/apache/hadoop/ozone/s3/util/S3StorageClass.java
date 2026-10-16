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
import static org.apache.hadoop.hdds.client.OzoneStoragePolicy.WARM;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_GLACIER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_REDUCED_REDUNDANCY;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_STANDARD;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_STANDARD_IA;

import java.util.Locale;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;

/**
 * The S3 storage class names Ozone supports, each paired with its default
 * {@link S3StorageClassSpec}.
 *
 * <p>This enum is the vocabulary only. The rules for mapping a class to Ozone settings and back are
 * in {@link S3StorageClassResolver}.
 *
 * <p>{@code STANDARD} and {@code REDUCED_REDUNDANCY} have no storage policy here. The policy is set
 * per deployment through a gateway setting and is unset by default, so writes inherit the bucket's
 * policy.
 */
public enum S3StorageClass {

  REDUCED_REDUNDANCY(S3_STORAGE_CLASS_REDUCED_REDUNDANCY, S3StorageClassSpec.newBuilder()
      .setReplicationType(ReplicationType.RATIS)
      .setReplicationFactor(ReplicationFactor.ONE)
      .build()),
  STANDARD(S3_STORAGE_CLASS_STANDARD, S3StorageClassSpec.newBuilder()
      .setReplicationType(ReplicationType.RATIS)
      .setReplicationFactor(ReplicationFactor.THREE)
      .build()),
  STANDARD_IA(S3_STORAGE_CLASS_STANDARD_IA, S3StorageClassSpec.newBuilder()
      .setReplicationType(ReplicationType.EC)
      .setStoragePolicy(WARM)
      .build()),
  GLACIER(S3_STORAGE_CLASS_GLACIER, S3StorageClassSpec.newBuilder()
      .setReplicationType(ReplicationType.RATIS)
      .setReplicationFactor(ReplicationFactor.THREE)
      .setStoragePolicy(COLD)
      .build());

  private final String s3StorageClass;
  private final S3StorageClassSpec spec;

  S3StorageClass(String s3StorageClass, S3StorageClassSpec spec) {
    this.s3StorageClass = s3StorageClass;
    this.spec = spec;
  }

  public String getS3StorageClass() {
    return s3StorageClass;
  }

  /**
   * The class's default spec, before any gateway setting is applied. Use
   * {@link S3StorageClassResolver#toSpec} to get the spec for a write.
   */
  public S3StorageClassSpec getSpec() {
    return spec;
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
      return S3StorageClass.valueOf(s3StorageClass.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Not supported s3StorageClass: " + s3StorageClass);
    }
  }
}
