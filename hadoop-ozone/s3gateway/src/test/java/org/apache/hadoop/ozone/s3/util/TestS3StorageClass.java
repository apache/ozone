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

import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_GLACIER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_STANDARD;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_STANDARD_IA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.junit.jupiter.api.Test;

/** Test the S3 storage-class ↔ Ozone StoragePolicy mapping. */
class TestS3StorageClass {

  @Test
  void testFromS3StorageClass() {
    // S3 storage-class string → S3StorageClass
    assertEquals(S3StorageClass.STANDARD,
        S3StorageClass.fromS3StorageClass(S3_STORAGE_CLASS_STANDARD));
    assertEquals(S3StorageClass.STANDARD_IA,
        S3StorageClass.fromS3StorageClass(S3_STORAGE_CLASS_STANDARD_IA));
    assertEquals(S3StorageClass.GLACIER,
        S3StorageClass.fromS3StorageClass(S3_STORAGE_CLASS_GLACIER));
    assertThrows(IllegalArgumentException.class,
        () -> S3StorageClass.fromS3StorageClass("INVALID"));
    assertThrows(IllegalArgumentException.class,
        () -> S3StorageClass.fromS3StorageClass(null));
  }

  @Test
  void testFromStoragePolicy() {
    // OzoneStoragePolicy → S3StorageClass
    assertEquals(S3StorageClass.STANDARD,
        S3StorageClass.fromStoragePolicy(OzoneStoragePolicy.HOT));
    assertEquals(S3StorageClass.STANDARD_IA,
        S3StorageClass.fromStoragePolicy(OzoneStoragePolicy.WARM));
    assertEquals(S3StorageClass.GLACIER,
        S3StorageClass.fromStoragePolicy(OzoneStoragePolicy.COLD));
    assertThrows(IllegalArgumentException.class,
        () -> S3StorageClass.fromStoragePolicy(null));
  }

  @Test
  void testMappingConsistency() {
    for (S3StorageClass s3StorageClass : S3StorageClass.values()) {
      assertEquals(s3StorageClass,
          S3StorageClass.fromS3StorageClass(s3StorageClass.getS3StorageClass()));
      assertEquals(s3StorageClass,
          S3StorageClass.fromStoragePolicy(s3StorageClass.getStoragePolicy()));
    }
  }
}
