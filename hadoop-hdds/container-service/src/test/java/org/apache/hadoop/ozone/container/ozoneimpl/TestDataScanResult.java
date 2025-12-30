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

package org.apache.hadoop.ozone.container.ozoneimpl;

import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getDataScanError;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getMetadataScanError;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeWriter;
import org.junit.jupiter.api.Test;

class TestDataScanResult {
  private static final ContainerMerkleTreeWriter TREE =
      ContainerMerkleTreeTestUtils.buildTestTree(new OzoneConfiguration());

  @Test
  void testFromEmptyErrors() {
    // No errors means the scan result is healthy.
    DataScanResult result = DataScanResult.fromErrors(Collections.emptyList(), TREE);
    assertFalse(result.hasErrors());
    assertFalse(result.isDeleted());
    assertTrue(result.getErrors().isEmpty());
    assertTrue(result.toString().contains("0 errors"));
    assertSame(TREE, result.getDataTree());
  }

  @Test
  void testUnhealthyMetadata() {
    MetadataScanResult metadataResult =
        MetadataScanResult.fromErrors(Collections.singletonList(getMetadataScanError()));
    DataScanResult result = DataScanResult.unhealthyMetadata(metadataResult);
    assertTrue(result.hasErrors());
    assertFalse(result.isDeleted());
    assertEquals(1, result.getErrors().size());
    assertTrue(result.toString().contains("1 error"));
    // Tree should be empty if the metadata scan failed, since the data scan could not proceed.
    assertEquals(0, result.getDataTree().toProto().getBlockMerkleTreeCount());
  }

  @Test
  void testFromErrors() {
    DataScanResult result = DataScanResult.fromErrors(Arrays.asList(getDataScanError(), getDataScanError()), TREE);
    assertTrue(result.hasErrors());
    assertFalse(result.isDeleted());
    assertEquals(2, result.getErrors().size());
    assertTrue(result.toString().contains("2 errors"));
    // Tree should just be passed through from the result. It will not have the errors we passed in.
    assertSame(TREE, result.getDataTree());
  }

  @Test
  void testDeleted() {
    DataScanResult result = DataScanResult.deleted();
    assertFalse(result.hasErrors());
    assertTrue(result.isDeleted());
    assertTrue(result.getErrors().isEmpty());
    assertTrue(result.toString().contains("deleted"));
    // Tree should be empty if the container was deleted.
    assertEquals(0, result.getDataTree().toProto().getBlockMerkleTreeCount());
  }
}
