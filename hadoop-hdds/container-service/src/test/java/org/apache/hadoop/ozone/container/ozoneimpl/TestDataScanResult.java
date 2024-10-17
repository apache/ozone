package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTree;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getDataScanError;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getMetadataScanError;
import static org.junit.jupiter.api.Assertions.*;

class TestDataScanResult {
  private static final ContainerMerkleTree TREE = ContainerMerkleTreeTestUtils.buildTestTree(new OzoneConfiguration());

  @Test
  void testFromEmptyErrors() {
    // No errors means the scan result is healthy.
    DataScanResult result = DataScanResult.fromErrors(Collections.emptyList(), TREE);
    assertTrue(result.isHealthy());
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
    assertFalse(result.isHealthy());
    assertFalse(result.isDeleted());
    assertEquals(1, result.getErrors().size());
    assertTrue(result.toString().contains("1 error"));
    // Tree should be empty if the metadata scan failed, since the data scan could not proceed.
    assertEquals(0, result.getDataTree().toProto().getBlockMerkleTreeCount());
  }

  @Test
  void testFromErrors() {
    DataScanResult result = DataScanResult.fromErrors(Arrays.asList(getDataScanError(), getDataScanError()), TREE);
    assertFalse(result.isHealthy());
    assertFalse(result.isDeleted());
    assertEquals(2, result.getErrors().size());
    assertTrue(result.toString().contains("2 errors"));
    // Tree should just be passed through from the result. It will not have the errors we passed in.
    assertSame(TREE, result.getDataTree());
  }

  @Test
  void testDeleted() {
    DataScanResult result = DataScanResult.deleted();
    assertTrue(result.isHealthy());
    assertTrue(result.isDeleted());
    assertTrue(result.getErrors().isEmpty());
    assertTrue(result.toString().contains("deleted"));
    // Tree should be empty if the container was deleted.
    assertEquals(0, result.getDataTree().toProto().getBlockMerkleTreeCount());
  }
}