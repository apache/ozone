package org.apache.hadoop.ozone.container.ozoneimpl;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getMetadataScanError;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestMetadataScanResult {
  @Test
  void testFromEmptyErrors() {
    // No errors means the scan result is healthy.
    MetadataScanResult result = MetadataScanResult.fromErrors(Collections.emptyList());
    assertTrue(result.isHealthy());
    assertFalse(result.isDeleted());
    assertTrue(result.getErrors().isEmpty());
    assertTrue(result.toString().contains("0 errors"));
  }

  @Test
  void testFromErrors() {
    MetadataScanResult result =
        MetadataScanResult.fromErrors(Arrays.asList(getMetadataScanError(), getMetadataScanError()));
    assertFalse(result.isHealthy());
    assertFalse(result.isDeleted());
    assertEquals(2, result.getErrors().size());
    assertTrue(result.toString().contains("2 errors"));
  }

  @Test
  void testDeleted() {
    MetadataScanResult result = MetadataScanResult.deleted();
    assertTrue(result.isHealthy());
    assertTrue(result.isDeleted());
    assertTrue(result.getErrors().isEmpty());
    assertTrue(result.toString().contains("deleted"));
  }
}
