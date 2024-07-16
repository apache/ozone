package org.apache.hadoop.ozone.container.common.interfaces;

import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTree;

import java.io.File;

/**
 * Encapsulates the result of a container scan.
 */
public interface ScanResult {
  /**
   * Represents the reason a container scan failed and a container should
   * be marked unhealthy.
   */
  enum FailureType {
    MISSING_CONTAINER_DIR,
    MISSING_METADATA_DIR,
    MISSING_CONTAINER_FILE,
    MISSING_CHUNKS_DIR,
    MISSING_CHUNK_FILE,
    CORRUPT_CONTAINER_FILE,
    CORRUPT_CHUNK,
    INCONSISTENT_CHUNK_LENGTH,
    INACCESSIBLE_DB,
    WRITE_FAILURE,
    DELETED_CONTAINER
  }

  boolean isHealthy();

  File getUnhealthyFile();

  FailureType getFailureType();

  Throwable getException();
}
