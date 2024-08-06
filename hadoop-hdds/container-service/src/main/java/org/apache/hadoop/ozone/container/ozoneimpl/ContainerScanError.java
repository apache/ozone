package org.apache.hadoop.ozone.container.ozoneimpl;

import java.io.File;

public class ContainerScanError {
  /**
   * Represents the reason a container scan failed and a container should
   * be marked unhealthy.
   */
  public enum FailureType {
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

  private final File unhealthyFile;
  private final FailureType failureType;
  private final Throwable exception;

  public ContainerScanError(FailureType failure, File unhealthyFile, Exception exception) {
    this.unhealthyFile = unhealthyFile;
    this.failureType = failure;
    this.exception = exception;
  }

  public File getUnhealthyFile() {
    return unhealthyFile;
  }

  public FailureType getFailureType() {
    return failureType;
  }

  public Throwable getException() {
    return exception;
  }
}
