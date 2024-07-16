package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTree;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;

import java.io.File;

public class MetadataScanResult implements ScanResult {

  private final boolean healthy;
  private final File unhealthyFile;
  private final FailureType failureType;
  private final Throwable exception;

  protected MetadataScanResult(boolean healthy, FailureType failureType, File unhealthyFile, Throwable exception) {
    this.healthy = healthy;
    this.unhealthyFile = unhealthyFile;
    this.failureType = failureType;
    this.exception = exception;
  }

  protected MetadataScanResult(MetadataScanResult other) {
    this.healthy = healthy;
    this.unhealthyFile = unhealthyFile;
    this.failureType = failureType;
    this.exception = exception;
  }

  public static MetadataScanResult healthy() {
    return new ScanResult(true, null, null, null);
  }

  public static MetadataScanResult unhealthy(FailureType type, File failingFile, Throwable exception) {
    return new ScanResult(false, type, failingFile, exception);
  }

  public boolean isHealthy() {
    return healthy;
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
