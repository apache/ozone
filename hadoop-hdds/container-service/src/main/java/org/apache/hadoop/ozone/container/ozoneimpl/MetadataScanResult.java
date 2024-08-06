package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;

import java.util.Collections;
import java.util.List;

/**
 * Represents the result of a container metadata scan.
 * A metadata scan only checks the existence of container metadata files and the checksum of the .container file.
 * It does not check the data in the container and therefore will not generate a ContainerMerkleTree.
 */
public class MetadataScanResult implements ScanResult {

  private final boolean healthy;
  private final ContainerScanError error;
  private static final MetadataScanResult HEALTHY_RESULT = new MetadataScanResult(true, null);

  protected MetadataScanResult(boolean healthy, ContainerScanError error) {
    this.healthy = healthy;
    this.error = error;
  }

  public static MetadataScanResult healthy() {
    return HEALTHY_RESULT;
  }

  public static MetadataScanResult unhealthy(ContainerScanError error) {
    return new MetadataScanResult(false, error);
  }

  public boolean isHealthy() {
    return healthy;
  }

  @Override
  public List<ContainerScanError> getErrors() {
    return Collections.singletonList(error);
  }
}
