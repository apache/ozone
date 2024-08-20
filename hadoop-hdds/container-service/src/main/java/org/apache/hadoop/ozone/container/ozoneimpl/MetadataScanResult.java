package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTree;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;

import java.util.Collections;
import java.util.List;

/**
 * Represents the result of a container metadata scan.
 * A metadata scan only checks the existence of container metadata files and the checksum of the .container file.
 * It does not check the data in the container and therefore will not generate a ContainerMerkleTree.
 */
public class MetadataScanResult implements ScanResult {

  private final List<ContainerScanError> errors;
  private final boolean deleted;
  // Results are immutable. Intern the common cases.
  private static final MetadataScanResult HEALTHY_RESULT = new MetadataScanResult(Collections.emptyList(), false);
  private static final MetadataScanResult DELETED = new MetadataScanResult(Collections.emptyList(), true);

  protected MetadataScanResult(List<ContainerScanError> errors, boolean deleted) {
    this.errors = errors;
    this.deleted = deleted;
  }

  public static MetadataScanResult fromErrors(List<ContainerScanError> errors) {
    if (errors.isEmpty()) {
      return HEALTHY_RESULT;
    } else {
      return new MetadataScanResult(errors, false);
    }
  }

  public static MetadataScanResult deleted() {
    return DELETED;
  }

  @Override
  public boolean isDeleted() {
    return deleted;
  }

  @Override
  public boolean isHealthy() {
    return errors.isEmpty();
  }

  @Override
  public List<ContainerScanError> getErrors() {
    return errors;
  }

  /**
   * @return A string representation of the first error in this result, or a string indicating the result is healthy.
   */
  @Override
  public String toString() {
    if (errors.isEmpty()) {
      return "Scan result has 0 errors";
    } else {
      return "Scan result has " + errors.size() + " errors. The first error is: " + errors.get(0);
    }
  }
}
