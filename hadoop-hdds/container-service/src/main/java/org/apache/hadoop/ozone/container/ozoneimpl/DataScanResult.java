package org.apache.hadoop.ozone.container.ozoneimpl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTree;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;

import java.util.Collections;
import java.util.List;

/**
 * Represents the result of a container data scan.
 * A container data scan will do a full metadata check, and check the contents of all block data within the container.
 * The result will contain all the errors seen while scanning the container, and a ContainerMerkleTree representing
 * the data that the scan saw on the disk when it ran.
 */
public class DataScanResult implements ScanResult {

  private final boolean healthy;
  private final List<ContainerScanError> errors;
  private final ContainerMerkleTree dataTree;

  private DataScanResult(boolean healthy, List<ContainerScanError> errors, ContainerMerkleTree dataTree) {
    this.healthy = healthy;
    this.errors = errors;
    this.dataTree = dataTree;
  }

  /**
   * Constructs a healthy data scan result with the provided container merkle tree representing the data that was
   * scanned. A healthy data scan implies a healthy metadata scan.
   */
  public static DataScanResult healthy(ContainerMerkleTree dataTree) {
    return new DataScanResult(true, Collections.emptyList(), dataTree);
  }

  /**
   * Constructs an unhealthy data scan result which was aborted before scanning any data due to a metadata error.
   * This data scan result will have an empty data tree with a zero checksum to indicate that no data was scanned.
   */
  public static DataScanResult unhealthy(MetadataScanResult result) {
    Preconditions.checkArgument(!result.isHealthy());
    return new DataScanResult(false, result.getErrors(), new ContainerMerkleTree());
  }

  /**
   * Constructs an unhealthy data scan result which had a successful metadata scan but unhealthy data scan.
   * Failure types should be
   */
  public static DataScanResult unhealthy(List<ContainerScanError> errors, ContainerMerkleTree dataTree) {
    return new DataScanResult(false, errors, dataTree);
  }

  public ContainerMerkleTree getDataTree() {
    return dataTree;
  }

  @Override
  public boolean isHealthy() {
    return healthy;
  }

  /**
   * Returns a list of failures in the order that they were encountered by the scanner.
   */
  public List<ContainerScanError> getErrors() {
    return errors;
  }
}
