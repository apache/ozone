package org.apache.hadoop.ozone.container.ozoneimpl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTree;

import java.util.Collections;
import java.util.List;

/**
 * Represents the result of a container data scan.
 * A container data scan will do a full metadata check, and check the contents of all block data within the container.
 * The result will contain all the errors seen while scanning the container, and a ContainerMerkleTree representing
 * the data that the scan saw on the disk when it ran.
 */
public final class DataScanResult extends MetadataScanResult {

  private final ContainerMerkleTree dataTree;
  // Only deleted results can be interned. Healthy results will still have different trees.
  private static final DataScanResult DELETED = new DataScanResult(Collections.emptyList(),
      new ContainerMerkleTree(), true);

  private DataScanResult(List<ContainerScanError> errors, ContainerMerkleTree dataTree, boolean deleted) {
    super(errors, deleted);
    this.dataTree = dataTree;
  }

  /**
   * Constructs an unhealthy data scan result which was aborted before scanning any data due to a metadata error.
   * This data scan result will have an empty data tree with a zero checksum to indicate that no data was scanned.
   */
  public static DataScanResult unhealthyMetadata(MetadataScanResult result) {
    Preconditions.checkArgument(!result.isHealthy());
    return new DataScanResult(result.getErrors(), new ContainerMerkleTree(), false);
  }

  public static DataScanResult deleted() {
    return DELETED;
  }

  /**
   * Constructs an unhealthy data scan result which had a successful metadata scan but unhealthy data scan.
   */
  public static DataScanResult fromErrors(List<ContainerScanError> errors, ContainerMerkleTree dataTree) {
    return new DataScanResult(errors, dataTree, false);
  }

  public ContainerMerkleTree getDataTree() {
    return dataTree;
  }
}
