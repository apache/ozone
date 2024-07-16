package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTree;

import java.io.File;

public class DataScanResult extends MetadataScanResult {

  private final ContainerMerkleTree dataTree;

  private DataScanResult(boolean healthy, FailureType failureType,
                             File unhealthyFile, Throwable exception, ContainerMerkleTree dataTree) {
    super(healthy, failureType, unhealthyFile, exception);
    this.dataTree = dataTree;
  }

  public DataScanResult(MetadataScanResult other, ContainerMerkleTree dataTree) {
    super(other);
    this.dataTree = dataTree;
  }

  public static DataScanResult healthy(ContainerMerkleTree dataTree) {
    return new DataScanResult(true, null, null, null, dataTree);
  }

  public static DataScanResult unhealthy(FailureType type, File failingFile,
                                             Throwable exception, ContainerMerkleTree dataTree) {
    return new DataScanResult(false, type, failingFile, exception, dataTree);
  }

  public ContainerMerkleTree getDataTree() {
    return dataTree;
  }
}
