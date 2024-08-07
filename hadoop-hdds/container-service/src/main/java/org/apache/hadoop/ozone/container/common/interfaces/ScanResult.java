package org.apache.hadoop.ozone.container.common.interfaces;

import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScanError;

import java.util.List;

/**
 * Encapsulates the result of a container scan.
 */
public interface ScanResult {
  boolean isHealthy();

  boolean isDeleted();

  List<ContainerScanError> getErrors();
}
