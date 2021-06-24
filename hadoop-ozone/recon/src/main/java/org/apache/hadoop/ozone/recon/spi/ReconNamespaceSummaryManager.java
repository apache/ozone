package org.apache.hadoop.ozone.recon.spi;

import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;

import java.io.IOException;

/**
 * Interface for DB operations on NS summary.
 */
@InterfaceStability.Unstable
public interface ReconNamespaceSummaryManager {

  // TODO: getDiskUsage? (w/w.o replication)
  void initNSSummaryTable() throws IOException;

  void storeNSSummary(long objectId, NSSummary nsSummary) throws IOException;

  NSSummary getNSSummary(long objectId) throws IOException;
}
