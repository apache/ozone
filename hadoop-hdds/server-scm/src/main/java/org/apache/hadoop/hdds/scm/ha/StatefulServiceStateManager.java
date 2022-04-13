package org.apache.hadoop.hdds.scm.ha;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.utils.db.Table;

import java.io.IOException;

/**
 * This interface defines an API for saving and reading configurations of a
 * {@link StatefulService}.
 */
public interface StatefulServiceStateManager {

  /**
   * Persists the specified configurationMessage to RocksDB and replicates it
   * through RATIS.
   * @param serviceName name of the {@link StatefulService}, obtained
   *                    through {@link SCMService#getServiceName()}
   * @param configurationMessage configuration to persist
   * @throws IOException on failure to persist configuration
   */
  @Replicate
  void saveConfiguration(String serviceName, Message configurationMessage)
      throws IOException;

  /**
   * Reads the persisted configuration mapped to the specified serviceName.
   * @param serviceName name of the {@link StatefulService}, obtained through
   * {@link SCMService#getServiceName()}
   * @return configuration as a ByteString
   * @throws IOException on failure
   */
  ByteString readConfiguration(String serviceName) throws IOException;

  void reinitialize(Table<String, ByteString> statefulServiceConfig);
}
