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
  @Replicate
  void saveConfiguration(String serviceName, Message configurationMessage)
      throws IOException;

  ByteString readConfiguration(String serviceName) throws IOException;

  void reinitialize(Table<String, ByteString> statefulServiceConfig);
}
