package org.apache.hadoop.hdds.scm.ha;

import com.google.protobuf.Message;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;

import java.io.IOException;

/**
 * A StatefulService is an SCMService that stores configuration in RocksDB.
 * The service must define this configuration as a Protobuf message.
 */
public abstract class StatefulService implements SCMService {
  private final StatefulServiceStateManager stateManager;

  protected StatefulService(StorageContainerManager scm) {
    stateManager = scm.getStatefulServiceStateManager();
  }
  protected final void saveConfiguration(Message configurationMessage)
      throws IOException {
    stateManager.saveConfiguration(getServiceName(), configurationMessage);
  }

  protected final Message readConfiguration(Message defaultInstanceForType)
      throws IOException {
    return defaultInstanceForType.getParserForType()
        .parseFrom(stateManager.readConfiguration(getServiceName()));
  }
}
