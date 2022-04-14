package org.apache.hadoop.hdds.scm.ha;

import com.google.protobuf.Message;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;

import java.io.IOException;

/**
 * A StatefulService is an SCMService that persists configuration to RocksDB.
 * The service must define this configuration as a Protobuf message.
 */
public abstract class StatefulService implements SCMService {
  private final StatefulServiceStateManager stateManager;

  /**
   * Initialize a StatefulService from an extending class.
   * @param scm {@link StorageContainerManager}
   */
  protected StatefulService(StorageContainerManager scm) {
    stateManager = scm.getStatefulServiceStateManager();
  }

  /**
   * Persists the specified {@link Message} configurationMessage to RocksDB
   * with this service's {@link SCMService#getServiceName()} as the key.
   * @param configurationMessage configuration Message to persist
   * @throws IOException on failure to persist configuration
   */
  protected final void saveConfiguration(Message configurationMessage)
      throws IOException {
    stateManager.saveConfiguration(getServiceName(), configurationMessage);
  }

  /**
   * Reads persisted configuration mapped to this service's
   * {@link SCMService#getServiceName()} name.
   *
   * @param defaultInstanceForType the
   * {@link Message#getDefaultInstanceForType()} for this message's actual type
   * @return the persisted {@link Message} that can be cast to the required type
   * @throws IOException on failure
   */
  protected final Message readConfiguration(Message defaultInstanceForType)
      throws IOException {
    return defaultInstanceForType.getParserForType()
        .parseFrom(stateManager.readConfiguration(getServiceName()));
  }
}
