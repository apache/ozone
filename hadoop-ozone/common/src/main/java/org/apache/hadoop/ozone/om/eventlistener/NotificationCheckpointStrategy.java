package org.apache.hadoop.ozone.om.eventlistener;

import java.io.IOException;

/**
 * Interface for implementations which load/save the current checkpoint
 * position representing the last known notification sent by an event
 * notification plugin.
 */
public interface NotificationCheckpointStrategy {

  String load() throws IOException;

  void save(String val) throws IOException;
}
