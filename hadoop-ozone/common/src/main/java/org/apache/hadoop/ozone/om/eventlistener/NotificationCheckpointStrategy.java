package org.apache.hadoop.ozone.om.eventlistener;

import java.io.IOException;

public interface NotificationCheckpointStrategy {

  String load() throws IOException;
  void save(String val) throws IOException;
}
