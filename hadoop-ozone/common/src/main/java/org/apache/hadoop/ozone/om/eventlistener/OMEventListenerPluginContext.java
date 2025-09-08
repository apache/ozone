package org.apache.hadoop.ozone.om.eventlistener;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.ozone.om.helpers.OperationInfo;

/**
 * A narrow set of functionality we are ok with exposing to plugin
 * implementations
 */
public interface OMEventListenerPluginContext {

  boolean isLeaderReady();

  // TODO: should we allow plugins to pass in maxResults or just limit
  // them to some predefined value for safety?  e.g. 10K
  List<OperationInfo> listOperationInfo(String startKey, int maxResults) throws IOException;

  // XXX: this probably doesn't belong here
  String getThreadNamePrefix();

  NotificationCheckpointStrategy getOzoneNotificationCheckpointStrategy();

}
