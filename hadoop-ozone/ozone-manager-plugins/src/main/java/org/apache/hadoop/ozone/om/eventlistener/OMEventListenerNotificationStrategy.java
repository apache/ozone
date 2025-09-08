package org.apache.hadoop.ozone.om.eventlistener;

import java.util.List;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;

public interface OMEventListenerNotificationStrategy {

  List<String> determineEventsForOperation(OmCompletedRequestInfo completedRequestInfo);
}
