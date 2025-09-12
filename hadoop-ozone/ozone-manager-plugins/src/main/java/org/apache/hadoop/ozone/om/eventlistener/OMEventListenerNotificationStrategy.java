package org.apache.hadoop.ozone.om.eventlistener;

import java.util.List;
import org.apache.hadoop.ozone.om.helpers.OperationInfo;

public interface OMEventListenerNotificationStrategy {

  List<String> determineEventsForOperation(OperationInfo operationInfo);
}
