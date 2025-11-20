package org.apache.hadoop.ozone.om.eventlistener;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;

/**
 * A narrow set of functionality we are ok with exposing to plugin
 * implementations
 */
public final class OMEventListenerPluginContextImpl implements OMEventListenerPluginContext {
  private final OzoneManager ozoneManager;

  public OMEventListenerPluginContextImpl(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;
  }

  @Override
  public boolean isLeaderReady() {
    return ozoneManager.isLeaderReady();
  }

  // TODO: should we allow plugins to pass in maxResults or just limit
  // them to some predefined value for safety?  e.g. 10K
  @Override
  public List<OmCompletedRequestInfo> listCompletedRequestInfo(String startKey, int maxResults) throws IOException {
    return ozoneManager.getMetadataManager().listCompletedRequestInfo(startKey, maxResults);
  }

  // TODO: it feels like this doesn't belong here
  @Override
  public String getThreadNamePrefix() {
    return ozoneManager.getThreadNamePrefix();
  }
}
