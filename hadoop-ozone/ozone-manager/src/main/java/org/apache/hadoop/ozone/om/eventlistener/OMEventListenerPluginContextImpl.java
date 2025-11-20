package org.apache.hadoop.ozone.om.eventlistener;

import org.apache.hadoop.ozone.om.OzoneManager;

/**
 * A narrow set of functionality we are ok with exposing to plugin
 * implementations
 */
public final class OMEventListenerPluginContextImpl implements OMEventListenerPluginContext {
  private final OzoneManager ozoneManager;

  public OMEventListenerPluginContextImpl(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;
  }

  // TODO: fill this out with capabilities we would like to expose to
  // plugin implementations.
}
