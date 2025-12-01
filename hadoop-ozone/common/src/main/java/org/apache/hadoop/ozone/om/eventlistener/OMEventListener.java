package org.apache.hadoop.ozone.om.eventlistener;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Interface for event listener plugin implementations
 */
public interface OMEventListener {

  void initialize(OzoneConfiguration conf, OMEventListenerPluginContext pluginContext);

  void start();

  void shutdown();
}
