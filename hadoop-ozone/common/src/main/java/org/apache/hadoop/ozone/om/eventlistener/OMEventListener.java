package org.apache.hadoop.ozone.om.eventlistener;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;

public interface OMEventListener {

  void initialize(OzoneConfiguration conf, OMEventListenerPluginContext pluginContext);

  void start();

  void shutdown();
}
