package org.apache.hadoop.ozone.upgrade;

import java.util.Map;

public interface LayoutVersionManagerMXBean {
  int getMetadataLayoutVersion();

  int getSoftwareLayoutVersion();

  LayoutFeature getLayoutFeatures();
}
