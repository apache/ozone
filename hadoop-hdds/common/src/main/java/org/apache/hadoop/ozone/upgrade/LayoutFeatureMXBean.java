package org.apache.hadoop.ozone.upgrade;

public interface LayoutFeatureMXBean {
  String getName();
  int getLayoutVersion();
  String getDescription();
}
