package org.apache.hadoop.ozone.upgrade;

import org.apache.hadoop.hdds.ComponentVersion;

public interface VersionManager {
  ComponentVersion getApparentVersion();

  ComponentVersion getSoftwareVersion();

  boolean isAllowed(ComponentVersion version);

  Iterable<ComponentVersion> getUnfinalizedVersions();

  void close();
}
