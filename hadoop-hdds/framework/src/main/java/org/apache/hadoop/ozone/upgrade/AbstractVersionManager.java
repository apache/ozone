package org.apache.hadoop.ozone.upgrade;

import org.apache.hadoop.hdds.ComponentVersion;

public class AbstractVersionManager implements VersionManager {
  private final ComponentVersion apparentVersion;
  private final ComponentVersion softwareVersion;

  public AbstractVersionManager(ComponentVersion apparentVersion, ComponentVersion softwareVersion) {
    this.apparentVersion = apparentVersion;
    this.softwareVersion = softwareVersion;
  }

  @Override
  public ComponentVersion getApparentVersion() {
    return apparentVersion;
  }

  @Override
  public ComponentVersion getSoftwareVersion() {
    return softwareVersion;
  }

  @Override
  public boolean isAllowed(ComponentVersion version) {
    return
  }

  @Override
  public Iterable<ComponentVersion> getUnfinalizedVersions() {
    return null;
  }

  @Override
  public void close() {

  }
}
