package org.apache.hadoop.ozone.upgrade;

import java.io.IOException;
import org.apache.hadoop.hdds.ComponentVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks information about the apparent version, software version, and finalization status of a component.
 */
public class ComponentVersionManager<V extends ComponentVersion<V>> implements VersionManager<V> {
  // Apparent version may be updated during the finalization process.
  private volatile V apparentVersion;
  // Software version will never change.
  private final V softwareVersion;
  private final ComponentVersionManagerMetrics metrics;

  private static final Logger LOG =
      LoggerFactory.getLogger(ComponentVersionManager.class);

  public ComponentVersionManager(V apparentVersion, V softwareVersion)
      throws IOException {
    this.apparentVersion = apparentVersion;
    this.softwareVersion = softwareVersion;

    if (apparentVersion.compareTo(softwareVersion) > 0) {
      throw new IOException(
          "Cannot initialize ComponentVersionManager. Apparent version "
              + apparentVersion + " is larger than software version "
              + softwareVersion);
    }

    LOG.info("Initializing version manager with apparent version {} and software version {}",
        apparentVersion, softwareVersion);
    this.metrics = ComponentVersionManagerMetrics.create(this);
  }

  @Override
  public V getApparentVersion() {
    return apparentVersion;
  }

  @Override
  public V getSoftwareVersion() {
    return softwareVersion;
  }

  @Override
  public boolean isAllowed(V version) {
    return version.compareTo(apparentVersion) <= 0;
  }

  @Override
  public boolean needsFinalization() {
    return apparentVersion.compareTo(softwareVersion) < 0;
  }

  @Override
  public Iterable<V> getUnfinalizedVersions() {
    return apparentVersion.nextVersions();
  }

  @Override
  public void markFinalized(V newApparentVersion) {
    String versionMsg = "Software version: " + softwareVersion
        + ", apparent version: " + apparentVersion
        + ", provided version: " + newApparentVersion
        + ".";

    if (newApparentVersion.compareTo(apparentVersion) <= 0) {
      LOG.info("Finalize attempt on a version which has already "
          + "been finalized. {} This can happen when " +
          "Raft Log is replayed during service restart.", versionMsg);
    } else {
      // newApparentVersion has already been validated to be larger than
      // apparentVersion.
      // Therefore, nextVersion will not return null.
      if (apparentVersion.nextVersion().equals(newApparentVersion)) {
        apparentVersion = newApparentVersion;
        LOG.info("Version {} has been finalized.", apparentVersion);
        if (!needsFinalization()) {
          LOG.info("Finalization is complete.");
        }
      } else {
        throw new IllegalArgumentException(
            "Finalize attempt on a version that is newer than the " +
                "next feature to be finalized. " + versionMsg);
      }
    }
  }

  @Override
  public void close() {
    metrics.unRegister();
  }
}
