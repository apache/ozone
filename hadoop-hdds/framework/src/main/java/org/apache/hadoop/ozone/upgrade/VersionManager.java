package org.apache.hadoop.ozone.upgrade;

import java.io.IOException;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.ozone.OzoneManagerVersion;

/**
 * Tracks information about the apparent version, software version, and finalization status of a component.
 * This interface also provides factory methods to create implementations based on component and apparent version.
 * When migrating from the old LayoutFeature based versioning to the new unified ComponentVersion in the ZDU
 * software version, a different implementation will be required.
 */
public interface VersionManager<V extends ComponentVersion<V>> {
  V getApparentVersion();

  V getSoftwareVersion();

  boolean isAllowed(V version);

  boolean needsFinalization();

  Iterable<V> getUnfinalizedVersions();

  void markFinalized(V version);

  void close();

  static ComponentVersionManager<OzoneManagerVersion> newForOM(int serializedApparentVersion) throws IOException {
    if (OzoneManagerVersion.ZDU.isSupportedBy(serializedApparentVersion)) {
      return new ComponentVersionManager<>(OzoneManagerVersion.deserialize(serializedApparentVersion),
          OzoneManagerVersion.SOFTWARE_VERSION);
    } else {
      // TODO return migration manager
      return null;
    }
  }

  static ComponentVersionManager<HDDSVersion> newForHDDS(int serializedApparentVersion) throws IOException {
    if (HDDSVersion.ZDU.isSupportedBy(serializedApparentVersion)) {
      return new ComponentVersionManager<>(HDDSVersion.deserialize(serializedApparentVersion),
          HDDSVersion.SOFTWARE_VERSION);
    } else {
      // TODO return migration manager
      return null;
    }
  }
}
