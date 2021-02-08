package org.apache.hadoop.ozone.container.upgrade;

import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Utility class to retrieve the version of a feature that corresponds to the
 * metadata layout version specified by the provided
 * {@link HDDSLayoutVersionManager}.
 */
public final class DatanodeMetadataFeatures {
  private static HDDSLayoutVersionManager versionManager;

  private DatanodeMetadataFeatures() { }

  public static synchronized void setHDDSLayoutVersionManager(
      HDDSLayoutVersionManager manager) {
    versionManager = manager;
  }

  public static synchronized String getSchemaVersion() {
    if (versionManager == null ||
        versionManager.getMetadataLayoutVersion() < 1) {
      return OzoneConsts.SCHEMA_V1;
    } else {
      return OzoneConsts.SCHEMA_V2;
    }
  }
}
