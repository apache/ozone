package org.apache.hadoop.ozone.container.upgrade;

import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.OzoneConsts;

public final class DatanodeMetadataFeatures {
  private static HDDSLayoutVersionManager versionManager;

  public static void setHDDSLayoutVersionManager(HDDSLayoutVersionManager manager) {
    versionManager = manager;
  }

  public static String getSchemaVersion() {
    if (versionManager == null ||
        versionManager.getMetadataLayoutVersion() < 1) {
      return OzoneConsts.SCHEMA_V1;
    } else {
      return OzoneConsts.SCHEMA_V1;
    }
  }
}
