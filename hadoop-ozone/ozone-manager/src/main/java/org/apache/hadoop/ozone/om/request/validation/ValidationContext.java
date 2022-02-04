package org.apache.hadoop.ozone.om.request.validation;

import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;

public interface ValidationContext {

  LayoutVersionManager versionManager();

  int serverVersion();

  int clientVersion();

  static ValidationContext of(
      LayoutVersionManager versionManager,
      int serverVersion,
      int clientVersion) {
    return new ValidationContext() {
      @Override
      public LayoutVersionManager versionManager() {
        return versionManager;
      }

      @Override
      public int serverVersion() {
        return serverVersion;
      }

      @Override
      public int clientVersion() {
        return clientVersion;
      }
    };
  }
}
