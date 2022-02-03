package org.apache.hadoop.ozone.om.request.validation;

import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.apache.hadoop.util.ComparableVersion;

public interface ValidationContext {

  LayoutVersionManager versionManager();

  ComparableVersion serverVersion();

  int clientVersion();
}
