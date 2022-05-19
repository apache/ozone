package org.apache.hadoop.ozone.upgrade;

import java.io.IOException;

public interface UpgradeFinalizationExecutor<T> {
  void execute(T component, BasicUpgradeFinalizer<T, ?> finalizer)
      throws IOException;
}
