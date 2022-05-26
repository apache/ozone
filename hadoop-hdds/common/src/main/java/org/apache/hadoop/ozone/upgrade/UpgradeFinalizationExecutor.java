package org.apache.hadoop.ozone.upgrade;

import java.io.IOException;

/**
 * An upgrade finalization executor runs the finalization methods of an
 * {@link UpgradeFinalizer}, providing them the state they need to operate
 * and optionally injecting actions in between.
 * @param <T> The component or context that the {@link UpgradeFinalizer}
 *    needs to run.
 */
public interface UpgradeFinalizationExecutor<T> {
  void execute(T component, BasicUpgradeFinalizer<T, ?> finalizer)
      throws IOException;
}
