/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.upgrade;

import static org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints.AFTER_COMPLETE_FINALIZATION;
import static org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints.AFTER_POST_FINALIZE_UPGRADE;
import static org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints.AFTER_PRE_FINALIZE_UPGRADE;
import static org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints.BEFORE_PRE_FINALIZE_UPGRADE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.FINALIZATION_REQUIRED;

import java.io.IOException;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Failure injected extension of DefaultUpgradeFinalizationExecutor that
 * can be used by Unit/Integration Tests.
 */
public class InjectedUpgradeFinalizationExecutor<T> extends
    DefaultUpgradeFinalizationExecutor<T> {
  static final Logger LOG =
      LoggerFactory.getLogger(InjectedUpgradeFinalizationExecutor.class);

  private Callable<Boolean> injectTestFunction;
  private UpgradeTestInjectionPoints testInjectionPoint;

  /**
   * Enum to denote failure injection points in finalization.
   */
  public enum UpgradeTestInjectionPoints {
    BEFORE_PRE_FINALIZE_UPGRADE(1),
    AFTER_PRE_FINALIZE_UPGRADE(2),
    AFTER_COMPLETE_FINALIZATION(4),
    AFTER_POST_FINALIZE_UPGRADE(5);

    private int val;
    UpgradeTestInjectionPoints(int value) {
      val = value;
    }

    public int getValue() {
      return val;
    }
  }

  static class UpgradeTestInjectionAbort extends Exception {
    UpgradeTestInjectionAbort() {
    }
  }

  @Override
  public void execute(T component, BasicUpgradeFinalizer<T, ?> finalizer)
      throws IOException {
    try {
      injectTestFunctionAtThisPoint(BEFORE_PRE_FINALIZE_UPGRADE);
      finalizer.emitStartingMsg();

      finalizer.preFinalizeUpgrade(component);
      injectTestFunctionAtThisPoint(AFTER_PRE_FINALIZE_UPGRADE);

      super.finalizeFeatures(component, finalizer,
          finalizer.getVersionManager().unfinalizedFeatures());
      injectTestFunctionAtThisPoint(AFTER_COMPLETE_FINALIZATION);

      finalizer.postFinalizeUpgrade(component);
      injectTestFunctionAtThisPoint(AFTER_POST_FINALIZE_UPGRADE);

      finalizer.emitFinishedMsg();
    } catch (Exception e) {
      LOG.warn("Upgrade Finalization failed with following Exception.", e);
      if (finalizer.getVersionManager().needsFinalization()) {
        finalizer.getVersionManager()
            .setUpgradeState(FINALIZATION_REQUIRED);
      }
    } finally {
      finalizer.markFinalizationDone();
    }
  }

  /**
   * Interface to inject arbitrary failures for stress testing.
   * @param  injectedTestFunction that will be called
   *        code execution reached injectTestFunctionAtThisPoint() location.
   * @param pointIndex code execution point for a given thread.
   */
  public void configureTestInjectionFunction(
      UpgradeTestInjectionPoints pointIndex,
      Callable<Boolean> injectedTestFunction) {
    injectTestFunction = injectedTestFunction;
    testInjectionPoint = pointIndex;
  }

  /**
   * Interface to inject error at a given point in an upgrade thread.
   * @param pointIndex TestFunction Injection point in an upgrade thread.
   * @return "true" if the calling thread should not continue with further
   *          upgrade processing, "false" otherwise.
   */
  public void injectTestFunctionAtThisPoint(
      UpgradeTestInjectionPoints pointIndex) throws Exception {
    if ((testInjectionPoint != null) &&
        (pointIndex.getValue() == testInjectionPoint.getValue()) &&
        (injectTestFunction != null) && injectTestFunction.call()) {
      throw new UpgradeTestInjectionAbort();
    }
    return;
  }
}
