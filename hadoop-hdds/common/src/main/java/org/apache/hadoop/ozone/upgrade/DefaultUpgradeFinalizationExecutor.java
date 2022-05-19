/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.upgrade;

import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_IN_PROGRESS;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_REQUIRED;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DefaultUpgradeFinalizationExecutor for driving the main part of finalization.
 * Unit/Integration tests can override this to provide error injected version
 * of this class.
 */
public class DefaultUpgradeFinalizationExecutor<T> {
  static final Logger LOG =
      LoggerFactory.getLogger(DefaultUpgradeFinalizationExecutor.class);

  public DefaultUpgradeFinalizationExecutor() {
  }

  public void execute(T component, BasicUpgradeFinalizer finalizer)
      throws IOException {
    try {
      finalizer.emitStartingMsg();
      finalizer.getVersionManager()
          .setUpgradeState(FINALIZATION_IN_PROGRESS);

      finalizer.preFinalizeUpgrade(component);

      finalizer.finalizeUpgrade(component);

      finalizer.postFinalizeUpgrade(component);

      finalizer.emitFinishedMsg();
    } catch (Exception e) {
      LOG.warn("Upgrade Finalization failed with following Exception. ", e);
      if (finalizer.getVersionManager().needsFinalization()) {
        finalizer.getVersionManager()
            .setUpgradeState(FINALIZATION_REQUIRED);
        throw (e);
      }
    } finally {
      finalizer.markFinalizationDone();
    }
  }
}
