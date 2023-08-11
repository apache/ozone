/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;

import java.time.Duration;
import java.util.List;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus.PREPARE_COMPLETED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.awaitility.Awaitility.await;

/**
 * Utility class to help test OM upgrade scenarios.
 */
public final class OMUpgradeTestUtils {

  private OMUpgradeTestUtils() {
    // Utility class.
  }

  public static void assertClusterPrepared(long preparedIndex,
                                           List<OzoneManager> ozoneManagers) {
    for (OzoneManager om : ozoneManagers) {
      await().atMost(Duration.ofSeconds(120))
          .pollInterval(Duration.ofSeconds(1))
          .until(() -> {
            if (!om.isRunning()) {
              return false;
            } else {
              boolean preparedAtIndex = false;
              OzoneManagerPrepareState.State state =
                  om.getPrepareState().getState();

              if (state.getStatus() == PREPARE_COMPLETED) {
                if (state.getIndex() == preparedIndex) {
                  preparedAtIndex = true;
                } else {
                  // State will not change if we are prepared at the wrong
                  // index. Break out of wait.
                  throw new Exception("OM " + om.getOMNodeId() + " prepared " +
                      "but prepare index " + state.getIndex() + " does not " +
                      "match expected prepare index " + preparedIndex);
                }
              }
              return preparedAtIndex;
            }
          });
    }
  }

  public static void waitForFinalization(OzoneManagerProtocol omClient) {
    await().atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofSeconds(2))
        .until(() -> FINALIZATION_DONE ==
            omClient.queryUpgradeFinalizationProgress("finalize-test",
                false, false).status());
  }
}
