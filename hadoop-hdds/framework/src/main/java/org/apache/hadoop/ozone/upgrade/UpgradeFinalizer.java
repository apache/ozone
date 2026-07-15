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

import java.io.IOException;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface to define the upgrade finalizer implementations.
 * The role of this class is to manage the LayoutFeature finalization and
 * activation, after an upgrade was done.
 * For different service types, where this has relevance, there should be
 * an implementation for this interface, that handles the finalization process
 * in tandem with the corresponding version manager, and Storage.
 * @param <T> The service type which the implementation is bound to, this
 *           defines the type that is provided to {@link LayoutFeature}'s
 *           {@link org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeAction}
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface UpgradeFinalizer<T> {

  Logger LOG = LoggerFactory.getLogger(UpgradeFinalizer.class);

  /**
   * Finalize the metadata upgrade.
   * The provided client ID will be eligible to get the status messages,
   * the service provided will be provided to the
   * {@link org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeAction}s of
   * the {@link LayoutFeature}s being finalized.
   * @param upgradeClientID the initiating client's identifier.
   * @param service the service on which we run finalization.
   * @return the status after running finalization logic, with messages to be
   *          provided to the client
   * @throws IOException if the finalization fails at any stage.
   */
  StatusAndMessages finalize(String upgradeClientID, T service)
      throws IOException;

  /**
   * Finalize the component if needed, and wait until completion.
   * @param upgradeClientID the initiating client's identifier.
   * @param service the service on which we run finalization.
   * @param timeoutInSeconds max time to wait for finalization in seconds.
   * @throws IOException
   */
  void finalizeAndWaitForCompletion(String upgradeClientID, T service,
                                    long timeoutInSeconds) throws IOException;

  /**
   * Gets a status report about the finalization process.
   * This method has a meaning, when the client polls the server from time to
   * time for the status, and the server runs the finalization in the
   * background.
   * The background finalization can supply the messages back to the polling
   * client in this method.
   * @param upgradeClientId the identifier of the client initiated finalization
   * @param takeover if a new client wants to take over, from the original
   *                 client, this should be set to true, and in this case, the
   *                 new client ID will be eligible to get status updates.
   *                 A finalizer implementation can decide to ignore this
   *                 parameter, in which case it may return status to any
   *                 client.
   * @return the status of the finalization.
   * @throws IOException if the implementation requires a dedicated client to
   *          report progress to, and if the client ID is not the initiating
   *          client ID while takover is not specified to be true.
   *          Or in any other I/O failure scenario.
   */
  StatusAndMessages reportStatus(String upgradeClientId, boolean takeover)
      throws IOException;

  /**
   * Get a readonly status of the finalization.
   * @return the status of the finalization
   */
  Status getStatus();

}
