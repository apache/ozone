/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.upgrade;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.INITIAL_VERSION;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.ScmHAUnfinalizedStateValidationAction;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneClusterImpl;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests that the SCM HA pre-finalize validation action is only triggered in
 * pre-finalize startup if SCM HA was not already being used in the cluster,
 * but has been turned on after.
 *
 * Starting a new SCM HA cluster finalized should not trigger the action. This
 * is tested by all other tests that use SCM HA from the latest version of the
 * code.
 *
 * Starting a new cluster finalized without SCM HA enabled should not trigger
 * the action. This is tested by all other tests that run non-HA clusters.
 */
// Tests are ignored to speed up CI runs. Run manually if changes are made
// relating to the SCM HA validation action.
//@Ignore
public class TestScmHAUnfinalizedStateValidationAction {
  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  @Test
  public void testHAEnabledAlreadyUsedFinalized() throws Exception {
    // Verification should pass.
    MiniOzoneCluster cluster = new MiniOzoneHAClusterImpl.Builder(CONF)
        .setNumDatanodes(1)
        .setNumOfStorageContainerManagers(3)
        .setNumOfOzoneManagers(1)
        .setSCMServiceId("id1")
        .setOMServiceId("id2")
        .build();

    // Manually trigger the upgrade action after setup, to see if it detects
    // that HA is being used based on disk structures.
    // This avoids saving disk state between mini ozone cluster restart.
    ScmHAUnfinalizedStateValidationAction action =
        new ScmHAUnfinalizedStateValidationAction();
    for (StorageContainerManager scm:
        ((MiniOzoneHAClusterImpl) cluster).getStorageContainerManagers()) {
      action.execute(scm);
    }

    cluster.shutdown();
  }

  @Test
  public void testHAEnabledNotAlreadyUsedPreFinalized() throws Exception {
    // Verification should fail.
    MiniOzoneCluster.Builder builder = new MiniOzoneHAClusterImpl.Builder(CONF)
        .setNumDatanodes(1)
        .setNumOfStorageContainerManagers(3)
        .setNumOfOzoneManagers(1)
        .setSCMServiceId("id1")
        .setOMServiceId("id2")
        .setScmLayoutVersion(INITIAL_VERSION.layoutVersion())
        .setDnLayoutVersion(INITIAL_VERSION.layoutVersion());

    LambdaTestUtils.intercept(UpgradeException.class, builder::build);
  }

  @Test
  public void testHANotEnabledNotAlreadyUsedPreFinalized() throws Exception {
    // Verification should pass.
    MiniOzoneCluster cluster = new MiniOzoneClusterImpl.Builder(CONF)
        .setNumDatanodes(1)
        .setOMServiceId("id1")
        .setScmLayoutVersion(INITIAL_VERSION.layoutVersion())
        .setDnLayoutVersion(INITIAL_VERSION.layoutVersion())
        .build();

    // If there was an action failure, it would have been thrown in cluster
    // build.
    cluster.shutdown();
  }
}
