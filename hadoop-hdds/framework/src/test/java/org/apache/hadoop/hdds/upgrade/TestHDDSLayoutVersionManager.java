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

package org.apache.hadoop.hdds.upgrade;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.DATANODE_SCHEMA_V2;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FINALIZE;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FIRST_UPGRADE_START;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.hadoop.hdds.upgrade.test.MockComponent;
import org.apache.hadoop.hdds.upgrade.test.MockComponent.MockDnUpgradeAction;
import org.apache.hadoop.hdds.upgrade.test.MockComponent.MockScmUpgradeAction;
import org.junit.jupiter.api.Test;

/**
 * Class to test HDDS upgrade action registrations.
 */
public class TestHDDSLayoutVersionManager {

  private static final String[] UPGRADE_ACTIONS_TEST_PACKAGES = new String[] {
      "org.apache.hadoop.hdds.upgrade.test"};

  @Test
  public void testUpgradeActionsRegistered() throws Exception {

    HDDSLayoutVersionManager lvm =
        new HDDSLayoutVersionManager(maxLayoutVersion());
    lvm.registerUpgradeActions(UPGRADE_ACTIONS_TEST_PACKAGES);

    //Cluster is finalized, hence should not register.
    Optional<HDDSUpgradeAction> action = INITIAL_VERSION.scmAction(ON_FINALIZE);
    assertFalse(action.isPresent());
    action = DATANODE_SCHEMA_V2.datanodeAction(ON_FIRST_UPGRADE_START);
    assertFalse(action.isPresent());

    // Start from an unfinalized version manager.
    lvm = mock(HDDSLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(-1);

    doCallRealMethod().when(lvm).registerUpgradeActions(any());
    lvm.registerUpgradeActions(UPGRADE_ACTIONS_TEST_PACKAGES);

    action = INITIAL_VERSION.scmAction(ON_FINALIZE);
    assertTrue(action.isPresent());
    assertEquals(MockScmUpgradeAction.class, action.get().getClass());
    assertFalse(INITIAL_VERSION.datanodeAction(ON_FINALIZE).isPresent());
    MockComponent mockObj = mock(MockComponent.class);
    action.get().execute(mockObj);
    verify(mockObj, times(1)).mockMethodScm();
    verify(mockObj, times(0)).mockMethodDn();

    action = DATANODE_SCHEMA_V2.datanodeAction(ON_FIRST_UPGRADE_START);
    assertTrue(action.isPresent());
    assertEquals(MockDnUpgradeAction.class, action.get().getClass());
    assertFalse(DATANODE_SCHEMA_V2.scmAction(ON_FIRST_UPGRADE_START).isPresent());
    mockObj = mock(MockComponent.class);
    action.get().execute(mockObj);
    verify(mockObj, times(0)).mockMethodScm();
    verify(mockObj, times(1)).mockMethodDn();
  }

  @Test
  public void testHDDSLayoutFeaturesHaveIncreasingLayoutVersion() {
    HDDSLayoutFeature[] values = HDDSLayoutFeature.values();
    int currVersion = -1;
    for (HDDSLayoutFeature lf : values) {
      assertEquals(currVersion + 1, lf.layoutVersion());
      currVersion = lf.layoutVersion();
    }
  }
}
