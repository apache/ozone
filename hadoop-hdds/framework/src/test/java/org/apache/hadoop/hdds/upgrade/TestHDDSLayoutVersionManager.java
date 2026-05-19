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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.upgrade.ComponentUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.UpgradeAction;
import org.junit.jupiter.api.Test;

/**
 * Class to test HDDS upgrade action registrations.
 */
public class TestHDDSLayoutVersionManager {

  /**
   * Mock component for testing upgrade actions.
   */
  public static class MockComponent {
    public void mockMethodScm() {
    }

    public void mockMethodDn() {
    }
  }

  /**
   * Mock SCM upgrade action for testing.
   */
  public static class MockScmUpgradeAction implements UpgradeAction<MockComponent> {
    @Override
    public void execute(MockComponent arg) {
      arg.mockMethodScm();
    }
  }

  /**
   * Mock Datanode upgrade action for testing.
   */
  public static class MockDnUpgradeAction implements UpgradeAction<MockComponent> {
    @Override
    public void execute(MockComponent arg) {
      arg.mockMethodDn();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUpgradeActionsRegistered() throws Exception {
    ComponentUpgradeActionProvider<UpgradeAction<MockComponent>> scmProvider = () -> {
      Map<ComponentVersion, UpgradeAction<MockComponent>> map = new HashMap<>();
      map.put(INITIAL_VERSION, new MockScmUpgradeAction());
      return map;
    };
    
    ComponentUpgradeActionProvider<UpgradeAction<MockComponent>> dnProvider = () -> {
      Map<ComponentVersion, UpgradeAction<MockComponent>> map = new HashMap<>();
      map.put(DATANODE_SCHEMA_V2, new MockDnUpgradeAction());
      return map;
    };

    //Cluster is finalized, hence should not register.
    Optional<UpgradeAction<?>> action = INITIAL_VERSION.scmAction();
    assertFalse(action.isPresent());
    Optional<UpgradeAction<?>> dnAction = DATANODE_SCHEMA_V2.datanodeAction();
    assertFalse(dnAction.isPresent());

    // Start from an unfinalized version manager.
    HDDSLayoutVersionManager lvm = mock(HDDSLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(-1);

    doCallRealMethod().when(lvm).registerUpgradeActions(any(), any());
    lvm.registerUpgradeActions(scmProvider, dnProvider);

    action = INITIAL_VERSION.scmAction();
    assertTrue(action.isPresent());
    assertEquals(MockScmUpgradeAction.class, action.get().getClass());
    assertFalse(INITIAL_VERSION.datanodeAction().isPresent());
    MockComponent mockObj = mock(MockComponent.class);
    ((UpgradeAction<MockComponent>) action.get()).execute(mockObj);
    verify(mockObj, times(1)).mockMethodScm();
    verify(mockObj, times(0)).mockMethodDn();

    dnAction = DATANODE_SCHEMA_V2.datanodeAction();
    assertTrue(dnAction.isPresent());
    assertEquals(MockDnUpgradeAction.class, dnAction.get().getClass());
    assertFalse(DATANODE_SCHEMA_V2.scmAction().isPresent());
    mockObj = mock(MockComponent.class);
    ((UpgradeAction<MockComponent>) dnAction.get()).execute(mockObj);
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
