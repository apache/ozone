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

package org.apache.hadoop.hdds.upgrade.test;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.DATANODE_SCHEMA_V2;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FINALIZE;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FIRST_UPGRADE_START;
import static org.apache.hadoop.ozone.upgrade.UpgradeActionHdds.Component.DATANODE;
import static org.apache.hadoop.ozone.upgrade.UpgradeActionHdds.Component.SCM;

import org.apache.hadoop.hdds.upgrade.HDDSUpgradeAction;
import org.apache.hadoop.ozone.upgrade.UpgradeActionHdds;

/**
 * Mock classes to test upgrade action registration.
 */
public class MockComponent {
  public void mockMethodScm() {
  }

  public void mockMethodDn() {
  }

  /**
   * Mock SCM Upgrade Action.
   */
  @UpgradeActionHdds(type = ON_FINALIZE, feature = INITIAL_VERSION,
      component = SCM)
  public static class MockScmUpgradeAction implements
      HDDSUpgradeAction<MockComponent> {
    @Override
    public void execute(MockComponent arg) {
      arg.mockMethodScm();
    }
  }

  /**
   * Mock DN Upgrade Action.
   */
  @UpgradeActionHdds(type = ON_FIRST_UPGRADE_START,
      feature = DATANODE_SCHEMA_V2, component = DATANODE)
  public static class MockDnUpgradeAction implements
      HDDSUpgradeAction<MockComponent> {
    @Override
    public void execute(MockComponent arg) {
      arg.mockMethodDn();
    }
  }

}
