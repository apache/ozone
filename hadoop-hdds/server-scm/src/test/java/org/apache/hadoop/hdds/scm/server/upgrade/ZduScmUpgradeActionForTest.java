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

package org.apache.hadoop.hdds.scm.server.upgrade;

import static org.apache.hadoop.hdds.HDDSVersion.ZDU;

import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.upgrade.ScmUpgradeAction;
import org.apache.hadoop.ozone.upgrade.ScmUpgradeActionForVersion;

/**
 * No-op upgrade action used only to verify that {@link ScmUpgradeActionForVersion} is scanned by
 * {@link org.apache.hadoop.hdds.upgrade.ScmUpgradeActionProvider} in tests.
 */
@ScmUpgradeActionForVersion(version = ZDU)
public class ZduScmUpgradeActionForTest implements ScmUpgradeAction {
  @Override
  public void execute(OzoneStorageContainerManager arg) {
  }
}
