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

package org.apache.hadoop.hdds.scm.server.upgrade; 

import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSUpgradeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SCM Upgrade Action for the very first Upgrade Version.
 */
public class SCMUpgradeActionFirstUpgradeVersion implements
    HDDSUpgradeAction<StorageContainerManager> {
  public static final Logger LOG =
      LoggerFactory.getLogger(SCMUpgradeActionFirstUpgradeVersion.class);
  @Override
  public void executeAction(StorageContainerManager arg) throws Exception {
    LOG.info("Executing SCM Upgrade action for Very first Upgrade Version");
  }
}
