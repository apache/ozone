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

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.DATANODE_SCHEMA_V2;
import static org.apache.hadoop.ozone.upgrade.UpgradeActionHdds.Component.SCM;

import org.apache.hadoop.hdds.upgrade.HDDSUpgradeAction;
import org.apache.hadoop.ozone.upgrade.UpgradeActionHdds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SCM Upgrade Action for the very first Upgrade Version.
 */
@UpgradeActionHdds(feature = DATANODE_SCHEMA_V2, component = SCM)
public class ScmOnFinalizeActionForDatanodeSchemaV2 implements
    HDDSUpgradeAction<SCMUpgradeFinalizationContext> {
  private static final Logger LOG =
      LoggerFactory.getLogger(ScmOnFinalizeActionForDatanodeSchemaV2.class);

  @Override
  public void execute(SCMUpgradeFinalizationContext context) throws Exception {
    LOG.info("Executing SCM On Finalize action for layout feature {}",
        DATANODE_SCHEMA_V2);
  }
}
