/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.ozone.om.upgrade;

import org.apache.hadoop.ozone.om.OzoneManager;

import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.MANAGED_INDEX;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FINALIZE;

/**
 * initialize om managed index generator to provide index for further request handling.
 */
@UpgradeActionOm(type = ON_FINALIZE, feature = MANAGED_INDEX)
public class OmManagedIndexUpgradeAction implements OmUpgradeAction {
  @Override
  public void execute(OzoneManager arg) throws Exception {
    // Prepare ensures the db and ratis at reached to a checkpoint where all changes are flushed
    // And no further operation is allowed
    // At this point, IndexGenerator finialize will re-init index from ratis index and this will be starting point
    // for index generation for further request as used by object and other operation
    arg.getOmRatisServer().getOmStateMachine().finalizeIndexGenerator();
  }
}
