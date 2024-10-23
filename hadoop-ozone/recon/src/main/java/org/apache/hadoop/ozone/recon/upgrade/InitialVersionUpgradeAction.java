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

package org.apache.hadoop.ozone.recon.upgrade;

import static org.apache.hadoop.ozone.recon.upgrade.ReconLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.recon.upgrade.ReconUpgradeAction.UpgradeActionType.FINALIZE;

@UpgradeActionRecon(feature = INITIAL_VERSION, type = FINALIZE)
public class InitialVersionUpgradeAction implements ReconUpgradeAction {

  @Override
  public void execute() throws Exception {
    // This action does nothing for now.
  }

  @Override
  public UpgradeActionType getType() {
    return FINALIZE;
  }
}
