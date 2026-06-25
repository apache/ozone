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

package org.apache.hadoop.ozone.recon.upgrade;

import static org.apache.hadoop.ozone.recon.upgrade.ReconVersion.INITIAL_VERSION;

import java.sql.SQLException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade action for the INITIAL schema version, which manages constraints
 * for the UNHEALTHY_CONTAINERS table.
 */
@UpgradeActionRecon(feature = INITIAL_VERSION)
public class InitialConstraintUpgradeAction implements ReconUpgradeAction {
  private static final Logger LOG = LoggerFactory.getLogger(InitialConstraintUpgradeAction.class);

  @Override
  public void execute(DataSource source) throws SQLException {
    ReconUpgradeAction.updateUnhealthyContainerStatesConstraint(source, LOG);
  }
}
