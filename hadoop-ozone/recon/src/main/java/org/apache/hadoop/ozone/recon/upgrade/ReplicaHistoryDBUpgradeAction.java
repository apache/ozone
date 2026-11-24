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

import com.google.inject.Injector;
import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.ReconGuiceServletContextListener;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.apache.hadoop.ozone.recon.spi.impl.ReconReplicaHistoryDBProvider;

/**
 * Preserve legacy container replica history before finalizing the separate history DB layout.
 */
@ReconUpgradeActionForVersion(version = ReconVersion.REPLICA_HISTORY_DB)
public class ReplicaHistoryDBUpgradeAction implements ReconUpgradeAction {
  @Override
  public void execute(DataSource source) throws Exception {
    Injector injector = ReconGuiceServletContextListener.getGlobalInjector();
    if (injector == null) {
      throw new IllegalStateException("Guice injector not initialized. Replica history migration cannot proceed.");
    }
    injector.getInstance(ReconReplicaHistoryDBProvider.class)
        .migrateReplicaHistory(injector.getInstance(ReconDBProvider.class).getDbStore());
  }
}
