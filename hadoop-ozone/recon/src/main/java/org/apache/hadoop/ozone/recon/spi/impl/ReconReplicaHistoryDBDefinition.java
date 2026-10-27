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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_REPLICA_HISTORY_DB;
import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition.REPLICA_HISTORY_V2;

import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;

/**
 * Replica history is independent of the OM-derived tables rebuilt during reprocess.
 */
public class ReconReplicaHistoryDBDefinition extends DBDefinition.WithMap {
  // Stores the migration completion marker in the destination DB so a retry can skip copying history
  // if the copy succeeded but persisting Recon's upgrade version failed.
  public static final DBColumnFamilyDefinition<String, String> METADATA =
      new DBColumnFamilyDefinition<>("metadata", StringCodec.get(), StringCodec.get());

  public ReconReplicaHistoryDBDefinition() {
    super(DBColumnFamilyDefinition.newUnmodifiableMap(REPLICA_HISTORY_V2, METADATA));
  }

  @Override
  public String getName() {
    return RECON_REPLICA_HISTORY_DB;
  }

  @Override
  public String getLocationConfigKey() {
    return ReconServerConfigKeys.OZONE_RECON_DB_DIR;
  }
}
