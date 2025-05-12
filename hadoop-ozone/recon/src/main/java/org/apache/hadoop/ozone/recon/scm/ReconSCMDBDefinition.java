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

package org.apache.hadoop.ozone.recon.scm;

import java.util.Map;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;

/**
 * Recon SCM db file for ozone.
 */
public final class ReconSCMDBDefinition extends SCMDBDefinition {
  private static final Codec<DatanodeID> DATANODE_ID_CODEC = new DelegatedCodec<>(
      StringCodec.get(), DatanodeID::fromUuidString, DatanodeID::toString,
      DatanodeID.class, DelegatedCodec.CopyType.SHALLOW);

  public static final String RECON_SCM_DB_NAME = "recon-scm.db";

  public static final DBColumnFamilyDefinition<DatanodeID, DatanodeDetails> NODES
      = new DBColumnFamilyDefinition<>("nodes", DATANODE_ID_CODEC, DatanodeDetails.getCodec());

  private static final Map<String, DBColumnFamilyDefinition<?, ?>>
      COLUMN_FAMILIES = DBColumnFamilyDefinition.newUnmodifiableMap(
          SCMDBDefinition.get().getMap(), NODES);

  private static final ReconSCMDBDefinition INSTANCE = new ReconSCMDBDefinition();

  public static ReconSCMDBDefinition get() {
    return INSTANCE;
  }

  private ReconSCMDBDefinition() {
    super(COLUMN_FAMILIES);
  }

  @Override
  public String getName() {
    return RECON_SCM_DB_NAME;
  }

  @Override
  public String getLocationConfigKey() {
    return ReconServerConfigKeys.OZONE_RECON_SCM_DB_DIR;
  }
}
