/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.recon.scm;

import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;

/**
 * Recon SCM db file for ozone.
 */
public class ReconSCMDBDefinition extends SCMDBDefinition {
  private static final Codec<UUID> UUID_CODEC = new DelegatedCodec<>(
      StringCodec.get(), UUID::fromString, UUID::toString,
      DelegatedCodec.CopyType.SHALLOW);

  public static final String RECON_SCM_DB_NAME = "recon-scm.db";

  public static final DBColumnFamilyDefinition<UUID, DatanodeDetails>
      NODES =
      new DBColumnFamilyDefinition<UUID, DatanodeDetails>(
          "nodes",
          UUID.class,
          UUID_CODEC,
          DatanodeDetails.class,
          DatanodeDetails.getCodec());

  private static final Map<String, DBColumnFamilyDefinition<?, ?>>
      COLUMN_FAMILIES = DBColumnFamilyDefinition.newUnmodifiableMap(
          new SCMDBDefinition().getMap(), NODES);

  public ReconSCMDBDefinition() {
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
