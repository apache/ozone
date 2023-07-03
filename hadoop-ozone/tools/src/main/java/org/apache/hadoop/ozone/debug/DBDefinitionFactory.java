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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.debug;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaOneDBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaTwoDBDefinition;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.recon.scm.ReconSCMDBDefinition;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition;

import com.amazonaws.services.kms.model.InvalidArnException;
import com.google.common.base.Preconditions;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_CONTAINER_KEY_DB;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OM_SNAPSHOT_DB;

/**
 * Utility class to get appropriate DBDefinition.
 */
public final class DBDefinitionFactory {

  private DBDefinitionFactory() {
  }

  private static HashMap<String, DBDefinition> dbMap;

  private static String dnDBSchemaVersion;

  static {
    dbMap = new HashMap<>();
    Arrays.asList(
      new SCMDBDefinition(),
        new OMDBDefinition(),
        new ReconSCMDBDefinition()
    ).forEach(dbDefinition -> dbMap.put(dbDefinition.getName(), dbDefinition));
  }

  public static DBDefinition getDefinition(String dbName) {
    if (dbMap.containsKey(dbName)) {
      return dbMap.get(dbName);
    }
    return getReconDBDefinition(dbName);
  }

  public static DBDefinition getDefinition(Path dbPath,
      ConfigurationSource config) {
    Preconditions.checkNotNull(dbPath,
        "Path is required to identify the used db scheme");
    final Path fileName = dbPath.getFileName();
    if (fileName == null) {
      throw new InvalidArnException(
          "Path is required to identify the used db scheme");
    }
    String dbName = fileName.toString();
    if (dbName.endsWith(OzoneConsts.CONTAINER_DB_SUFFIX)) {
      switch (dnDBSchemaVersion) {
      case "V1":
        return new DatanodeSchemaOneDBDefinition(
            dbPath.toAbsolutePath().toString(), config);
      case "V3":
        return new DatanodeSchemaThreeDBDefinition(
            dbPath.toAbsolutePath().toString(), config);
      default:
        return new DatanodeSchemaTwoDBDefinition(
            dbPath.toAbsolutePath().toString(), config);
      }
    }
    return getDefinition(dbName);
  }

  private static DBDefinition getReconDBDefinition(String dbName) {
    if (dbName.startsWith(RECON_CONTAINER_KEY_DB)) {
      return new ReconDBDefinition(dbName);
    } else if (dbName.startsWith(RECON_OM_SNAPSHOT_DB)) {
      return new OMDBDefinition();
    }
    return null;
  }

  public static void setDnDBSchemaVersion(String dnDBSchemaVersion) {
    DBDefinitionFactory.dnDBSchemaVersion = dnDBSchemaVersion;
  }
}
