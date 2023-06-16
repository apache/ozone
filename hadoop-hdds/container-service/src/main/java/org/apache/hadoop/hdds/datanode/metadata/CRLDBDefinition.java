/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.datanode.metadata;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.security.x509.certificate.client.DNCertificateClient;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.OzoneConsts;

import java.io.File;
import java.util.Map;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;

/**
 * Class defines the structure and types of the crl.db.
 */
public class CRLDBDefinition extends DBDefinition.WithMap {

  public static final DBColumnFamilyDefinition<Long, CRLInfo> PENDING_CRLS =
      new DBColumnFamilyDefinition<>(
          "pendingCrls",
          Long.class,
          LongCodec.get(),
          CRLInfo.class,
          CRLInfo.getCodec());

  public static final DBColumnFamilyDefinition<String, Long>
      CRL_SEQUENCE_ID =
      new DBColumnFamilyDefinition<>(
          "crlSequenceId",
          String.class,
          StringCodec.get(),
          Long.class,
          LongCodec.get());

  private static final Map<String, DBColumnFamilyDefinition<?, ?>>
      COLUMN_FAMILIES = DBColumnFamilyDefinition.newUnmodifiableMap(
          PENDING_CRLS, CRL_SEQUENCE_ID);

  public CRLDBDefinition() {
    // TODO: change it to singleton
    super(COLUMN_FAMILIES);
  }

  @Override
  public String getName() {
    return OzoneConsts.DN_CRL_DB;
  }

  @Override
  public String getLocationConfigKey() {
    throw new UnsupportedOperationException(
        "No location config key available for datanode databases.");
  }

  @Override
  public File getDBLocation(ConfigurationSource conf) {
    // Please Note: To make it easy for our customers we will attempt to read
    // HDDS metadata dir and if that is not set, we will use Ozone directory.
    String metadataDir = conf.get(HDDS_METADATA_DIR_NAME,
        conf.get(OZONE_METADATA_DIRS));
    Preconditions.checkNotNull(metadataDir, "Metadata directory can't be"
        + " null. Please check configs.");

    // create directories in the path if they do not already exist
    HddsUtils.createDir(metadataDir
        + File.separator
        + DNCertificateClient.COMPONENT_NAME);

    return HddsUtils.createDir(metadataDir
        + File.separator
        + DNCertificateClient.COMPONENT_NAME
        + File.separator
        + OzoneConsts.CRL_DB_DIRECTORY_NAME);
  }
}
