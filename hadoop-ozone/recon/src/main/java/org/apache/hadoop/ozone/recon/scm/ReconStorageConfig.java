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

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_STORAGE_DIR;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import javax.inject.Inject;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.ozone.recon.ReconUtils;

/**
 * Recon's extension of SCMStorageConfig.
 */
public class ReconStorageConfig extends SCMStorageConfig {

  public static final String RECON_CERT_SERIAL_ID = "reconCertSerialId";
  public static final String RECON_ID = "uuid";

  @Inject
  public ReconStorageConfig(OzoneConfiguration conf, ReconUtils reconUtils)
      throws IOException {
    super(NodeType.RECON, reconUtils.getReconDbDir(conf, OZONE_RECON_DB_DIR),
        RECON_STORAGE_DIR);
  }

  public void setReconCertSerialId(String certSerialId) throws IOException {
    getStorageInfo().setProperty(RECON_CERT_SERIAL_ID, certSerialId);
  }

  public void setReconId(String uuid) throws IOException {
    if (getState() == StorageState.INITIALIZED) {
      throw new IOException("Recon is already initialized.");
    } else {
      getStorageInfo().setProperty(RECON_ID, uuid);
    }
  }

  /**
   * Retrieves the Recon ID from the version file.
   * @return RECON_ID
   */
  public String getReconId() {
    return getStorageInfo().getProperty(RECON_ID);
  }

  @Override
  protected Properties getNodeProperties() {
    String reconId = getReconId();
    if (reconId == null) {
      reconId = UUID.randomUUID().toString();
    }
    Properties reconProperties = new Properties();
    reconProperties.setProperty(RECON_ID, reconId);

    if (getReconCertSerialId() != null) {
      reconProperties.setProperty(RECON_CERT_SERIAL_ID, getReconCertSerialId());
    }
    return reconProperties;
  }

  /**
   * Retrieves the serial id of certificate issued by SCM.
   * @return RECON_CERT_SERIAL_ID
   */
  public String getReconCertSerialId() {
    return getStorageInfo().getProperty(RECON_CERT_SERIAL_ID);
  }

  public void unsetReconCertSerialId() {
    getStorageInfo().unsetProperty(RECON_CERT_SERIAL_ID);
  }
}
