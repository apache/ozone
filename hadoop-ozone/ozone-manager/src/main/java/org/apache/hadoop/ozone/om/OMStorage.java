/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;

import static org.apache.hadoop.ozone.om.OmUpgradeConfig.ConfigStrings.OZONE_OM_INIT_DEFAULT_LAYOUT_VERSION;

/**
 * OMStorage is responsible for management of the StorageDirectories used by
 * the Ozone Manager.
 */
public class OMStorage extends Storage {

  public static final String STORAGE_DIR = "om";
  public static final String OM_ID = "omUuid";
  public static final String OM_CERT_SERIAL_ID = "omCertSerialId";
  public static final String OM_NODE_ID = "nodeId";

  /**
   * Construct OMStorage.
   * @throws IOException if any directories are inaccessible.
   */
  public OMStorage(OzoneConfiguration conf) throws IOException {
    super(NodeType.OM, getOmDbDir(conf), STORAGE_DIR,
        getInitLayoutVersion(conf, OZONE_OM_INIT_DEFAULT_LAYOUT_VERSION,
            OMLayoutVersionManager::maxLayoutVersion));
  }

  public void setOmCertSerialId(String certSerialId) throws IOException {
    getStorageInfo().setProperty(OM_CERT_SERIAL_ID, certSerialId);
  }

  public void setOmId(String omId) throws IOException {
    if (getState() == StorageState.INITIALIZED) {
      throw new IOException("OM is already initialized.");
    } else {
      getStorageInfo().setProperty(OM_ID, omId);
    }
  }

  /**
   * Set the OM NodeId property.
   * @param nodeId OM NodeId
   * @param persistToDisk if true, persist the nodeId to VERSION file if not
   *                      already present
   * @throws IOException if existing nodeId from VERSION file does not match
   * the new nodeId.
   */
  public void setOmNodeId(String nodeId, boolean persistToDisk)
      throws IOException {
    String nodeIdFromDisk = getOmNodeId();
    if (nodeIdFromDisk != null) {
      // Verify the existing NodeId from VERSION file equals that from the
      // configuration
      if (!nodeIdFromDisk.equals(nodeId)) {
        throw new IOException("OM NodeId: " + nodeId + " does not match " +
            "existing nodeId from VERSION file: " + nodeIdFromDisk);
      } else {
        // NodeId is already set correctly.
        return;
      }
    }

    getStorageInfo().setProperty(OM_NODE_ID, nodeId);

    if (persistToDisk) {
      // Current VERSION file does not have the nodeId. Persist the nodeId to
      // disk.
      persistCurrentState();
    }
  }

  /**
   * Retrieves the OM ID from the version file.
   * @return OM_ID
   */
  public String getOmId() {
    return getStorageInfo().getProperty(OM_ID);
  }

  /**
   * Retrieves the OM NodeId from the version file.
   * @return OM_NODE_ID
   */
  public String getOmNodeId() {
    return getStorageInfo().getProperty(OM_NODE_ID);
  }

  /**
   * Retrieves the serial id of certificate issued by SCM.
   * @return OM_ID
   */
  public String getOmCertSerialId() {
    return getStorageInfo().getProperty(OM_CERT_SERIAL_ID);
  }

  @Override
  protected Properties getNodeProperties() {
    String omId = getOmId();
    if (omId == null) {
      omId = UUID.randomUUID().toString();
    }
    Properties omProperties = new Properties();
    omProperties.setProperty(OM_ID, omId);
    String nodeId = getOmNodeId();
    if (nodeId != null) {
      omProperties.setProperty(OM_NODE_ID, nodeId);
    }

    if (getOmCertSerialId() != null) {
      omProperties.setProperty(OM_CERT_SERIAL_ID, getOmCertSerialId());
    }
    return omProperties;
  }

  /**
   * Get the location where OM should store its metadata directories.
   * Fall back to OZONE_METADATA_DIRS if not defined.
   *
   * @param conf - Config
   * @return File path, after creating all the required Directories.
   */
  public static File getOmDbDir(ConfigurationSource conf) {
    return ServerUtils.getDBPath(conf, OMConfigKeys.OZONE_OM_DB_DIRS);
  }
}