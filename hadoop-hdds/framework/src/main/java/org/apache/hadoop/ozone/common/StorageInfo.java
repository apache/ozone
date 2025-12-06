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

package org.apache.hadoop.ozone.common;

import static org.apache.hadoop.ozone.common.Storage.STORAGE_FILE_VERSION;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common class for storage information. This class defines the common
 * properties and functions to set them , write them into the version file
 * and read them from the version file.
 *
 */
@InterfaceAudience.Private
public class StorageInfo {

  private static final Logger LOG = LoggerFactory.getLogger(StorageInfo.class);

  private Properties properties = new Properties();

  /**
   * Property to hold node type.
   */
  private static final String NODE_TYPE = "nodeType";
  /**
   * Property to hold ID of the cluster.
   */
  private static final String CLUSTER_ID = "clusterID";
  /**
   * Property to hold creation time of the storage.
   */
  private static final String CREATION_TIME = "cTime";
  /**
   * Property to hold the layout version.
   */
  private static final String LAYOUT_VERSION = "layoutVersion";

  private static final String FIRST_UPGRADE_ACTION_LAYOUT_VERSION =
      "firstUpgradeActionLayoutVersion";

  private static final int INVALID_LAYOUT_VERSION = -1;

  /**
   * Constructs StorageInfo instance.
   * @param type
   *          Type of the node using the storage
   * @param cid
   *          Cluster ID
   * @param cT
   *          Cluster creation Time

   * @throws IOException - on Error.
   */
  public StorageInfo(NodeType type, String cid, long cT, int layout)
      throws IOException {
    Objects.requireNonNull(type, "type == null");
    Objects.requireNonNull(cid, "cid == null");
    properties.setProperty(NODE_TYPE, type.name());
    properties.setProperty(CLUSTER_ID, cid);
    properties.setProperty(CREATION_TIME, String.valueOf(cT));
    properties.setProperty(LAYOUT_VERSION, Integer.toString(layout));
  }

  public StorageInfo(NodeType type, File propertiesFile)
      throws IOException {
    this.properties = readFrom(propertiesFile);
    verifyNodeType(type);
    verifyClusterId();
    verifyCreationTime();
    verifyLayoutVersion();
  }

  public NodeType getNodeType() {
    return NodeType.valueOf(properties.getProperty(NODE_TYPE));
  }

  public String getClusterID() {
    return properties.getProperty(CLUSTER_ID);
  }

  public Long  getCreationTime() {
    String creationTime = properties.getProperty(CREATION_TIME);
    if (creationTime != null) {
      return Long.parseLong(creationTime);
    }
    return null;
  }

  public int getLayoutVersion() {
    String layout = properties.getProperty(LAYOUT_VERSION);
    if (layout != null) {
      return Integer.parseInt(layout);
    }
    return 0;
  }

  private void verifyLayoutVersion() {
    String layout = getProperty(LAYOUT_VERSION);
    if (layout == null) {
      LOG.warn("Found " + STORAGE_FILE_VERSION + " file without any layout " +
          "version. Defaulting to 0.");
      setProperty(LAYOUT_VERSION, "0");
    }
  }

  public int getFirstUpgradeActionLayoutVersion() {
    String upgradingTo =
        properties.getProperty(FIRST_UPGRADE_ACTION_LAYOUT_VERSION);
    if (upgradingTo != null) {
      return Integer.parseInt(upgradingTo);
    }
    return INVALID_LAYOUT_VERSION;
  }

  public void setFirstUpgradeActionLayoutVersion(int layoutVersion) {
    properties.setProperty(
        FIRST_UPGRADE_ACTION_LAYOUT_VERSION, Integer.toString(layoutVersion));
  }

  public String getProperty(String key) {
    return properties.getProperty(key);
  }

  public void setProperty(String key, String value) {
    properties.setProperty(key, value);
  }

  public void unsetProperty(String key) {
    properties.remove(key);
  }

  public void setClusterId(String clusterId) {
    properties.setProperty(CLUSTER_ID, clusterId);
  }

  public void setLayoutVersion(int version) {
    properties.setProperty(LAYOUT_VERSION, Integer.toString(version));
  }

  private void verifyNodeType(NodeType type)
      throws InconsistentStorageStateException {
    NodeType nodeType = getNodeType();
    Objects.requireNonNull(nodeType, "nodeType == null");
    if (type != nodeType) {
      throw new InconsistentStorageStateException("Expected NodeType: " + type +
          ", but found: " + nodeType);
    }
  }

  private void verifyClusterId()
      throws InconsistentStorageStateException {
    String clusterId = getClusterID();
    Objects.requireNonNull(clusterId, "clusterId == null");
    if (clusterId.isEmpty()) {
      throw new InconsistentStorageStateException("Cluster ID not found");
    }
  }

  private void verifyCreationTime() {
    Long creationTime = getCreationTime();
    Objects.requireNonNull(creationTime, "creationTime == null");
  }

  public void writeTo(File to)
      throws IOException {
    IOUtils.writePropertiesToFile(to, properties);
  }

  private Properties readFrom(File from) throws IOException {
    return IOUtils.readPropertiesFromFile(from);
  }

  /**
   * Generate new clusterID.
   *
   * clusterID is a persistent attribute of the cluster.
   * It is generated when the cluster is created and remains the same
   * during the life cycle of the cluster.  When a new SCM node is initialized,
   * if this is a new cluster, a new clusterID is generated and stored.
   * @return new clusterID
   */
  public static String newClusterID() {
    return OzoneConsts.CLUSTER_ID_PREFIX + UUID.randomUUID().toString();
  }
}
