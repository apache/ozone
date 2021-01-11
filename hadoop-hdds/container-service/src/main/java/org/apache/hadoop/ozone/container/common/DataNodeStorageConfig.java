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
package org.apache.hadoop.ozone.container.common;

import static org.apache.hadoop.ozone.OzoneConsts.DATANODE_STORAGE_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.DATANODE_UUID;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.common.Storage;

/**
 * DataNodeStorageConfig is responsible for management of the
 * StorageDirectories used by the DataNode.
 */
public class DataNodeStorageConfig extends Storage {

  /**
   * Construct DataNodeStorageConfig.
   * @throws IOException if any directories are inaccessible.
   */
  public DataNodeStorageConfig(OzoneConfiguration conf, String dataNodeId)
      throws IOException {
    super(NodeType.DATANODE, ServerUtils.getOzoneMetaDirPath(conf),
        DATANODE_STORAGE_DIR);
    setDataNodeId(dataNodeId);
  }

  public DataNodeStorageConfig(NodeType type, File root, String sdName)
      throws IOException {
    super(type, root, sdName);
  }

  public void setDataNodeId(String dataNodeId) throws IOException {
    getStorageInfo().setProperty(DATANODE_UUID, dataNodeId);
  }

  /**
   * Retrieves the DataNode ID from the version file.
   * @return DataNodeId
   */
  public String getDataNodeId() {
    return getStorageInfo().getProperty(DATANODE_UUID);
  }

  @Override
  protected Properties getNodeProperties() {
    String dataNodeId = getDataNodeId();
    if (dataNodeId == null) {
      dataNodeId = UUID.randomUUID().toString();
    }
    Properties datanodeProperties = new Properties();
    datanodeProperties.setProperty(DATANODE_UUID, dataNodeId);
    return datanodeProperties;
  }
}
