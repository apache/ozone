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

package org.apache.hadoop.ozone.container.common.helpers;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * This is a utility class which helps to create the version file on datanode
 * and also validate the content of the version file.
 */
public class DatanodeVersionFile {

  private final String storageId;
  private final String clusterId;
  private final String datanodeUuid;
  private final long cTime;
  private final int layoutVersion;

  public DatanodeVersionFile(String storageId, String clusterId,
      String datanodeUuid, long cTime, int layoutVersion) {
    this.storageId = storageId;
    this.clusterId = clusterId;
    this.datanodeUuid = datanodeUuid;
    this.cTime = cTime;
    this.layoutVersion = layoutVersion;
  }

  private Properties createProperties() {
    Properties properties = new Properties();
    properties.setProperty(OzoneConsts.STORAGE_ID, storageId);
    properties.setProperty(OzoneConsts.CLUSTER_ID, clusterId);
    properties.setProperty(OzoneConsts.DATANODE_UUID, datanodeUuid);
    properties.setProperty(OzoneConsts.CTIME, String.valueOf(cTime));
    properties.setProperty(OzoneConsts.LAYOUTVERSION, String.valueOf(
        layoutVersion));
    return properties;
  }

  /**
   * Creates a version File in specified path.
   * @param path
   * @throws IOException
   */
  public void createVersionFile(File path) throws
      IOException {
    IOUtils.writePropertiesToFile(path, createProperties());
  }

  /**
   * Creates a property object from the specified file content.
   * @param  versionFile
   * @return Properties
   * @throws IOException
   */
  public static Properties readFrom(File versionFile) throws IOException {
    return IOUtils.readPropertiesFromFile(versionFile);
  }
}
