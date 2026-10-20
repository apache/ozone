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

package org.apache.hadoop.ozone.jobworker.volume;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Properties;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * This is a utility class which helps to create the version file on Jobworker
 * and also validate the content of the version file.
 */
public class JobworkerVersionFile {
  private final String storageId;
  private final String clusterId;
  private final String jobworkerUuid;
  private final long cTime;
  private final int layoutVersion;
  private final String pid;

  public JobworkerVersionFile(String storageId, String clusterId,
                              String jobworkerUuid, long cTime, int layoutVersion, String pid) {
    this.storageId = storageId;
    this.clusterId = clusterId;
    this.jobworkerUuid = jobworkerUuid;
    this.cTime = cTime;
    this.layoutVersion = layoutVersion;
    this.pid = pid;
  }

  /**
   * Creates a property object from the specified file content.
   *
   * @param versionFile
   * @return Properties
   * @throws IOException
   */
  public static Properties readFrom(File versionFile) throws IOException {
    try (InputStream in = Files.newInputStream(versionFile.toPath())) {
      Properties props = new Properties();
      props.load(in);
      return props;
    }
  }

  private Properties createProperties() {
    Properties properties = new Properties();
    properties.setProperty(OzoneConsts.STORAGE_ID, storageId);
    properties.setProperty(OzoneConsts.CLUSTER_ID, clusterId);
    properties.setProperty(OzoneConsts.JOBWORKER_UUID, jobworkerUuid);
    properties.setProperty(OzoneConsts.CTIME, String.valueOf(cTime));
    properties.setProperty(OzoneConsts.LAYOUTVERSION, String.valueOf(layoutVersion));
    properties.setProperty(OzoneConsts.PID, pid);
    return properties;
  }

  /**
   * Creates a version File in specified path.
   *
   * @param path
   * @throws IOException
   */
  public void createVersionFile(File path) throws
      IOException {
    try (OutputStream out = Files.newOutputStream(path.toPath())) {
      Properties properties = createProperties();
      /*
       * If server is interrupted before this line,
       * the version file will remain unchanged.
       */
      properties.store(out, null);
    }
  }
}
