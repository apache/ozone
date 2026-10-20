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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestJobworkerVersionFile {

  private File versionFile;

  @TempDir
  private Path folder;

  @BeforeEach
  public void setup() throws IOException {
    versionFile = Files.createFile(
        folder.resolve("VersionFile")).toFile();
  }

  @Test
  public void testCreateAndReadVersionFile() throws IOException {
    String storageID = UUID.randomUUID().toString();
    String clusterID = UUID.randomUUID().toString();
    String jobworkerUUID = UUID.randomUUID().toString();
    long cTime = Time.now();
    int layout = JobworkerVolumeLayoutVersion.getLatestVersion().getVersion();
    int pid = new Random().nextInt();
    JobworkerVersionFile jobworkerVersionFile = new JobworkerVersionFile(
        storageID, clusterID, jobworkerUUID, cTime, layout, String.valueOf(pid));
    jobworkerVersionFile.createVersionFile(versionFile);
    Properties properties = JobworkerVersionFile.readFrom(versionFile);

    //Check VersionFile exists
    assertTrue(versionFile.exists());
    assertEquals(storageID, properties.get(OzoneConsts.STORAGE_ID));
    assertEquals(clusterID, properties.get(OzoneConsts.CLUSTER_ID));
    assertEquals(jobworkerUUID, properties.get(OzoneConsts.JOBWORKER_UUID));
    assertEquals(String.valueOf(cTime), properties.get(OzoneConsts.CTIME));
    assertEquals(String.valueOf(layout), properties.get(OzoneConsts.LAYOUTVERSION));
    assertEquals(String.valueOf(pid), properties.get(OzoneConsts.PID));
  }

}
