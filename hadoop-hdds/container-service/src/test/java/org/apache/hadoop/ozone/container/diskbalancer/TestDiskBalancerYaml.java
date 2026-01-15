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

package org.apache.hadoop.ozone.container.diskbalancer;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_SCM_DATANODE_DISK_BALANCER_INFO_FILE_DEFAULT;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerRunningStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests to test DiskBalancer's YAML operation.
 */
public class TestDiskBalancerYaml {
  @TempDir
  private Path tmpDir;

  @Test
  public void testCreateYaml() throws IOException {
    double threshold = 10;
    long bandwidthInMB = 100;
    int parallelThread = 5;
    boolean stopAfterDiskEven = true;
    DiskBalancerVersion version = DiskBalancerVersion.DEFAULT_VERSION;

    File file = new File(tmpDir.toString(),
        OZONE_SCM_DATANODE_DISK_BALANCER_INFO_FILE_DEFAULT);

    DiskBalancerInfo info = new DiskBalancerInfo(DiskBalancerRunningStatus.RUNNING, threshold,
        bandwidthInMB, parallelThread, stopAfterDiskEven, version);

    DiskBalancerYaml.createDiskBalancerInfoFile(info, file);

    DiskBalancerInfo newInfo = DiskBalancerYaml.readDiskBalancerInfoFile(file);

    Assertions.assertEquals(info, newInfo);
  }
}
