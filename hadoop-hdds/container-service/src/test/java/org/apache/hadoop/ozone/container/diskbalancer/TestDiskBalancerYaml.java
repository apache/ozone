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
import java.util.stream.Stream;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerRunningStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to test DiskBalancer's YAML operation.
 */
public class TestDiskBalancerYaml {
  @TempDir
  private Path tmpDir;

  @ParameterizedTest
  @MethodSource("diskBalancerYamlCases")
  public void testDiskBalancerYamlRoundTrip(double threshold, long bandwidthInMB, int parallelThread,
      boolean stopAfterDiskEven, String containerStates) throws IOException {
    File file = new File(tmpDir.toString(), OZONE_SCM_DATANODE_DISK_BALANCER_INFO_FILE_DEFAULT);
    DiskBalancerInfo info = new DiskBalancerInfo(
        DiskBalancerRunningStatus.RUNNING,
        threshold,
        bandwidthInMB,
        parallelThread,
        stopAfterDiskEven,
        containerStates,
        DiskBalancerVersion.DEFAULT_VERSION);
    DiskBalancerYaml.createDiskBalancerInfoFile(info, file);
    DiskBalancerInfo newInfo = DiskBalancerYaml.readDiskBalancerInfoFile(file);
    Assertions.assertEquals(info, newInfo);
  }

  public static Stream<Arguments> diskBalancerYamlCases() {
    return Stream.of(
        Arguments.of(10d, 100L, 5, true, DiskBalancerConfiguration.DEFAULT_CONTAINER_STATES),
        Arguments.of(15d, 200L, 10, false, "CLOSED")
    );
  }
}
