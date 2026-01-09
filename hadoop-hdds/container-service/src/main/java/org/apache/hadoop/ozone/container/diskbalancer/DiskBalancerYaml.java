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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerRunningStatus;
import org.apache.hadoop.hdds.server.YamlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/**
 * Class for creating diskbalancer.info file in yaml format.
 */

public final class DiskBalancerYaml {

  private static final Logger LOG =
      LoggerFactory.getLogger(DiskBalancerYaml.class);

  private DiskBalancerYaml() {
    // static helper methods only, no state.
  }

  /**
   * Creates a yaml file to store DiskBalancer info.
   *
   * @param diskBalancerInfo {@link DiskBalancerInfo}
   * @param path            Path to diskBalancer.info file
   */
  public static void createDiskBalancerInfoFile(
      DiskBalancerInfo diskBalancerInfo, File path)
      throws IOException {
    DumperOptions options = new DumperOptions();
    options.setPrettyFlow(true);
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.FLOW);
    Yaml yaml = new Yaml(options);

    final DiskBalancerInfoYaml data = getDiskBalancerInfoYaml(diskBalancerInfo);
    YamlUtils.dump(yaml, data, path, LOG);
  }

  /**
   * Read DiskBalancerConfiguration from file.
   */
  public static DiskBalancerInfo readDiskBalancerInfoFile(File path)
      throws IOException {
    DiskBalancerInfo diskBalancerInfo;

    try (InputStream inputFileStream = Files.newInputStream(path.toPath())) {
      DiskBalancerInfoYaml diskBalancerInfoYaml;
      try {
        diskBalancerInfoYaml =
            YamlUtils.loadAs(inputFileStream, DiskBalancerInfoYaml.class);
      } catch (Exception e) {
        throw new IOException("Unable to parse yaml file.", e);
      }

      diskBalancerInfo = new DiskBalancerInfo(
          diskBalancerInfoYaml.operationalState,
          diskBalancerInfoYaml.getThreshold(),
          diskBalancerInfoYaml.getBandwidthInMB(),
          diskBalancerInfoYaml.getParallelThread(),
          diskBalancerInfoYaml.isStopAfterDiskEven(),
          DiskBalancerVersion.getDiskBalancerVersion(
              diskBalancerInfoYaml.version));
    }

    return diskBalancerInfo;
  }

  /**
   * Datanode DiskBalancer Info to be written to the yaml file.
   */
  public static class DiskBalancerInfoYaml {
    private DiskBalancerRunningStatus operationalState;
    private double threshold;
    private long bandwidthInMB;
    private int parallelThread;
    private boolean stopAfterDiskEven;

    private int version;

    public DiskBalancerInfoYaml() {
      // Needed for snake-yaml introspection.
    }

    private DiskBalancerInfoYaml(DiskBalancerRunningStatus operationalState, double threshold,
        long bandwidthInMB, int parallelThread, boolean stopAfterDiskEven, int version) {
      this.operationalState = operationalState;
      this.threshold = threshold;
      this.bandwidthInMB = bandwidthInMB;
      this.parallelThread = parallelThread;
      this.stopAfterDiskEven = stopAfterDiskEven;
      this.version = version;
    }

    public DiskBalancerRunningStatus getOperationalState() {
      return operationalState;
    }

    public void setOperationalState(DiskBalancerRunningStatus operationalState) {
      this.operationalState = operationalState;
    }

    public void setThreshold(double threshold) {
      this.threshold = threshold;
    }

    public double getThreshold() {
      return threshold;
    }

    public void setBandwidthInMB(long bandwidthInMB) {
      this.bandwidthInMB = bandwidthInMB;
    }

    public long getBandwidthInMB() {
      return this.bandwidthInMB;
    }

    public void setParallelThread(int parallelThread) {
      this.parallelThread = parallelThread;
    }

    public int getParallelThread() {
      return this.parallelThread;
    }

    public boolean isStopAfterDiskEven() {
      return stopAfterDiskEven;
    }

    public void setStopAfterDiskEven(boolean stopAfterDiskEven) {
      this.stopAfterDiskEven = stopAfterDiskEven;
    }

    public void setVersion(int version) {
      this.version = version;
    }

    public int getVersion() {
      return this.version;
    }
  }

  private static DiskBalancerInfoYaml getDiskBalancerInfoYaml(
      DiskBalancerInfo diskBalancerInfo) {

    return new DiskBalancerInfoYaml(
        diskBalancerInfo.getOperationalState(),
        diskBalancerInfo.getThreshold(),
        diskBalancerInfo.getBandwidthInMB(),
        diskBalancerInfo.getParallelThread(),
        diskBalancerInfo.isStopAfterDiskEven(),
        diskBalancerInfo.getVersion().getVersion());
  }
}
