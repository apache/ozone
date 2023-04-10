/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.diskbalancer;

import org.apache.hadoop.hdds.server.YamlUtils;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

/**
 * Class for creating diskbalancer.info file in yaml format.
 */

public final class DiskBalancerYaml {

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

    try (Writer writer = new OutputStreamWriter(
        new FileOutputStream(path), StandardCharsets.UTF_8)) {
      yaml.dump(getDiskBalancerInfoYaml(diskBalancerInfo), writer);
    }
  }

  /**
   * Read DiskBalancerConfiguration from file.
   */
  public static DiskBalancerInfo readDiskBalancerInfoFile(File path)
      throws IOException {
    DiskBalancerInfo diskBalancerInfo;

    try (FileInputStream inputFileStream = new FileInputStream(path)) {
      DiskBalancerInfoYaml diskBalancerInfoYaml;
      try {
        diskBalancerInfoYaml =
            YamlUtils.loadAs(inputFileStream, DiskBalancerInfoYaml.class);
      } catch (Exception e) {
        throw new IOException("Unable to parse yaml file.", e);
      }

      diskBalancerInfo = new DiskBalancerInfo(
          diskBalancerInfoYaml.isShouldRun(),
          diskBalancerInfoYaml.getThreshold(),
          diskBalancerInfoYaml.getBandwidthInMB(),
          diskBalancerInfoYaml.getParallelThread(),
          DiskBalancerVersion.getDiskBalancerVersion(
              diskBalancerInfoYaml.version));
    }

    return diskBalancerInfo;
  }

  /**
   * Datanode DiskBalancer Info to be written to the yaml file.
   */
  public static class DiskBalancerInfoYaml {
    private boolean shouldRun;
    private double threshold;
    private long bandwidthInMB;
    private int parallelThread;

    private int version;

    public DiskBalancerInfoYaml() {
      // Needed for snake-yaml introspection.
    }

    private DiskBalancerInfoYaml(boolean shouldRun, double threshold,
        long bandwidthInMB, int parallelThread, int version) {
      this.shouldRun = shouldRun;
      this.threshold = threshold;
      this.bandwidthInMB = bandwidthInMB;
      this.parallelThread = parallelThread;
      this.version = version;
    }

    public boolean isShouldRun() {
      return shouldRun;
    }

    public void setShouldRun(boolean shouldRun) {
      this.shouldRun = shouldRun;
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
        diskBalancerInfo.isShouldRun(),
        diskBalancerInfo.getThreshold(),
        diskBalancerInfo.getBandwidthInMB(),
        diskBalancerInfo.getParallelThread(),
        diskBalancerInfo.getVersion().getVersion());
  }
}
