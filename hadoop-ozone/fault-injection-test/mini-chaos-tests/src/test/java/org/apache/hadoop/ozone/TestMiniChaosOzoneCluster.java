/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.MiniOzoneChaosCluster.FailureService;
import org.apache.hadoop.ozone.failure.Failures;
import org.apache.hadoop.ozone.loadgenerators.RandomLoadGenerator;
import org.apache.hadoop.ozone.loadgenerators.ReadOnlyLoadGenerator;
import org.apache.hadoop.ozone.loadgenerators.FilesystemLoadGenerator;
import org.apache.hadoop.ozone.loadgenerators.AgedLoadGenerator;
import org.apache.hadoop.ozone.loadgenerators.AgedDirLoadGenerator;
import org.apache.hadoop.ozone.loadgenerators.RandomDirLoadGenerator;
import org.apache.hadoop.ozone.loadgenerators.NestedDirLoadGenerator;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Test Read Write with Mini Ozone Chaos Cluster.
 */
@Ignore
@Command(description = "Starts IO with MiniOzoneChaosCluster",
    name = "chaos", mixinStandardHelpOptions = true)
public class TestMiniChaosOzoneCluster extends GenericCli {
  static final Logger LOG =
      LoggerFactory.getLogger(TestMiniChaosOzoneCluster.class);

  @Option(names = {"-d", "--num-datanodes", "--numDatanodes"},
      description = "num of datanodes. Full name --numDatanodes will be" +
          " removed in later versions.")
  private static int numDatanodes = 20;

  @Option(names = {"-o", "--num-ozone-manager", "--numOzoneManager"},
      description = "num of ozoneManagers. Full name --numOzoneManager will" +
          " be removed in later versions.")
  private static int numOzoneManagers = 1;

  @Option(names = {"-s", "--failure-service", "--failureService"},
      description = "service (datanode or ozoneManager) to test chaos on. " +
          "Full --failureService name will be removed in later versions.",
      defaultValue = "datanode")
  private static String failureService = "datanode";

  @Option(names = {"-t", "--num-threads", "--numThreads"},
      description = "num of IO threads. Full name --numThreads will be" +
          " removed in later versions.")
  private static int numThreads = 5;

  @Option(names = {"-b", "--num-buffers", "--numBuffers"},
      description = "num of IO buffers. Full name --numBuffers will be" +
          " removed in later versions.")
  private static int numBuffers = 16;

  @Option(names = {"-m", "--num-minutes", "--numMinutes"},
      description = "total run time. Full name --numMinutes will be " +
          "removed in later versions.")
  private static int numMinutes = 1440; // 1 day by default

  @Option(names = {"-v", "--num-data-volume", "--numDataVolume"},
      description = "number of datanode volumes to create. Full name " +
          "--numDataVolume will be removed in later versions.")
  private static int numDataVolumes = 3;

  @Option(names = {"-i", "--failure-interval", "--failureInterval"},
      description = "time between failure events in seconds. Full name " +
          "--failureInterval will be removed in later versions.")
  private static int failureInterval = 300; // 5 minute period between failures.

  private static MiniOzoneChaosCluster cluster;
  private static MiniOzoneLoadGenerator loadGenerator;

  private static final String OM_SERVICE_ID = "ozoneChaosTest";

  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration configuration = new OzoneConfiguration();
    FailureService service = FailureService.of(failureService);
    String omServiceID;

    MiniOzoneChaosCluster.Builder builder =
        new MiniOzoneChaosCluster.Builder(configuration);

    switch (service) {
    case DATANODE:
      omServiceID = null;
      builder
          .addFailures(Failures.DatanodeRestartFailure.class)
          .addFailures(Failures.DatanodeStartStopFailure.class);
      break;
    case OZONE_MANAGER:
      omServiceID = OM_SERVICE_ID;
      builder
          .addFailures(Failures.OzoneManagerStartStopFailure.class)
          .addFailures(Failures.OzoneManagerRestartFailure.class);
      break;
    default:
      throw new IllegalArgumentException();
    }

    builder
        .setNumDatanodes(numDatanodes)
        .setNumOzoneManagers(numOzoneManagers)
        .setOMServiceID(omServiceID)
        .setNumDataVolumes(numDataVolumes);

    cluster = builder.build();
    cluster.waitForClusterToBeReady();

    String volumeName = RandomStringUtils.randomAlphabetic(10).toLowerCase();
    ObjectStore store = cluster.getRpcClient().getObjectStore();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);

    loadGenerator = new MiniOzoneLoadGenerator.Builder()
        .setVolume(volume)
        .setConf(configuration)
        .setNumBuffers(numBuffers)
        .setNumThreads(numThreads)
        .setOMServiceId(omServiceID)
        .addLoadGenerator(RandomLoadGenerator.class)
        .addLoadGenerator(AgedLoadGenerator.class)
        .addLoadGenerator(FilesystemLoadGenerator.class)
        .addLoadGenerator(ReadOnlyLoadGenerator.class)
        .addLoadGenerator(RandomDirLoadGenerator.class)
        .addLoadGenerator(AgedDirLoadGenerator.class)
        .addLoadGenerator(NestedDirLoadGenerator.class)
        .build();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (loadGenerator != null) {
      loadGenerator.shutdownLoadGenerator();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Override
  public Void call() throws Exception {
    try {
      init();
      cluster.startChaos(failureInterval, failureInterval, TimeUnit.SECONDS);
      loadGenerator.startIO(numMinutes, TimeUnit.MINUTES);
    } finally {
      shutdown();
    }
    return null;
  }

  public static void main(String... args) {
    new TestMiniChaosOzoneCluster().run(args);
  }

  @Test
  public void testReadWriteWithChaosCluster() throws Exception {
    cluster.startChaos(5, 10, TimeUnit.SECONDS);
    loadGenerator.startIO(120, TimeUnit.SECONDS);
  }
}
