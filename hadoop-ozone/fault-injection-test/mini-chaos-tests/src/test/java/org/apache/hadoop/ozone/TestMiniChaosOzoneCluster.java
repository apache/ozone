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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.MiniOzoneChaosCluster.FailureService;
import org.apache.hadoop.util.ExitUtil;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Test Read Write with Mini Ozone Chaos Cluster.
 */
@Ignore
@Command(description = "Starts IO with MiniOzoneChaosCluster",
    name = "chaos", mixinStandardHelpOptions = true)
public class TestMiniChaosOzoneCluster implements Runnable {
  static final Logger LOG =
      LoggerFactory.getLogger(TestMiniChaosOzoneCluster.class);

  @Option(names = {"-d", "--numDatanodes"},
      description = "num of datanodes")
  private static int numDatanodes = 20;

  @Option(names = {"-o", "--numOzoneManager"},
      description = "num of ozoneManagers")
  private static int numOzoneManagers = 1;

  @Option(names = {"-s", "--failureService"},
      description = "service (datanode or ozoneManager) to test chaos on",
      defaultValue = "datanode")
  private static String failureService = "datanode";

  @Option(names = {"-t", "--numThreads"},
      description = "num of IO threads")
  private static int numThreads = 5;

  @Option(names = {"-b", "--numBuffers"},
      description = "num of IO buffers")
  private static int numBuffers = 16;

  @Option(names = {"-m", "--numMinutes"},
      description = "total run time")
  private static int numMinutes = 1440; // 1 day by default

  @Option(names = {"-n", "--numClients"},
      description = "no of clients writing to OM")
  private static int numClients = 3;

  @Option(names = {"-v", "--numDataVolume"},
      description = "no of datanode volumes")
  private static int numDataVolumes = 3;

  @Option(names = {"-i", "--failureInterval"},
      description = "time between failure events in seconds")
  private static int failureInterval = 300; // 5 minute period between failures.

  private static MiniOzoneChaosCluster cluster;
  private static MiniOzoneLoadGenerator loadGenerator;

  private static final String OM_SERVICE_ID = "ozoneChaosTest";

  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration configuration = new OzoneConfiguration();
    String omServiceID =
        FailureService.of(failureService) == FailureService.OZONE_MANAGER ?
            OM_SERVICE_ID : null;

    cluster = new MiniOzoneChaosCluster.Builder(configuration)
        .setNumDatanodes(numDatanodes)
        .setNumOzoneManagers(numOzoneManagers)
        .setFailureService(failureService)
        .setOMServiceID(omServiceID)
        .setNumDatanodesDirs(numDataVolumes)
        .build();
    cluster.waitForClusterToBeReady();

    String volumeName = RandomStringUtils.randomAlphabetic(10).toLowerCase();
    ObjectStore store = cluster.getRpcClient().getObjectStore();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);

    loadGenerator =
        new MiniOzoneLoadGenerator(volume, numClients, numThreads,
            numBuffers, configuration, omServiceID);
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

  public void run() {
    try {
      init();
      cluster.startChaos(failureInterval, failureInterval, TimeUnit.SECONDS);
      loadGenerator.startIO(numMinutes, TimeUnit.MINUTES);
    } catch (Exception e) {
      LOG.error("terminating with exception", e);
      ExitUtil.terminate(-2, e);
    } finally {
      shutdown();
    }
  }

  public static void main(String... args) {
    CommandLine.run(new TestMiniChaosOzoneCluster(), System.err, args);
  }

  @Test
  public void testReadWriteWithChaosCluster() {
    cluster.startChaos(5, 10, TimeUnit.SECONDS);
    loadGenerator.startIO(120, TimeUnit.SECONDS);
  }
}
