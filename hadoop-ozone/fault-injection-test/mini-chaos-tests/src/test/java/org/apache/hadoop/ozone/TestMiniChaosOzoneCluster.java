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

import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.failure.Failures;
import org.apache.hadoop.ozone.freon.FreonReplicationOptions;
import org.apache.hadoop.ozone.loadgenerators.LoadGenerator;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test Read Write with Mini Ozone Chaos Cluster.
 */
@Ignore
@Command(description = "Starts IO with MiniOzoneChaosCluster",
    name = "chaos", mixinStandardHelpOptions = true)
public class TestMiniChaosOzoneCluster extends GenericCli {

  private static List<Class<? extends Failures>> failureClasses
      = new ArrayList<>();

  private static List<Class<? extends LoadGenerator>> loadClasses
      = new ArrayList<>();

  enum AllowedBucketLayouts { FILE_SYSTEM_OPTIMIZED, OBJECT_STORE }

  @Option(names = {"-d", "--num-datanodes", "--numDatanodes"},
      description = "num of datanodes. Full name --numDatanodes will be" +
          " removed in later versions.")
  private static int numDatanodes = 20;

  @Option(names = {"-o", "--num-ozone-manager", "--numOzoneManager"},
      description = "num of ozoneManagers. Full name --numOzoneManager will" +
          " be removed in later versions.")
  private static int numOzoneManagers = 1;

  @Option(names = {"-s", "--num-storage-container-manager",
      "--numStorageContainerManagers"},
      description = "num of storageContainerManagers." +
          "Full name --numStorageContainerManagers will" +
          " be removed in later versions.")
  private static int numStorageContainerManagerss = 1;

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

  @CommandLine.Mixin
  private static FreonReplicationOptions freonReplication =
          new FreonReplicationOptions();

  @Option(names = {"-l", "--layout"},
      description = "Allowed Bucket Layouts: ${COMPLETION-CANDIDATES}")
  private static AllowedBucketLayouts allowedBucketLayout =
      AllowedBucketLayouts.FILE_SYSTEM_OPTIMIZED;

  private static MiniOzoneChaosCluster cluster;
  private static MiniOzoneLoadGenerator loadGenerator;

  private static String omServiceId = null;
  private static String scmServiceId = null;

  private static final String OM_SERVICE_ID = "ozoneChaosTest";
  private static final String SCM_SERVICE_ID = "scmChaosTest";

  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration configuration = new OzoneConfiguration();

    MiniOzoneChaosCluster.Builder chaosBuilder =
        new MiniOzoneChaosCluster.Builder(configuration);

    chaosBuilder
        .setNumDatanodes(numDatanodes)
        .setNumOzoneManagers(numOzoneManagers)
        .setOMServiceID(omServiceId)
        .setNumStorageContainerManagers(numStorageContainerManagerss)
        .setSCMServiceID(scmServiceId)
        .setNumDataVolumes(numDataVolumes);
    failureClasses.forEach(chaosBuilder::addFailures);

    cluster = chaosBuilder.build();
    cluster.waitForClusterToBeReady();

    String volumeName = RandomStringUtils.randomAlphabetic(10).toLowerCase();
    ObjectStore store = cluster.getRpcClient().getObjectStore();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);

    BucketLayout bucketLayout =
        BucketLayout.valueOf(allowedBucketLayout.toString());
    final BucketArgs.Builder builder = BucketArgs.newBuilder();

    freonReplication.fromParams(configuration).ifPresent(config ->
            builder.setDefaultReplicationConfig(
                    new DefaultReplicationConfig(config)));
    builder.setBucketLayout(bucketLayout);

    MiniOzoneLoadGenerator.Builder loadBuilder =
        new MiniOzoneLoadGenerator.Builder()
        .setVolume(volume)
        .setConf(configuration)
        .setNumBuffers(numBuffers)
        .setNumThreads(numThreads)
        .setOMServiceId(omServiceId)
        .setBucketArgs(builder.build());
    loadClasses.forEach(loadBuilder::addLoadGenerator);
    loadGenerator = loadBuilder.build();
  }

  static void addFailureClasses(Class<? extends Failures> clz) {
    failureClasses.add(clz);
  }

  static void addLoadClasses(Class<? extends LoadGenerator> clz) {
    loadClasses.add(clz);
  }

  static void setNumDatanodes(int nDns) {
    numDatanodes = nDns;
  }

  static void setNumManagers(int nOms, int numScms, boolean enableHA) {

    if (nOms > 1 || enableHA) {
      omServiceId = OM_SERVICE_ID;
    }
    numOzoneManagers = nOms;

    if (numScms > 1 || enableHA) {
      scmServiceId = SCM_SERVICE_ID;
    }
    numStorageContainerManagerss = numScms;
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

  public void startChaosCluster() throws Exception {
    try {
      init();
      cluster.startChaos(failureInterval, failureInterval, TimeUnit.SECONDS);
      loadGenerator.startIO(numMinutes, TimeUnit.MINUTES);
    } finally {
      shutdown();
    }
  }

  @Test
  public void testReadWriteWithChaosCluster() throws Exception {
    cluster.startChaos(5, 10, TimeUnit.SECONDS);
    loadGenerator.startIO(120, TimeUnit.SECONDS);
  }
}
