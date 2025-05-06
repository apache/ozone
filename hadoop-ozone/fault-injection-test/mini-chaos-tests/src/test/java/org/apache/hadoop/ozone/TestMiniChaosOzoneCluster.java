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

package org.apache.hadoop.ozone;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.failure.Failures;
import org.apache.hadoop.ozone.freon.FreonReplicationOptions;
import org.apache.hadoop.ozone.loadgenerators.LoadGenerator;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Test Read Write with Mini Ozone Chaos Cluster.
 */
@Command(description = "Starts IO with MiniOzoneChaosCluster",
    name = "chaos", mixinStandardHelpOptions = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Unhealthy("HDDS-3131")
public class TestMiniChaosOzoneCluster extends GenericCli {

  private final List<Class<? extends Failures>> failureClasses
      = new ArrayList<>();

  private final List<Class<? extends LoadGenerator>> loadClasses
      = new ArrayList<>();

  @Option(names = {"-d", "--num-datanodes", "--numDatanodes"},
      description = "num of datanodes. Full name --numDatanodes will be" +
          " removed in later versions.")
  private int numDatanodes = 20;

  @Option(names = {"-o", "--num-ozone-manager", "--numOzoneManager"},
      description = "num of ozoneManagers. Full name --numOzoneManager will" +
          " be removed in later versions.")
  private int numOzoneManagers = 1;

  @Option(names = {"-s", "--num-storage-container-manager",
      "--numStorageContainerManagers"},
      description = "num of storageContainerManagers." +
          "Full name --numStorageContainerManagers will" +
          " be removed in later versions.")
  private int numStorageContainerManagerss = 1;

  @Option(names = {"-t", "--num-threads", "--numThreads"},
      description = "num of IO threads. Full name --numThreads will be" +
          " removed in later versions.")
  private int numThreads = 5;

  @Option(names = {"-b", "--num-buffers", "--numBuffers"},
      description = "num of IO buffers. Full name --numBuffers will be" +
          " removed in later versions.")
  private int numBuffers = 16;

  @Option(names = {"-m", "--num-minutes", "--numMinutes"},
      description = "total run time. Full name --numMinutes will be " +
          "removed in later versions.")
  private int numMinutes = 1440; // 1 day by default

  @Option(names = {"-v", "--num-data-volume", "--numDataVolume"},
      description = "number of datanode volumes to create. Full name " +
          "--numDataVolume will be removed in later versions.")
  private int numDataVolumes = 3;

  @Option(names = {"--initial-delay"},
      description = "time (in seconds) before first failure event")
  private int initialDelay = 300; // seconds

  @Option(names = {"-i", "--failure-interval", "--failureInterval"},
      description = "time between failure events in seconds. Full name " +
          "--failureInterval will be removed in later versions.")
  private int failureInterval = 300; // 5 minute period between failures.

  @CommandLine.Mixin
  private FreonReplicationOptions freonReplication =
          new FreonReplicationOptions();

  @Option(names = {"-l", "--layout"},
      description = "Allowed Bucket Layouts: ${COMPLETION-CANDIDATES}")
  private AllowedBucketLayouts allowedBucketLayout =
      AllowedBucketLayouts.FILE_SYSTEM_OPTIMIZED;

  private MiniOzoneChaosCluster cluster;
  private OzoneClient client;
  private MiniOzoneLoadGenerator loadGenerator;

  private String omServiceId;
  private String scmServiceId;

  private static final String OM_SERVICE_ID = "ozoneChaosTest";
  private static final String SCM_SERVICE_ID = "scmChaosTest";

  private void init() throws Exception {
    OzoneConfiguration configuration = new OzoneConfiguration();

    MiniOzoneChaosCluster.Builder chaosBuilder =
        new MiniOzoneChaosCluster.Builder(configuration);

    chaosBuilder
        .setNumDatanodes(numDatanodes)
        .setNumOzoneManagers(numOzoneManagers)
        .setOMServiceID(omServiceId)
        .setNumStorageContainerManagers(numStorageContainerManagerss)
        .setSCMServiceID(scmServiceId)
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder()
            .setNumDataVolumes(numDataVolumes)
            .build());
    failureClasses.forEach(chaosBuilder::addFailures);

    cluster = chaosBuilder.build();
    cluster.waitForClusterToBeReady();

    client = cluster.newClient();
    ObjectStore store = client.getObjectStore();
    String volumeName = RandomStringUtils.secure().nextAlphabetic(10).toLowerCase();
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

  void addFailureClasses(Class<? extends Failures> clz) {
    failureClasses.add(clz);
  }

  void addLoadClasses(Class<? extends LoadGenerator> clz) {
    loadClasses.add(clz);
  }

  void setNumDatanodes(int nDns) {
    numDatanodes = nDns;
  }

  void setNumManagers(int nOms, int numScms, boolean enableHA) {

    if (nOms > 1 || enableHA) {
      omServiceId = OM_SERVICE_ID;
    }
    numOzoneManagers = nOms;

    if (numScms > 1 || enableHA) {
      scmServiceId = SCM_SERVICE_ID;
    }
    numStorageContainerManagerss = numScms;
  }

  private void shutdown() {
    if (loadGenerator != null) {
      loadGenerator.shutdownLoadGenerator();
    }

    IOUtils.closeQuietly(client, cluster);
  }

  public void startChaosCluster() throws Exception {
    try {
      init();
      cluster.startChaos(initialDelay, failureInterval, TimeUnit.SECONDS);
      loadGenerator.startIO(numMinutes, TimeUnit.MINUTES);
    } finally {
      shutdown();
    }
  }

  @Test
  void test() throws Exception {
    initialDelay = 5; // seconds
    failureInterval = 10; // seconds
    numMinutes = 2;
    startChaosCluster();
  }

  enum AllowedBucketLayouts { FILE_SYSTEM_OPTIMIZED, OBJECT_STORE }
}
