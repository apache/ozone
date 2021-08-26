/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Class to create mini-clusters in the background. In general creating a mini
 * Cluster takes 15 - 30 seconds and destroying one can take about 10 seconds.
 *
 * When there are tests that must create a new cluster per test, the time taken
 * to create the clusters can greatly increase the test runtime. If the test
 * logic runs for longer than the time to create the cluster, then it can make
 * sense to create a new cluster in the background while the first test is
 * running. Then when test 1 completes, we can destroy its cluster in the
 * background and immediately give a new cluster to test 2.
 *
 * If the runtime of the test logic is very fast (only a second or two) after
 * the cluster is started, this class may not help much, as there will be no
 * time to create the new cluster in the background before the next test starts,
 * however shutting down the cluster in the background while the new cluster is
 * getting created will likely save about 10 seconds per test.
 *
 * To use this class, setup the Cluster Provider in a static method annotated
 * with @BeforeClass, eg:
 *
 *   @BeforeClass
 *   public static void init() {
 *     OzoneConfiguration conf = new OzoneConfiguration();
 *     final int interval = 100;
 *
 *     conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
 *         interval, TimeUnit.MILLISECONDS);
 *     ...
 *
 *     ReplicationManagerConfiguration replicationConf =
 *         conf.getObject(ReplicationManagerConfiguration.class);
 *     replicationConf.setInterval(Duration.ofSeconds(1));
 *     conf.setFromObject(replicationConf);
 *
 *     MiniOzoneCluster.Builder builder = MiniOzoneCluster.newBuilder(conf)
 *         .setNumDatanodes(numOfDatanodes);
 *
 *     clusterProvider = new MiniOzoneClusterProvider(conf, builder);
 *   }
 *
 * Ensure you shutdown the provider in a @AfterClass annotated method:
 *
 *   @AfterClass
 *   public static void shutdown() throws InterruptedException {
 *     if (clusterProvider != null) {
 *       clusterProvider.shutdown();
 *     }
 *   }
 *
 * Then in the @Before method, or in the test itself, obtain a cluster:
 *
 *   @Before
 *   public void setUp() throws Exception {
 *     cluster = clusterProvider.provide();
 *   }
 *
 *   @After
 *   public void tearDown() throws InterruptedException, IOException {
 *     if (cluster != null) {
 *       clusterProvider.destroy(cluster);
 *     }
 *   }
 *
 *  This only works if the same config / builder object can be passed to each
 *  cluster in the test suite.
 */
public class MiniOzoneClusterProvider {

  private static Logger LOG
      = LoggerFactory.getLogger(MiniOzoneClusterProvider.class);

  private int preCreatedLimit = 1;
  private volatile boolean shutdown = false;

  private final OzoneConfiguration conf;
  private final MiniOzoneCluster.Builder builder;
  private Thread createThread;
  private Thread reapThread;

  private final Set<MiniOzoneCluster> createdClusters = new HashSet<>();
  private final BlockingQueue<MiniOzoneCluster> clusters
      = new ArrayBlockingQueue<>(preCreatedLimit);
  private final BlockingQueue<MiniOzoneCluster> expiredClusters
      = new ArrayBlockingQueue<>(4);

  public MiniOzoneClusterProvider(OzoneConfiguration conf,
      MiniOzoneCluster.Builder builder) {
    this.conf = conf;
    this.builder = builder;
    createThread = createClusters();
    reapThread = reapClusters();
  }

  public synchronized MiniOzoneCluster provide()
      throws InterruptedException, IOException {
    ensureNotShutdown();
    MiniOzoneCluster cluster = clusters.poll(100, SECONDS);
    createdClusters.add(cluster);
    return cluster;
  }

  public synchronized void destroy(MiniOzoneCluster c)
      throws InterruptedException, IOException {
    ensureNotShutdown();
    createdClusters.remove(c);
    expiredClusters.put(c);
  }

  public synchronized void shutdown() throws InterruptedException {
    createThread.interrupt();
    createThread.join();
    destroyRemainingClusters();
    shutdown = true;
    reapThread.join();
  }

  private void ensureNotShutdown() throws IOException {
    if (shutdown) {
      throw new IOException("The mini-cluster provider is shutdown");
    }
  }

  private Thread reapClusters() {
    Thread t = new Thread(() -> {
      while(!shutdown || !expiredClusters.isEmpty()) {
        try {
          // Why not just call take and wait forever until interrupt is
          // thrown? Inside MiniCluster.shutdown, there are places where it
          // waits on things to happen, and the interrupt can interrupt those
          // and prevent the shutdown from happening correctly. Therefore it is
          // safer to just poll and avoid interrupting this thread.
          MiniOzoneCluster c = expiredClusters.poll(100,
              TimeUnit.MILLISECONDS);
          if (c != null) {
            c.shutdown();
          }
        } catch (Exception e) {
          LOG.error("Unexpected exception received", e);
        }
      }
    });
    t.setName("Mini-Cluster-Provider-Reap");
    t.start();
    return t;
  }

  private Thread createClusters() {
    Thread t = new Thread(() -> {
      while (!Thread.interrupted()) {
        MiniOzoneCluster cluster = null;
        try {
          builder.setClusterId(UUID.randomUUID().toString());

          OzoneConfiguration newConf = new OzoneConfiguration(conf);
          List<Integer> portList = getFreePortList(4);
          newConf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY,
              "127.0.0.1:" + portList.get(0));
          newConf.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY,
              "127.0.0.1:" + portList.get(1));
          newConf.set(OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY,
              "127.0.0.1:" + portList.get(2));
          newConf.setInt(OMConfigKeys.OZONE_OM_RATIS_PORT_KEY,
              portList.get(3));
          builder.setConf(newConf);

          cluster = builder.build();
          cluster.waitForClusterToBeReady();
          clusters.put(cluster);
        } catch (InterruptedException e) {
          if (cluster != null) {
            cluster.shutdown();
          }
          break;
        } catch (IOException | TimeoutException e) {
          throw new RuntimeException("Unable to build cluster", e);
        }
      }
    });
    t.setName("Mini-Cluster-Provider-Create");
    t.start();
    return t;
  }

  private void destroyRemainingClusters() {
    while(!clusters.isEmpty()) {
      try {
        MiniOzoneCluster cluster = clusters.poll();
        if (cluster != null) {
          destroy(cluster);
        }
      } catch (InterruptedException | IOException e) {
        LOG.error("Caught exception when destroying clusters", e);
        // Do nothing as we want to ensure all clusters try to shutdown.
      }
    }
    // If there are any clusters remaining in createdClusters, then destroy
    // them too. This could be due to a test failing to return the cluster
    // for some reason
    MiniOzoneCluster[] remaining
        = createdClusters.toArray(new MiniOzoneCluster[0]);
    for (MiniOzoneCluster c : remaining) {
      try {
        destroy(c);
      } catch (InterruptedException | IOException e) {
        LOG.error("Caught exception when destroying remaining clusters", e);
      }
    }
    createdClusters.clear();
  }

  private List<Integer> getFreePortList(int size) {
    return org.apache.ratis.util.NetUtils.createLocalServerAddress(size)
        .stream()
        .map(inetSocketAddress -> inetSocketAddress.getPort())
        .collect(Collectors.toList());
  }
}
