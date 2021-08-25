package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;

import java.io.IOException;
import java.util.List;
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

  private int preCreatedLimit = 1;
  private boolean shutdown = false;

  private final OzoneConfiguration conf;
  private final MiniOzoneCluster.Builder builder;
  private Thread createThread;
  private Thread reapThread;

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
    return clusters.poll(100, SECONDS);
  }

  public synchronized void destroy(MiniOzoneCluster c)
      throws InterruptedException, IOException {
    ensureNotShutdown();
    expiredClusters.put(c);
  }

  public synchronized void shutdown() throws InterruptedException {
    createThread.interrupt();
    createThread.join();
    destroyRemainingClusters();
    reapThread.interrupt();
    reapThread.join();
    shutdown = true;
  }

  private void ensureNotShutdown() throws IOException {
    if (shutdown) {
      throw new IOException("The mini-cluster provider is shutdown");
    }
  }

  private Thread reapClusters() {
    Thread t = new Thread(() -> {
      boolean shouldRun = true;
      while(shouldRun || !expiredClusters.isEmpty()) {
        try {
          if (Thread.interrupted()) {
            throw new InterruptedException();
          }
          MiniOzoneCluster c = expiredClusters.take();
          c.shutdown();
        } catch (InterruptedException e) {
          shouldRun = false;
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

  private void destroyRemainingClusters() throws InterruptedException {
    while(!clusters.isEmpty()) {
      try {
        expiredClusters.add(clusters.poll(100, TimeUnit.MILLISECONDS));
      } catch (InterruptedException e) {
        // Do nothing
      }
    }
  }

  private List<Integer> getFreePortList(int size) {
    return org.apache.ratis.util.NetUtils.createLocalServerAddress(size)
        .stream()
        .map(inetSocketAddress -> inetSocketAddress.getPort())
        .collect(Collectors.toList());
  }
}
