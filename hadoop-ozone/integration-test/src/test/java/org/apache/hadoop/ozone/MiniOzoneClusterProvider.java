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
 * Class to create mini-clusters in the background.
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
