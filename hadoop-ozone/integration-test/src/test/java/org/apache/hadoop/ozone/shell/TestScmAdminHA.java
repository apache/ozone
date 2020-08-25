package org.apache.hadoop.ozone.shell;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.UUID;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.apache.hadoop.ozone.admin.scm.ScmAdmin;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import picocli.CommandLine;
import picocli.CommandLine.RunLast;

public class TestScmAdminHA {
  private static OzoneAdmin ozoneAdmin;
  private static OzoneConfiguration conf;
  private static String omServiceId;
  private static int numOfOMs;
  private static String clusterId;
  private static String scmId;
  private static MiniOzoneCluster cluster;

  @BeforeClass
  public static void init() throws Exception {
    ozoneAdmin = new OzoneAdmin();
    conf = new OzoneConfiguration();

    // Init HA cluster
    omServiceId = "om-service-test1";
    numOfOMs = 3;
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .build();
    conf.setQuietMode(false);
    // enable ratis for Scm.
    conf.setBoolean(ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY, true);
    cluster.waitForClusterToBeReady();
  }

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetLeaderStatus() {
    InetSocketAddress address = cluster.getStorageContainerManager().getClientRpcAddress();
    String hostPort = address.getHostName() + ":" + address.getPort();
    String[] args = {"--scm", hostPort, "scmha", "listratisstatus"};
    ozoneAdmin.execute(args);
  }
}
