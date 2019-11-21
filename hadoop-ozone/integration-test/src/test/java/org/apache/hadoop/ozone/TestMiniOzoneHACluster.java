package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;

/**
 * This class tests MiniOzoneHAClusterImpl.
 */
public class TestMiniOzoneHACluster {

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private int numOfOMs = 3;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public Timeout timeout = new Timeout(300_000);

  /**
   * Create a MiniOzoneHAClusterImpl for testing.
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "omServiceId1";
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS,
        OZONE_ADMINISTRATORS_WILDCARD);
    conf.setInt(OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS, 2);
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = OzoneClientFactory.getRpcClient(omServiceId, conf)
        .getObjectStore();
  }

  /**
   * Shutdown MiniOzoneHAClusterImpl.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetOMLeader() {
    // Start the HA cluster; check the leader OM
    OzoneManager ozoneManager = cluster.getOMLeader();
    Assert.assertNotNull(ozoneManager);
    Assert.assertTrue(ozoneManager.isLeader());
  }
}
