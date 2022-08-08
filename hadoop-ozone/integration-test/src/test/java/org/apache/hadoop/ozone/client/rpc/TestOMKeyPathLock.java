package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;

/**
 * This class is to test Ozone Manager KeyPathLock, multipleExecutors in
 * OzoneManagerStateMachine.
 */
@Timeout(300)
public class TestOMKeyPathLock extends TestOzoneRpcClientAbstract {
  private static OzoneConfiguration conf;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_OM_KEY_PATH_LOCK_ENABLED, true);

    conf.setBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS,
        OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    startCluster(conf);
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  @AfterAll
  public static void shutdown() throws IOException {
    shutdownCluster();
  }
}
