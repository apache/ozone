package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * Test utility for creating a write client to the OM.
 */
public final class OmTestUtils {

  private static OzoneManagerProtocol writeClient;
  private static OzoneManager om;
  private static KeyManager keyManager;
  private static OMMetadataManager metadataManager;
  private static VolumeManager volumeManager;
  private static BucketManager bucketManager;
  private static PrefixManager prefixManager;

  private OmTestUtils() {
  }
  public static OzoneManager getTestOm() {
    return om;
  }

  public static OzoneManagerProtocol getWriteClient() {
    return writeClient;
  }

  public static BucketManager getBucketManager() {
    return bucketManager;
  }
  public static VolumeManager getVolumeManager() {
    return volumeManager;
  }
  public static PrefixManager getPrefixManager() {
    return prefixManager;
  }
  public static OMMetadataManager getMetadataManager() {
    return metadataManager;
  }
  public static KeyManager getKeyManager() {
    return keyManager;
  }


  public static void initOmWithTestClient(OzoneConfiguration conf,
      ScmBlockLocationProtocol blockClient,
      StorageContainerLocationProtocol containerClient)
      throws AuthenticationException, IOException {
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    DefaultMetricsSystem.setMiniClusterMode(true);
    OMStorage omStorage = new OMStorage(conf);
    omStorage.setClusterId("omtest");
    omStorage.setOmId("omtest");
    omStorage.initialize();
    OzoneManager.setTestSecureOmFlag(true);
    om = OzoneManager.createOm(conf,
        OzoneManager.StartupOption.REGUALR, containerClient, blockClient);

    keyManager = (KeyManagerImpl) HddsWhiteboxTestUtils
        .getInternalState(om, "keyManager");
    ScmClient scmClient = new ScmClient(blockClient, containerClient);
    HddsWhiteboxTestUtils.setInternalState(keyManager,
        "scmClient", scmClient);
    HddsWhiteboxTestUtils.setInternalState(keyManager,
        "secretManager", Mockito.mock(OzoneBlockTokenSecretManager.class));

    om.start();
    writeClient = OzoneClientFactory.getRpcClient(conf)
        .getObjectStore().getClientProxy().getOzoneManagerClient();
    metadataManager = (OmMetadataManagerImpl) HddsWhiteboxTestUtils
        .getInternalState(om, "metadataManager");
    volumeManager = (VolumeManagerImpl)HddsWhiteboxTestUtils
        .getInternalState(om, "volumeManager");
    bucketManager = (BucketManagerImpl)HddsWhiteboxTestUtils
        .getInternalState(om, "bucketManager");
    prefixManager = (PrefixManagerImpl)HddsWhiteboxTestUtils
        .getInternalState(om, "prefixManager");

  }

}
