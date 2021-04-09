package org.apache.hadoop.hdds.scm.upgrade;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.UUID;

public class TestSCMHAUnfinalized {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private StorageContainerManager scm;

  @Before
  public void setup() {
  }

  @Test
  public void testSCMHAConfigsUsedUnfinalized() throws Exception {
    // Build unfinalized SCM with HA configuration enabled.
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    conf.setInt(ScmConfig.ConfigStrings.HDDS_SCM_INIT_DEFAULT_LAYOUT_VERSION,
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        temporaryFolder.newFolder().getAbsolutePath());

    StorageContainerManager.scmInit(conf, UUID.randomUUID().toString());
    scm = new StorageContainerManager(conf);

    LambdaTestUtils.intercept(UpgradeException.class, scm::start);
  }
}
