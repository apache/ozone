package org.apache.hadoop.ozone.util;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Class tests OzoneUtils.
 */
public class TestOzoneUtils {

  @Test
  public void testCheckExternalAuthorizer() {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    Assert.assertFalse(OzoneUtils.checkExternalAuthorizer(ozoneConfiguration));

    ozoneConfiguration.set(OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS,
        "RangerAuthorizer");
    Assert.assertTrue(OzoneUtils.checkExternalAuthorizer(ozoneConfiguration));
  }
}
