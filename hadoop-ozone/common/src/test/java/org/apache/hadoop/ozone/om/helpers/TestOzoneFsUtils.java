package org.apache.hadoop.ozone.om.helpers;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test OzoneFsUtils.
 */
public class TestOzoneFsUtils {

  @Test
  public void testPaths() {
    Assert.assertTrue(OzoneFSUtils.isValidName("/a/b"));
    Assert.assertFalse(OzoneFSUtils.isValidName("../../../a/b"));
    Assert.assertFalse(OzoneFSUtils.isValidName("/./."));
    Assert.assertFalse(OzoneFSUtils.isValidName("/:/"));
    Assert.assertFalse(OzoneFSUtils.isValidName("a/b"));
    Assert.assertFalse(OzoneFSUtils.isValidName("/a:/b"));
    Assert.assertFalse(OzoneFSUtils.isValidName("/a//b"));
  }
}
