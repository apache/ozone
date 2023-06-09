package org.apache.hadoop.ozone.container.common.utils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Tests {@link DiskCheckUtil} does not incorrectly identify an unhealthy
 * disk or mount point.
 * Tests that it identifies an improperly configured directory mount point.
 *
 */
public class TestDiskCheckUtil {
  @Rule
  public TemporaryFolder tempTestDir = new TemporaryFolder();

  private File testDir;

  @Before
  public void setup() {
    testDir = tempTestDir.getRoot();
  }

  @Test
  public void testPermissions() {
    // Ensure correct test setup before testing the disk check.
    Assert.assertTrue(testDir.canRead());
    Assert.assertTrue(testDir.canWrite());
    Assert.assertTrue(testDir.canExecute());
    Assert.assertTrue(DiskCheckUtil.checkPermissions(testDir));

    // Test failure without read permissiosns.
    Assert.assertTrue(testDir.setReadable(false));
    Assert.assertFalse(DiskCheckUtil.checkPermissions(testDir));
    Assert.assertTrue(testDir.setReadable(true));

    // Test failure without write permissiosns.
    Assert.assertTrue(testDir.setWritable(false));
    Assert.assertFalse(DiskCheckUtil.checkPermissions(testDir));
    Assert.assertTrue(testDir.setWritable(true));

    // Test failure without execute permissiosns.
    Assert.assertTrue(testDir.setExecutable(false));
    Assert.assertFalse(DiskCheckUtil.checkPermissions(testDir));
    Assert.assertTrue(testDir.setExecutable(true));
  }

  @Test
  public void testExistence() {
    // Ensure correct test setup before testing the disk check.
    Assert.assertTrue(testDir.exists());
    Assert.assertTrue(DiskCheckUtil.checkExistence(testDir));

    Assert.assertTrue(testDir.delete());
    Assert.assertFalse(DiskCheckUtil.checkExistence(testDir));
  }

  @Test
  public void testReadWrite() {
    Assert.assertTrue(DiskCheckUtil.checkReadWrite(testDir, testDir, 10));

    // Test file should have been deleted.
    File[] children = testDir.listFiles();
    Assert.assertNotNull(children);
    Assert.assertEquals(0, children.length);
  }

  // TODO this needs to be moved ot HddsVolume tests.
//  private class FailingDiskChecks implements DiskCheckUtil.DiskChecks {
//
//    @Override
//    public boolean checkExistence(File storageDir) {
//      return false;
//    }
//
//    @Override
//    public boolean checkPermissions(File storageDir) {
//      return false;
//    }
//
//    @Override
//    public boolean checkReadWrite(File storageDir, File testFileDir, int numBytesToWrite) {
//      return false;
//    }
//  }
}
