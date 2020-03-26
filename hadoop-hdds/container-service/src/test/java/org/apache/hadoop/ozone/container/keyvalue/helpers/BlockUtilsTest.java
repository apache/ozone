package org.apache.hadoop.ozone.container.keyvalue.helpers;


import java.io.File;

import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.MetadataStore;
import org.apache.hadoop.hdds.utils.MetadataStoreBuilder;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.utils.ReferenceDB;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link BlockUtils}.
 */
public class BlockUtilsTest {
  private static String testRoot = new FileSystemTestHelper().getTestRootDir();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private void createContainerDB(OzoneConfiguration conf, File dbFile)
      throws Exception {
    MetadataStore store = MetadataStoreBuilder.newBuilder().setConf(conf)
        .setCreateIfMissing(true).setDbFile(dbFile).build();

    // we close since the SCM pre-creates containers.
    // we will open and put Db handle into a cache when keys are being created
    // in a container.

    store.close();
  }

  @Test
  public void testContainerCacheEviction() throws Exception {
    File root = new File(testRoot);
    root.mkdirs();

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE, 2);

    File containerDir1 = new File(root, "cont1");
    File containerDir2 = new File(root, "cont2");
    File containerDir3 = new File(root, "cont3");
    File containerDir4 = new File(root, "cont4");

    createContainerDB(conf, containerDir1);
    createContainerDB(conf, containerDir2);
    createContainerDB(conf, containerDir3);
    createContainerDB(conf, containerDir4);

    // Get 2 references out of the same db and verify the objects are same.
    ReferenceDB db1 = BlockUtils.cache.get(
        new BlockUtils.DbHandlerCacheKey(1,
            containerDir1.getPath(), "RocksDB"));
    ReferenceDB db2 = BlockUtils.cache.get(new BlockUtils.DbHandlerCacheKey(1,
          containerDir1.getPath(), "RocksDB"));
    Assert.assertEquals(db1, db2);

    // add one more references to ContainerCache.
    ReferenceDB db3 = BlockUtils.cache.get(new BlockUtils.DbHandlerCacheKey(2,
        containerDir2.getPath(), "RocksDB"));

    // and close the reference
    db3.close();

    // add one more reference to ContainerCache and verify that it will not
    // evict the least recent entry as it has reference.
    ReferenceDB db4 = BlockUtils.cache.get(new BlockUtils.DbHandlerCacheKey(3,
        containerDir3.getPath(), "RocksDB"));

    Assert.assertEquals(2, BlockUtils.cache.size());

    // Now close both the references for container1
    db1.close();
    db2.close();

    // The reference count for container1 is 0 but it is not evicted.
    ReferenceDB db5 = BlockUtils.cache.get(new BlockUtils.DbHandlerCacheKey(1,
        containerDir1.getPath(), "RocksDB"));
    Assert.assertEquals(db1, db5);
    db5.close();
    db4.close();

    // Decrementing reference count below zero should fail.
    thrown.expect(IllegalArgumentException.class);
    db5.close();
  }
}