package org.apache.hadoop.ozone.debug;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import picocli.CommandLine;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_LIST_CACHE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test cases for LeaseRecoverer.
 */
public class TestLeaseRecoverer {
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static String clusterId;
  private static String scmId;
  private static String omId;

  private static OzoneBucket legacyOzoneBucket;
  private static OzoneBucket fsoOzoneBucket;
  private static OzoneBucket legacyOzoneBucket2;
  private static OzoneBucket fsoOzoneBucket2;
  private static OzoneClient client;

  @Rule
  public Timeout timeout = new Timeout(1200000);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
        true);
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 3);
    conf.setInt(OZONE_CLIENT_LIST_CACHE_SIZE, 3);
    cluster = MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
        .setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    // create a volume and a LEGACY bucket
    legacyOzoneBucket = TestDataUtil
        .createVolumeAndBucket(client, BucketLayout.LEGACY);
    String volumeName = legacyOzoneBucket.getVolumeName();

    OzoneVolume ozoneVolume = client.getObjectStore().getVolume(volumeName);

    // create buckets
    BucketArgs omBucketArgs;
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    omBucketArgs = builder.build();

    String fsoBucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    ozoneVolume.createBucket(fsoBucketName, omBucketArgs);
    fsoOzoneBucket = ozoneVolume.getBucket(fsoBucketName);

    fsoBucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    ozoneVolume.createBucket(fsoBucketName, omBucketArgs);
    fsoOzoneBucket2 = ozoneVolume.getBucket(fsoBucketName);

    builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(BucketLayout.LEGACY);
    omBucketArgs = builder.build();
    String legacyBucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    ozoneVolume.createBucket(legacyBucketName, omBucketArgs);
    legacyOzoneBucket2 = ozoneVolume.getBucket(legacyBucketName);

    //initFSNameSpace();
  }

  @AfterClass
  public static void teardownClass() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testCLI() throws IOException {
    // open a file, write something and hsync
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    FileSystem fs = FileSystem.get(conf);
    final String dir = rootPath + fsoOzoneBucket.getVolumeName()
        + OZONE_URI_DELIMITER + fsoOzoneBucket.getName();
    final Path file = new Path(dir, "file");
    final int dataSize = 1024;
    final byte[] data = new byte[dataSize];
    ThreadLocalRandom.current().nextBytes(data);

    FSDataOutputStream os = fs.create(file, true);
    os.write(data);
    os.hsync();
    // call lease recovery cli
    String[] args = new String[] {
        "--path", file.toUri().toString()};
    StringWriter stdout = new StringWriter();
    PrintWriter pstdout = new PrintWriter(stdout);
    StringWriter stderr = new StringWriter();
    PrintWriter pstderr = new PrintWriter(stderr);

    CommandLine cmd = new CommandLine(new LeaseRecoverer())
        .setOut(pstdout)
        .setErr(pstderr);
    cmd.execute(args);

    assertEquals("", stderr.toString());

    // make sure file is visible and closed
    FileStatus fileStatus = fs.getFileStatus(file);
    assertEquals(dataSize, fileStatus.getLen());
    // make sure the writer can not write again.
    // TODO: write does not fail here. Looks like a bug.
    //assertThrows(IllegalArgumentException.class, () -> os.write(data));
    os.write(data);
    fileStatus = fs.getFileStatus(file);
    assertEquals(dataSize, fileStatus.getLen());
    // make sure hsync fails
    assertThrows(OMException.class, () -> os.hsync());
    fileStatus = fs.getFileStatus(file);
    assertEquals(dataSize, fileStatus.getLen());
    assertThrows(OMException.class, () -> os.close());
    fileStatus = fs.getFileStatus(file);
    assertEquals(dataSize, fileStatus.getLen());
  }
}
