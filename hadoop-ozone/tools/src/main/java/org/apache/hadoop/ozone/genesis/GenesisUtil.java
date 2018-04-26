package org.apache.hadoop.ozone.genesis;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdsl.protocol.DatanodeDetails;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Random;

/**
 * Utility class for benchmark test cases.
 */
public class GenesisUtil {

  private GenesisUtil() {
    // private constructor.
  }

  public static final String DEFAULT_TYPE = "default";
  public static final String CACHE_10MB_TYPE = "Cache10MB";
  public static final String CACHE_1GB_TYPE = "Cache1GB";
  public static final String CLOSED_TYPE = "ClosedContainer";

  private static final int DB_FILE_LEN = 7;
  private static final String TMP_DIR = "java.io.tmpdir";


  public static MetadataStore getMetadataStore(String dbType) throws IOException {
    Configuration conf = new Configuration();
    MetadataStoreBuilder builder = MetadataStoreBuilder.newBuilder();
    builder.setConf(conf);
    builder.setCreateIfMissing(true);
    builder.setDbFile(
        Paths.get(System.getProperty(TMP_DIR))
            .resolve(RandomStringUtils.randomNumeric(DB_FILE_LEN))
            .toFile());
    switch (dbType) {
    case DEFAULT_TYPE:
      break;
    case CLOSED_TYPE:
      break;
    case CACHE_10MB_TYPE:
      builder.setCacheSize((long) StorageUnit.MB.toBytes(10));
      break;
    case CACHE_1GB_TYPE:
      builder.setCacheSize((long) StorageUnit.GB.toBytes(1));
      break;
    default:
      throw new IllegalStateException("Unknown type: " + dbType);
    }
    return builder.build();
  }

  public static DatanodeDetails createDatanodeDetails(String uuid) {
    Random random = new Random();
    String ipAddress =
        random.nextInt(256) + "." + random.nextInt(256) + "." + random
            .nextInt(256) + "." + random.nextInt(256);

    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(uuid)
        .setHostName("localhost")
        .setIpAddress(ipAddress)
        .setInfoPort(0)
        .setInfoSecurePort(0)
        .setContainerPort(0)
        .setRatisPort(0)
        .setOzoneRestPort(0);
    return builder.build();
  }
}
