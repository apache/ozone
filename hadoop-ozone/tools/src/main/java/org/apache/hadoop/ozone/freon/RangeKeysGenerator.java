package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.concurrent.Callable;
import java.util.function.Function;

import static org.apache.hadoop.ozone.freon.KeyGeneratorUtil.PURE_INDEX;
import static org.apache.hadoop.ozone.freon.KeyGeneratorUtil.MD5;
import static org.apache.hadoop.ozone.freon.KeyGeneratorUtil.FILE_DIR_SEPARATOR;

/**
 * Ozone range keys generator for performance test.
 */
public class RangeKeysGenerator extends BaseFreonGenerator
        implements Callable<Void> {

  private static final Logger LOG =
          LoggerFactory.getLogger(RangeKeysGenerator.class);

  @CommandLine.Option(names = {"-v", "--volume"},
          description = "Name of the volume which contains the test data. " +
                  "Will be created if missing.",
          defaultValue = "vol1")
  private String volumeName;

  @CommandLine.Option(names = {"-b", "--bucket"},
          description = "Name of the bucket which contains the test data.",
          defaultValue = "bucket1")
  private String bucketName;

  @CommandLine.Option(names = {"-r", "--range-each-client-write"},
          description = "Write range for each client.",
          defaultValue = "0")
  private int range;

  @CommandLine.Option(names = {"-k", "--key-encode"},
          description = "The algorithm to generate key names. " +
                  "Options are pureIndex, md5, simpleHash",
          defaultValue = "simpleHash")
  private String encodeFormat;

  @CommandLine.Option(names = {"-g", "--size"},
          description = "Generated data size (in bytes) of " +
                  "each key/file to be " +
                  "written.",
          defaultValue = "256")
  private int writeSizeInBytes;

  @CommandLine.Option(names = {"--clients"},
          description =
                  "Number of clients, defaults 1.",
          defaultValue = "1")
  private int clientsCount = 1;

  @CommandLine.Option(
          names = "--om-service-id",
          description = "OM Service ID"
  )
  private String omServiceID = null;
  private KeyGeneratorUtil kg;
  private OzoneClient[] rpcClients;
  private byte[] keyContent;
  private Timer timer;


  @Override
  public Void call() throws Exception {
    init();
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    rpcClients = new OzoneClient[clientsCount];
    for (int i = 0; i < clientsCount; i++) {
      rpcClients[i] = createOzoneClient(omServiceID, ozoneConfiguration);
    }

    ensureVolumeAndBucketExist(rpcClients[0], volumeName, bucketName);
    if (writeSizeInBytes >= 0) {
      keyContent = RandomUtils.nextBytes(writeSizeInBytes);
    }
    timer = getMetrics().timer("key-read-write");

    kg = new KeyGeneratorUtil();
    runTests(this::generateRangeKeys);
    for (int i = 0; i < clientsCount; i++) {
      if (rpcClients[i] != null) {
        rpcClients[i].close();
      }
    }

    return null;
  }

  public void generateRangeKeys(long count) throws Exception {
    int clientIndex = (int)(count % clientsCount);
    OzoneClient client = rpcClients[clientIndex];
    int startIndex = (int)count * range;
    int endIndex = startIndex + range;

    timer.time(() -> {
      switch (encodeFormat) {
      case PURE_INDEX:
        loopRunner(kg.pureIndexKeyNameFunc(), client, startIndex, endIndex);
        break;
      case MD5:
        loopRunner(kg.md5KeyNameFunc(), client, startIndex, endIndex);
        break;
      default:
        loopRunner(kg.simpleHashKeyNameFunc(), client, startIndex, endIndex);
        break;
      }
      return null;
    });
  }


  public void loopRunner(Function<Integer, String> f, OzoneClient client,
                         int startIndex, int endIndex) throws Exception {
    OzoneBucket ozbk = client.getObjectStore().getVolume(volumeName)
            .getBucket(bucketName);

    String keyName;
    for (int i = startIndex; i < endIndex + 1; i++) {
      keyName = getPrefix() + FILE_DIR_SEPARATOR + f.apply(i);
      try (OzoneOutputStream out = ozbk.createKey(keyName, writeSizeInBytes)) {
        out.write(keyContent);
        out.flush();
      }
    }
  }
}
