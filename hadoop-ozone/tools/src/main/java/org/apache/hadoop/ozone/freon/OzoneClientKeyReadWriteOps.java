package org.apache.hadoop.ozone.freon;


import com.codahale.metrics.Timer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.*;
import java.util.concurrent.*;

@CommandLine.Command(name = "ockrw",
        aliases = "ozone-client-key-read-write-ops",
        description = "Read and write keys with the help of the ozone clients.",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true,
        showDefaultValues = true)
public class OzoneClientKeyReadWriteOps extends BaseFreonGenerator
        implements Callable<Void> {

  @CommandLine.Option(names = {"--time"},
          description = "Total time (in minutes) of test.",
          defaultValue = "5")
  private String time;

  @CommandLine.Option(names = {"-v", "--volume"},
          description = "Name of the volume which contains the test data. Will be"
                  + " created if missing.",
          defaultValue = "vol1")
  private String volumeName;

  @CommandLine.Option(names = {"-b", "--bucket"},
          description = "Name of the bucket which contains the test data.",
          defaultValue = "bucket1")
  private String bucketName;

  @CommandLine.Option(names = {"-m", "--read-metadata-only"},
          description = "If only read key's metadata. Supported values are Y, F.",
          defaultValue = "false")
  private boolean readMetadataOnly;

  @CommandLine.Option(names = {"-s", "--start-index-for-read"},
          description = "start-index for read operations.",
          defaultValue = "0")
  private int startIndexForRead;

  @CommandLine.Option(names = {"-e", "--end-index-for-read"},
          description = "end-index for read operations.",
          defaultValue = "0")
  private int endIndexForRead;

  @CommandLine.Option(names = {"-i", "--start-index-for-write"},
          description = "start-index for write operations.",
          defaultValue = "0")
  private int startIndexForWrite;

  @CommandLine.Option(names = {"-x", "--end-index-for-write"},
          description = "end-index for write operations.",
          defaultValue = "0")
  private int endIndexForWrite;

  @CommandLine.Option(names = {"-g", "--size"},
          description = "Generated data size (in bytes) of each key/file to be " +
                  "written.",
          defaultValue = "256")
  private int wSizeInBytes;

  @CommandLine.Option(names = {"--write-range-keys"},
          description = "Generate the range of keys based on option start-index-for-write and end-index-for-write.",
          defaultValue = "false")
  private boolean writeRangeKeys;

  @CommandLine.Option(names = {"--keySorted"},
          description = "Generated sorted key or not. The key name will be generated via md5 hash if choose to use unsorted key.",
          defaultValue = "false")
  private boolean keySorted;

  @CommandLine.Option(names = {"--mix-workload"},
          description = "Set to True if you would like to generate mix workload (Read and Write).",
          defaultValue = "false")
  private boolean ifMixWorkload;

  @CommandLine.Option(names = {"--percentage-read"},
          description = "Percentage of read tasks in mix workload.",
          defaultValue = "0")
  private int percentageRead;


  @CommandLine.Option(
          names = "--om-service-id",
          description = "OM Service ID"
  )
  private String omServiceID = null;

  private Timer timer;

  private OzoneClient rpcClient;
  private OzoneBucket ozbk;

  private byte[] keyContent;
  private String keyName;

  private static final Logger LOG =
          LoggerFactory.getLogger(OzoneClientKeyReadWriteOps.class);

  private final String READ_TASK = "READ_TASK";
  private final String WRITE_TASK = "WRITE_TASK";


  @Override
  public Void call() throws Exception {
    init();
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    rpcClient = createOzoneClient(omServiceID, ozoneConfiguration);
    ozbk = rpcClient.getObjectStore().getVolume(volumeName)
            .getBucket(bucketName);

    ensureVolumeAndBucketExist(rpcClient, volumeName, bucketName);
    timer = getMetrics().timer("key-read-write");
    if (wSizeInBytes >= 0) {
      keyContent = RandomUtils.nextBytes(wSizeInBytes);
    }
//    pre-generated integer to md5 hash value mapping
//    for (int i = 0; i < 100000; i++){
//      String encodedStr = DigestUtils.md5Hex(String.valueOf(i));
//      intToMd5Hash.put(i, encodedStr.substring(0,7));
//    }
    int startIdx = 0, endIdx = 0;
    switch (decideReadOrWriteTask()){
      case READ_TASK:
        startIdx = startIndexForRead;
        endIdx = endIndexForRead;
        break;
      case WRITE_TASK:
        startIdx = startIndexForWrite;
        endIdx = endIndexForWrite;
        break;
    }

    Random r = new Random();
    int randomIdxWithinRange = r.nextInt(endIdx + 1 - startIdx) + startIdx;

    if (keySorted) {
      keyName = generateObjectName(randomIdxWithinRange);
    } else {
      keyName = generateMd5ObjectName(randomIdxWithinRange);
    }
    runTests(this::readWriteKeys);
    rpcClient.close();
    return null;
  }

  public void readWriteKeys(long counter) throws Exception{
    List<Future<Object>> readWriteResults = timer.time(() -> {
      List<Future<Object>> readResults = null;
      if (!ifMixWorkload){
        if (endIndexForRead - startIndexForRead > 0) {
          processReadTasks();
        }
        if (endIndexForWrite - startIndexForWrite > 0) {
          processWriteTasks();
        }

      }else{
        switch (decideReadOrWriteTask()){
          case READ_TASK:
            processReadTasks();
            break;
          case WRITE_TASK:
            processWriteTasks();
            break;
        }
      }
      return readResults;
    });
  }

  public void processReadTasks() throws Exception{
      if (readMetadataOnly) {
          ozbk.getKey(keyName);
      }else {
          byte[] data = new byte[wSizeInBytes];
          OzoneInputStream introStream = ozbk.readKey(keyName);
          introStream.read(data);
          introStream.close();
      }
  }

  public String generateMd5ObjectName(int number) {
    String encodedStr = DigestUtils.md5Hex(String.valueOf(number));
    String md5Hash = encodedStr.substring(0,7);
    return getPrefix() + "/" + md5Hash;
  }
  public void processWriteTasks() throws Exception{
    if (writeRangeKeys) {
      for (int i = startIndexForWrite; i < endIndexForWrite + 1; i++){
        createKeyAndWrite(generateKeyName(i));
      }
    } else {
      createKeyAndWrite(keyName);
    }
  }

  public void createKeyAndWrite(String keyName) throws Exception{
    OzoneOutputStream out = ozbk.createKey(keyName, wSizeInBytes);
    out.write(keyContent);
    out.flush();
    out.close();
  }

  public String generateKeyName(int number) {
    String keyName;
    if (keySorted) {
      keyName = generateObjectName(number);
    } else {
      keyName = generateMd5ObjectName(number);
    }
    return keyName;
  }

  public String decideReadOrWriteTask( ) {
    if (!ifMixWorkload){
      if (endIndexForRead - startIndexForRead > 0) {
        return READ_TASK;
      }else if ((endIndexForWrite - startIndexForWrite) > 0) {
        return WRITE_TASK;
      }
    }
    //mix workload
    Random r = new Random();
    int tmp = r.nextInt(100) + 1; // 1 ~ 100
    if (tmp < percentageRead) {
      return READ_TASK;
    }else{
      return WRITE_TASK;
    }
  }

}
