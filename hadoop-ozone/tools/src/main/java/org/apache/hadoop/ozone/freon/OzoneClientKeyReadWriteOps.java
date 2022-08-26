package org.apache.hadoop.ozone.freon;


import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@CommandLine.Command(name = "ockrw",
        aliases = "ozone-client-key-read-write-ops",
        description = "Read and write keys with the help of the ozone clients.",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true,
        showDefaultValues = true)
public class OzoneClientKeyReadWriteOps extends BaseFreonGenerator
        implements Callable<Void> {

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

  @CommandLine.Option(names = {"-r", "--read-thread-count"},
          description = "number of threads to execute read task.",
          defaultValue = "1")
  private int readThreadCount;

  @CommandLine.Option(names = {"-s", "--start-index-for-read"},
          description = "start-index for read operations.",
          defaultValue = "0")
  private int startIndexForRead;

  @CommandLine.Option(names = {"-c", "--count-for-read"},
          description = "Number of keys for read operations.",
          defaultValue = "0")
  private int cntForRead;

  @CommandLine.Option(names = {"-w", "--write-thread-count"},
          description = "number of threads to execute write task.",
          defaultValue = "1")
  private int writeThreadCount;

  @CommandLine.Option(names = {"-i", "--start-index-for-write"},
          description = "start-index for write operations.",
          defaultValue = "0")
  private int startIndexForWrite;

  @CommandLine.Option(names = {"-l", "--count-for-write"},
          description = "Number of keys for write operations.",
          defaultValue = "0")
  private int cntForWrite;

  @CommandLine.Option(names = {"-g", "--size"},
          description = "Generated data size (in bytes) of each key/file to be " +
                  "written.",
          defaultValue = "256")
  private int wSizeInBytes;

  @CommandLine.Option(names = {"-o", "--keySorted"},
          description = "Generated sorted key or not. The key name will be generated via md5 hash if choose to use unsorted key.",
          defaultValue = "false")
  private boolean keySorted;

  @CommandLine.Option(
          names = "--om-service-id",
          description = "OM Service ID"
  )
  private String omServiceID = null;

  private Timer timer;

  private OzoneClient rpcClient;

  private byte[] keyContent;
  private static final Logger LOG =
          LoggerFactory.getLogger(OzoneClientKeyReadWriteOps.class);


  private HashMap<Integer, String> intToMd5Hash = new HashMap<>();

  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();

    rpcClient = createOzoneClient(omServiceID, ozoneConfiguration);
    ensureVolumeAndBucketExist(rpcClient, volumeName, bucketName);
    timer = getMetrics().timer("key-read-write");

    if (wSizeInBytes >= 0) {
      keyContent = RandomUtils.nextBytes(wSizeInBytes);
    }

    for (int i = 0; i < 100000; i++){
      String encodedStr = DigestUtils.md5Hex(String.valueOf(i));
      intToMd5Hash.put(i, encodedStr.substring(0,7));
    }


    runTests(this::readWriteKeys);

    rpcClient.close();

    return null;
  }

  public void readWriteKeys(long counter) throws Exception{
    List<Future<Object>> readResults = timer.time(() -> {
      List<Future<Object>> results = null;

      if (cntForRead > 0) {
        results = asyncKeyReadOps(readThreadCount, startIndexForRead, cntForRead);
      }
      if (cntForWrite > 0) {
        asyncKeyWriteOps(writeThreadCount, startIndexForWrite, cntForWrite);
      }

      return results;
    });
    int readTotalBytes = 0;
    if (readResults != null && readResults.size() > 0) {
      for (Future<Object> readF:readResults) {
        readTotalBytes += Integer.parseInt((String)readF.get());
      }
      print("readTotalBytes: " + readTotalBytes);
    }
  }

  public List asyncKeyReadOps(int threadCount, int startIdx, int cnt) throws Exception{
    OzoneBucket ozbk = rpcClient.getObjectStore().getVolume(volumeName)
            .getBucket(bucketName);

    ExecutorService es = Executors.newFixedThreadPool(threadCount);
    List<Callable<Object>> todo = new ArrayList<>(cnt);

    for (int i = startIdx; i < startIdx + cnt; i++){
      String keyName;
      if (keySorted) {
        keyName = generateObjectName(i);
      } else {
        keyName = generateMd5ObjectName(i);
      }

      Callable readTask;
      if (readMetadataOnly) {
        readTask = () -> {
          ozbk.getKey(keyName);
          return 1;
        };
      }else {
        readTask = () -> {
          byte[] data = new byte[wSizeInBytes];
          OzoneInputStream introStream = ozbk.readKey(keyName);
          introStream.read(data);
          introStream.close();
          return 1;
        };
      }
      todo.add(readTask);
    }

    LOG.error("#### #### #### envoke read thread pool #### #### ###");
    List<Future<Object>> results = es.invokeAll(todo);
    LOG.error("#### #### #### done read thread #### #### ###");

    return results;
  }

  public String generateMd5ObjectName(int index) {
    String md5Hash = intToMd5Hash.get(index);
    return getPrefix() + "/" + md5Hash;
  }
  public void asyncKeyWriteOps(int threadCount, int startIdx, int cnt) throws Exception{
    OzoneBucket ozbk = rpcClient.getObjectStore().getVolume(volumeName)
            .getBucket(bucketName);

    ExecutorService es = Executors.newFixedThreadPool(threadCount);
    List<Callable<Object>> todo = new ArrayList<>(cnt);

    for (int i = startIdx; i < startIdx + cnt; i++){
      String keyName;
      if (keySorted) {
        keyName = generateObjectName(i);
      } else {
        keyName = generateMd5ObjectName(i);
      }

      Callable writeTask = () -> {
        LOG.error("#### #### #### write key: " + keyName + " ####### ###### ###### ");

        try {
          OzoneOutputStream out = ozbk.createKey(keyName, wSizeInBytes);
          LOG.error("#### #### #### write keyContent: " + keyContent + " ####### ###### ###### ");

          out.write(keyContent);
          LOG.error("#### #### #### flush:  ####### ###### ###### ");

          out.flush();
          out.close();

        }catch (Exception ex) {

        }
        return 1;
      };
      todo.add(writeTask);
    }
    LOG.error("#### #### #### envoke write thread pool #### #### ###");
    es.invokeAll(todo);
    LOG.error("#### #### #### done write thread #### #### ###");

  }

}
