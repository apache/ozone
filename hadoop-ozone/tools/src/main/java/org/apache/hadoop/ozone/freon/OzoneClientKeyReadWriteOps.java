package org.apache.hadoop.ozone.freon;


import com.codahale.metrics.Timer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import picocli.CommandLine;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
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

  @CommandLine.Option(names = {"-r", "--read-thread-count"},
          description = "number of threads to execute read task.",
          defaultValue = "1")
  private int readThreadCount;

  @CommandLine.Option(names = {"-s", "--start-index-for-read"},
          description = "start-index for read operations.",
          defaultValue = "0")
  private int startIndexForRead;

  @CommandLine.Option(names = {"-c", "--cnt-for-read"},
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

  @CommandLine.Option(names = {"-l", "--cnt-for-write"},
          description = "Number of keys for write operations.",
          defaultValue = "0")
  private int cntForWrite;

  @CommandLine.Option(names = {"-g", "--size"},
          description = "Generated data size (in bytes) of each key/file to be " +
                  "written.",
          defaultValue = "256")
  private int wSizeInBytes;

  @CommandLine.Option(
          names = "--om-service-id",
          description = "OM Service ID"
  )
  private String omServiceID = null;

  private Timer timer;

  private OzoneClient rpcClient;

  private byte[] keyContent;

  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();

    rpcClient = createOzoneClient(omServiceID, ozoneConfiguration);

    timer = getMetrics().timer("key-read-write");

    if (wSizeInBytes > 0) {
      keyContent = RandomUtils.nextBytes(wSizeInBytes);
    }

    runTests(this::readWriteKeys);

    rpcClient.close();

    return null;
  }

  public void readWriteKeys(long counter) throws Exception{
    List<Future<Object>> readResults = null;
    if (cntForRead > 0) {
      readResults = asyncKeyReadOps(readThreadCount, startIndexForRead, cntForRead);
    }
    if (cntForWrite > 0) {
      asyncKeyWriteOps(writeThreadCount, startIndexForWrite, cntForWrite);
    }

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
      String keyName =  generateObjectName(i);
      Callable readTask = () -> {
        InputStream in = ozbk.readKey(keyName);
        return IOUtils.toByteArray(in).length;
      };
      todo.add(readTask);
    }
    List<Future<Object>> results = es.invokeAll(todo);
    return results;
  }

  public void asyncKeyWriteOps(int threadCount, int startIdx, int cnt) throws Exception{
    OzoneBucket ozbk = rpcClient.getObjectStore().getVolume(volumeName)
            .getBucket(bucketName);

    ExecutorService es = Executors.newFixedThreadPool(threadCount);
    List<Callable<Object>> todo = new ArrayList<>(cnt);

    for (int i = startIdx; i < startIdx + cnt; i++){
      String keyName =  generateObjectName(i);
      Callable writeTask = () -> {
        OzoneOutputStream out = ozbk.createKey(keyName, wSizeInBytes);
        out.write(keyContent);
        return 1;
      };
      todo.add(writeTask);
    }
    es.invokeAll(todo);
  }

}
