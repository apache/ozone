/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;

/**
 * Synthetic read/write key operations workload generator tool.
 */
@Command(name = "obrwk",
    aliases = "om-bucket-read-write-key-ops",
    description = "Creates keys, performs respective read/write " +
        "operations to measure lock performance.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)

public class OmBucketReadWriteKeyOps extends AbstractOmBucketReadWriteOps {

  private static final Logger LOG =
      LoggerFactory.getLogger(OmBucketReadWriteKeyOps.class);

  @Option(names = {"-v", "--volume"},
      description = "Name of the volume which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "vol1")
  private String volumeName;

  @Option(names = {"-b", "--bucket"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "bucket1")
  private String bucketName;

  @Option(names = {"-k", "--key-count-for-read"},
      description = "Number of keys to be created for read operations.",
      defaultValue = "100")
  private int keyCountForRead;

  @Option(names = {"-w", "--key-count-for-write"},
      description = "Number of keys to be created for write operations.",
      defaultValue = "10")
  private int keyCountForWrite;

  @Option(names = {"-g", "--key-size"},
      description = "Size of the generated key (in bytes).",
      defaultValue = "256")
  private long keySizeInBytes;

  @Option(names = {"-B", "--buffer"},
      description = "Size of buffer used to generated the key content.",
      defaultValue = "64")
  private int bufferSize;

  @Option(names = {"-l", "--name-len"},
      description = "Length of the random name of path you want to create.",
      defaultValue = "10")
  private int length;

  @Option(names = {"-c", "--total-thread-count"},
      description = "Total number of threads to be executed.",
      defaultValue = "100")
  private int totalThreadCount;

  @Option(names = {"-T", "--read-thread-percentage"},
      description = "Percentage of the total number of threads to be " +
          "allocated for read operations. The remaining percentage of " +
          "threads will be allocated for write operations.",
      defaultValue = "90")
  private int readThreadPercentage;

  @Option(names = {"-R", "--num-of-read-operations"},
      description = "Number of read operations to be performed by each thread.",
      defaultValue = "50")
  private int numOfReadOperations;

  @Option(names = {"-W", "--num-of-write-operations"},
      description = "Number of write operations to be performed by each " +
          "thread.",
      defaultValue = "10")
  private int numOfWriteOperations;

  @Option(names = {"-o", "--om-service-id"},
      description = "OM Service ID"
  )
  private String omServiceID = null;

  @CommandLine.Mixin
  private FreonReplicationOptions replication;

  private Timer timer;

  private ContentGenerator contentGenerator;

  private Map<String, String> metadata;

  private ReplicationConfig replicationConfig;

  private OzoneBucket bucket;

  private int readThreadCount;
  private int writeThreadCount;

  @Override
  public Void call() throws Exception {
    init();

    readThreadCount = (readThreadPercentage * totalThreadCount) / 100;
    writeThreadCount = totalThreadCount - readThreadCount;

    print("volumeName: " + volumeName);
    print("bucketName: " + bucketName);
    print("keyCountForRead: " + keyCountForRead);
    print("keyCountForWrite: " + keyCountForWrite);
    print("keySizeInBytes: " + keySizeInBytes);
    print("bufferSize: " + bufferSize);
    print("totalThreadCount: " + totalThreadCount);
    print("readThreadPercentage: " + readThreadPercentage);
    print("writeThreadPercentage: " + (100 - readThreadPercentage));
    print("readThreadCount: " + readThreadCount);
    print("writeThreadCount: " + writeThreadCount);
    print("numOfReadOperations: " + numOfReadOperations);
    print("numOfWriteOperations: " + numOfWriteOperations);
    print("omServiceID: " + omServiceID);

    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    replicationConfig = replication.fromParamsOrConfig(ozoneConfiguration);

    contentGenerator = new ContentGenerator(keySizeInBytes, bufferSize);
    metadata = new HashMap<>();

    try (OzoneClient rpcClient = createOzoneClient(omServiceID,
        ozoneConfiguration)) {
      ensureVolumeAndBucketExist(rpcClient, volumeName, bucketName);
      bucket = rpcClient.getObjectStore().getVolume(volumeName)
          .getBucket(bucketName);

      timer = getMetrics().timer("key-create");

      runTests(this::mainMethod);
    }

    return null;
  }

  private void mainMethod(long counter) throws Exception {

    int readResult = readOperations();
    int writeResult = writeOperations();

    print("Total Keys Read: " + readResult);
    print("Total Keys Written: " + writeResult * keyCountForWrite);

    // TODO: print read/write lock metrics (HDDS-6435, HDDS-6436).
  }

  @Override
  public int readOperations() throws Exception {

    // Create keyCountForRead (defaultValue = 100) keys under
    // rootPath/readPath
    String readPath = "".concat(OzoneConsts.OM_KEY_PREFIX).concat("readPath");
    createKeys(readPath, keyCountForRead);

    // Start readThreadCount (defaultValue = 90) concurrent read threads
    // performing numOfReadOperations (defaultValue = 50) iterations
    // of read operations (bucket.listKeys(readPath))
    ExecutorService readService = Executors.newFixedThreadPool(readThreadCount);
    CompletionService<Integer> readExecutorCompletionService =
        new ExecutorCompletionService<>(readService);
    List<Future<Integer>> readFutures = new ArrayList<>();
    for (int i = 0; i < readThreadCount; i++) {
      readFutures.add(readExecutorCompletionService.submit(() -> {
        int readCount = 0;
        try {
          for (int j = 0; j < numOfReadOperations; j++) {
            Iterator<? extends OzoneKey> ozoneKeyIterator =
                bucket.listKeys("/readPath/");
            while (ozoneKeyIterator.hasNext()) {
              ozoneKeyIterator.next();
              ++readCount;
            }
          }
        } catch (IOException e) {
          LOG.warn("Exception while listing keys ", e);
        }
        return readCount;
      }));
    }

    int readResult = 0;
    for (int i = 0; i < readFutures.size(); i++) {
      try {
        readResult += readExecutorCompletionService.take().get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }
    readService.shutdown();

    return readResult;
  }

  @Override
  public int writeOperations() throws Exception {

    // Start writeThreadCount (defaultValue = 10) concurrent write threads
    // performing numOfWriteOperations (defaultValue = 10) iterations
    // of write operations (createKeys(writePath))
    String writePath = "".concat(OzoneConsts.OM_KEY_PREFIX).concat("writePath");

    ExecutorService writeService =
        Executors.newFixedThreadPool(writeThreadCount);
    CompletionService<Integer> writeExecutorCompletionService =
        new ExecutorCompletionService<>(writeService);
    List<Future<Integer>> writeFutures = new ArrayList<>();
    for (int i = 0; i < writeThreadCount; i++) {
      writeFutures.add(writeExecutorCompletionService.submit(() -> {
        int writeCount = 0;
        try {
          for (int j = 0; j < numOfWriteOperations; j++) {
            createKeys(writePath, keyCountForWrite);
            writeCount++;
          }
        } catch (IOException e) {
          LOG.warn("Exception while creating key ", e);
        }
        return writeCount;
      }));
    }

    int writeResult = 0;
    for (int i = 0; i < writeFutures.size(); i++) {
      try {
        writeResult += writeExecutorCompletionService.take().get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }
    writeService.shutdown();

    return writeResult;
  }

  @Override
  public void create(String path) throws Exception {
    String keyName = path.concat(OzoneConsts.OM_KEY_PREFIX)
        .concat(RandomStringUtils.randomAlphanumeric(length));

    timer.time(() -> {
      try (OutputStream stream = bucket.createKey(keyName, keySizeInBytes,
          replicationConfig, metadata)) {
        contentGenerator.write(stream);
        stream.flush();
      }
      return null;
    });
  }

  private void createKeys(String path, int keyCount) throws Exception {
    for (int i = 0; i < keyCount; i++) {
      create(path);
    }
  }
}
