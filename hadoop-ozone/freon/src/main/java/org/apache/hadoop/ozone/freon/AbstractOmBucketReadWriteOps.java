/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

/**
 * Abstract class for OmBucketReadWriteFileOps/KeyOps Freon class
 * implementations.
 */
public abstract class AbstractOmBucketReadWriteOps extends BaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractOmBucketReadWriteOps.class);

  @Option(names = {"-g", "--size"},
      description = "Generated data size of each key/file to be written. " +
          StorageSizeConverter.STORAGE_SIZE_DESCRIPTION,
      defaultValue = "256B",
      converter = StorageSizeConverter.class)
  private StorageSize size;

  @Option(names = {"--buffer"},
      description = "Size of buffer used for generating the key/file content.",
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

  private Timer timer;
  private ContentGenerator contentGenerator;
  private int readThreadCount;
  private int writeThreadCount;

  protected abstract void display();

  protected abstract void initialize(OzoneConfiguration configuration)
      throws Exception;

  @Override
  public Void call() throws Exception {
    init();

    readThreadCount = (readThreadPercentage * totalThreadCount) / 100;
    writeThreadCount = totalThreadCount - readThreadCount;

    display();
    print("SizeInBytes: " + size.toBytes());
    print("bufferSize: " + bufferSize);
    print("totalThreadCount: " + totalThreadCount);
    print("readThreadPercentage: " + readThreadPercentage);
    print("writeThreadPercentage: " + (100 - readThreadPercentage));
    print("readThreadCount: " + readThreadCount);
    print("writeThreadCount: " + writeThreadCount);
    print("numOfReadOperations: " + numOfReadOperations);
    print("numOfWriteOperations: " + numOfWriteOperations);

    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    contentGenerator = new ContentGenerator(size.toBytes(), bufferSize);
    timer = getMetrics().timer("om-bucket-read-write-ops");

    initialize(ozoneConfiguration);

    return null;
  }

  protected abstract String createPath(String path) throws IOException;

  protected int readOperations(int keyCountForRead) throws Exception {

    // Create keyCountForRead/fileCountForRead (defaultValue = 1000) keys/files
    // under rootPath/readPath
    String readPath = createPath("readPath");
    create(readPath, keyCountForRead);

    // Start readThreadCount (defaultValue = 90) concurrent read threads
    // performing numOfReadOperations (defaultValue = 50) iterations
    // of read operations (bucket.listKeys(readPath) or
    // fileSystem.listStatus(rootPath/readPath))
    ExecutorService readService = Executors.newFixedThreadPool(readThreadCount);
    CompletionService<Integer> readExecutorCompletionService =
        new ExecutorCompletionService<>(readService);
    List<Future<Integer>> readFutures = new ArrayList<>();
    for (int i = 0; i < readThreadCount; i++) {
      readFutures.add(readExecutorCompletionService.submit(() -> {
        int readCount = 0;
        try {
          for (int j = 0; j < numOfReadOperations; j++) {
            readCount = getReadCount(readCount, "readPath");
          }
        } catch (IOException e) {
          LOG.warn("Exception while listing keys/files ", e);
        }
        return readCount;
      }));
    }

    int readResult = 0;
    int readFuturesCount = readFutures.size();

    for (int i = 0; i < readFuturesCount; i++) {
      readResult += readExecutorCompletionService.take().get();
    }

    readService.shutdown();

    return readResult;
  }

  protected abstract int getReadCount(int readCount, String readPath)
      throws IOException;

  protected int writeOperations(int keyCountForWrite) throws Exception {

    // Start writeThreadCount (defaultValue = 10) concurrent write threads
    // performing numOfWriteOperations (defaultValue = 10) iterations
    // of write operations (createKeys(writePath) or
    // createFiles(rootPath/writePath))
    String writePath = createPath("writePath");

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
            create(writePath, keyCountForWrite);
            writeCount++;
          }
        } catch (IOException e) {
          LOG.warn("Exception while creating keys/files ", e);
        }
        return writeCount;
      }));
    }

    int writeResult = 0;
    int writeFuturesCount = writeFutures.size();

    for (int i = 0; i < writeFuturesCount; i++) {
      writeResult += writeExecutorCompletionService.take().get();
    }

    writeService.shutdown();

    return writeResult;
  }

  protected void create(String path, int keyCount)
      throws Exception {
    for (int i = 0; i < keyCount; i++) {
      String keyName = path.concat(OzoneConsts.OM_KEY_PREFIX)
          .concat(RandomStringUtils.secure().nextAlphanumeric(length));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Key/FileName : {}", keyName);
      }
      timer.time(() -> {
        try (OutputStream stream = create(keyName)) {
          contentGenerator.write(stream);
          stream.flush();
        }
        return null;
      });
    }
  }

  protected abstract OutputStream create(String pathName) throws IOException;

  protected long getSizeInBytes() {
    return size.toBytes();
  }
}
