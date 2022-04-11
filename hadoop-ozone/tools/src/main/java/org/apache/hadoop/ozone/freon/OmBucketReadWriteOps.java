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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;

/**
 * Synthetic read/write operations workload generator tool.
 */
@Command(name = "obrwo",
    aliases = "om-bucket-read-write-ops",
    description = "Creates files, performs respective read/write " +
        "operations to measure lock performance.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)

public class OmBucketReadWriteOps extends BaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(OmBucketReadWriteOps.class);

  @Option(names = {"-P", "--root-path", "--rootPath"},
      description = "Root path. Full name --rootPath will be " +
          "removed in later versions.",
      defaultValue = "o3fs://bucket1.vol1/dir1/dir2")
  private String rootPath;

  @Option(names = {"-r", "--file-count-for-read", "--fileCountForRead"},
      description = "Number of files to be written in the read directory. " +
          "Full name --fileCountForRead will be removed in later versions.",
      defaultValue = "100")
  private int fileCountForRead;

  @Option(names = {"-w", "--file-count-for-write", "--fileCountForWrite"},
      description = "Number of files to be written in the write directory. " +
          "Full name --fileCountForWrite will be removed in later versions.",
      defaultValue = "10")
  private int fileCountForWrite;

  @Option(names = {"-g", "--file-size", "--fileSize"},
      description = "Generated data size (in bytes) of each file to be " +
          "written in each directory. Full name --fileSize will be removed " +
          "in later versions.",
      defaultValue = "256")
  private long fileSizeInBytes;

  @Option(names = {"-b", "--buffer"},
      description = "Size of buffer used to generated the file content.",
      defaultValue = "64")
  private int bufferSize;

  @Option(names = {"-l", "--name-len", "--nameLen"},
      description =
          "Length of the random name of directory you want to create. Full " +
              "name --nameLen will be removed in later versions.",
      defaultValue = "10")
  private int length;

  @Option(names = {"-c", "--total-thread-count", "--totalThreadCount"},
      description = "Total number of threads to be executed. Full name " +
          "--totalThreadCount will be removed in later versions.",
      defaultValue = "100")
  private int totalThreadCount;

  @Option(names = {"-T", "--read-thread-percentage", "--readThreadPercentage"},
      description = "Percentage of the total number of threads to be " +
          "allocated for read operations. The remaining percentage of " +
          "threads will be allocated for write operations. Full name " +
          "--readThreadPercentage will be removed in later versions.",
      defaultValue = "90")
  private int readThreadPercentage;

  @Option(names = {"-R", "--num-of-read-operations", "--numOfReadOperations"},
      description = "Number of read operations to be performed by each " +
          "thread. Full name --numOfReadOperations will be removed in later " +
          "versions.",
      defaultValue = "50")
  private int numOfReadOperations;

  @Option(names = {"-W", "--num-of-write-operations",
      "--numOfWriteOperations"},
      description = "Number of write operations to be performed by each " +
          "thread. Full name --numOfWriteOperations will be removed in later " +
          "versions.",
      defaultValue = "10")
  private int numOfWriteOperations;

  private Timer timer;

  private ContentGenerator contentGenerator;

  private FileSystem fileSystem;

  private int readThreadCount;
  private int writeThreadCount;

  @Override
  public Void call() throws Exception {
    init();

    readThreadCount = (readThreadPercentage * totalThreadCount) / 100;
    writeThreadCount = totalThreadCount - readThreadCount;

    print("rootPath: " + rootPath);
    print("fileCountForRead: " + fileCountForRead);
    print("fileCountForWrite: " + fileCountForWrite);
    print("fileSizeInBytes: " + fileSizeInBytes);
    print("bufferSize: " + bufferSize);
    print("totalThreadCount: " + totalThreadCount);
    print("readThreadPercentage: " + readThreadPercentage);
    print("writeThreadPercentage: " + (100 - readThreadPercentage));
    print("readThreadCount: " + readThreadCount);
    print("writeThreadCount: " + writeThreadCount);
    print("numOfReadOperations: " + numOfReadOperations);
    print("numOfWriteOperations: " + numOfWriteOperations);

    OzoneConfiguration configuration = createOzoneConfiguration();
    fileSystem = FileSystem.get(URI.create(rootPath), configuration);

    contentGenerator = new ContentGenerator(fileSizeInBytes, bufferSize);
    timer = getMetrics().timer("file-create");

    runTests(this::mainMethod);
    return null;
  }

  private void mainMethod(long counter) throws Exception {

    int readResult = readOperations();
    int writeResult = writeOperations();

    print("Total Files Read: " + readResult);
    print("Total Files Written: " + writeResult * fileCountForWrite);

    // TODO: print read/write lock metrics (HDDS-6435, HDDS-6436).
  }

  private int readOperations() throws Exception {

    // Create fileCountForRead (defaultValue = 1000) files under
    // rootPath/readPath directory
    String readPath =
        rootPath.concat(OzoneConsts.OM_KEY_PREFIX).concat("readPath");
    fileSystem.mkdirs(new Path(readPath));
    createFiles(readPath, fileCountForRead);

    // Start readThreadCount (defaultValue = 90) concurrent read threads
    // performing numOfReadOperations (defaultValue = 50) iterations
    // of read operations (fileSystem.listStatus(rootPath/readPath))
    ExecutorService readService = Executors.newFixedThreadPool(readThreadCount);
    CompletionService<Integer> readExecutorCompletionService =
        new ExecutorCompletionService<>(readService);
    List<Future<Integer>> readFutures = new ArrayList<>();
    for (int i = 0; i < readThreadCount; i++) {
      readFutures.add(readExecutorCompletionService.submit(() -> {
        int readCount = 0;
        try {
          for (int j = 0; j < numOfReadOperations; j++) {
            FileStatus[] status =
                fileSystem.listStatus(new Path(readPath));
            readCount += status.length;
          }
        } catch (IOException e) {
          LOG.warn("Exception while listing status ", e);
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

  private int writeOperations() throws Exception {

    // Start writeThreadCount (defaultValue = 10) concurrent write threads
    // performing numOfWriteOperations (defaultValue = 10) iterations
    // of write operations (createFiles(rootPath/writePath))
    String writePath =
        rootPath.concat(OzoneConsts.OM_KEY_PREFIX).concat("writePath");
    fileSystem.mkdirs(new Path(writePath));

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
            createFiles(writePath, fileCountForWrite);
            writeCount++;
          }
        } catch (IOException e) {
          LOG.warn("Exception while creating file ", e);
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

  private void createFile(String dir, long counter) throws Exception {
    String fileName = dir.concat(OzoneConsts.OM_KEY_PREFIX)
        .concat(RandomStringUtils.randomAlphanumeric(length));
    Path file = new Path(fileName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("FilePath:{}", file);
    }
    timer.time(() -> {
      try (FSDataOutputStream output = fileSystem.create(file)) {
        contentGenerator.write(output);
      }
      return null;
    });
  }

  private void createFiles(String dir, int fileCount) throws Exception {
    for (int i = 0; i < fileCount; i++) {
      createFile(dir, i);
    }
  }
}
