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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Synthetic read/write file operations workload generator tool.
 */
@Command(name = "obrwf",
    aliases = "om-bucket-read-write-file-ops",
    description = "Creates files, performs respective read/write " +
        "operations to measure lock performance.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)

@SuppressWarnings("java:S2245") // no need for secure random
public class OmBucketReadWriteFileOps extends AbstractOmBucketReadWriteOps {

  @Option(names = {"-P", "--root-path"},
      description = "Root path",
      defaultValue = "o3fs://bucket1.vol1/dir1/dir2")
  private String rootPath;

  @Option(names = {"-r", "--file-count-for-read"},
      description = "Number of files to be written in the read directory.",
      defaultValue = "100")
  private int fileCountForRead;

  @Option(names = {"-w", "--file-count-for-write"},
      description = "Number of files to be written in the write directory.",
      defaultValue = "10")
  private int fileCountForWrite;

  @Option(names = {"-g", "--file-size"},
      description = "Generated data size (in bytes) of each file to be " +
          "written in each directory.",
      defaultValue = "256")
  private long fileSizeInBytes;

  @Option(names = {"-b", "--buffer"},
      description = "Size of buffer used to generated the file content.",
      defaultValue = "64")
  private int bufferSize;

  @Option(names = {"-l", "--name-len"},
      description = "Length of the random name of directory you want to " +
          "create.",
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

    int readResult =
        readOperations(readThreadCount, numOfReadOperations, fileCountForRead,
            length);
    int writeResult = writeOperations(writeThreadCount, numOfWriteOperations,
        fileCountForWrite, length);

    print("Total Files Read: " + readResult);
    print("Total Files Written: " + writeResult * fileCountForWrite);

    // TODO: print read/write lock metrics (HDDS-6435, HDDS-6436).
  }

  @Override
  protected String createPath(String path) throws IOException {
    String fullPath = rootPath.concat(OzoneConsts.OM_KEY_PREFIX).concat(path);
    fileSystem.mkdirs(new Path(fullPath));
    return fullPath;
  }

  @Override
  protected int getReadCount(int readCount, String readPath)
      throws IOException {
    FileStatus[] status = fileSystem.listStatus(new Path(createPath(readPath)));
    readCount += status.length;
    return readCount;
  }

  @Override
  protected OutputStream create(String fileName) throws IOException {
    Path file = new Path(fileName);
    return fileSystem.create(file);
  }

  @Override
  protected Timer getTimer() {
    return timer;
  }

  @Override
  protected ContentGenerator getContentGenerator() {
    return contentGenerator;
  }
}
