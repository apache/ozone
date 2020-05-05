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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * Directory & File Generator tool to test OM performance.
 */
@Command(name = "dtsg",
    aliases = "dfs-tree-generator",
    description =
        "Create nested directories and create given number of files in each " +
                "dir in any dfs compatible file system.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class HadoopDirTreeGenerator extends BaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HadoopDirTreeGenerator.class);

  @Option(names = {"-r", "--rpath"},
      description = "Hadoop FS directory system path",
      defaultValue = "o3fs://bucket2.vol2")
  private String rootPath;

  @Option(names = {"-d", "--depth"},
      description = "Number of directories to be generated recursively",
      defaultValue = "5")
  private int depth;

  @Option(names = {"-c", "--fileCount"},
      description = "Number of files to be written in each directory",
      defaultValue = "2")
  private int fileCount;

  @Option(names = {"-g", "--fileSize"},
      description = "Generated data size(in KB) of each file to be written" +
              " in each directory",
      defaultValue = "1")
  private int fileSize;

  @Option(names = {"-s", "--span"},
      description =
          "Number of child directories to be created in each directory.",
      defaultValue = "10")
  private int span;

  @Option(names = {"-l", "--nameLen"},
      description =
          "Length of the random name of directory you want to create.",
      defaultValue = "10")
  private int length;

  private long totalDirsCnt;

  private FileSystem fileSystem;

  @Override
  public Void call() throws Exception {

    init();
    OzoneConfiguration configuration = createOzoneConfiguration();
    fileSystem = FileSystem.get(URI.create(rootPath), configuration);
    runTests(this::createDir);
    return null;

  }

  /*
      Nested directories will be created like this,
      suppose you pass depth=3, span=3 and number of tests=1

      Directory Structure:-
                            |-- Dir111
                            |
                |-- Dir11 --|-- Dir112
                |           |
                |           |-- Dir113
                |
                |
                |           |-- Dir121
                |           |
       Dir1   --|-- Dir12 --|-- Dir122
                |           |
                |           |-- Dir123
                |
                |
                |           |-- Dir131
                |           |
                |-- Dir13 --|-- Dir132
                            |
                            |-- Dir133

     In each directory 'c' number of files with file size in KBs 'g' will be
     created.
   */
  private void createDir(long counter) throws Exception {
    if (depth <= 0) {
      LOG.info("Invalid depth value, at least one depth should be passed!");
      return;
    }
    if (span <= 0) {
      LOG.info("Invalid span value, at least one span should be passed!");
      return;
    }
    String dir = makeDirWithGivenNumberOfFiles(rootPath);
    if (depth > 1) {
      createSubDirRecursively(dir, 1, 1);
    }
    System.out.println("Successfully created directories & files. Total Dir " +
            "Count=" + totalDirsCnt);
  }

  private void createSubDirRecursively(String parent, int depthIndex,
                                       int spanIndex)
          throws Exception {
    if (depthIndex < depth) {
      String depthSubDir = makeDirWithGivenNumberOfFiles(parent);
      ++depthIndex;

      if (LOG.isDebugEnabled()) {
        LOG.debug("SubDir:{}, depthIndex:{} +", depthSubDir, depthIndex);
      }
      // only non-leaf nodes will be iterated recursively..
      if (depthIndex < depth) {
        createSubDirRecursively(depthSubDir, depthIndex, spanIndex);
      }
    }

    while(spanIndex < span) {
      String levelSubDir = makeDirWithGivenNumberOfFiles(parent);
      ++spanIndex;

      if (LOG.isDebugEnabled()) {
        LOG.debug("SpanSubDir:{}, depthIndex:{}, spanIndex:{} +", levelSubDir,
                depthIndex, spanIndex);
      }
      // only non-leaf nodes will be iterated recursively..
      if (depthIndex < depth) {
        createSubDirRecursively(levelSubDir, depthIndex, 1);
      }
    }
  }

  private String makeDirWithGivenNumberOfFiles(String parent) throws IOException {
    String dir = RandomStringUtils.randomAlphanumeric(length);
    dir = parent.toString().concat("/").concat(dir);
    fileSystem.mkdirs(new Path(dir));
    totalDirsCnt++;
    // Add given number of files into the created directory.
    createFiles(dir);
    return dir;
  }

  private void createFiles(String dir) throws IOException {
    for (int i = 0; i < fileCount; i++) {
      String fileName = dir.concat("/").concat(RandomStringUtils.
              randomAlphanumeric(length));

      if (LOG.isDebugEnabled()) {
        LOG.debug("FilePath:{}", fileName);
      }

      FSDataOutputStream out =
              fileSystem.create(new Path(fileName), true);
      int fSizeInKBs = fileSize * 1024;
      if (fSizeInKBs > 0) {
        int bufferLen = 1024;
        int seed = 0;
        byte[] toWrite = new byte[bufferLen];
        Random rb = new Random(seed);
        long bytesToWrite = fileSize;
        while (bytesToWrite > 0) {
          rb.nextBytes(toWrite);
          int bytesToWriteNext = (bufferLen < bytesToWrite) ? bufferLen
                  : (int) bytesToWrite;

          out.write(toWrite, 0, bytesToWriteNext);
          bytesToWrite -= bytesToWriteNext;
        }
      }
      out.flush();
      out.close();
    }
  }
}
