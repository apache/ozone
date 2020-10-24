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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

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
      description = "Hadoop FS root path",
      defaultValue = "o3fs://bucket2.vol2")
  private String rootPath;

  @Option(names = {"-d", "--depth"},
      description = "Number of directories to be generated recursively",
      defaultValue = "5")
  private int depth;

  @Option(names = {"-c", "--file-count", "--fileCount"},
      description = "Number of files to be written in each directory. Full" +
          " name --fileCount will be removed in later versions.",
      defaultValue = "2")
  private int fileCount;

  @Option(names = {"-g", "--file-size", "--fileSize"},
      description = "Generated data size(in bytes) of each file to be " +
          "written in each directory. Full name --fileSize will be removed " +
          "in later versions.",
      defaultValue = "4096")
  private int fileSizeInBytes;

  @Option(names = {"-b", "--buffer"},
          description = "Size of buffer used to generated the file content.",
          defaultValue = "1024")
  private int bufferSize;

  @Option(names = {"-s", "--span"},
      description =
          "Number of child directories to be created in each directory.",
      defaultValue = "10")
  private int span;

  @Option(names = {"-l", "--name-len", "--nameLen"},
      description =
          "Length of the random name of directory you want to create. Full " +
              "name --nameLen will be removed in later versions.",
      defaultValue = "10")
  private int length;

  private AtomicLong totalDirsCnt = new AtomicLong();

  private Timer timer;

  private ContentGenerator contentGenerator;

  private FileSystem fileSystem;

  @Override
  public Void call() throws Exception {
    String s;
    if (depth <= 0) {
      s = "Invalid depth value, depth value should be greater than zero!";
      print(s);
    } else if (span <= 0) {
      s = "Invalid span value, span value should be greater than zero!";
      print(s);
    } else {
      init();
      OzoneConfiguration configuration = createOzoneConfiguration();
      fileSystem = FileSystem.get(URI.create(rootPath), configuration);

      contentGenerator = new ContentGenerator(fileSizeInBytes, bufferSize);
      timer = getMetrics().timer("file-create");

      runTests(this::createDir);
    }
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
    String dir = makeDirWithGivenNumberOfFiles(rootPath);
    if (depth > 1) {
      createSubDirRecursively(dir, 1, 1);
    }
    String message = "Successfully created directories & files. Total Dirs " +
            "Count=" + totalDirsCnt.get() + ", Total Files Count=" +
            timer.getCount();
    print(message);
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

  private String makeDirWithGivenNumberOfFiles(String parent)
          throws Exception {
    String dir = RandomStringUtils.randomAlphanumeric(length);
    dir = parent.toString().concat("/").concat(dir);
    fileSystem.mkdirs(new Path(dir));
    totalDirsCnt.incrementAndGet();
    // Add given number of files into the created directory.
    createFiles(dir);
    return dir;
  }

  private void createFile(String dir, long counter) throws Exception {
    String fileName = dir.concat("/").concat(RandomStringUtils.
            randomAlphanumeric(length));
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

  private void createFiles(String dir) throws Exception {
    for (int i = 0; i < fileCount; i++) {
      createFile(dir, i);
    }
  }
}
