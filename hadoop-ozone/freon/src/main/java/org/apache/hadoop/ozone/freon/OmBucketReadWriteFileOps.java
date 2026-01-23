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

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

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
@MetaInfServices(FreonSubcommand.class)
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

  private FileSystem fileSystem;

  @Override
  protected void display() {
    print("rootPath: " + rootPath);
    print("fileCountForRead: " + fileCountForRead);
    print("fileCountForWrite: " + fileCountForWrite);
  }

  @Override
  protected void initialize(OzoneConfiguration ozoneConfiguration)
      throws IOException {
    fileSystem = FileSystem.get(URI.create(rootPath), ozoneConfiguration);
    runTests(this::mainMethod);
  }

  private void mainMethod(long counter) throws Exception {

    int readResult = readOperations(fileCountForRead);
    int writeResult = writeOperations(fileCountForWrite);

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
}
