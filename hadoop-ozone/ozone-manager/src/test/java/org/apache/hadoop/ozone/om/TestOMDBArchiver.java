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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.OM_HARDLINK_FILE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class for OMDBArchiver.
 */
public class TestOMDBArchiver {

  @TempDir
  private Path folder;

  @Test
  public void testRecordFileEntry() throws IOException {
    OMDBArchiver omdbArchiver = new OMDBArchiver();
    Path tmpDir = Files.createTempDirectory(folder, "TestOMDBArchiver");
    omdbArchiver.setTmpDir(tmpDir);
    assertThat(omdbArchiver.getFilesToWriteIntoTarball()).isNotNull();
    assertThat(omdbArchiver.getTmpDir()).isEqualTo(tmpDir);
    File dummyFile = new File(tmpDir.toFile(), "dummy.txt");
    Files.write(dummyFile.toPath(), "dummy".getBytes(StandardCharsets.UTF_8));
    String entryName = "dummy-hardlink.txt";
    long result = omdbArchiver.recordFileEntry(dummyFile, entryName);
    assertThat(omdbArchiver.getFilesToWriteIntoTarball().size()).isEqualTo(1);
    Optional<File> file = omdbArchiver.getFilesToWriteIntoTarball()
        .values().stream().findFirst();
    assertThat(file.isPresent()).isTrue();
    file.ifPresent(value -> {
      // because it's a hardlink
      assertThat(value).isNotEqualTo(dummyFile);
      try {
        Object inodeLink = IOUtils.getINode(value.toPath());
        Object inodeFile = IOUtils.getINode(dummyFile.toPath());
        assertThat(inodeLink).isEqualTo(inodeFile);
      } catch (IOException ex) {
        Assertions.fail(ex.getMessage());
      }
    });
    assertThat(result).isEqualTo(dummyFile.length());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteToArchive(boolean completed) throws IOException {
    OMDBArchiver omdbArchiver = new OMDBArchiver();
    Path tmpDir = Files.createTempDirectory(folder, "TestOMDBArchiver");
    omdbArchiver.setTmpDir(tmpDir);
    assertThat(omdbArchiver.getFilesToWriteIntoTarball()).isNotNull();
    assertThat(omdbArchiver.getTmpDir()).isEqualTo(tmpDir);
    Map<String, String> hardLinkFileMap = new HashMap<String, String>();
    for (int i = 0; i < 10; i++) {
      String fileName = "hardlink-" + i;
      File dummyFile = new File(tmpDir.toFile(), fileName);
      Files.write(dummyFile.toPath(), "dummy".getBytes(StandardCharsets.UTF_8));
      omdbArchiver.getFilesToWriteIntoTarball().put(fileName, dummyFile);
      hardLinkFileMap.put(dummyFile.getAbsolutePath(), dummyFile.getName());
    }
    omdbArchiver.setHardLinkFileMap(hardLinkFileMap);

    File tarFile = new File(folder.toFile(), "test-archive.tar");
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.metadata.dirs", String.valueOf(folder.toAbsolutePath()));
    try (OutputStream outputStream = Files.newOutputStream(tarFile.toPath())) {
      omdbArchiver.setCompleted(completed);
      omdbArchiver.writeToArchive(conf, outputStream);
    }
    assertThat(tarFile.exists()).isTrue();
    assertThat(tarFile.length()).isGreaterThan(0);
    // Untar the file
    File extractDir = new File(folder.toFile(), "extracted");
    assertThat(extractDir.mkdirs()).isTrue();
    FileUtil.unTar(tarFile, extractDir);
    // Verify contents
    int fileCount = 10;
    for (int i = 0; i < 10; i++) {
      String fileName = "hardlink-" + i;
      File extractedFile = new File(extractDir, fileName);
      assertThat(extractedFile.exists())
          .as("File %s should exist in extracted archive", fileName).isTrue();
      assertThat(extractedFile.length())
          .as("File %s should have content", fileName)
          .isEqualTo("dummy".getBytes().length);

      byte[] content = Files.readAllBytes(extractedFile.toPath());
      assertThat(new String(content,
          StandardCharsets.UTF_8)).isEqualTo("dummy");
    }

    if (completed) {
      File hardlinkFile = new File(extractDir, OM_HARDLINK_FILE);
      File completeMarker = new File(extractDir, OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME);
      assertThat(completeMarker.exists()).isTrue();
      assertThat(hardlinkFile.exists()).isTrue();
      fileCount += 2;
    }

    try (Stream<Path> stream = Files.list(extractDir.toPath())) {
      List<Path> extractedFiles = stream.filter(Files::isRegularFile)
          .collect(java.util.stream.Collectors.toList());
      assertThat(extractedFiles.size()).isEqualTo(fileCount);
    }
  }
}
